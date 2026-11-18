package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SherClockHolmes/webpush-go"
	"github.com/labstack/echo-contrib/v5/echoprometheus"
	echootel "github.com/labstack/echo-otel/v5"
	"github.com/labstack/echo/v5"
	echomiddleware "github.com/labstack/echo/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/xinguang/go-recaptcha"
	"go.opentelemetry.io/otel/trace"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/infra/cluster"
	"github.com/concrnt/concrnt/internal/infra/config"
	"github.com/concrnt/concrnt/internal/infra/database"
	"github.com/concrnt/concrnt/internal/infra/gateway"
	"github.com/concrnt/concrnt/internal/infra/health"
	"github.com/concrnt/concrnt/internal/infra/jobqueue"
	"github.com/concrnt/concrnt/internal/infra/kvs"
	"github.com/concrnt/concrnt/internal/infra/pubsub"
	"github.com/concrnt/concrnt/internal/infra/push"
	pgabuse "github.com/concrnt/concrnt/internal/infra/repository/postgres/abuse"
	pgentitymeta "github.com/concrnt/concrnt/internal/infra/repository/postgres/entitymeta"
	pglease "github.com/concrnt/concrnt/internal/infra/repository/postgres/lease"
	pgnotification "github.com/concrnt/concrnt/internal/infra/repository/postgres/notification"
	pgrecord "github.com/concrnt/concrnt/internal/infra/repository/postgres/record"
	pgserver "github.com/concrnt/concrnt/internal/infra/repository/postgres/server"
	pgstate "github.com/concrnt/concrnt/internal/infra/repository/postgres/state"
	"github.com/concrnt/concrnt/internal/migrations"
	"github.com/concrnt/concrnt/internal/present/rest"
	"github.com/concrnt/concrnt/internal/present/rest/middleware"
	"github.com/concrnt/concrnt/internal/service"
	"github.com/concrnt/concrnt/internal/usecase/abuse"
	"github.com/concrnt/concrnt/internal/usecase/chunkline"
	"github.com/concrnt/concrnt/internal/usecase/notification"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/internal/usecase/residence"
	"github.com/concrnt/concrnt/internal/usecase/server"
	"github.com/concrnt/concrnt/internal/usecase/subscription"
	"github.com/concrnt/concrnt/internal/utils"
	"github.com/concrnt/concrnt/internal/worker"
)

// readinessTimeout bounds one /ready sweep. The kubelet gives a probe one
// second by default, so a backend that has not answered by then must
// already be reported as down inside that window.
const readinessTimeout = 800 * time.Millisecond

var (
	version      = "unknown"
	buildMachine = "unknown"
	buildTime    = "unknown"
	goVersion    = "unknown"
)

// traceHandler stamps every record with the trace and span of the context it
// was logged under, so application and access logs can be joined with traces.
type traceHandler struct {
	slog.Handler
}

func (h *traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanFromContext(ctx).SpanContext(); sc.IsValid() {
		r.AddAttrs(
			slog.String("traceID", sc.TraceID().String()),
			slog.String("spanID", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{h.Handler.WithAttrs(attrs)}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{h.Handler.WithGroup(name)}
}

// httpErrorHandler logs what echo's default handler stays silent about, i.e.
// recovered panics and anything answered with a 5xx, before delegating to it.
func httpErrorHandler(logger *slog.Logger) echo.HTTPErrorHandler {
	fallback := echo.DefaultHTTPErrorHandler(false)
	return func(c *echo.Context, err error) {
		// the request logger has already handed this error over
		if res, _ := echo.UnwrapResponse(c.Response()); res != nil && res.Committed {
			return
		}
		ctx := c.Request().Context()
		var panicked *echomiddleware.PanicStackError
		if errors.As(err, &panicked) {
			logger.ErrorContext(ctx, "panic recovered",
				slog.String("error", panicked.Err.Error()),
				slog.String("stack", string(panicked.Stack)),
			)
		} else if code := echo.StatusCode(err); code == 0 || code >= http.StatusInternalServerError {
			logger.ErrorContext(ctx, "request failed", slog.String("error", err.Error()))
		}
		fallback(c, err)
	}
}

func main() {

	logger := slog.New(&traceHandler{slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})})
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Fprint(os.Stderr, concrnt.Banner)

	configPath := os.Getenv("CONCRNT_CONFIG")
	if configPath == "" {
		configPath = "/etc/concrnt/config"
	}

	conf, err := config.Load(configPath)
	if err != nil {
		panic("failed to load config: " + err.Error())
	}

	domainConfig := conf.DomainConfig()

	slog.Info("concrnt starting", slog.String("version", version))
	slog.Info(
		"config loaded",
		slog.String("csid", domainConfig.CSID),
		slog.String("fqdn", domainConfig.FQDN),
		slog.String("layer", domainConfig.Layer),
	)

	// both listeners log through the application logger, so echo's own
	// messages carry the same shape and trace ids as everything else
	echoConfig := echo.Config{
		Logger:           logger,
		HTTPErrorHandler: httpErrorHandler(logger),
	}

	e := echo.NewWithConfig(echoConfig)

	e.Use(echomiddleware.Recover())

	// HTTP metrics are collected on the public listener only; they are served
	// from /metrics on the internal listener below, which carries no
	// middleware, so probe and coordination traffic never shows up in them
	e.Use(echoprometheus.NewMiddlewareWithConfig(echoprometheus.MiddlewareConfig{
		Namespace: "concrnt",
		LabelFuncs: map[string]echoprometheus.LabelValueFunc{
			// which service answered: proxied requests carry the target
			// service's name in the cc-service header set by rest.Proxy;
			// everything else is handled in-process
			"service": func(c *echo.Context, err error) string {
				if service := c.Response().Header().Get("cc-service"); service != "" {
					return service
				}
				return "concrnt"
			},
		},
		Skipper: func(c *echo.Context) bool {
			// long-lived websocket upgrades would skew the duration histogram
			return c.Request().Header.Get("Upgrade") == "websocket"
		},
	}))

	if conf.Observability.EnableTrace {
		cleanup, err := utils.SetupTraceProvider(conf.Observability.TraceEndpoint, conf.Concrnt.FQDN+"/ccapi", version)
		if err != nil {
			panic(err)
		}
		defer cleanup()

		e.Use(echootel.NewMiddleware(conf.Concrnt.FQDN))

		e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
			return func(c *echo.Context) error {
				span := trace.SpanFromContext(c.Request().Context())
				c.Response().Header().Set("trace-id", span.SpanContext().TraceID().String())
				return next(c)
			}
		})
	}

	e.Use(echomiddleware.RequestLoggerWithConfig(echomiddleware.RequestLoggerConfig{
		Skipper: func(c *echo.Context) bool {
			return c.Path() == "/.well-known/concrnt"
		},
		// errors are answered here, so the status logged is the one sent
		HandleError:      true,
		LogLatency:       true,
		LogRemoteIP:      true,
		LogHost:          true,
		LogMethod:        true,
		LogURI:           true,
		LogStatus:        true,
		LogContentLength: true,
		LogResponseSize:  true,
		LogValuesFunc: func(c *echo.Context, v echomiddleware.RequestLoggerValues) error {
			attrs := []slog.Attr{
				slog.String("remote_ip", v.RemoteIP),
				slog.String("host", v.Host),
				slog.String("method", v.Method),
				slog.String("uri", v.URI),
				slog.Int("status", v.Status),
				slog.Duration("latency", v.Latency),
				slog.String("bytes_in", v.ContentLength),
				slog.Int64("bytes_out", v.ResponseSize),
			}
			if v.Error != nil {
				attrs = append(attrs, slog.String("error", v.Error.Error()))
			}
			// the request context carries the span, so the trace ids come
			// from traceHandler like on every other line
			logger.LogAttrs(c.Request().Context(), slog.LevelInfo, "request", attrs...)
			return nil
		},
	}))

	softwareInfo := concrnt.SoftwareInfo{
		Version:      version,
		BuildMachine: buildMachine,
		BuildTime:    buildTime,
		GoVersion:    goVersion,
	}

	// the usual *_build_info shape: a constant 1 whose labels carry the build
	// identity, so dashboards can filter and join on the running version
	buildInfo := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "concrnt",
		Name:      "build_info",
		Help:      "Build information of the running concrnt; the value is always 1.",
		ConstLabels: prometheus.Labels{
			"version":       version,
			"go_version":    goVersion,
			"build_time":    buildTime,
			"build_machine": buildMachine,
		},
	})
	buildInfo.Set(1)
	prometheus.MustRegister(buildInfo)

	db, err := database.NewPostgres(conf.Backends.PostgresDsn)
	if err != nil {
		panic("failed to connect database")
	}

	mc := database.NewMemcached(conf.Backends.MemcachedAddr)
	defer mc.Close()

	redis := database.NewRedis(conf.Backends.RedisAddr, "", conf.Backends.RedisDB)

	cl := client.New(domainConfig.FQDN)
	if conf.Backends.GatewayAddr != "" {
		cl.AddHostRemapping(domainConfig.FQDN, conf.Backends.GatewayAddr)
	}
	cl.SetUserAgent("concrnt", version)

	abuseRepo := pgabuse.New(db)
	entityMetaRepo := pgentitymeta.New(db)
	notificationRepo := pgnotification.New(db)
	recordRepo := pgrecord.New(db)
	serverRepo := pgserver.New(&domainConfig, db, cl)
	stateRepo := pgstate.New(db)

	migrationLease := pglease.New(db)
	err = migrations.Migrate(ctx, migrationLease, abuseRepo, entityMetaRepo, notificationRepo, recordRepo, serverRepo, stateRepo)
	if err != nil {
		panic("failed to migrate database: " + err.Error())
	}

	migrator := migrations.NewMigrator(stateRepo, migrationLease)

	err = migrator.Do(ctx, version)
	if err != nil {
		panic("failed to run migrations: " + err.Error())
	}

	// the internal (operational) listener port, alongside the fixed public
	// :8000; never expose it outside the cluster
	internalPort := os.Getenv("CONCRNT_INTERNAL_PORT")
	if internalPort == "" {
		internalPort = "8001"
	}

	clustered := conf.Concrnt.Cluster.Enable
	if clustered && conf.Concrnt.Cluster.ElectorEndpoint == "" {
		panic("concrnt.cluster.enable requires concrnt.cluster.electorEndpoint")
	}

	// every long-running loop reports here; a stalled one fails liveness
	watchdog := health.NewWatchdog()

	var elector cluster.Elector
	var discovery worker.PeerDiscovery // nil in standalone mode: no peers to poll
	if clustered {
		httpElector := cluster.NewHTTPElector(conf.Concrnt.Cluster.ElectorEndpoint, watchdog)
		elector = httpElector
		discovery = httpElector
	} else {
		elector = cluster.AlwaysLeader{}
	}

	moduleManager := service.NewModuleManager(rest.Endpoints, conf.Services)

	redisPubsub := pubsub.NewRedisPubsub(redis)
	redisKVS := kvs.NewRedis(redis)
	jobQueue := jobqueue.NewRedisJobQueue(redis, jobqueue.WithWatchdog(watchdog))
	policy := service.NewPolicyService(
		GetGlobalPolicy(),
		service.GlobalParameters{
			FQDN: domainConfig.FQDN,
		},
		cl,
	)

	serverUC := server.New(serverRepo, &domainConfig, softwareInfo, moduleManager, cl)

	recordUC := record.New(recordRepo, entityMetaRepo, serverUC, &domainConfig, cl, redisPubsub, policy, jobQueue, redisKVS)
	residenceUC := residence.New(recordRepo, entityMetaRepo, recordUC, &domainConfig)

	chunklineRepo := pgrecord.NewChunklineRepository(db)

	// web push is gated on VAPID keys; without them neither the reactor nor
	// the out-of-band counter-reset push exist
	var webpushOpts *webpush.Options
	if conf.Integrations.VapidPublicKey != "" && conf.Integrations.VapidPrivateKey != "" {
		webpushOpts = &webpush.Options{
			Subscriber:      "mailto:admin@" + domainConfig.FQDN,
			VAPIDPublicKey:  conf.Integrations.VapidPublicKey,
			VAPIDPrivateKey: conf.Integrations.VapidPrivateKey,
			TTL:             30,
			// webpush-go zero-pads every message up to RecordSize, so the wire
			// body is always exactly RecordSize regardless of payload length.
			// The default (4096) base64-encodes to ~5.5KB, which overflows the
			// 4096-byte FCM/APNs data limit at webpush-relay and gets its
			// encrypted payload dropped. 2048 keeps the base64 body (~2.7KB)
			// within that limit while leaving ~1.9KB of plaintext room — ample
			// for the minimal notification payload (see NotificationReactor).
			RecordSize: 2048,
		}
	}
	var notificationPusher notification.Pusher
	if webpushOpts != nil {
		notificationPusher = push.NewWebPush(*webpushOpts)
	}

	notificationUC := notification.New(notificationRepo, redisKVS, notificationPusher)

	leaderSub := worker.NewLeaderSubscriber(&domainConfig, cl, redisPubsub, discovery, watchdog)
	workerSub := worker.NewWorkerSubscriber(elector)
	subscriber := worker.NewSubscriberManager(elector, leaderSub, workerSub)

	subscriptionUC := subscription.New(subscriber, redisPubsub)
	leaderSub.RegisterClient(subscriptionUC)

	// sampled at scrape time; the replica-local client socket count, and the
	// federation subscriber's peer hosts (non-zero only on the leader)
	prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "concrnt",
		Name:      "realtime_connections",
		Help:      "Realtime websocket connections from clients currently open on this replica.",
	}, func() float64 {
		return float64(subscriptionUC.SessionCount())
	}))
	peerOpts := func(state string) prometheus.GaugeOpts {
		return prometheus.GaugeOpts{
			Namespace:   "concrnt",
			Name:        "peer_connections",
			Help:        "Peer realtime hosts the federation subscriber keeps websockets to: desired (demanded) vs current (websocket established).",
			ConstLabels: prometheus.Labels{"state": state},
		}
	}
	prometheus.MustRegister(prometheus.NewGaugeFunc(peerOpts("desired"), func() float64 {
		desired, _ := leaderSub.ConnectionCounts()
		return float64(desired)
	}))
	prometheus.MustRegister(prometheus.NewGaugeFunc(peerOpts("current"), func() float64 {
		_, current := leaderSub.ConnectionCounts()
		return float64(current)
	}))

	chunklineGateway := gateway.NewChunklineGateway(cl, mc, subscriber, redisPubsub)
	chunklineUC := chunkline.New(chunklineRepo, chunklineGateway, redisKVS, recordUC)

	go func() {
		if err := jobQueue.Run(ctx); err != nil {
			slog.Error("job queue stopped", slog.String("error", err.Error()))
		}
	}()

	abuseUC := abuse.New(abuseRepo)

	var notificationReactor *worker.NotificationReactor
	if webpushOpts != nil {
		// cross-replica push dedup only matters when a leadership handover can
		// overlap two reactors; standalone deployments skip the redis round
		// trip per notification
		var notificationDeduper worker.NotificationDeduper
		if clustered {
			notificationDeduper = pubsub.NewRedisDeduper(redis)
		}
		notificationReactor = worker.NewNotificationReactor(notificationUC, subscriptionUC, notificationDeduper, *webpushOpts, watchdog)
	}

	// singleton workers run only while this replica holds the leadership: the
	// federation subscriber (one peer websocket per remote host for the
	// whole cluster), the chunkline cache updater, and the push reactor
	go elector.Run(ctx, func(leadCtx context.Context) {
		leaderSub.Start(leadCtx)
		chunklineGateway.StartWorker(leadCtx, leaderSub)
		if notificationReactor != nil {
			notificationReactor.Start(leadCtx)
		}
	})

	authMiddleware := middleware.NewAuthMiddleware(domainConfig, cl, serverUC, recordUC)

	meta := conf.Meta
	meta["captchaSiteKey"] = conf.Integrations.CaptchaSitekey
	meta["vapidKey"] = conf.Integrations.VapidPublicKey
	meta["registration"] = conf.Concrnt.Registration

	additionalWellKnown, err := rest.RenderWellKnown(conf.AdditionalWellKnown)
	if err != nil {
		panic("invalid additionalWellKnown: " + err.Error())
	}

	wellKnownHandler := rest.NewWellKnownHandler(serverUC, meta, additionalWellKnown)

	wellKnownHandler.RegisterRoutes(e)

	apiHandler := rest.NewHandler(
		domainConfig,
		residenceUC,
		recordUC,
		chunklineUC,
		serverUC,
		notificationUC,
		abuseUC,
		subscriptionUC,
	)
	api := e.Group("", authMiddleware.IdentifyIdentity)

	if conf.Integrations.CaptchaSecret != "" {
		validator, err := recaptcha.NewWithSecert(conf.Integrations.CaptchaSecret)
		if err != nil {
			panic("failed to initialize recaptcha: " + err.Error())
		}
		captchaMiddleware := middleware.Recaptcha(validator)
		api.Use(captchaMiddleware)
	}

	apiHandler.RegisterRoutes(e, api)

	proxy := rest.NewProxy(conf.Services, authMiddleware.IdentifyIdentity)
	proxy.RegisterRoutes(e)

	static := e.Group("", echomiddleware.CORS("*"))

	rest.RegisterStaticFiles(static, conf.Static)

	// the internal listener carries everything operational — liveness and
	// readiness probes, the Prometheus scrape endpoint, and the
	// replica-to-replica subscriber coordination API. it must never be
	// exposed outside the cluster.
	internal := echo.NewWithConfig(echoConfig)

	internal.GET("/metrics", echoprometheus.NewHandler())

	sqlDB, err := db.DB()
	if err != nil {
		panic("failed to get sql.DB: " + err.Error())
	}

	// connection pool usage (go_sql_*): the pool is small, so saturation
	// and wait time are the first thing to check when everything gets slow
	prometheus.MustRegister(collectors.NewDBStatsCollector(sqlDB, "concrnt"))

	// every backend the public API needs gates readiness: postgres holds
	// the data, redis carries delivery jobs and realtime events, memcached
	// serves the timeline chunks. the sweep stays under the kubelet's probe
	// timeout so a hung backend reads as "down" rather than as a timed-out
	// probe with no body
	readiness := health.NewReadiness(readinessTimeout)
	readiness.Add("postgres", sqlDB.PingContext)
	readiness.Add("redis", func(ctx context.Context) error {
		return redis.Ping(ctx).Err()
	})
	readiness.Add("memcached", func(ctx context.Context) error {
		return mc.Ping()
	})

	probes := rest.NewProbeHandler(watchdog, readiness)
	probes.RegisterRoutes(internal)

	coordination := rest.NewSubscriberCoordinationHandler(subscriptionUC, leaderSub, leaderSub, elector)
	coordination.RegisterRoutes(internal)

	// one StartConfig per listener, each with its own context: shutdown is
	// staged by the order the contexts are cancelled in, and Start returns
	// once its server has finished shutting down
	publicCtx, stopPublic := context.WithCancel(context.Background())
	defer stopPublic()
	internalCtx, stopInternal := context.WithCancel(context.Background())
	defer stopInternal()

	start := func(ctx context.Context, address string, handler *echo.Echo, done chan<- error) {
		done <- echo.StartConfig{
			Address:         address,
			HideBanner:      true,
			HidePort:        true,
			GracefulTimeout: 15 * time.Second,
			BeforeServeFunc: func(s *http.Server) error {
				// repository imports and proxied uploads may legitimately
				// take longer than echo's default 30s whole-request timeout
				s.ReadTimeout = 0
				s.ReadHeaderTimeout = 10 * time.Second
				return nil
			},
		}.Start(ctx, handler)
	}

	publicDone := make(chan error, 1)
	internalDone := make(chan error, 1)
	go start(internalCtx, ":"+internalPort, internal, internalDone)
	go start(publicCtx, ":8000", e, publicDone)

	select {
	case <-ctx.Done():
	case err := <-publicDone:
		// e.g. the listen address was already bound: exit non-zero so
		// supervisors keying on exit status restart the process
		slog.Error("server stopped unexpectedly", slog.String("error", err.Error()))
		os.Exit(1)
	case err := <-internalDone:
		// the probes target this listener, so without it the process would
		// be restarted by its supervisor anyway
		slog.Error("internal listener stopped unexpectedly", slog.String("error", err.Error()))
		os.Exit(1)
	}

	slog.Info("shutting down")
	probes.Drain()

	if clustered {
		// keep serving briefly so the endpoint controller stops routing to
		// this pod before connections are closed; the internal listener stays
		// up through this window so /ready keeps answering 503
		time.Sleep(3 * time.Second)
	}

	stopPublic()
	if err := <-publicDone; err != nil {
		slog.Error("failed to shut down gracefully", slog.String("error", err.Error()))
	}

	// last: peers keep getting coordination answers until the public API is
	// fully drained
	stopInternal()
	if err := <-internalDone; err != nil {
		slog.Error("failed to shut down internal listener", slog.String("error", err.Error()))
	}
}
