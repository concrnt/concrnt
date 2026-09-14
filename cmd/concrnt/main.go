package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/SherClockHolmes/webpush-go"
	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo/v4"
	echomiddleware "github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/xinguang/go-recaptcha"
	"go.opentelemetry.io/contrib/instrumentation/github.com/labstack/echo/otelecho"
	"go.opentelemetry.io/otel/trace"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/infra/cluster"
	"github.com/concrnt/concrnt/internal/infra/config"
	"github.com/concrnt/concrnt/internal/infra/database"
	"github.com/concrnt/concrnt/internal/infra/gateway"
	"github.com/concrnt/concrnt/internal/infra/jobqueue"
	"github.com/concrnt/concrnt/internal/infra/kvs"
	"github.com/concrnt/concrnt/internal/infra/pubsub"
	"github.com/concrnt/concrnt/internal/infra/push"
	"github.com/concrnt/concrnt/internal/infra/repository"
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

var (
	version      = "unknown"
	buildMachine = "unknown"
	buildTime    = "unknown"
	goVersion    = "unknown"
)

type CustomHandler struct {
	slog.Handler
}

func (h *CustomHandler) Handle(ctx context.Context, r slog.Record) error {

	r.AddAttrs(slog.String("type", "app"))

	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		r.AddAttrs(slog.String("traceID", span.SpanContext().TraceID().String()))
		r.AddAttrs(slog.String("spanID", span.SpanContext().SpanID().String()))
	}

	return h.Handler.Handle(ctx, r)
}

func main() {

	lh := &CustomHandler{Handler: slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})}
	slogger := slog.New(lh)
	slog.SetDefault(slogger)

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

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

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
			"service": func(c echo.Context, err error) string {
				if service := c.Response().Header().Get("cc-service"); service != "" {
					return service
				}
				return "concrnt"
			},
		},
		Skipper: func(c echo.Context) bool {
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

		e.Use(otelecho.Middleware(conf.Concrnt.FQDN))

		e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
			return func(c echo.Context) error {
				span := trace.SpanFromContext(c.Request().Context())
				c.Response().Header().Set("trace-id", span.SpanContext().TraceID().String())
				return next(c)
			}
		})
	}

	e.Use(echomiddleware.LoggerWithConfig(echomiddleware.LoggerConfig{
		Skipper: func(c echo.Context) bool {
			return c.Path() == "/.well-known/concrnt"
		},
		Format: `{"time":"${time_rfc3339_nano}",${custom},"remote_ip":"${remote_ip}",` +
			`"host":"${host}","method":"${method}","uri":"${uri}","status":${status},` +
			`"error":"${error}","latency":${latency},"latency_human":"${latency_human}",` +
			`"bytes_in":${bytes_in},"bytes_out":${bytes_out}}` + "\n",
		CustomTagFunc: func(c echo.Context, buf *bytes.Buffer) (int, error) {
			span := trace.SpanFromContext(c.Request().Context())
			fmt.Fprintf(buf, "\"%s\":\"%s\"", "traceID", span.SpanContext().TraceID().String())
			fmt.Fprintf(buf, ",\"%s\":\"%s\"", "spanID", span.SpanContext().SpanID().String())
			return 0, nil
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

	cl := client.New(domainConfig.FQDN)
	if conf.Backends.GatewayAddr != "" {
		cl.AddHostRemapping(domainConfig.FQDN, conf.Backends.GatewayAddr)
	}
	cl.SetUserAgent("concrnt", version)

	repos, err := repository.Open(ctx, conf.Backends, repository.Deps{Client: cl, DomainConfig: domainConfig})
	if err != nil {
		panic(err.Error())
	}
	defer repos.Close()
	slog.Info("database backend ready", slog.String("backend", repos.Name))

	mc := database.NewMemcached(conf.Backends.MemcachedAddr)
	defer mc.Close()

	redis := database.NewRedis(conf.Backends.RedisAddr, "", conf.Backends.RedisDB)

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

	var elector cluster.Elector
	var discovery worker.PeerDiscovery // nil in standalone mode: no peers to poll
	if clustered {
		httpElector := cluster.NewHTTPElector(conf.Concrnt.Cluster.ElectorEndpoint)
		elector = httpElector
		discovery = httpElector
	} else {
		elector = cluster.AlwaysLeader{}
	}

	moduleManager := service.NewModuleManager(rest.Endpoints, conf.Services)

	redisPubsub := pubsub.NewRedisPubsub(redis)
	redisKVS := kvs.NewRedis(redis)
	jobQueue := jobqueue.NewRedisJobQueue(redis)
	policy := service.NewPolicyService(
		GetGlobalPolicy(),
		service.GlobalParameters{
			FQDN: domainConfig.FQDN,
		},
		cl,
	)

	serverUC := server.New(repos.Server, &domainConfig, softwareInfo, moduleManager, cl)

	recordUC := record.New(repos.Record, repos.Residence, serverUC, &domainConfig, cl, redisPubsub, policy, jobQueue, redisKVS)
	residenceUC := residence.New(repos.Residence, recordUC, &domainConfig)

	chunklineRepo := repos.Chunkline

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

	notificationUC := notification.New(repos.Notification, redisKVS, notificationPusher)

	leaderSub := worker.NewLeaderSubscriber(&domainConfig, cl, redisPubsub, discovery)
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
	chunklineUC := chunkline.New(chunklineRepo, chunklineGateway, redisKVS)

	go func() {
		if err := jobQueue.Run(ctx); err != nil {
			slog.Error("job queue stopped", slog.String("error", err.Error()))
		}
	}()

	abuseUC := abuse.New(repos.Abuse)

	var notificationReactor *worker.NotificationReactor
	if webpushOpts != nil {
		// cross-replica push dedup only matters when a leadership handover can
		// overlap two reactors; standalone deployments skip the redis round
		// trip per notification
		var notificationDeduper worker.NotificationDeduper
		if clustered {
			notificationDeduper = pubsub.NewRedisDeduper(redis)
		}
		notificationReactor = worker.NewNotificationReactor(notificationUC, subscriptionUC, notificationDeduper, *webpushOpts)
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

	wellKnownHandler := rest.NewWellKnownHandler(serverUC, meta)

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
	api := e.Group("", authMiddleware.IdentifyIdentity, authMiddleware.IdentifyIdentity)

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

	static := e.Group("", echomiddleware.CORS())

	static.GET("/tos", func(c echo.Context) (err error) {
		return c.File("/etc/concrnt/static/tos.txt")
	})
	static.OPTIONS("/tos", handleNop)

	static.GET("/code-of-conduct", func(c echo.Context) (err error) {
		return c.File("/etc/concrnt/static/code-of-conduct.txt")
	})
	static.OPTIONS("/code-of-conduct", handleNop)

	static.GET("/register-template", func(c echo.Context) (err error) {
		return c.File("/etc/concrnt/static/register-template.json")
	})
	static.OPTIONS("/register-template", handleNop)

	// the internal listener carries everything operational — liveness and
	// readiness probes, the Prometheus scrape endpoint, and the
	// replica-to-replica subscriber coordination API. it must never be
	// exposed outside the cluster.
	internal := echo.New()
	internal.HideBanner = true
	internal.HidePort = true

	internal.GET("/health", func(c echo.Context) (err error) {
		return c.String(http.StatusOK, "ok")
	})

	internal.GET("/metrics", echoprometheus.NewHandler())

	var ready atomic.Bool
	ready.Store(true)

	// backend-specific store metrics (go_sql_* for Postgres: the pool is
	// small, so saturation and wait time are the first thing to check when
	// everything gets slow; operation latency for Firestore)
	for _, collector := range repos.Collectors {
		prometheus.MustRegister(collector)
	}

	internal.GET("/ready", func(c echo.Context) (err error) {
		if !ready.Load() {
			return c.String(http.StatusServiceUnavailable, "shutting down")
		}

		// only the database gates readiness: redis and memcached are soft
		// dependencies (cache misses fall back to origin, publish failures
		// are logged), and failing all replicas at once on a cache-tier blip
		// would turn a degradation into a full outage
		if err := repos.Ready(c.Request().Context()); err != nil {
			return c.String(http.StatusServiceUnavailable, "db error")
		}

		return c.String(http.StatusOK, "ok")
	})

	coordination := rest.NewSubscriberCoordinationHandler(subscriptionUC, leaderSub, leaderSub, elector)
	coordination.RegisterRoutes(internal)

	go func() {
		if err := internal.Start(":" + internalPort); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// the probes target this listener, so a dead internal listener
			// gets the process restarted by its supervisor; the public API
			// keeps serving in the meantime
			slog.Error("internal listener stopped unexpectedly", slog.String("error", err.Error()))
		}
	}()

	var serverFailed atomic.Bool
	go func() {
		if err := e.Start(":8000"); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server stopped unexpectedly", slog.String("error", err.Error()))
			serverFailed.Store(true)
			stop()
		}
	}()

	<-ctx.Done()

	slog.Info("shutting down")
	ready.Store(false)

	if clustered {
		// keep serving briefly so the endpoint controller stops routing to
		// this pod before connections are closed; the internal listener stays
		// up through this window so /ready keeps answering 503
		time.Sleep(3 * time.Second)
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelShutdown()

	if err := e.Shutdown(shutdownCtx); err != nil {
		slog.Error("failed to shut down gracefully", slog.String("error", err.Error()))
	}

	// Shutdown does not touch hijacked connections: this is what terminates
	// the realtime websockets so clients reconnect to another replica
	e.Close()

	// last: peers keep getting coordination answers until the public API is
	// fully drained
	if err := internal.Shutdown(shutdownCtx); err != nil {
		slog.Error("failed to shut down internal listener", slog.String("error", err.Error()))
	}

	if serverFailed.Load() {
		// e.g. the listen address was already bound: exit non-zero so
		// supervisors keying on exit status restart the process
		os.Exit(1)
	}
}

func handleNop(c echo.Context) error {
	return c.NoContent(http.StatusOK)
}
