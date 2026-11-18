package main

import (
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
	"github.com/labstack/echo-contrib/v5/echoprometheus"
	"github.com/labstack/echo/v5"
	echomiddleware "github.com/labstack/echo/v5/middleware"
	"github.com/xinguang/go-recaptcha"

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
	"github.com/concrnt/concrnt/internal/worker"
)

var (
	version      = "unknown"
	buildMachine = "unknown"
	buildTime    = "unknown"
	goVersion    = "unknown"
)

const readinessTimeout = 800 * time.Millisecond

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

	defaultErrorHandler := echo.DefaultHTTPErrorHandler(false)
	echoConfig := echo.Config{
		Logger:     logger,
		Filesystem: echo.NewDefaultFS("/"),
		HTTPErrorHandler: func(c *echo.Context, err error) {
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
			defaultErrorHandler(c, err)
		},
	}

	e := echo.NewWithConfig(echoConfig)

	e.Use(echomiddleware.Recover())
	setupMetrics(e)

	if conf.Observability.EnableTrace {
		cleanup := setupTrace(e, conf.Observability.TraceEndpoint, conf.Concrnt.FQDN, version)
		defer cleanup()
	}

	e.Use(echomiddleware.RequestLoggerWithConfig(echomiddleware.RequestLoggerConfig{
		Skipper: func(c *echo.Context) bool {
			return c.Path() == "/.well-known/concrnt"
		},
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

	observeBuildInfo()

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

	sqlDB, err := db.DB()
	if err != nil {
		panic("failed to get sql.DB: " + err.Error())
	}
	observeDatabase(sqlDB)

	watchdog := health.NewWatchdog()

	readiness := health.NewReadiness(readinessTimeout)
	readiness.Add("postgres", sqlDB.PingContext)
	readiness.Add("redis", func(ctx context.Context) error {
		return redis.Ping(ctx).Err()
	})
	readiness.Add("memcached", func(ctx context.Context) error {
		return mc.Ping()
	})

	probes := rest.NewProbeHandler(watchdog, readiness)
	probes.Starting("schema-migration")

	internalPort := os.Getenv("CONCRNT_INTERNAL_PORT")
	if internalPort == "" {
		internalPort = "8001"
	}

	// The internal listener opens before the migrations so that /ready and
	// /metrics answer while they run; the routes that need the rest of the
	// wiring are swapped in afterwards, since echo's router cannot take
	// routes once it is serving.
	bootstrap := echo.NewWithConfig(echoConfig)
	bootstrap.GET("/metrics", echoprometheus.NewHandler())
	probes.RegisterRoutes(bootstrap)

	var internalHandler atomic.Pointer[echo.Echo]
	internalHandler.Store(bootstrap)

	start := func(ctx context.Context, address string, handler http.Handler, done chan<- error) {
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

	internalDone := make(chan error, 1)
	go start(context.Background(), ":"+internalPort, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		internalHandler.Load().ServeHTTP(w, r)
	}), internalDone)

	migrationLease := pglease.New(db)
	err = migrations.Migrate(ctx, migrationLease, abuseRepo, entityMetaRepo, notificationRepo, recordRepo, serverRepo, stateRepo)
	if err != nil {
		panic("failed to migrate database: " + err.Error())
	}

	probes.Starting("data-migration")
	migrator := migrations.NewMigrator(stateRepo, migrationLease)

	err = migrator.Do(context.WithValue(ctx, migrations.PostgresKey, db), version)
	if err != nil {
		panic("failed to run migrations: " + err.Error())
	}

	probes.Starting("wiring")

	clustered := conf.Concrnt.Cluster.Enable
	if clustered && conf.Concrnt.Cluster.ElectorEndpoint == "" {
		panic("concrnt.cluster.enable requires concrnt.cluster.electorEndpoint")
	}

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

	var webpushOpts *webpush.Options
	if conf.Integrations.VapidPublicKey != "" && conf.Integrations.VapidPrivateKey != "" {
		webpushOpts = &webpush.Options{
			Subscriber:      "mailto:admin@" + domainConfig.FQDN,
			VAPIDPublicKey:  conf.Integrations.VapidPublicKey,
			VAPIDPrivateKey: conf.Integrations.VapidPrivateKey,
			TTL:             30,
			RecordSize:      2048, // keeps the base64 body under the 4096-byte FCM/APNs limit
		}
	}
	var notificationPusher notification.Pusher
	if webpushOpts != nil {
		notificationPusher = push.NewWebPush(*webpushOpts)
	}

	notificationUC := notification.New(notificationRepo, redisKVS, notificationPusher)

	leaderSub := worker.NewLeaderSubscriber(&domainConfig, cl, redisPubsub, discovery, watchdog)
	observeLeaderSubscriber(leaderSub)
	workerSub := worker.NewWorkerSubscriber(elector)
	subscriber := worker.NewSubscriberManager(elector, leaderSub, workerSub)

	subscriptionUC := subscription.New(subscriber, redisPubsub)
	observeSubscription(subscriptionUC)
	leaderSub.RegisterClient(subscriptionUC)

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
		var notificationDeduper worker.NotificationDeduper
		if clustered {
			notificationDeduper = pubsub.NewRedisDeduper(redis)
		}
		notificationReactor = worker.NewNotificationReactor(notificationUC, subscriptionUC, notificationDeduper, *webpushOpts, watchdog)
	}

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
	staticFiles := conf.Static.WithDefaults()
	for route, path := range map[string]string{
		"/tos":               staticFiles.TOS,
		"/code-of-conduct":   staticFiles.CodeOfConduct,
		"/register-template": staticFiles.RegisterTemplate,
		"/robots.txt":        staticFiles.RobotsTxt,
	} {
		static.GET(route, func(c *echo.Context) error {
			return c.File(path)
		})
		static.OPTIONS(route, func(c *echo.Context) error {
			return c.NoContent(http.StatusOK)
		})
	}

	internal := echo.NewWithConfig(echoConfig)
	internal.GET("/metrics", echoprometheus.NewHandler())
	probes.RegisterRoutes(internal)

	coordination := rest.NewSubscriberCoordinationHandler(subscriptionUC, leaderSub, leaderSub, elector)
	coordination.RegisterRoutes(internal)

	internalHandler.Store(internal)

	apiCtx, stopAPI := context.WithCancel(context.Background())
	defer stopAPI()

	apiDone := make(chan error, 1)
	go start(apiCtx, ":8000", e, apiDone)
	probes.Started()

	select {
	case <-ctx.Done():
	case err := <-apiDone:
		slog.Error("server stopped unexpectedly", slog.String("error", err.Error()))
		os.Exit(1)
	case err := <-internalDone:
		slog.Error("internal listener stopped unexpectedly", slog.String("error", err.Error()))
		os.Exit(1)
	}

	slog.Info("shutting down")
	probes.Drain()

	if clustered {
		// keep serving briefly so the endpoint controller stops routing to
		// this pod before connections are closed
		time.Sleep(3 * time.Second)
	}

	stopAPI()
	if err := <-apiDone; err != nil {
		slog.Error("failed to shut down gracefully", slog.String("error", err.Error()))
	}
}
