package main

import (
	"context"
	"database/sql"
	"log/slog"

	"github.com/labstack/echo-contrib/v5/echoprometheus"
	echootel "github.com/labstack/echo-otel/v5"
	"github.com/labstack/echo/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/concrnt/concrnt/internal/usecase/subscription"
	"github.com/concrnt/concrnt/internal/worker"
)

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

func setupMetrics(e *echo.Echo) {
	e.Use(echoprometheus.NewMiddlewareWithConfig(echoprometheus.MiddlewareConfig{
		Namespace: "concrnt",
		LabelFuncs: map[string]echoprometheus.LabelValueFunc{
			"service": func(c *echo.Context, err error) string {
				if service := c.Response().Header().Get("cc-service"); service != "" {
					return service
				}
				return "concrnt"
			},
		},
		Skipper: func(c *echo.Context) bool {
			return c.Request().Header.Get("Upgrade") == "websocket"
		},
	}))
}

func setupTrace(e *echo.Echo, endpoint string, fqdn string, version string) func() {
	exporter, err := otlptracehttp.New(
		context.Background(),
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		panic(err)
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(fqdn+"/concrnt"),
			semconv.ServiceVersionKey.String(version),
		)),
	)
	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	e.Use(echootel.NewMiddleware(fqdn))

	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			span := trace.SpanFromContext(c.Request().Context())
			c.Response().Header().Set("trace-id", span.SpanContext().TraceID().String())
			return next(c)
		}
	})

	return func() {
		if err := tracerProvider.Shutdown(context.Background()); err != nil {
			slog.Error("failed to shutdown tracer provider", slog.String("error", err.Error()))
		}
	}
}

func observeBuildInfo() {
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
}

func observeDatabase(sqlDB *sql.DB) {
	prometheus.MustRegister(collectors.NewDBStatsCollector(sqlDB, "concrnt"))
}

func observeSubscription(subscriptionUC *subscription.Usecase) {
	prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "concrnt",
		Name:      "realtime_connections",
		Help:      "Realtime websocket connections from clients currently open on this replica.",
	}, func() float64 {
		return float64(subscriptionUC.SessionCount())
	}))
}

func observeLeaderSubscriber(leaderSub *worker.LeaderSubscriber) {
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
}
