package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/health"
)

func probe(t *testing.T, e *echo.Echo, path string) (int, probeReport) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	var report probeReport
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &report), rec.Body.String())
	return rec.Code, report
}

func TestProbeHealthReflectsWatchdog(t *testing.T) {
	watchdog := health.NewWatchdog()
	loop := watchdog.Register("worker/loop", time.Millisecond)
	slow := watchdog.Register("worker/slow", time.Hour)
	watchdog.Register("worker/leader-only", time.Hour)
	e := echo.New()
	NewProbeHandler(watchdog, health.NewReadiness(time.Second)).RegisterRoutes(e)

	loop.Beat()
	slow.Beat()
	code, report := probe(t, e, "/health")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "ok", report.Status)
	require.Equal(t, map[string]string{"worker/loop": "ok", "worker/slow": "ok", "worker/leader-only": "idle"}, report.Checks)

	time.Sleep(5 * time.Millisecond)
	code, report = probe(t, e, "/health")
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.Equal(t, "stalled", report.Status)
	require.Contains(t, report.Checks["worker/loop"], "no heartbeat for")
	require.Equal(t, "ok", report.Checks["worker/slow"])

	// a loop that stopped on purpose is not a stall
	loop.Stop()
	code, report = probe(t, e, "/health")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "idle", report.Checks["worker/loop"])
}

func TestProbeReadyReflectsBackendsAndDrain(t *testing.T) {
	var redisErr error
	readiness := health.NewReadiness(time.Second)
	readiness.Add("postgres", func(ctx context.Context) error { return nil })
	readiness.Add("redis", func(ctx context.Context) error { return redisErr })
	e := echo.New()
	h := NewProbeHandler(health.NewWatchdog(), readiness)
	h.RegisterRoutes(e)

	code, report := probe(t, e, "/ready")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "ok", report.Status)
	require.Equal(t, map[string]string{"postgres": "ok", "redis": "ok"}, report.Checks)

	redisErr = errors.New("dial tcp: connection refused")
	code, report = probe(t, e, "/ready")
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.Equal(t, "unavailable", report.Status)
	require.Equal(t, "ok", report.Checks["postgres"])
	require.Equal(t, "dial tcp: connection refused", report.Checks["redis"])

	// draining wins even with healthy backends, and never touches liveness
	redisErr = nil
	h.Drain()
	code, report = probe(t, e, "/ready")
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.Equal(t, "draining", report.Status)
	require.Empty(t, report.Checks)
	code, _ = probe(t, e, "/health")
	require.Equal(t, http.StatusOK, code)
}
