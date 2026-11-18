package rest

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/concrnt/concrnt/internal/infra/health"
)

// ProbeHandler serves the liveness and readiness probes. Mount it only on
// the internal listener.
//
// Liveness (/health) asks whether this process is worth keeping: it fails
// only when one of the process's own loops has stalled, never because a
// backend is down — restarting would not bring the backend back. Readiness
// (/ready) asks whether this replica should receive traffic: it fails while
// the process is still starting up, while any backend the public API depends
// on is unreachable, and while draining.
type ProbeHandler struct {
	watchdog  *health.Watchdog
	readiness *health.Readiness
	phase     atomic.Pointer[string] // startup phase in progress; nil once started
	draining  atomic.Bool
}

func NewProbeHandler(watchdog *health.Watchdog, readiness *health.Readiness) *ProbeHandler {
	return &ProbeHandler{watchdog: watchdog, readiness: readiness}
}

func (h *ProbeHandler) RegisterRoutes(e *echo.Echo) {
	e.GET("/health", h.handleHealth)
	e.GET("/ready", h.handleReady)
}

// Starting makes /ready answer 503 naming phase, until Started is called.
func (h *ProbeHandler) Starting(phase string) {
	h.phase.Store(&phase)
}

func (h *ProbeHandler) Started() {
	h.phase.Store(nil)
}

// Drain makes /ready answer 503 from now on, so the endpoint controller
// stops routing here before the listeners close. Liveness is unaffected.
func (h *ProbeHandler) Drain() {
	h.draining.Store(true)
}

// probeReport is the body of either probe: the verdict, and every check by
// name so a failing probe says what failed without a log lookup.
type probeReport struct {
	Status string            `json:"status"`
	Phase  string            `json:"phase,omitempty"`
	Checks map[string]string `json:"checks,omitempty"`
}

func (h *ProbeHandler) handleHealth(c *echo.Context) error {
	report := probeReport{Status: "ok", Checks: map[string]string{}}
	for name, loop := range h.watchdog.Loops(time.Now()) {
		switch loop.State {
		case health.Idle:
			report.Checks[name] = "idle"
		case health.Running:
			report.Checks[name] = "ok"
		case health.Stalled:
			report.Status = "stalled"
			report.Checks[name] = "no heartbeat for " + loop.Since.Truncate(time.Second).String()
		}
	}

	if report.Status != "ok" {
		return c.JSON(http.StatusServiceUnavailable, report)
	}
	return c.JSON(http.StatusOK, report)
}

func (h *ProbeHandler) handleReady(c *echo.Context) error {
	if h.draining.Load() {
		return c.JSON(http.StatusServiceUnavailable, probeReport{Status: "draining"})
	}
	if phase := h.phase.Load(); phase != nil {
		return c.JSON(http.StatusServiceUnavailable, probeReport{Status: "starting", Phase: *phase})
	}

	report := probeReport{Status: "ok", Checks: map[string]string{}}
	for name, err := range h.readiness.Run(c.Request().Context()) {
		if err != nil {
			report.Status = "unavailable"
			report.Checks[name] = err.Error()
			continue
		}
		report.Checks[name] = "ok"
	}

	if report.Status != "ok" {
		return c.JSON(http.StatusServiceUnavailable, report)
	}
	return c.JSON(http.StatusOK, report)
}
