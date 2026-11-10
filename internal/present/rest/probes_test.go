package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
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
	NewProbeHandler(watchdog, health.NewReadiness(health.DefaultReadinessPolicy)).RegisterRoutes(e)

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

// probePolicy is health.DefaultReadinessPolicy scaled down 20x.
var probePolicy = health.ReadinessPolicy{
	Timeout:          100 * time.Millisecond,
	RetryInterval:    50 * time.Millisecond,
	FailureThreshold: 3,
}

const probeCheckInterval = 100 * time.Millisecond

func startReadiness(t *testing.T, checks map[string]health.Check) *health.Readiness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	readiness := health.NewReadiness(probePolicy)
	for name, check := range checks {
		readiness.Add(name, probeCheckInterval, check)
	}
	readiness.Start(ctx)
	return readiness
}

func TestProbeReadyReflectsBackendsAndDrain(t *testing.T) {
	var redisDown atomic.Bool
	release := make(chan struct{})
	readiness := startReadiness(t, map[string]health.Check{
		"postgres": func(ctx context.Context) error {
			<-release
			return nil
		},
		"redis": func(ctx context.Context) error {
			if redisDown.Load() {
				return errors.New("dial tcp: connection refused")
			}
			return nil
		},
	})
	e := echo.New()
	h := NewProbeHandler(health.NewWatchdog(), readiness)
	h.RegisterRoutes(e)
	ready := func() bool {
		code, _ := probe(t, e, "/ready")
		return code == http.StatusOK
	}

	// not ready before every backend has answered once
	require.Eventually(t, func() bool {
		_, report := probe(t, e, "/ready")
		return report.Checks["redis"] == "ok"
	}, time.Second, time.Millisecond)
	code, report := probe(t, e, "/ready")
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.Equal(t, "unavailable", report.Status)
	require.Equal(t, map[string]string{"postgres": "starting", "redis": "ok"}, report.Checks)

	close(release)
	require.Eventually(t, ready, time.Second, time.Millisecond)
	code, report = probe(t, e, "/ready")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "ok", report.Status)
	require.Equal(t, map[string]string{"postgres": "ok", "redis": "ok"}, report.Checks)

	redisDown.Store(true)
	require.Eventually(t, func() bool { return !ready() }, time.Second, time.Millisecond)
	code, report = probe(t, e, "/ready")
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.Equal(t, "unavailable", report.Status)
	require.Equal(t, "ok", report.Checks["postgres"])
	require.Equal(t, "dial tcp: connection refused", report.Checks["redis"])

	// draining wins even with healthy backends, and never touches liveness
	redisDown.Store(false)
	require.Eventually(t, ready, time.Second, time.Millisecond)
	h.Drain()
	code, report = probe(t, e, "/ready")
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.Equal(t, "draining", report.Status)
	require.Empty(t, report.Checks)
	code, _ = probe(t, e, "/health")
	require.Equal(t, http.StatusOK, code)
}

// servingPool is a connection pool the way database/sql keeps one: a fixed
// number of connections, and a caller that finds none waits until one is
// returned or its context ends. Waiters are served in arrival order, the
// kindest a pool gets (go-redis; database/sql picks one at random).
type servingPool struct {
	mu      sync.Mutex
	free    int
	waiters []chan struct{}
}

func (p *servingPool) acquire(ctx context.Context) error {
	p.mu.Lock()
	if p.free > 0 {
		p.free--
		p.mu.Unlock()
		return nil
	}
	turn := make(chan struct{})
	p.waiters = append(p.waiters, turn)
	p.mu.Unlock()

	select {
	case <-turn:
		return nil
	case <-ctx.Done():
		p.mu.Lock()
		defer p.mu.Unlock()
		for i, w := range p.waiters {
			if w == turn {
				p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
				return ctx.Err()
			}
		}
		// the turn was handed over while the context ended: give it back
		p.releaseLocked()
		return ctx.Err()
	}
}

func (p *servingPool) release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.releaseLocked()
}

func (p *servingPool) releaseLocked() {
	if len(p.waiters) > 0 {
		close(p.waiters[0])
		p.waiters = p.waiters[1:]
		return
	}
	p.free++
}

func (p *servingPool) ping(ctx context.Context) error {
	if err := p.acquire(ctx); err != nil {
		return err
	}
	p.release()
	return nil
}

// The load-test failure: a burst keeps every pooled connection in use, the
// replica serves all of it, and /ready has to stay 200 and fast. A ping
// through the serving pool never gets a connection in time (which is what
// took the replicas out of the load balancer), so readiness checks the
// backend on a connection of its own and the probe only reads the verdict.
func TestProbeReadyHoldsWhileTheServingPoolIsSaturated(t *testing.T) {
	pool := &servingPool{free: 4}
	var served atomic.Int64
	stop := make(chan struct{})
	var requests sync.WaitGroup
	for range 64 {
		requests.Add(1)
		go func() {
			defer requests.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if pool.acquire(context.Background()) == nil {
					time.Sleep(20 * time.Millisecond)
					pool.release()
					served.Add(1)
				}
			}
		}()
	}
	defer requests.Wait()
	defer close(stop)

	readiness := startReadiness(t, map[string]health.Check{
		// the dedicated readiness connection: the database answers at once
		"database": func(context.Context) error { return nil },
	})
	e := echo.New()
	NewProbeHandler(health.NewWatchdog(), readiness).RegisterRoutes(e)
	require.Eventually(t, func() bool {
		code, _ := probe(t, e, "/ready")
		return code == http.StatusOK
	}, time.Second, time.Millisecond)

	// the check the probe used to run: through the pool, within its
	// deadline. 60 requests queue for 4 connections held 20ms each, so a
	// ping's turn comes after about 300ms
	ctx, cancel := context.WithTimeout(context.Background(), probePolicy.Timeout)
	defer cancel()
	require.ErrorIs(t, pool.ping(ctx), context.DeadlineExceeded, "the serving pool is saturated")

	before := served.Load()
	for range 50 {
		began := time.Now()
		code, report := probe(t, e, "/ready")
		require.Equal(t, http.StatusOK, code, report.Checks)
		require.Less(t, time.Since(began), 50*time.Millisecond, "the probe waits on nothing")
		time.Sleep(2 * time.Millisecond)
	}
	require.Greater(t, served.Load(), before, "the replica was serving all along")
}

// A check that cannot avoid the load (no dedicated connection) and misses
// its deadline now and then does not flip the probe either, and the probe
// itself never waits for it.
func TestProbeReadyHoldsThroughSlowChecks(t *testing.T) {
	var n atomic.Int64
	readiness := startReadiness(t, map[string]health.Check{
		"database": func(ctx context.Context) error {
			// two of every three checks outlast the timeout
			if n.Add(1)%3 == 1 {
				return nil
			}
			<-ctx.Done()
			return ctx.Err()
		},
	})
	e := echo.New()
	NewProbeHandler(health.NewWatchdog(), readiness).RegisterRoutes(e)
	require.Eventually(t, func() bool {
		code, _ := probe(t, e, "/ready")
		return code == http.StatusOK
	}, time.Second, time.Millisecond)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		began := time.Now()
		code, report := probe(t, e, "/ready")
		require.Equal(t, http.StatusOK, code, report.Checks)
		require.Equal(t, "ok", report.Status)
		require.Less(t, time.Since(began), 50*time.Millisecond, "the probe waits on nothing")
		time.Sleep(5 * time.Millisecond)
	}
	require.Greater(t, n.Load(), int64(9))
}

// A backend that is really gone still takes the replica out of rotation,
// within the bound of the policy.
func TestProbeReadyFailsForADeadBackendWithinTheBound(t *testing.T) {
	var dead atomic.Bool
	readiness := startReadiness(t, map[string]health.Check{
		"database": func(ctx context.Context) error {
			if dead.Load() {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		},
	})
	e := echo.New()
	NewProbeHandler(health.NewWatchdog(), readiness).RegisterRoutes(e)
	require.Eventually(t, func() bool {
		code, _ := probe(t, e, "/ready")
		return code == http.StatusOK
	}, time.Second, time.Millisecond)

	dead.Store(true)
	bound := probeCheckInterval + time.Duration(probePolicy.FailureThreshold)*probePolicy.Timeout +
		time.Duration(probePolicy.FailureThreshold-1)*probePolicy.RetryInterval
	require.Eventually(t, func() bool {
		code, _ := probe(t, e, "/ready")
		return code == http.StatusServiceUnavailable
	}, bound+100*time.Millisecond, time.Millisecond, "not detected within %v", bound)
	_, report := probe(t, e, "/ready")
	require.Equal(t, "unavailable", report.Status)
	require.Equal(t, "context deadline exceeded", report.Checks["database"])

	// liveness is a different question: a dead backend never restarts the
	// process
	code, _ := probe(t, e, "/health")
	require.Equal(t, http.StatusOK, code)
}
