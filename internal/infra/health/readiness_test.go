package health_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/health"
)

// testPolicy is DefaultReadinessPolicy scaled down 20x, with the same
// threshold.
var testPolicy = health.ReadinessPolicy{
	Timeout:          100 * time.Millisecond,
	RetryInterval:    50 * time.Millisecond,
	FailureThreshold: 3,
}

const testInterval = 100 * time.Millisecond

// detectionBound is the documented worst case for a backend that stops
// answering: the wait for the next check, then FailureThreshold timeouts
// with the retry pauses between them.
func detectionBound(p health.ReadinessPolicy, interval time.Duration) time.Duration {
	return interval + time.Duration(p.FailureThreshold)*p.Timeout + time.Duration(p.FailureThreshold-1)*p.RetryInterval
}

// scriptedBackend answers each check with the current mode.
type scriptedBackend struct {
	mu     sync.Mutex
	mode   func(ctx context.Context) error
	checks atomic.Int64
}

func (b *scriptedBackend) set(mode func(ctx context.Context) error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mode = mode
}

func (b *scriptedBackend) check(ctx context.Context) error {
	b.checks.Add(1)
	b.mu.Lock()
	mode := b.mode
	b.mu.Unlock()
	return mode(ctx)
}

func answering(context.Context) error { return nil }

// hanging is a backend that neither answers nor refuses.
func hanging(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

var errRefused = errors.New("connection refused")

func refusing(context.Context) error { return errRefused }

func start(t *testing.T, name string, interval time.Duration, check health.Check) *health.Readiness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r := health.NewReadiness(testPolicy)
	r.Add(name, interval, check)
	r.Start(ctx)
	return r
}

func up(r *health.Readiness, name string) bool {
	return r.Status()[name].Up
}

func requireUp(t *testing.T, r *health.Readiness, name string) {
	t.Helper()
	require.Eventually(t, func() bool { return up(r, name) }, time.Second, time.Millisecond)
}

func TestReadinessIsDownUntilTheFirstAnswer(t *testing.T) {
	release := make(chan struct{})
	r := health.NewReadiness(testPolicy)
	r.Add("database", testInterval, func(ctx context.Context) error {
		<-release
		return nil
	})
	require.Equal(t, health.BackendStatus{Up: false, Detail: "starting"}, r.Status()["database"])

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx)
	require.False(t, up(r, "database"), "a check in flight is not an answer")

	close(release)
	requireUp(t, r, "database")
	require.Equal(t, "ok", r.Status()["database"].Detail)
}

// A busy but serving replica: its checks keep missing their deadline or
// failing, but never FailureThreshold times in a row. It must stay ready
// throughout.
func TestReadinessRidesOutChecksThatMissTheirDeadline(t *testing.T) {
	var n atomic.Int64
	backend := func(ctx context.Context) error {
		// two of every three checks hang past the timeout
		if n.Add(1)%3 == 1 {
			return nil
		}
		return hanging(ctx)
	}
	r := start(t, "database", time.Millisecond, backend)
	requireUp(t, r, "database")

	sawTolerated := false
	deadline := time.Now().Add(8 * detectionBound(testPolicy, time.Millisecond))
	for time.Now().Before(deadline) {
		status := r.Status()["database"]
		require.True(t, status.Up, "flipped after %d checks: %s", n.Load(), status.Detail)
		if status.Detail != "ok" {
			sawTolerated = true
			require.Contains(t, status.Detail, "failures tolerated: context deadline exceeded")
		}
		time.Sleep(time.Millisecond)
	}
	require.True(t, sawTolerated, "the failures being ridden out are reported")
	require.Greater(t, n.Load(), int64(9), "several rounds of slow checks ran")
}

// A backend that stops answering is reported down within the documented
// bound, and not before FailureThreshold checks have failed.
func TestReadinessDetectsABackendThatHangs(t *testing.T) {
	backend := &scriptedBackend{mode: answering}
	r := start(t, "database", testInterval, backend.check)
	requireUp(t, r, "database")

	before := backend.checks.Load()
	backend.set(hanging)
	died := time.Now()
	bound := detectionBound(testPolicy, testInterval)
	require.Eventually(t, func() bool { return !up(r, "database") }, bound+100*time.Millisecond, time.Millisecond,
		"not detected within %v", bound)
	require.GreaterOrEqual(t, time.Since(died), time.Duration(testPolicy.FailureThreshold)*testPolicy.Timeout,
		"one or two missed deadlines must not be enough")
	require.GreaterOrEqual(t, backend.checks.Load()-before, int64(testPolicy.FailureThreshold))
	require.Equal(t, "context deadline exceeded", r.Status()["database"].Detail)
}

// A backend that refuses outright fails each check at once, so it is
// confirmed down in the retry pauses alone.
func TestReadinessDetectsABackendThatRefusesFaster(t *testing.T) {
	backend := &scriptedBackend{mode: answering}
	r := start(t, "redis", testInterval, backend.check)
	requireUp(t, r, "redis")

	backend.set(refusing)
	bound := testInterval + time.Duration(testPolicy.FailureThreshold-1)*testPolicy.RetryInterval
	require.Eventually(t, func() bool { return !up(r, "redis") }, bound+100*time.Millisecond, time.Millisecond,
		"not detected within %v", bound)
	require.Equal(t, errRefused.Error(), r.Status()["redis"].Detail)
}

func TestReadinessRecoversOnOneAnswer(t *testing.T) {
	backend := &scriptedBackend{mode: refusing}
	r := start(t, "redis", testInterval, backend.check)
	require.Eventually(t, func() bool { return r.Status()["redis"].Detail == errRefused.Error() }, time.Second, time.Millisecond)
	require.False(t, up(r, "redis"))

	backend.set(answering)
	// a down backend is retried on the retry interval, not its own
	require.Eventually(t, func() bool { return up(r, "redis") }, testPolicy.RetryInterval+100*time.Millisecond, time.Millisecond)
	require.Equal(t, "ok", r.Status()["redis"].Detail)
}

// A healthy backend is checked once per interval, whatever the number of
// probes: the cost of readiness does not grow with who asks.
func TestReadinessChecksOnItsOwnCadence(t *testing.T) {
	backend := &scriptedBackend{mode: answering}
	r := start(t, "database", testInterval, backend.check)
	requireUp(t, r, "database")

	began := time.Now()
	for time.Since(began) < 5*testInterval {
		r.Status()
	}
	require.LessOrEqual(t, backend.checks.Load(), int64(7), "about one check per interval")
}

func TestReadinessBoundsCheckThatIgnoresContext(t *testing.T) {
	backend := &scriptedBackend{mode: answering}
	r := start(t, "memcached", testInterval, backend.check)
	requireUp(t, r, "memcached")

	stuck := make(chan struct{})
	defer close(stuck)
	backend.set(func(context.Context) error {
		<-stuck
		return nil
	})
	bound := detectionBound(testPolicy, testInterval)
	require.Eventually(t, func() bool { return !up(r, "memcached") }, bound+100*time.Millisecond, time.Millisecond)

	// and the verdict is read without waiting on the stuck checks
	began := time.Now()
	r.Status()
	require.Less(t, time.Since(began), 10*time.Millisecond)
}

func TestReadinessStopsWithItsContext(t *testing.T) {
	backend := &scriptedBackend{mode: answering}
	ctx, cancel := context.WithCancel(context.Background())
	r := health.NewReadiness(testPolicy)
	r.Add("database", time.Millisecond, backend.check)
	r.Start(ctx)
	requireUp(t, r, "database")

	// a shutdown cancels the check in flight; that is not a failure of the
	// backend
	backend.set(hanging)
	cancel()
	time.Sleep(20 * time.Millisecond)
	checks := backend.checks.Load()
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, checks, backend.checks.Load(), "no check after the context is done")
	require.Equal(t, health.BackendStatus{Up: true, Detail: "ok"}, r.Status()["database"])
}

func TestDefaultReadinessPolicyDetectionTime(t *testing.T) {
	require.Equal(t, 10*time.Second, detectionBound(health.DefaultReadinessPolicy, health.DefaultCheckInterval),
		"the bound documented on DefaultReadinessPolicy")
}
