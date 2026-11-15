package health

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Check probes one backend; nil means reachable. It must honour ctx, or be
// bounded by its own timeout: a check that never returns is abandoned at
// the policy's timeout and leaks its goroutine.
type Check func(ctx context.Context) error

// ReadinessPolicy is how the watcher turns check results into a verdict.
type ReadinessPolicy struct {
	// Timeout bounds one check; a check that has not answered by then
	// counts as one failure.
	Timeout time.Duration
	// RetryInterval is the pause after a failed check, shorter than a
	// backend's own interval so an outage is confirmed quickly.
	RetryInterval time.Duration
	// FailureThreshold is how many consecutive failures take a backend
	// down. One success brings it back.
	FailureThreshold int
}

// DefaultReadinessPolicy tolerates what a busy but serving replica shows —
// a check that waits seconds behind a burst, a single lost packet — and
// still reports a dead backend within
// interval + FailureThreshold*Timeout + (FailureThreshold-1)*RetryInterval
// (10s at DefaultCheckInterval when the backend hangs; about 4s when it
// refuses outright).
var DefaultReadinessPolicy = ReadinessPolicy{
	Timeout:          2 * time.Second,
	RetryInterval:    time.Second,
	FailureThreshold: 3,
}

// DefaultCheckInterval is how often a healthy backend is checked.
const DefaultCheckInterval = 2 * time.Second

// Readiness watches the backends the public API cannot serve without. Each
// one is checked in the background on its own cadence and the probe only
// reads the last verdict, so answering it costs no I/O and its latency
// does not depend on the backends or on how busy this process is: the
// probe of a replica that serves under load must not time out or fail
// because a check queued behind that load.
type Readiness struct {
	policy   ReadinessPolicy
	backends []*backend
}

type backend struct {
	name     string
	interval time.Duration
	check    Check

	mu sync.Mutex
	// up starts false: a backend that has never answered is not ready
	up       bool
	failures int
	lastErr  error
}

func NewReadiness(policy ReadinessPolicy) *Readiness {
	return &Readiness{policy: policy}
}

// Add registers a backend checked every interval while it answers. Call it
// before Start.
func (r *Readiness) Add(name string, interval time.Duration, check Check) {
	r.backends = append(r.backends, &backend{name: name, interval: interval, check: check})
}

// Start begins watching every backend, the first check at once, until ctx
// is done.
func (r *Readiness) Start(ctx context.Context) {
	for _, b := range r.backends {
		go r.watch(ctx, b)
	}
}

func (r *Readiness) watch(ctx context.Context, b *backend) {
	for {
		err := r.probe(ctx, b)
		if ctx.Err() != nil {
			// shutting down: the cancelled check says nothing about the
			// backend
			return
		}
		wait := b.interval
		if err != nil {
			wait = r.policy.RetryInterval
		}
		b.record(err, r.policy.FailureThreshold)

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// probe runs one check, abandoning it at the timeout even when it ignores
// its context (no context-aware client).
func (r *Readiness) probe(ctx context.Context, b *backend) error {
	ctx, cancel := context.WithTimeout(ctx, r.policy.Timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- b.check(ctx) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *backend) record(err error, threshold int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err == nil {
		b.up, b.failures, b.lastErr = true, 0, nil
		return
	}
	b.failures++
	b.lastErr = err
	if b.failures >= threshold {
		b.up = false
	}
}

// BackendStatus is the watcher's current verdict on one backend.
type BackendStatus struct {
	// Up is false until the first successful check and after
	// FailureThreshold consecutive failures.
	Up bool
	// Detail says why for the probe's body: "ok", "starting", the last
	// error, or the failures an up backend is currently riding out.
	Detail string
}

// Status returns every backend's verdict keyed by name. It never blocks on
// a backend.
func (r *Readiness) Status() map[string]BackendStatus {
	status := make(map[string]BackendStatus, len(r.backends))
	for _, b := range r.backends {
		b.mu.Lock()
		s := BackendStatus{Up: b.up}
		switch {
		case b.lastErr == nil && b.up:
			s.Detail = "ok"
		case b.lastErr == nil:
			s.Detail = "starting"
		case b.up:
			s.Detail = fmt.Sprintf("ok (%d of %d failures tolerated: %s)", b.failures, r.policy.FailureThreshold-1, b.lastErr)
		default:
			s.Detail = b.lastErr.Error()
		}
		b.mu.Unlock()
		status[b.name] = s
	}
	return status
}
