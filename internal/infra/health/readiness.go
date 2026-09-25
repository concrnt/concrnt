package health

import (
	"context"
	"sync"
	"time"
)

// Check probes one backend; nil means reachable. It must honour ctx, or be
// bounded by its own timeout, because the probe answers within one period.
type Check func(ctx context.Context) error

// Readiness sweeps every registered backend concurrently, so the probe's
// latency is the slowest check rather than their sum.
type Readiness struct {
	timeout time.Duration
	names   []string
	checks  []Check
}

// NewReadiness bounds one sweep by timeout: a backend that neither answers
// nor refuses within it counts as down.
func NewReadiness(timeout time.Duration) *Readiness {
	return &Readiness{timeout: timeout}
}

func (r *Readiness) Add(name string, check Check) {
	r.names = append(r.names, name)
	r.checks = append(r.checks, check)
}

// Run returns every backend's outcome keyed by name; nil means reachable.
func (r *Readiness) Run(ctx context.Context) map[string]error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	results := make([]error, len(r.checks))
	var wg sync.WaitGroup
	for i, check := range r.checks {
		wg.Add(1)
		go func(i int, check Check) {
			defer wg.Done()
			done := make(chan error, 1)
			go func() { done <- check(ctx) }()
			// a check that ignores ctx (no context-aware client) still
			// cannot hold the probe past the sweep timeout
			select {
			case results[i] = <-done:
			case <-ctx.Done():
				results[i] = ctx.Err()
			}
		}(i, check)
	}
	wg.Wait()

	outcome := make(map[string]error, len(r.names))
	for i, name := range r.names {
		outcome[name] = results[i]
	}
	return outcome
}
