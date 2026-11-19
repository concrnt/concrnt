package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/concrnt/concrnt/internal/infra/health"
)

const (
	// trustRecomputeInterval is how often the entities' trust scores are
	// recomputed.
	trustRecomputeInterval = 5 * time.Minute
	// trustRecomputeTimeout bounds one recomputation, so that a slow
	// database fails that round instead of stalling the loop.
	trustRecomputeTimeout = 2 * time.Minute
	// trustRecomputerDeadline is the loop's watchdog deadline: an interval
	// plus a recomputation that ran into its timeout, with margin.
	trustRecomputerDeadline = 10 * time.Minute
)

// TrustScorer recomputes and stores the entities' trust scores; implemented
// by trust.Usecase.
type TrustScorer interface {
	Recompute(ctx context.Context) error
}

// TrustRecomputer keeps the entities' trust scores up to date. It is a
// singleton worker: only the cluster's leader runs it, so replicas do not
// each compute and store the same result.
type TrustRecomputer struct {
	scorer    TrustScorer
	interval  time.Duration
	heartbeat *health.Heartbeat
}

// NewTrustRecomputer wires the worker; watchdog (nil to skip) restarts the
// process if the loop stalls.
func NewTrustRecomputer(scorer TrustScorer, watchdog *health.Watchdog) *TrustRecomputer {
	return &TrustRecomputer{
		scorer:    scorer,
		interval:  trustRecomputeInterval,
		heartbeat: watchdog.Register("worker/trust-recomputer", trustRecomputerDeadline),
	}
}

// Start recomputes now and then on every interval, until ctx — the
// leadership context — is cancelled.
func (r *TrustRecomputer) Start(ctx context.Context) {
	go r.run(ctx)
}

func (r *TrustRecomputer) run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	defer r.heartbeat.Stop()

	for {
		r.heartbeat.Beat()
		r.recompute(ctx)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *TrustRecomputer) recompute(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, trustRecomputeTimeout)
	defer cancel()

	if err := r.scorer.Recompute(ctx); err != nil && ctx.Err() != context.Canceled {
		slog.Error("failed to recompute entity trust", slog.String("error", err.Error()))
	}
}
