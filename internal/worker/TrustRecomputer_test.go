package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/concrnt/concrnt/internal/infra/health"
)

type countingScorer struct {
	runs atomic.Int64
	err  error
}

func (s *countingScorer) Recompute(ctx context.Context) error {
	s.runs.Add(1)
	return s.err
}

func waitForRuns(t *testing.T, scorer *countingScorer, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for scorer.runs.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("recomputed %d times, want at least %d", scorer.runs.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// The worker recomputes at once and then on its interval, and stops when
// leadership is lost.
func TestTrustRecomputerRunsUntilLeadershipIsLost(t *testing.T) {
	scorer := &countingScorer{}
	recomputer := NewTrustRecomputer(scorer, nil)
	recomputer.interval = 5 * time.Millisecond

	leadCtx, loseLeadership := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		recomputer.run(leadCtx)
		close(done)
	}()

	waitForRuns(t, scorer, 3)

	loseLeadership()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("worker still running after leadership was lost")
	}
	stopped := scorer.runs.Load()
	time.Sleep(30 * time.Millisecond)
	if scorer.runs.Load() != stopped {
		t.Fatalf("recomputed after leadership was lost")
	}
}

// A failed round is retried on the next interval rather than ending the loop.
func TestTrustRecomputerKeepsGoingAfterAFailure(t *testing.T) {
	scorer := &countingScorer{err: errors.New("database down")}
	recomputer := NewTrustRecomputer(scorer, nil)
	recomputer.interval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	recomputer.Start(ctx)

	waitForRuns(t, scorer, 3)
}

// The watchdog sees the loop running while this replica leads, and idle —
// not stalled — once it has stopped on purpose.
func TestTrustRecomputerReportsToTheWatchdog(t *testing.T) {
	watchdog := health.NewWatchdog()
	scorer := &countingScorer{}
	recomputer := NewTrustRecomputer(scorer, watchdog)
	recomputer.interval = 5 * time.Millisecond

	const name = "worker/trust-recomputer"
	if got := watchdog.Loops(time.Now())[name].State; got != health.Idle {
		t.Fatalf("state before leading = %v, want idle", got)
	}

	leadCtx, loseLeadership := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		recomputer.run(leadCtx)
		close(done)
	}()
	waitForRuns(t, scorer, 1)
	if got := watchdog.Loops(time.Now())[name].State; got != health.Running {
		t.Fatalf("state while leading = %v, want running", got)
	}
	if stalled := watchdog.Stalled(time.Now().Add(trustRecomputerDeadline + time.Second)); len(stalled) != 1 {
		t.Fatalf("stalled = %v, want the loop once it misses its deadline", stalled)
	}

	loseLeadership()
	<-done
	if got := watchdog.Loops(time.Now())[name].State; got != health.Idle {
		t.Fatalf("state after losing leadership = %v, want idle", got)
	}
}
