package health_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/health"
)

func TestReadinessReportsEveryCheck(t *testing.T) {
	r := health.NewReadiness(time.Second)
	r.Add("up", func(ctx context.Context) error { return nil })
	down := errors.New("connection refused")
	r.Add("down", func(ctx context.Context) error { return down })

	outcome := r.Run(context.Background())
	require.Len(t, outcome, 2)
	require.NoError(t, outcome["up"])
	require.ErrorIs(t, outcome["down"], down)
}

func TestReadinessRunsChecksConcurrently(t *testing.T) {
	r := health.NewReadiness(time.Second)
	for i := 0; i < 3; i++ {
		r.Add("slow", func(ctx context.Context) error {
			time.Sleep(100 * time.Millisecond)
			return nil
		})
	}

	start := time.Now()
	r.Run(context.Background())
	require.Less(t, time.Since(start), 250*time.Millisecond, "checks must not run in sequence")
}

func TestReadinessBoundsCheckThatIgnoresContext(t *testing.T) {
	r := health.NewReadiness(50 * time.Millisecond)
	r.Add("honours-ctx", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	r.Add("ignores-ctx", func(ctx context.Context) error {
		time.Sleep(time.Second)
		return nil
	})

	start := time.Now()
	outcome := r.Run(context.Background())
	require.Less(t, time.Since(start), 500*time.Millisecond, "sweep must end at its timeout")
	require.ErrorIs(t, outcome["honours-ctx"], context.DeadlineExceeded)
	require.ErrorIs(t, outcome["ignores-ctx"], context.DeadlineExceeded)
}
