package health_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/health"
)

func TestWatchdogStalledOnlyAfterDeadline(t *testing.T) {
	w := health.NewWatchdog()
	fast := w.Register("fast", time.Second)
	slow := w.Register("slow", time.Hour)

	// registered but never started: silence is not a stall
	require.Empty(t, w.Stalled(time.Now().Add(24*time.Hour)))
	require.Equal(t, map[string]health.Loop{"fast": {State: health.Idle}, "slow": {State: health.Idle}}, w.Loops(time.Now()))

	fast.Beat()
	slow.Beat()
	require.Empty(t, w.Stalled(time.Now()))
	for name, loop := range w.Loops(time.Now()) {
		require.Equal(t, health.Running, loop.State, name)
	}

	stalled := w.Stalled(time.Now().Add(2 * time.Second))
	require.Contains(t, stalled, "fast")
	require.NotContains(t, stalled, "slow")
	require.GreaterOrEqual(t, stalled["fast"], 2*time.Second)
	require.Equal(t, health.Stalled, w.Loops(time.Now().Add(2 * time.Second))["fast"].State)

	// a fresh beat clears it
	fast.Beat()
	require.Empty(t, w.Stalled(time.Now()))
}

func TestWatchdogStopExcludesLoop(t *testing.T) {
	w := health.NewWatchdog()
	h := w.Register("leader-only", time.Second)
	h.Beat()
	require.Contains(t, w.Stalled(time.Now().Add(time.Minute)), "leader-only")

	// the loop returned on purpose (leadership lost): not a stall
	h.Stop()
	require.Empty(t, w.Stalled(time.Now().Add(time.Minute)))

	// a later term starts beating again
	h.Beat()
	require.Empty(t, w.Stalled(time.Now()))
	require.Contains(t, w.Stalled(time.Now().Add(time.Minute)), "leader-only")
}

func TestWatchdogNilIsNoop(t *testing.T) {
	var w *health.Watchdog
	h := w.Register("anything", time.Second)
	require.Nil(t, h)
	h.Beat()
	h.Stop()
	require.Empty(t, w.Stalled(time.Now()))
	require.Empty(t, w.Loops(time.Now()))
}
