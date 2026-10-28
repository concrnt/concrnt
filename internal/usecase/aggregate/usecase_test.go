package aggregate_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/aggregate/storetest"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

type delta struct {
	Key string
	N   int64
}

// countingDef is a map[string]int64 aggregate whose Compute reads a
// controllable "primary data" map and counts how often it ran.
type countingDef struct {
	def      *aggregate.Definition[map[string]int64, delta]
	primary  map[string]int64
	computes atomic.Int64
	// before runs inside Compute, after the counter moved (to race a store)
	before func()
}

func newCountingDef() *countingDef {
	d := &countingDef{primary: map[string]int64{}}
	d.def = &aggregate.Definition[map[string]int64, delta]{
		Name: "counts",
		Compute: func(ctx context.Context, target string) (map[string]int64, error) {
			d.computes.Add(1)
			if d.before != nil {
				d.before()
			}
			out := map[string]int64{}
			for k, v := range d.primary {
				out[k] = v
			}
			return out, nil
		},
		Apply: func(cur map[string]int64, dl delta) map[string]int64 {
			if cur == nil {
				cur = map[string]int64{}
			}
			cur[dl.Key] += dl.N
			if cur[dl.Key] <= 0 {
				delete(cur, dl.Key)
			}
			return cur
		},
	}
	return d
}

func TestGetReadsThroughOnce(t *testing.T) {
	ctx := context.Background()
	store := storetest.NewInMemory()
	uc := aggregate.New(store, 0, 0)
	d := newCountingDef()
	d.primary["like"] = 2

	got, err := aggregate.Get(ctx, uc, d.def, "t1")
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"like": 2}, got)
	require.EqualValues(t, 1, d.computes.Load())

	// a hit never computes, even when the primary data moved
	d.primary["like"] = 5
	got, err = aggregate.Get(ctx, uc, d.def, "t1")
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"like": 2}, got)
	require.EqualValues(t, 1, d.computes.Load())

	// another target is its own entry
	_, err = aggregate.Get(ctx, uc, d.def, "t2")
	require.NoError(t, err)
	require.EqualValues(t, 2, d.computes.Load())
}

func TestTouchComputesWhenAbsentAndAppliesWhenPresent(t *testing.T) {
	ctx := context.Background()
	store := storetest.NewInMemory()
	uc := aggregate.New(store, 0, 0)
	d := newCountingDef()

	// the write is already committed when Touch runs, so the primary data
	// already holds it and the computed value must not be double-counted
	d.primary["like"] = 1
	require.NoError(t, aggregate.Touch(ctx, uc, d.def, "t1", delta{"like", 1}, time.Now()))
	require.EqualValues(t, 1, d.computes.Load())
	got, err := aggregate.Get(ctx, uc, d.def, "t1")
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"like": 1}, got)

	// with an entry present only the delta is applied
	d.primary["like"] = 2
	require.NoError(t, aggregate.Touch(ctx, uc, d.def, "t1", delta{"like", 1}, time.Now()))
	require.EqualValues(t, 1, d.computes.Load())
	got, err = aggregate.Get(ctx, uc, d.def, "t1")
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"like": 2}, got)

	// a negative delta that empties a key removes it
	require.NoError(t, aggregate.Touch(ctx, uc, d.def, "t1", delta{"like", -2}, time.Now()))
	got, err = aggregate.Get(ctx, uc, d.def, "t1")
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestTouchLostRaceAppliesOnlyToOlderComputation(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name       string
		winnerAsOf func(receivedAt time.Time) time.Time
		want       int64
	}{
		{"winner computed before the write landed", func(r time.Time) time.Time { return r.Add(-time.Second) }, 2},
		{"winner computed after the write landed", func(r time.Time) time.Time { return r.Add(time.Second) }, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := storetest.NewInMemory()
			uc := aggregate.New(store, 0, 0)
			d := newCountingDef()
			d.primary["like"] = 1
			receivedAt := time.Now()

			// the competing computation lands while ours is running, so our
			// PutIfAbsent loses; the winner's value is what it saw (1)
			d.before = func() {
				_, err := store.PutIfAbsent(ctx, aggregate.Key("counts", "t1"), aggregate.Entry{Value: []byte(`{"like":1}`), AsOf: tc.winnerAsOf(receivedAt)}, time.Hour)
				require.NoError(t, err)
			}
			require.NoError(t, aggregate.Touch(ctx, uc, d.def, "t1", delta{"like", 1}, receivedAt))
			require.EqualValues(t, 1, d.computes.Load())

			got, err := aggregate.Get(ctx, uc, d.def, "t1")
			require.NoError(t, err)
			require.Equal(t, map[string]int64{"like": tc.want}, got)
		})
	}
}

func TestGetLostRaceReturnsWinner(t *testing.T) {
	ctx := context.Background()
	store := storetest.NewInMemory()
	uc := aggregate.New(store, 0, 0)
	d := newCountingDef()
	d.primary["like"] = 7
	d.before = func() {
		_, err := store.PutIfAbsent(ctx, aggregate.Key("counts", "t1"), aggregate.Entry{Value: []byte(`{"like":3}`), AsOf: time.Now()}, time.Hour)
		require.NoError(t, err)
	}
	got, err := aggregate.Get(ctx, uc, d.def, "t1")
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"like": 3}, got, "the stored winner is what every reader sees")
}

func TestMaxDeltasInvalidates(t *testing.T) {
	ctx := context.Background()
	store := storetest.NewInMemory()
	uc := aggregate.New(store, 0, 3)
	d := newCountingDef()

	require.NoError(t, aggregate.Touch(ctx, uc, d.def, "t1", delta{"like", 1}, time.Now()))
	require.EqualValues(t, 1, d.computes.Load())
	for i := range 3 {
		d.primary["like"] = int64(i + 1)
		require.NoError(t, aggregate.Touch(ctx, uc, d.def, "t1", delta{"like", 1}, time.Now()))
	}
	require.EqualValues(t, 1, d.computes.Load(), "deltas up to the limit do not recompute")
	_, ok, err := store.Get(ctx, aggregate.Key("counts", "t1"))
	require.NoError(t, err)
	require.True(t, ok)

	// the delta past the limit drops the entry; the next read recomputes
	d.primary["like"] = 4
	require.NoError(t, aggregate.Touch(ctx, uc, d.def, "t1", delta{"like", 1}, time.Now()))
	_, ok, err = store.Get(ctx, aggregate.Key("counts", "t1"))
	require.NoError(t, err)
	require.False(t, ok)
	got, err := aggregate.Get(ctx, uc, d.def, "t1")
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"like": 4}, got)
	require.EqualValues(t, 2, d.computes.Load())
}

func TestInvalidateAllRecomputesOnBothPaths(t *testing.T) {
	ctx := context.Background()
	store := storetest.NewInMemory()
	uc := aggregate.New(store, 0, 0)
	d := newCountingDef()
	d.primary["like"] = 1

	_, err := aggregate.Get(ctx, uc, d.def, "t1")
	require.NoError(t, err)
	_, err = aggregate.Get(ctx, uc, d.def, "t2")
	require.NoError(t, err)
	require.EqualValues(t, 2, d.computes.Load())

	require.NoError(t, uc.InvalidateAll(ctx, ""))
	d.primary["like"] = 9

	got, err := aggregate.Get(ctx, uc, d.def, "t1")
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"like": 9}, got)
	require.NoError(t, aggregate.Touch(ctx, uc, d.def, "t2", delta{"like", 1}, time.Now()))
	got, err = aggregate.Get(ctx, uc, d.def, "t2")
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"like": 9}, got, "a write on a dropped entry recomputes rather than applies")
	require.EqualValues(t, 4, d.computes.Load())

	require.NoError(t, uc.Invalidate(ctx, "counts", "t1"))
	_, ok, err := aggregate.Cached(ctx, uc, d.def, "t1")
	require.NoError(t, err)
	require.False(t, ok)
	_, ok, err = aggregate.Cached(ctx, uc, d.def, "t2")
	require.NoError(t, err)
	require.True(t, ok)
}

func TestTouchWithoutApplyInvalidates(t *testing.T) {
	ctx := context.Background()
	store := storetest.NewInMemory()
	uc := aggregate.New(store, 0, 0)
	d := newCountingDef()
	d.def.Apply = nil
	d.primary["like"] = 1

	_, err := aggregate.Get(ctx, uc, d.def, "t1")
	require.NoError(t, err)
	require.NoError(t, aggregate.Touch(ctx, uc, d.def, "t1", delta{}, time.Now()))
	_, ok, err := aggregate.Cached(ctx, uc, d.def, "t1")
	require.NoError(t, err)
	require.False(t, ok, "without a delta form a write drops the entry")
	require.EqualValues(t, 1, d.computes.Load(), "and does not compute eagerly")
}
