package aggregate_test

import (
	"context"
	"maps"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/aggregate/storetest"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

// countingDef is an aggregate whose Compute reads a controllable "primary
// data" map and counts how often it ran.
type countingDef struct {
	def      *aggregate.Definition
	primary  map[string]int64
	computes atomic.Int64
	// before runs inside Compute, after the counter moved (to race a store)
	before func()
}

func newCountingDef() *countingDef {
	d := &countingDef{primary: map[string]int64{}}
	d.def = &aggregate.Definition{
		Name: "counts",
		Compute: func(ctx context.Context, target string) (aggregate.Counters, error) {
			d.computes.Add(1)
			if d.before != nil {
				d.before()
			}
			return aggregate.Counters{Counts: maps.Clone(d.primary)}, nil
		},
	}
	return d
}

func inc(name string, n int64) aggregate.Delta {
	return aggregate.Delta{Inc: map[string]int64{name: n}}
}

func getCounts(t *testing.T, uc *aggregate.Usecase, d *countingDef, target string) map[string]int64 {
	t.Helper()
	got, err := uc.Get(context.Background(), d.def, target)
	require.NoError(t, err)
	return got.Counts
}

func TestGetReadsThroughOnce(t *testing.T) {
	store := storetest.NewInMemory()
	uc := aggregate.New(store, 0, 0)
	d := newCountingDef()
	d.primary["like"] = 2

	require.Equal(t, map[string]int64{"like": 2}, getCounts(t, uc, d, "t1"))
	require.EqualValues(t, 1, d.computes.Load())

	// a hit never computes, even when the primary data moved
	d.primary["like"] = 5
	require.Equal(t, map[string]int64{"like": 2}, getCounts(t, uc, d, "t1"))
	require.EqualValues(t, 1, d.computes.Load())

	// another target is its own entry
	getCounts(t, uc, d, "t2")
	require.EqualValues(t, 2, d.computes.Load())
}

// An entry without counters reads as empty maps, computed or cached, so a
// caller can index and serialise it either way.
func TestGetEmptyEntryHasMaps(t *testing.T) {
	ctx := context.Background()
	uc := aggregate.New(storetest.NewInMemory(), 0, 0)
	d := newCountingDef()
	d.primary = nil

	for range 2 {
		got, err := uc.Get(ctx, d.def, "t1")
		require.NoError(t, err)
		require.NotNil(t, got.Counts)
		require.NotNil(t, got.Minima)
		require.Empty(t, got.Counts)
	}
	require.EqualValues(t, 1, d.computes.Load())
}

func TestTouchComputesWhenAbsentAndAddsWhenPresent(t *testing.T) {
	ctx := context.Background()
	store := storetest.NewInMemory()
	uc := aggregate.New(store, 0, 0)
	d := newCountingDef()

	// the write is already committed when Touch runs, so the primary data
	// already holds it and the computed value must not be double-counted
	d.primary["like"] = 1
	require.NoError(t, uc.Touch(ctx, d.def, "t1", inc("like", 1), time.Now()))
	require.EqualValues(t, 1, d.computes.Load())
	require.Equal(t, map[string]int64{"like": 1}, getCounts(t, uc, d, "t1"))

	// with an entry present only the delta is added
	d.primary["like"] = 2
	require.NoError(t, uc.Touch(ctx, d.def, "t1", aggregate.Delta{
		Inc:        map[string]int64{"like": 1},
		Min:        map[string]int64{"like": 7},
		AddMembers: []string{"con1a"},
	}, time.Now()))
	require.EqualValues(t, 1, d.computes.Load())
	got, err := uc.Get(ctx, d.def, "t1")
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"like": 2}, got.Counts)
	require.Equal(t, map[string]int64{"like": 7}, got.Minima)
	require.Equal(t, []string{"con1a"}, got.Members)
	require.EqualValues(t, 1, got.Deltas)
}

func TestTouchLostRaceAddsOnlyToOlderComputation(t *testing.T) {
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
				_, err := store.PutIfAbsent(ctx, aggregate.Key("counts", "t1"), aggregate.Counters{Counts: map[string]int64{"like": 1}, AsOf: tc.winnerAsOf(receivedAt)}, time.Hour)
				require.NoError(t, err)
			}
			require.NoError(t, uc.Touch(ctx, d.def, "t1", inc("like", 1), receivedAt))
			require.EqualValues(t, 1, d.computes.Load())

			require.Equal(t, map[string]int64{"like": tc.want}, getCounts(t, uc, d, "t1"))
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
		_, err := store.PutIfAbsent(ctx, aggregate.Key("counts", "t1"), aggregate.Counters{Counts: map[string]int64{"like": 3}, AsOf: time.Now()}, time.Hour)
		require.NoError(t, err)
	}
	require.Equal(t, map[string]int64{"like": 3}, getCounts(t, uc, d, "t1"), "the stored winner is what every reader sees")
}

// Writers add blind and never look at the entry: it is the reader that
// drops one that absorbed more than MaxDeltas deltas and recomputes it.
func TestMaxDeltasRecomputesOnRead(t *testing.T) {
	ctx := context.Background()
	store := storetest.NewInMemory()
	uc := aggregate.New(store, 0, 3)
	d := newCountingDef()

	require.NoError(t, uc.Touch(ctx, d.def, "t1", inc("like", 1), time.Now()))
	require.EqualValues(t, 1, d.computes.Load())
	for range 3 {
		require.NoError(t, uc.Touch(ctx, d.def, "t1", inc("like", 1), time.Now()))
	}
	// the cache drifted from the primary data, which a read within the
	// limit does not notice
	d.primary["like"] = 9
	require.Equal(t, map[string]int64{"like": 3}, getCounts(t, uc, d, "t1"))
	require.EqualValues(t, 1, d.computes.Load(), "deltas up to the limit do not recompute")

	// the delta past the limit is added like any other
	require.NoError(t, uc.Touch(ctx, d.def, "t1", inc("like", 1), time.Now()))
	require.EqualValues(t, 1, d.computes.Load(), "a writer never recomputes an entry it can add to")
	stored, ok, err := store.Get(ctx, aggregate.Key("counts", "t1"))
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 4, stored.Deltas)

	// and the next read replaces the entry
	require.Equal(t, map[string]int64{"like": 9}, getCounts(t, uc, d, "t1"))
	require.EqualValues(t, 2, d.computes.Load())
	stored, ok, err = store.Get(ctx, aggregate.Key("counts", "t1"))
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 0, stored.Deltas)
	require.Equal(t, map[string]int64{"like": 9}, getCounts(t, uc, d, "t1"))
	require.EqualValues(t, 2, d.computes.Load())
}

func TestSeedLosesToAnExistingEntry(t *testing.T) {
	ctx := context.Background()
	uc := aggregate.New(storetest.NewInMemory(), 0, 0)
	d := newCountingDef()
	d.primary["like"] = 1

	// a seeded entry saves the first reader's computation
	before := time.Now()
	require.NoError(t, uc.Seed(ctx, d.def, "t1", aggregate.Counters{}))
	require.Empty(t, getCounts(t, uc, d, "t1"))
	require.EqualValues(t, 0, d.computes.Load())
	seeded, ok, err := uc.Cached(ctx, d.def, "t1")
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, seeded.AsOf.Before(before), "a seed is as of now")

	// and never replaces a computed one
	getCounts(t, uc, d, "t2")
	require.NoError(t, uc.Seed(ctx, d.def, "t2", aggregate.Counters{}))
	require.Equal(t, map[string]int64{"like": 1}, getCounts(t, uc, d, "t2"))
}

func TestInvalidateAllRecomputesOnBothPaths(t *testing.T) {
	ctx := context.Background()
	store := storetest.NewInMemory()
	uc := aggregate.New(store, 0, 0)
	d := newCountingDef()
	d.primary["like"] = 1

	getCounts(t, uc, d, "t1")
	getCounts(t, uc, d, "t2")
	require.EqualValues(t, 2, d.computes.Load())

	require.NoError(t, uc.InvalidateAll(ctx))
	d.primary["like"] = 9

	require.Equal(t, map[string]int64{"like": 9}, getCounts(t, uc, d, "t1"))
	require.NoError(t, uc.Touch(ctx, d.def, "t2", inc("like", 1), time.Now()))
	require.Equal(t, map[string]int64{"like": 9}, getCounts(t, uc, d, "t2"), "a write on a dropped entry recomputes rather than adds")
	require.EqualValues(t, 4, d.computes.Load())

	require.NoError(t, uc.Invalidate(ctx, "counts", "t1"))
	_, ok, err := uc.Cached(ctx, d.def, "t1")
	require.NoError(t, err)
	require.False(t, ok)
	_, ok, err = uc.Cached(ctx, d.def, "t2")
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, uc.InvalidateDefinition(ctx, "counts"))
	_, ok, err = uc.Cached(ctx, d.def, "t2")
	require.NoError(t, err)
	require.False(t, ok)
}
