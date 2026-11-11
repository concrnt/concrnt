package spanner_test

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gspanner "cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/aggregate/spanner"
	"github.com/concrnt/concrnt/internal/infra/aggregate/storetest"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testutil.PurgeSpanner()
	os.Exit(code)
}

func TestSpannerStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) aggregate.Store {
		client, _ := testutil.CreateSpanner(t)
		return spanner.New(client)
	})
}

func countDeltaRows(t *testing.T, client *gspanner.Client) int64 {
	t.Helper()
	row, err := client.Single().Query(context.Background(), gspanner.Statement{SQL: "SELECT COUNT(*) FROM aggregate_deltas"}).Next()
	require.NoError(t, err)
	var n int64
	require.NoError(t, row.Column(0, &n))
	return n
}

// An Add is a delta row; a Get that finds more of them than it tolerates
// replaces them by one row holding their sum. The entry reads the same
// before and after (the sum counts for the Adds it stands for), and the
// rows are gone with the entry.
func TestSpannerStoreCompactsDeltaRows(t *testing.T) {
	ctx := context.Background()
	client, _ := testutil.CreateSpanner(t)
	s := spanner.New(client)

	asOf := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	won, err := s.PutIfAbsent(ctx, "counts:a", aggregate.Counters{Counts: map[string]int64{"x": 1}, AsOf: asOf}, time.Hour)
	require.NoError(t, err)
	require.True(t, won)

	const few, many = 10, 100
	read := func(adds int64) {
		t.Helper()
		got, ok, err := s.Get(ctx, "counts:a")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, map[string]int64{"x": 1 + adds}, got.Counts)
		require.Equal(t, map[string]int64{"x": -adds}, got.Minima)
		require.Len(t, got.Members, int(adds))
		require.Equal(t, adds, got.Deltas)
		require.True(t, got.AsOf.Equal(asOf))
	}
	add := func(from, to int64) {
		t.Helper()
		for i := from; i < to; i++ {
			ok, err := s.Add(ctx, "counts:a", aggregate.Delta{Inc: map[string]int64{"x": 1}, Min: map[string]int64{"x": -i - 1}, AddMembers: []string{time.Duration(i).String()}})
			require.NoError(t, err)
			require.True(t, ok)
		}
	}

	add(0, few)
	read(few)
	require.EqualValues(t, few, countDeltaRows(t, client), "a few delta rows are left alone")

	add(few, many)
	read(many)
	require.EqualValues(t, 1, countDeltaRows(t, client), "the read folded the delta rows into one")
	read(many)

	add(many, many+few)
	read(many + few)
	require.EqualValues(t, 1+few, countDeltaRows(t, client))

	// the sum is a delta row like any other: the next fold takes it along
	add(many+few, 2*many)
	read(2 * many)
	require.EqualValues(t, 1, countDeltaRows(t, client))
	read(2 * many)

	require.NoError(t, s.Invalidate(ctx, "counts:a"))
	require.Zero(t, countDeltaRows(t, client), "the delta rows go with their entry")
}

// Two folds of the same delta rows, as two server processes reading one
// entry at the same moment start them: the second finds the rows taken and
// changes nothing, so no Add is counted twice.
func TestSpannerStoreConcurrentFoldsCountOnce(t *testing.T) {
	ctx := context.Background()
	client, _ := testutil.CreateSpanner(t)
	// one store per process: a store folds a key once at a time by itself
	stores := []*spanner.Store{spanner.New(client), spanner.New(client), spanner.New(client), spanner.New(client)}

	won, err := stores[0].PutIfAbsent(ctx, "counts:folded", aggregate.Counters{AsOf: time.Now()}, time.Hour)
	require.NoError(t, err)
	require.True(t, won)

	const adds = 150
	for i := range adds {
		ok, err := stores[0].Add(ctx, "counts:folded", aggregate.Delta{Inc: map[string]int64{"x": 1}, AddMembers: []string{time.Duration(i).String()}})
		require.NoError(t, err)
		require.True(t, ok)
	}

	for round := range 3 {
		var wg sync.WaitGroup
		for _, s := range stores {
			wg.Add(1)
			go func() {
				defer wg.Done()
				got, ok, err := s.Get(ctx, "counts:folded")
				if err != nil || !ok || got.Counts["x"] != adds || got.Deltas != adds || len(got.Members) != adds {
					t.Errorf("round %d: %+v, %v, %v", round, got, ok, err)
				}
			}()
		}
		wg.Wait()
	}
	require.EqualValues(t, 1, countDeltaRows(t, client))
}

// PutIfAbsent with several callers at once, as the readers that found the
// entry missing offer what they computed: exactly one stores, the others
// are told so, and none of them waits for another.
func TestSpannerStoreConcurrentPutIfAbsentHasOneWinner(t *testing.T) {
	ctx := context.Background()
	client, _ := testutil.CreateSpanner(t)
	s := spanner.New(client)

	const callers = 16
	var winners atomic.Int32
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			won, err := s.PutIfAbsent(ctx, "counts:raced", aggregate.Counters{Counts: map[string]int64{"x": int64(i)}, AsOf: time.Now()}, time.Hour)
			if err != nil {
				t.Errorf("caller %d: %v", i, err)
			}
			if won {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, winners.Load())

	_, ok, err := s.Get(ctx, "counts:raced")
	require.NoError(t, err)
	require.True(t, ok)
}
