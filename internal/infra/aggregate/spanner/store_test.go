package spanner_test

import (
	"context"
	"os"
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
// folds them into the entry row. The entry reads the same before and after,
// and the rows are gone with the entry.
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
	require.Zero(t, countDeltaRows(t, client), "the read folded the delta rows into the entry")
	read(many)

	add(many, many+few)
	read(many + few)
	require.EqualValues(t, few, countDeltaRows(t, client))

	require.NoError(t, s.Invalidate(ctx, "counts:a"))
	require.Zero(t, countDeltaRows(t, client), "the delta rows go with their entry")
}
