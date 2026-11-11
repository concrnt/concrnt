package record

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	"github.com/concrnt/concrnt/internal/testutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// Between the removal of a key and the end of its Cleanup the associations
// of the ended incarnation are still stored. They must not take the place
// of live rows in a bounded page of the key's next incarnation.
func TestAssociationPageSkipsEndedIncarnation(t *testing.T) {
	ctx := context.Background()
	client := testutil.CreateFirestore(t)
	repo := New(client)
	stored := func(collection string) int {
		t.Helper()
		snaps, err := client.Collection(collection).Documents(ctx).GetAll()
		require.NoError(t, err)
		return len(snaps)
	}

	at := time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC)
	key := "cckv://con1owner/timeline/reused"
	apply := func(id string, createdAt time.Time, author string) {
		t.Helper()
		ac := budgetAssociation(id, key, author, createdAt)
		ac.Unique = id
		status, err := repo.ApplyAssociation(ctx, ac)
		require.NoError(t, err)
		require.Equal(t, ucrecord.Applied, status)
	}
	listed := func(limit int, order string) []string {
		t.Helper()
		rows, err := repo.GetAssociatedRecords(ctx, key, "", "", "", nil, nil, limit, order)
		require.NoError(t, err)
		ids := make([]string, 0, len(rows))
		for _, row := range rows {
			parsed, err := concrnt.ParseCCURI(*row.Row.CCFS)
			require.NoError(t, err)
			ids = append(ids, parsed.CDID)
		}
		return ids
	}

	_, err := repo.ApplyRecord(ctx, budgetRecord("reused-1", key, at))
	require.NoError(t, err)
	for i := range 5 {
		apply(fmt.Sprintf("old-%d", i), at.Add(time.Duration(2*i)*time.Minute), "con1alice")
	}
	outcome, err := repo.ApplyDelete(ctx, ucrecord.DeleteCommit{Removals: []ucrecord.Removal{{Kind: ucrecord.RemoveKey, URI: key}}})
	require.NoError(t, err)
	require.Len(t, outcome.Cleanups, 1)

	_, err = repo.ApplyRecord(ctx, budgetRecord("reused-2", key, at.Add(time.Hour)))
	require.NoError(t, err)
	// interleaved with the old ones in createdAt order
	for i := range 3 {
		apply(fmt.Sprintf("new-%d", i), at.Add(time.Duration(2*i+1)*time.Minute), "con1bob")
	}

	require.Equal(t, []string{"new-0", "new-1"}, listed(2, "asc"))
	require.Equal(t, []string{"new-2", "new-1", "new-0"}, listed(3, "desc"))
	require.Equal(t, []string{"new-0", "new-1", "new-2"}, listed(0, "asc"))
	require.Equal(t, 8, stored(fs.ColAssociations), "the ended incarnation's associations wait for Cleanup")
	require.Equal(t, 8, stored(fs.ColAssocUniques))

	token := outcome.Cleanups[0]
	for token != "" {
		token, err = repo.Cleanup(ctx, token)
		require.NoError(t, err)
	}
	require.Equal(t, []string{"new-0", "new-1"}, listed(2, "asc"))
	require.Equal(t, 3, stored(fs.ColAssociations), "Cleanup removed them physically, with their markers")
	require.Equal(t, 3, stored(fs.ColAssocUniques))

	_, err = repo.Cleanup(ctx, "not-a-token")
	require.Error(t, err, "a token of another backend is not silently done")
}

// A Cleanup step is bounded: an incarnation with more associations than a
// page takes several, the token staying valid in between, and ends with
// nothing of it stored.
func TestCleanupPages(t *testing.T) {
	ctx := context.Background()
	client := testutil.CreateFirestore(t)
	repo := New(client)

	at := time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC)
	key := "cckv://con1owner/timeline/popular"
	_, err := repo.ApplyRecord(ctx, budgetRecord("popular", key, at))
	require.NoError(t, err)
	for i := range cleanupPage + 20 {
		status, err := repo.ApplyAssociation(ctx, budgetAssociation(fmt.Sprintf("popular-like-%03d", i), key, fmt.Sprintf("con1author%03d", i), at))
		require.NoError(t, err)
		require.Equal(t, ucrecord.Applied, status)
	}
	outcome, err := repo.ApplyDelete(ctx, ucrecord.DeleteCommit{Removals: []ucrecord.Removal{{Kind: ucrecord.RemoveKey, URI: key}}})
	require.NoError(t, err)
	require.Len(t, outcome.Cleanups, 1)

	next, err := repo.Cleanup(ctx, outcome.Cleanups[0])
	require.NoError(t, err)
	require.NotEmpty(t, next, "a full page leaves work for another step")
	snaps, err := client.Collection(fs.ColAssociations).Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, snaps, 20)

	next, err = repo.Cleanup(ctx, next)
	require.NoError(t, err)
	require.Empty(t, next)
	for _, collection := range []string{fs.ColAssociations, fs.ColAssocUniques} {
		snaps, err := client.Collection(collection).Documents(ctx).GetAll()
		require.NoError(t, err)
		require.Empty(t, snaps, collection)
	}
}

// Two documents of the same logical association racing on a target: the
// uniqueness marker is part of each transaction's read, so exactly one is
// stored.
func TestAssociationUniquenessRace(t *testing.T) {
	ctx := context.Background()
	repo := New(testutil.CreateFirestore(t))

	at := time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC)
	key := "cckv://con1owner/timeline/contended"
	_, err := repo.ApplyRecord(ctx, budgetRecord("contended", key, at))
	require.NoError(t, err)

	// few rounds: the emulator resolves a contended transaction slowly
	const rounds = 3
	for round := range rounds {
		const writers = 2
		statuses := make([]ucrecord.ApplyStatus, writers)
		errs := make([]error, writers)
		var wg sync.WaitGroup
		for i := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ac := budgetAssociation(fmt.Sprintf("race-%d-%d", round, i), key, "con1alice", at)
				ac.Unique = fmt.Sprintf("race-%d", round)
				statuses[i], errs[i] = repo.ApplyAssociation(ctx, ac)
			}()
		}
		wg.Wait()
		applied := 0
		for i := range writers {
			require.NoError(t, errs[i])
			if statuses[i] == ucrecord.Applied {
				applied++
			} else {
				require.Equal(t, ucrecord.Stale, statuses[i])
			}
		}
		require.Equal(t, 1, applied, "round %d", round)
	}
	rows, err := repo.GetAssociatedRecords(ctx, key, "", "", "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Len(t, rows, rounds)
}
