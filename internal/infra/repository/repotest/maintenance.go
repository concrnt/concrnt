package repotest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/cdid"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// RunMaintenanceSuite runs the contract tests of the offline-tooling methods
// of record.Repository (conctl gc-commitlog / dump-commitlog). open must
// return a fresh, isolated backend per call.
func RunMaintenanceSuite(t *testing.T, open func(t *testing.T) Backend) {
	t.Run("GcCandidates", func(t *testing.T) { testMaintenanceGcCandidates(t, open(t)) })
	t.Run("ListCommitLogs", func(t *testing.T) { testMaintenanceListCommitLogs(t, open(t)) })
}

// seedMaintenanceCommit commits a record whose commit log id is the
// time-kind CDID of createdAt, the shape gc cutoffs compare against.
func seedMaintenanceCommit(t *testing.T, ctx context.Context, repo record.Repository, owner string, n byte, createdAt time.Time) string {
	t.Helper()
	id := cdid.New([10]byte{n}, createdAt).String()
	key := "cckv://" + owner + "/" + id
	sd := SignedDocument(t, concrnt.Document[map[string]string]{
		Kind:      "record",
		Key:       key,
		Value:     map[string]string{"id": id},
		Author:    owner,
		Schema:    "https://schema.example/post.json",
		CreatedAt: createdAt,
	})
	WithCommit(t, ctx, repo, id, "10.0.0."+string(rune('1'+n)), sd, owner, func(tx record.RepositoryTx) error {
		_, err := repo.CreateRecord(ctx, tx, id, key, owner, owner, "https://schema.example/post.json", nil, nil, []string{}, nil, createdAt)
		return err
	})
	return id
}

// Only gc-flagged commits below the cutoff id are candidates; they page by
// id, carry everything a backup line needs, and deleting them takes the
// dependent record along.
func testMaintenanceGcCandidates(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record
	owner := "con1gcowner"

	base := time.Date(2026, 9, 21, 9, 0, 0, 0, time.UTC)
	old1 := seedMaintenanceCommit(t, ctx, repo, owner, 0, base)
	old2 := seedMaintenanceCommit(t, ctx, repo, owner, 1, base.Add(time.Hour))
	recent := seedMaintenanceCommit(t, ctx, repo, owner, 2, base.Add(3*time.Hour))
	oldKept := seedMaintenanceCommit(t, ctx, repo, owner, 3, base.Add(30*time.Minute))
	InTx(t, ctx, repo, func(tx record.RepositoryTx) {
		for _, id := range []string{old1, old2, recent} {
			require.NoError(t, repo.MarkCommitLogGcCandidate(ctx, tx, id))
		}
	})
	cutoff := cdid.New([10]byte{}, base.Add(2*time.Hour)).String()

	count, err := repo.CountGcCandidates(ctx, cutoff)
	require.NoError(t, err)
	require.Equal(t, int64(2), count, "the recent flag and the unflagged old commit are not candidates")

	page, err := repo.ListGcCandidates(ctx, cutoff, "", 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, old1, page[0].ID)
	require.Equal(t, owner, page[0].Owner)
	require.Equal(t, "10.0.0.1", page[0].IP)
	require.False(t, page[0].CDate.IsZero())
	require.Contains(t, page[0].Document, old1)
	require.Equal(t, concrnt.ProofTypeNone, page[0].Proof.Type)

	page, err = repo.ListGcCandidates(ctx, cutoff, old1, 10)
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, old2, page[0].ID)

	page, err = repo.ListGcCandidates(ctx, cutoff, old2, 10)
	require.NoError(t, err)
	require.Empty(t, page)

	deleted, err := repo.DeleteCommitLogs(ctx, []string{old1, old2, "missing"})
	require.NoError(t, err)
	require.Equal(t, 2, deleted)
	require.False(t, commitExists(t, ctx, be.Inspect, old1))
	require.False(t, recordExists(t, ctx, be.Inspect, old1))
	require.True(t, commitExists(t, ctx, be.Inspect, oldKept))
	require.True(t, recordExists(t, ctx, be.Inspect, recent))

	count, err = repo.CountGcCandidates(ctx, cutoff)
	require.NoError(t, err)
	require.Zero(t, count)

	deleted, err = repo.DeleteCommitLogs(ctx, nil)
	require.NoError(t, err)
	require.Zero(t, deleted)
}

// ListCommitLogs pages the whole log in id order with inclusive from/until,
// an exclusive after cursor and an owner filter.
func testMaintenanceListCommitLogs(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	base := time.Date(2026, 9, 21, 9, 0, 0, 0, time.UTC)
	a1 := seedMaintenanceCommit(t, ctx, repo, "con1aaaa", 0, base)
	b1 := seedMaintenanceCommit(t, ctx, repo, "con1bbbb", 1, base.Add(time.Minute))
	a2 := seedMaintenanceCommit(t, ctx, repo, "con1aaaa", 2, base.Add(2*time.Minute))

	ids := func(entries []record.CommitLogEntry) []string {
		out := make([]string, 0, len(entries))
		for _, e := range entries {
			out = append(out, e.ID)
		}
		return out
	}

	all, err := repo.ListCommitLogs(ctx, record.CommitLogPage{Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []string{a1, b1, a2}, ids(all))
	require.Equal(t, "con1aaaa", all[0].Owner)
	require.Equal(t, concrnt.ProofTypeNone, all[0].Proof.Type)
	require.False(t, all[0].CDate.IsZero())

	page, err := repo.ListCommitLogs(ctx, record.CommitLogPage{Limit: 2})
	require.NoError(t, err)
	require.Equal(t, []string{a1, b1}, ids(page))
	page, err = repo.ListCommitLogs(ctx, record.CommitLogPage{AfterID: page[1].ID, Limit: 2})
	require.NoError(t, err)
	require.Equal(t, []string{a2}, ids(page))

	page, err = repo.ListCommitLogs(ctx, record.CommitLogPage{FromID: b1, UntilID: b1, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []string{b1}, ids(page))

	page, err = repo.ListCommitLogs(ctx, record.CommitLogPage{Owner: "con1aaaa", Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []string{a1, a2}, ids(page))
}
