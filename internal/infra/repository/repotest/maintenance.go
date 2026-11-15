package repotest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/cdid"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// RunMaintenanceSuite runs the contract tests of the offline-tooling methods
// of record.Repository (conctl gc-commitlog / dump-commitlog). open must
// return a fresh, isolated backend per call.
func RunMaintenanceSuite(t *testing.T, open func(t *testing.T) Backend) {
	t.Run("GcCandidates", func(t *testing.T) { testMaintenanceGcCandidates(t, open(t)) })
	t.Run("DeleteCommitLogsTakesDependents", func(t *testing.T) { testMaintenanceDeleteCommitLogsTakesDependents(t, open(t)) })
	t.Run("ListCommitLogs", func(t *testing.T) { testMaintenanceListCommitLogs(t, open(t)) })
}

// seedMaintenanceCommit commits a record whose commit log id is the
// time-kind CDID of createdAt, the shape gc cutoffs compare against.
func seedMaintenanceCommit(t *testing.T, ctx context.Context, repo record.Repository, owner string, n byte, createdAt time.Time) string {
	t.Helper()
	id := cdid.New([10]byte{n}, createdAt).String()
	rc := recordCommit(t, id, "cckv://"+owner+"/"+id, createdAt)
	rc.IP = "10.0.0." + string(rune('1'+n))
	outcome, err := repo.ApplyRecord(ctx, rc)
	require.NoError(t, err)
	require.Equal(t, record.Applied, outcome.Status)
	return id
}

// flagForGc flags commits the way a delete does.
func flagForGc(t *testing.T, ctx context.Context, repo record.Repository, ids ...string) {
	t.Helper()
	outcome, err := repo.ApplyDelete(ctx, record.DeleteCommit{GcCommitIDs: ids})
	require.NoError(t, err)
	require.Equal(t, record.Applied, outcome.Status)
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
	flagForGc(t, ctx, repo, old1, old2, recent)
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
	require.False(t, commitExists(t, ctx, repo, old1))
	require.Nil(t, resolve(t, ctx, repo, "cckv://"+owner+"/"+old1), "the record went with its commit")
	require.Nil(t, resolve(t, ctx, repo, ccfsOf(owner, old1)))
	require.True(t, commitExists(t, ctx, repo, oldKept))
	require.NotNil(t, resolve(t, ctx, repo, "cckv://"+owner+"/"+recent))

	count, err = repo.CountGcCandidates(ctx, cutoff)
	require.NoError(t, err)
	require.Zero(t, count)

	deleted, err = repo.DeleteCommitLogs(ctx, []string{old1})
	require.NoError(t, err)
	require.Zero(t, deleted, "deleting again is harmless")

	deleted, err = repo.DeleteCommitLogs(ctx, nil)
	require.NoError(t, err)
	require.Zero(t, deleted)
}

// Everything anchored to a commit goes with it: an association, an ack
// state, an entity, and a record with its key, which ends the key's
// incarnation like any other removal.
func testMaintenanceDeleteCommitLogsTakesDependents(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	at := time.Date(2026, 9, 21, 9, 0, 0, 0, time.UTC)
	like := "https://schema.example/like.json"
	key := "cckv://con1owner/gc/post"
	other := "cckv://con1owner/gc/other"
	putRecord(t, ctx, repo, "gc-post", key, at)
	putRecord(t, ctx, repo, "gc-other", other, at)
	putAssociation(t, ctx, repo, key, associationFixture{"gc-post-like", "con1alice", like, nil, at})
	putAssociation(t, ctx, repo, other, associationFixture{"gc-other-like-1", "con1alice", like, nil, at})
	putAssociation(t, ctx, repo, other, associationFixture{"gc-other-like-2", "con1bob", like, nil, at.Add(time.Minute)})
	outcome, err := repo.ApplyAck(ctx, ackCommit(t, "gc-ack", record.AckerSide, "con1alice", "con1owner", true, at))
	require.NoError(t, err)
	require.Equal(t, record.Applied, outcome.Status)
	status, err := repo.ApplyEntity(ctx, record.EntityCommit{
		Commit: record.Commit{ID: "gc-entity", Document: "{}", Owner: "con1gone"}, CCID: "con1gone", Domain: "example.com", CreatedAt: at,
	})
	require.NoError(t, err)
	require.Equal(t, record.Applied, status)

	deleted, err := repo.DeleteCommitLogs(ctx, []string{"gc-post", "gc-other-like-1", "gc-ack", "gc-entity"})
	require.NoError(t, err)
	require.Equal(t, 4, deleted)

	require.Nil(t, resolve(t, ctx, repo, key))
	rows, err := repo.GetAssociatedRecords(ctx, other, "", "", "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Equal(t, []string{"gc-other-like-2"}, rowIDs(t, rows))
	rows, err = repo.GetAcknowledgeRecords(ctx, "con1alice", "", "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Empty(t, rows)
	_, err = repo.GetEntityByCCID(ctx, "con1gone")
	require.True(t, errors.Is(err, domain.ErrNotFound), "expected not found, got %v", err)

	// the key's incarnation ended: a record written to it starts clean
	_, recreated := putRecord(t, ctx, repo, "gc-post-again", key, at.Add(time.Hour))
	require.True(t, recreated.FreshKey)
	rows, err = repo.GetAssociatedRecords(ctx, key, "", "", "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Empty(t, rows)
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
