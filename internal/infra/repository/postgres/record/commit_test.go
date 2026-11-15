package record

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/testutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// Only gc-flagged commits below the cutoff id are candidates; they page by
// id and carry everything a backup line needs.
func TestGcCandidates(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := New(db)

	seed := func(id string, gc bool) {
		t.Helper()
		require.NoError(t, db.Create(&models.CommitLog{ID: id, IP: "10.0.0.1", Document: `{"n":"` + id + `"}`, Proof: `{"type":"none"}`, Owner: "con1gcowner", GcCandidate: gc}).Error)
	}
	seed("a1", true)
	seed("a2", false)
	seed("a3", true)
	seed("b1", true)

	count, err := repo.CountGcCandidates(ctx, "b0")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)

	page, err := repo.ListGcCandidates(ctx, "b0", "", 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, "a1", page[0].ID)
	require.Equal(t, "10.0.0.1", page[0].IP)
	require.Equal(t, `{"n":"a1"}`, page[0].Document)
	require.Equal(t, "none", page[0].Proof.Type)
	require.Equal(t, "con1gcowner", page[0].Owner)
	require.False(t, page[0].CDate.IsZero())

	page, err = repo.ListGcCandidates(ctx, "b0", "a1", 10)
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, "a3", page[0].ID)
}

// Missing ids are ignored and the number of commit logs actually removed is
// reported.
func TestDeleteCommitLogs(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := New(db)

	require.NoError(t, db.Create(&models.CommitLog{ID: "a1", Document: "{}", Proof: "{}"}).Error)
	require.NoError(t, db.Create(&models.CommitLog{ID: "a2", Document: "{}", Proof: "{}"}).Error)

	deleted, err := repo.DeleteCommitLogs(ctx, []string{"a1", "missing"})
	require.NoError(t, err)
	require.Equal(t, 1, deleted)

	var left []string
	require.NoError(t, db.Model(&models.CommitLog{}).Pluck("id", &left).Error)
	require.Equal(t, []string{"a2"}, left)
}

// Pages are ordered by id; FromID and UntilID are inclusive, AfterID is
// exclusive, and Owner restricts the page to one owner's commits.
func TestListCommitLogs(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := New(db)

	for _, cl := range []models.CommitLog{
		{ID: "a1", Owner: "con1alice"},
		{ID: "a2", Owner: "con1bob"},
		{ID: "a3", Owner: "con1alice"},
		{ID: "a4", Owner: "con1alice"},
	} {
		cl.Document, cl.Proof = "{}", "{}"
		require.NoError(t, db.Create(&cl).Error)
	}

	ids := func(page ucrecord.CommitLogPage) []string {
		t.Helper()
		logs, err := repo.ListCommitLogs(ctx, page)
		require.NoError(t, err)
		out := []string{}
		for _, cl := range logs {
			out = append(out, cl.ID)
		}
		return out
	}

	require.Equal(t, []string{"a1", "a2", "a3", "a4"}, ids(ucrecord.CommitLogPage{}))
	require.Equal(t, []string{"a2", "a3"}, ids(ucrecord.CommitLogPage{FromID: "a2", UntilID: "a3"}))
	require.Equal(t, []string{"a3", "a4"}, ids(ucrecord.CommitLogPage{AfterID: "a2"}))
	require.Equal(t, []string{"a1", "a3"}, ids(ucrecord.CommitLogPage{Owner: "con1alice", Limit: 2}))
}
