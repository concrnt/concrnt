package record

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/testutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func testCommit(id, owner string) ucrecord.Commit {
	return ucrecord.Commit{ID: id, Document: "{}", Owner: owner}
}

func requireCommitLogged(t *testing.T, repo *Repository, id string, logged bool) {
	t.Helper()
	var count int64
	require.NoError(t, repo.db.Model(&models.CommitLog{}).Where("id = ?", id).Count(&count).Error)
	require.Equal(t, logged, count == 1, "commit %s in the log", id)
}

// A newer entity document replaces the stored one, an equal or older one
// stores nothing (not even its commit), and a redelivered commit changes
// nothing.
func TestApplyEntity(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := &Repository{db: db}

	ccid := "con1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	entity := func(id, domain string, createdAt time.Time) ucrecord.EntityCommit {
		return ucrecord.EntityCommit{Commit: testCommit(id, ccid), CCID: ccid, Domain: domain, CreatedAt: createdAt}
	}

	status, err := repo.ApplyEntity(ctx, entity("entity-1", "a.example.com", at))
	require.NoError(t, err)
	require.Equal(t, ucrecord.Applied, status)

	status, err = repo.ApplyEntity(ctx, entity("entity-1", "b.example.com", at.Add(time.Hour)))
	require.NoError(t, err)
	require.Equal(t, ucrecord.Duplicate, status)

	status, err = repo.ApplyEntity(ctx, entity("entity-0", "b.example.com", at))
	require.NoError(t, err)
	require.Equal(t, ucrecord.Stale, status)
	requireCommitLogged(t, repo, "entity-0", false)

	stored, err := repo.GetEntityByCCID(ctx, ccid)
	require.NoError(t, err)
	require.Equal(t, "a.example.com", stored.Domain)

	status, err = repo.ApplyEntity(ctx, entity("entity-2", "b.example.com", at.Add(time.Hour)))
	require.NoError(t, err)
	require.Equal(t, ucrecord.Applied, status)

	stored, err = repo.GetEntityByCCID(ctx, ccid)
	require.NoError(t, err)
	require.Equal(t, "b.example.com", stored.Domain)
}
