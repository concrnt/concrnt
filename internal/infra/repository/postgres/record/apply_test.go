package record

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/domain"
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

// A record takes its key when it is newer than the version there, reports
// what it replaced, and an equal or older one stores nothing.
func TestApplyRecord(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := &Repository{db: db}

	owner := "con1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
	key := "cckv://" + owner + "/posts/a"
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	record := func(id string, createdAt time.Time) ucrecord.RecordCommit {
		return ucrecord.RecordCommit{Commit: testCommit(id, owner), Key: key, Author: owner, Schema: "https://schema.example.com/post.json", CreatedAt: createdAt}
	}

	outcome, err := repo.ApplyRecord(ctx, record("record-1", at))
	require.NoError(t, err)
	require.Equal(t, ucrecord.RecordOutcome{Status: ucrecord.Applied, FreshKey: true}, outcome)

	outcome, err = repo.ApplyRecord(ctx, record("record-1", at.Add(time.Hour)))
	require.NoError(t, err)
	require.Equal(t, ucrecord.RecordOutcome{Status: ucrecord.Duplicate}, outcome)

	outcome, err = repo.ApplyRecord(ctx, record("record-0", at))
	require.NoError(t, err)
	require.Equal(t, ucrecord.RecordOutcome{Status: ucrecord.Stale}, outcome)
	requireCommitLogged(t, repo, "record-0", false)

	outcome, err = repo.ApplyRecord(ctx, record("record-2", at.Add(time.Hour)))
	require.NoError(t, err)
	require.Equal(t, ucrecord.RecordOutcome{Status: ucrecord.Applied, PreviousID: "record-1"}, outcome)

	sd, err := repo.GetSignedDocument(ctx, key)
	require.NoError(t, err)
	require.Equal(t, "ccfs://"+owner+"/concrnt/record-2", *sd.CCFS)

	candidates, err := repo.ListGcCandidates(ctx, "zzzz", "", 10)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, "record-1", candidates[0].ID)
}

// An association needs its target key to exist, and a second one with the
// same unique value stores nothing.
func TestApplyAssociation(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := &Repository{db: db}

	owner := "con1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
	target := "cckv://" + owner + "/posts/a"
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	association := func(id, unique string) ucrecord.AssociationCommit {
		return ucrecord.AssociationCommit{Commit: testCommit(id, owner), Target: target, Author: owner, Schema: "https://schema.example.com/like.json", Unique: unique, CreatedAt: at}
	}

	_, err := repo.ApplyAssociation(ctx, association("association-1", "u1"))
	require.ErrorIs(t, err, domain.ErrNotFound)
	requireCommitLogged(t, repo, "association-1", false)

	_, err = repo.ApplyRecord(ctx, ucrecord.RecordCommit{Commit: testCommit("record-1", owner), Key: target, Author: owner, CreatedAt: at})
	require.NoError(t, err)

	status, err := repo.ApplyAssociation(ctx, association("association-1", "u1"))
	require.NoError(t, err)
	require.Equal(t, ucrecord.Applied, status)

	status, err = repo.ApplyAssociation(ctx, association("association-1", "u1"))
	require.NoError(t, err)
	require.Equal(t, ucrecord.Duplicate, status)

	status, err = repo.ApplyAssociation(ctx, association("association-2", "u1"))
	require.NoError(t, err)
	require.Equal(t, ucrecord.Stale, status)
	requireCommitLogged(t, repo, "association-2", false)

	rows, err := repo.GetAssociatedRecords(ctx, target, "", "", "", nil, nil, 10, "desc")
	require.NoError(t, err)
	require.Len(t, rows, 1)
}
