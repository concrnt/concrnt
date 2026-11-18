package record

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/testutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
	uctrust "github.com/concrnt/concrnt/internal/usecase/trust"
)

// GetLocalAckEdges feeds the trust computation: only the valid
// acknowledgements local users make count, each with its target's domain.
func TestGetLocalAckEdges(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	testGetLocalAckEdges(t, db)
}

func testGetLocalAckEdges(t *testing.T, db *gorm.DB) {
	require.NoError(t, New(db).Migrate())

	ctx := context.Background()
	repo := New(db)
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	const (
		follow = "https://schema.example/follow.json"
		vouch  = "https://schema.example/vouch.json"
	)

	seq := 0
	commit := func(owner string, fn func(tx ucrecord.RepositoryTx, id string) error) {
		t.Helper()
		seq++
		id := "doc-" + string(rune('a'+seq))
		sd := repositorySignedDocument(t, concrnt.Document[any]{Author: owner, CreatedAt: now})
		withRepositoryTx(t, ctx, repo, id, "127.0.0.1", sd, owner, func(tx ucrecord.RepositoryTx) error {
			return fn(tx, id)
		})
	}
	entity := func(ccid, domain string) {
		t.Helper()
		commit(ccid, func(tx ucrecord.RepositoryTx, id string) error {
			_, err := repo.CreateEntity(ctx, tx, ccid, nil, domain, id, now)
			return err
		})
	}
	ack := func(from, to, schema string, at time.Time) {
		t.Helper()
		commit(from, func(tx ucrecord.RepositoryTx, id string) error {
			_, err := repo.Acknowledge(ctx, tx, id, from, to, schema, at)
			return err
		})
	}
	unack := func(from, to, schema string, at time.Time) {
		t.Helper()
		commit(from, func(tx ucrecord.RepositoryTx, id string) error {
			_, err := repo.UnAcknowledge(ctx, tx, id, from, to, schema, at)
			return err
		})
	}

	entity("con1alice", "example.com")
	entity("con1bob", "example.com")
	entity("con1moved", "elsewhere.example.org") // once local, now lives elsewhere
	entity("con1remote", "remote.example.net")

	ack("con1alice", "con1bob", follow, now)
	ack("con1alice", "con1remote", follow, now)
	ack("con1bob", "con1remote", vouch, now)
	ack("con1alice", "con1stranger", follow, now) // target entity not held
	ack("con1moved", "con1remote", follow, now)   // acker no longer local
	ack("con1bob", "con1alice", follow, now)
	unack("con1bob", "con1alice", follow, now.Add(time.Hour)) // withdrawn

	edges := func(schemas ...string) []uctrust.AckEdge {
		t.Helper()
		got, err := repo.GetLocalAckEdges(ctx, "example.com", schemas)
		require.NoError(t, err)
		sort.Slice(got, func(i, j int) bool {
			return got[i].From+got[i].To < got[j].From+got[j].To
		})
		return got
	}

	require.Equal(t, []uctrust.AckEdge{
		{From: "con1alice", To: "con1bob", ToDomain: "example.com"},
		{From: "con1alice", To: "con1remote", ToDomain: "remote.example.net"},
		{From: "con1bob", To: "con1remote", ToDomain: "remote.example.net"},
	}, edges())

	require.Equal(t, []uctrust.AckEdge{
		{From: "con1bob", To: "con1remote", ToDomain: "remote.example.net"},
	}, edges(vouch))

	require.Len(t, edges(follow, vouch), 3)
}
