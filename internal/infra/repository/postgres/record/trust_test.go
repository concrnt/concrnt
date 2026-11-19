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

// The trust computation's storage: seeds are a flag on the entity, the edges
// are the valid acknowledgements local users make, and the result is stored
// per entity and added up per domain.
func TestTrustRepository(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	testTrustRepository(t, db)
}

func testTrustRepository(t *testing.T, db *gorm.DB) {
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

	// seeds
	seeds, err := repo.GetTrustSeeds(ctx)
	require.NoError(t, err)
	require.Empty(t, seeds)

	found, err := repo.SetTrustSeed(ctx, "con1alice", true)
	require.NoError(t, err)
	require.True(t, found)
	found, err = repo.SetTrustSeed(ctx, "con1stranger", true)
	require.NoError(t, err)
	require.False(t, found, "an entity this server does not hold cannot be a seed")

	seeds, err = repo.GetTrustSeeds(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"con1alice"}, seeds)

	// scores
	stored := func() map[string]float64 {
		t.Helper()
		var rows []Entity
		require.NoError(t, db.Where("trust > 0").Find(&rows).Error)
		got := map[string]float64{}
		for _, row := range rows {
			got[row.ID] = row.Trust
		}
		return got
	}

	require.NoError(t, repo.SaveEntityTrust(ctx, map[string]float64{
		"con1alice":    1,
		"con1bob":      0.5,
		"con1remote":   0.25,
		"con1moved":    0.125,
		"con1stranger": 0.75, // no entity row: nowhere to store it
	}))
	require.Equal(t, map[string]float64{"con1alice": 1, "con1bob": 0.5, "con1remote": 0.25, "con1moved": 0.125}, stored())

	totals, err := repo.GetDomainTrust(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]float64{
		"example.com":           1.5,
		"remote.example.net":    0.25,
		"elsewhere.example.org": 0.125,
	}, totals)

	// a newer entity document must leave the seed flag and the score alone
	commit("con1alice", func(tx ucrecord.RepositoryTx, id string) error {
		applied, err := repo.CreateEntity(ctx, tx, "con1alice", nil, "example.com", id, now.Add(time.Hour))
		require.True(t, applied)
		return err
	})
	seeds, err = repo.GetTrustSeeds(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"con1alice"}, seeds)
	require.Equal(t, 1.0, stored()["con1alice"])

	// the next result replaces the last one: entities it leaves out drop to 0
	require.NoError(t, repo.SaveEntityTrust(ctx, map[string]float64{"con1alice": 1, "con1remote": 0.5}))
	require.Equal(t, map[string]float64{"con1alice": 1, "con1remote": 0.5}, stored())

	require.NoError(t, repo.SaveEntityTrust(ctx, nil))
	require.Empty(t, stored())
	totals, err = repo.GetDomainTrust(ctx)
	require.NoError(t, err)
	require.Empty(t, totals)

	found, err = repo.SetTrustSeed(ctx, "con1alice", false)
	require.NoError(t, err)
	require.True(t, found)
	seeds, err = repo.GetTrustSeeds(ctx)
	require.NoError(t, err)
	require.Empty(t, seeds)
}
