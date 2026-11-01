package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/concrnt/concrnt/schemas"
)

const repairFQDN = "example.com"

// legacyState seeds a database the way the multi-owner era left it: commit
// logs without an owner, a commit_owners join table, raw acks recorded on
// both the acker's and the target's side, and entities without created_at.
type legacyState struct {
	alice, bob, carol string // alice and bob are local, carol is remote
	ackAliceBob       string // same-server ack, co-owned in commit_owners
	ackCarolBob       string // receiving-side copy of a remote ack
	ackAliceCarol     string // outbound ack to a remote target
	deleteAlice       string // delete with no commit_owners row at all
	entityAt          time.Time
	ackAt             time.Time
}

func seedLegacyState(t *testing.T, db *gorm.DB) legacyState {
	t.Helper()
	ctx := context.Background()

	st := legacyState{
		alice:    "con1alice",
		bob:      "con1bob",
		carol:    "con1carol",
		entityAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ackAt:    time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
	}

	require.NoError(t, db.Exec(`CREATE TABLE commit_owners (commit_log_id text NOT NULL, owner text NOT NULL, PRIMARY KEY (commit_log_id, owner))`).Error)

	commit := func(id string, doc any, owners ...string) {
		t.Helper()
		docBytes, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.WithContext(ctx).Create(&postgres.CommitLog{ID: id, IP: "127.0.0.1", Document: string(docBytes), Proof: `{"type":"none"}`}).Error)
		for _, owner := range owners {
			require.NoError(t, db.Exec(`INSERT INTO commit_owners (commit_log_id, owner) VALUES (?, ?)`, id, owner).Error)
		}
	}
	entity := func(ccid, host string) {
		t.Helper()
		id := "entity-" + ccid
		commit(id, concrnt.Document[schemas.Entity]{Kind: "entity", Value: schemas.Entity{Domain: host}, Author: ccid, CreatedAt: st.entityAt}, ccid)
		require.NoError(t, db.WithContext(ctx).Create(&postgres.Entity{ID: ccid, Domain: host, DocumentID: id}).Error)
	}
	ack := func(id, from, to string, owners ...string) {
		t.Helper()
		associate := "cckv://" + to
		commit(id, concrnt.Document[map[string]string]{Kind: "ack", Value: map[string]string{"context": "follow"}, Author: from, Schema: "https://example.com/follow.json", CreatedAt: st.ackAt, Associate: &associate}, owners...)
		require.NoError(t, db.WithContext(ctx).Create(&postgres.Ack{From: from, To: to, Schema: "https://example.com/follow.json", DocumentID: id, Valid: true, CreatedAt: st.ackAt}).Error)
	}

	entity(st.alice, repairFQDN)
	entity(st.bob, repairFQDN)
	entity(st.carol, "remote.example.net")

	st.ackAliceBob = "ack-alice-bob"
	ack(st.ackAliceBob, st.alice, st.bob, st.alice, st.bob)
	st.ackCarolBob = "ack-carol-bob"
	ack(st.ackCarolBob, st.carol, st.bob, st.bob)
	st.ackAliceCarol = "ack-alice-carol"
	ack(st.ackAliceCarol, st.alice, st.carol, st.alice)

	st.deleteAlice = "delete-alice"
	commit(st.deleteAlice, concrnt.Document[schemas.Delete]{Kind: "delete", Value: schemas.Delete("cckv://" + st.alice + "/posts/*"), Author: st.alice, Schema: "https://schema.concrnt.net/delete.json", CreatedAt: st.ackAt})

	// the owner column did not exist before v1.11, so the startup migration
	// adds it as NULL on every pre-existing row — not ''
	require.NoError(t, db.Exec(`UPDATE commit_logs SET owner = NULL`).Error)

	return st
}

func TestRepairSingleOwner(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)
	ctx := context.Background()

	st := seedLegacyState(t, db)

	snapshot := func() (int64, int64, int64) {
		var commits, acks, ackeds int64
		require.NoError(t, db.Model(&postgres.CommitLog{}).Where("owner <> '' AND owner IS NOT NULL").Count(&commits).Error)
		require.NoError(t, db.Model(&postgres.Ack{}).Count(&acks).Error)
		require.NoError(t, db.Model(&postgres.Acked{}).Count(&ackeds).Error)
		return commits, acks, ackeds
	}

	// dry run reports the work without doing it
	stats, err := repairSingleOwner(ctx, db, repairFQDN, true)
	require.NoError(t, err)
	require.Equal(t, repairSingleOwnerStats{Owners: 6, EntityTimestamps: 3, AckedHoldings: 2, DisownedRawAcks: 1}, stats)
	owned, acks, ackeds := snapshot()
	require.Zero(t, owned)
	require.EqualValues(t, 3, acks)
	require.Zero(t, ackeds)

	stats, err = repairSingleOwner(ctx, db, repairFQDN, false)
	require.NoError(t, err)
	require.Equal(t, repairSingleOwnerStats{Owners: 6, EntityTimestamps: 3, AckedHoldings: 2, DisownedRawAcks: 1}, stats)

	ownerOf := func(id string) (string, bool) {
		var log postgres.CommitLog
		require.NoError(t, db.Where("id = ?", id).Take(&log).Error)
		return log.Owner, log.GcCandidate
	}

	// step 1: owners — single legacy owner verbatim, co-owned ack to its
	// author, ownerless delete derived from its target
	for id, want := range map[string]string{
		"entity-" + st.alice: st.alice,
		st.ackAliceBob:       st.alice,
		st.ackAliceCarol:     st.alice,
		st.deleteAlice:       st.alice,
	} {
		owner, gc := ownerOf(id)
		require.Equal(t, want, owner, "owner of %s", id)
		require.False(t, gc, "%s must not be gc-flagged", id)
	}

	// step 2: entity timestamps
	var bob postgres.Entity
	require.NoError(t, db.Where("id = ?", st.bob).Take(&bob).Error)
	require.True(t, bob.CreatedAt.Equal(st.entityAt))

	// step 3: every ack whose target is local now has the target's holding
	for _, from := range []string{st.alice, st.carol} {
		var holding postgres.Acked
		require.NoError(t, db.Where(`"from" = ? AND "to" = ?`, from, st.bob).Take(&holding).Error, "acked holding for %s -> bob", from)
		require.True(t, holding.Valid)
		require.True(t, holding.CreatedAt.Equal(st.ackAt))
		owner, gc := ownerOf(holding.DocumentID)
		require.Equal(t, st.bob, owner)
		require.False(t, gc)

		var log postgres.CommitLog
		require.NoError(t, db.Where("id = ?", holding.DocumentID).Take(&log).Error)
		var doc concrnt.Document[map[string]string]
		require.NoError(t, json.Unmarshal([]byte(log.Document), &doc))
		require.Equal(t, "acked", doc.Kind)
		require.Equal(t, from, doc.Author)
		var proof concrnt.Proof
		require.NoError(t, json.Unmarshal([]byte(log.Proof), &proof))
		require.Equal(t, concrnt.ProofTypeDocumentDirect, proof.Type)
		require.NotNil(t, proof.Document)
	}
	// no holding for a remote target
	var count int64
	require.NoError(t, db.Model(&postgres.Acked{}).Where(`"to" = ?`, st.carol).Count(&count).Error)
	require.Zero(t, count)

	// step 4: the receiving-side raw ack is gone from acks, its commit
	// disowned and flagged; the acker-side rows stay
	require.NoError(t, db.Model(&postgres.Ack{}).Where("document_id = ?", st.ackCarolBob).Count(&count).Error)
	require.Zero(t, count)
	owner, gc := ownerOf(st.ackCarolBob)
	require.Empty(t, owner)
	require.True(t, gc)
	_, acks, ackeds = snapshot()
	require.EqualValues(t, 2, acks)
	require.EqualValues(t, 2, ackeds)

	// re-running is a no-op
	stats, err = repairSingleOwner(ctx, db, repairFQDN, false)
	require.NoError(t, err)
	require.Equal(t, repairSingleOwnerStats{}, stats)
}

// A fresh database (no commit_owners table) still derives owners from the
// documents themselves.
func TestRepairSingleOwnerWithoutLegacyTable(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)
	ctx := context.Background()

	docBytes, err := json.Marshal(concrnt.Document[map[string]string]{
		Kind: "record", Key: "cckv://con1owner/posts/1", Value: map[string]string{"body": "x"},
		Author: "con1author", Schema: "https://example.com/post.json", CreatedAt: time.Now(),
	})
	require.NoError(t, err)
	require.NoError(t, db.Create(&postgres.CommitLog{ID: "rec-1", Document: string(docBytes), Proof: `{"type":"none"}`}).Error)
	// enough rows to span several batches
	filler := make([]postgres.CommitLog, 0, 1200)
	for i := range 1200 {
		filler = append(filler, postgres.CommitLog{ID: fmt.Sprintf("rec-filler-%04d", i), Document: string(docBytes), Proof: `{"type":"none"}`})
	}
	require.NoError(t, db.CreateInBatches(&filler, 200).Error)
	require.NoError(t, db.Exec(`UPDATE commit_logs SET owner = NULL`).Error)

	stats, err := repairSingleOwner(ctx, db, repairFQDN, false)
	require.NoError(t, err)
	require.Equal(t, repairSingleOwnerStats{Owners: 1201}, stats)
	var unowned int64
	require.NoError(t, db.Model(&postgres.CommitLog{}).Where("owner IS NULL OR owner = ''").Count(&unowned).Error)
	require.Zero(t, unowned)

	var log postgres.CommitLog
	require.NoError(t, db.Where("id = ?", "rec-1").Take(&log).Error)
	require.Equal(t, "con1owner", log.Owner)
}
