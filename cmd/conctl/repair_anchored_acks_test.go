package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/concrnt/concrnt/schemas"
)

// betaState seeds a database the way the v1.11.0-beta builds left it: no
// ackeds table, no entities.created_at, acks rows keyed by the original ack's
// CDID with no foreign key and double-anchored to the ack commit and the
// ack-reference mirror commit, and NULL owners on pass-through commits.
type betaState struct {
	alice, bob, carol string // alice and bob are local, carol is remote
	ackAt             time.Time

	ackAliceBob, mirrorAliceBob         string // both local; the value's key order makes the beta mirror hash differently
	ackBobAlice, mirrorBobAlice         string // both local, sorted value: same CDID, only the proof type changes
	ackCarolBob, mirrorCarolBob         string // remote author: receiving-side row, no ack commit
	ackAliceCarol                       string // remote target: acker-side row only
	ackAliceBobLost, mirrorAliceBobLost string // local author whose ack commit is gone, only the mirror remains
	staleMirror                         string // superseded mirror, gc-flagged, no acks row
	passThrough                         string // record commit with a NULL owner
}

const (
	followSchema = "https://example.com/follow.json"
	lostSchema   = "https://example.com/lost.json"
)

func betaMirrorOf(t *testing.T, original concrnt.SignedDocument) (string, concrnt.SignedDocument) {
	t.Helper()
	var doc concrnt.Document[json.RawMessage]
	require.NoError(t, json.Unmarshal([]byte(original.Document), &doc))
	doc.Kind = "acked"
	mirrorDoc, err := json.Marshal(doc)
	require.NoError(t, err)
	mirror := concrnt.SignedDocument{
		Document: string(mirrorDoc),
		Proof:    concrnt.Proof{Type: legacyProofTypeAckReference, Document: &original.Document, Proof: &original.Proof},
	}
	id, err := mirror.CDID()
	require.NoError(t, err)
	return id, mirror
}

func seedBetaState(t *testing.T, db *gorm.DB) betaState {
	t.Helper()
	ctx := context.Background()

	st := betaState{
		alice: "con1alice",
		bob:   "con1bob",
		carol: "con1carol",
		ackAt: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
	}

	// back the schema down to the beta shape
	for _, stmt := range []string{
		`ALTER TABLE acks DROP CONSTRAINT fk_acks_document`,
		`ALTER TABLE acks ADD COLUMN ack_commit_id text, ADD COLUMN acked_commit_id text`,
		`ALTER TABLE acks ADD CONSTRAINT fk_acks_ack_commit FOREIGN KEY (ack_commit_id) REFERENCES commit_logs(id) ON DELETE SET NULL`,
		`ALTER TABLE acks ADD CONSTRAINT fk_acks_acked_commit FOREIGN KEY (acked_commit_id) REFERENCES commit_logs(id) ON DELETE SET NULL`,
		`DROP TABLE ackeds`,
		`ALTER TABLE entities DROP COLUMN created_at`,
	} {
		require.NoError(t, db.Exec(stmt).Error)
	}

	commit := func(sd concrnt.SignedDocument, owner *string, gc bool) string {
		t.Helper()
		id, err := sd.CDID()
		require.NoError(t, err)
		proofBytes, err := json.Marshal(sd.Proof)
		require.NoError(t, err)
		require.NoError(t, db.WithContext(ctx).Create(&postgres.CommitLog{ID: id, IP: "127.0.0.1", Document: sd.Document, Proof: string(proofBytes), GcCandidate: gc}).Error)
		require.NoError(t, db.Exec(`UPDATE commit_logs SET owner = ? WHERE id = ?`, owner, id).Error)
		return id
	}
	signed := func(doc any) concrnt.SignedDocument {
		t.Helper()
		docBytes, err := json.Marshal(doc)
		require.NoError(t, err)
		return concrnt.SignedDocument{Document: string(docBytes), Proof: concrnt.Proof{Type: concrnt.ProofTypeNone}}
	}
	entity := func(ccid, host string) {
		t.Helper()
		id := commit(signed(concrnt.Document[schemas.Entity]{Kind: "entity", Value: schemas.Entity{Domain: host}, Author: ccid, CreatedAt: st.ackAt}), &ccid, false)
		// raw SQL: the model has created_at, which this schema no longer has
		require.NoError(t, db.Exec(`INSERT INTO entities (id, domain, document_id) VALUES (?, ?, ?)`, ccid, host, id).Error)
	}
	// rawAck builds the original ack; value is raw JSON so key order is preserved
	rawAck := func(from, to, schema string, value string, at time.Time) concrnt.SignedDocument {
		associate := "cckv://" + to
		return signed(concrnt.Document[json.RawMessage]{Kind: "ack", Value: json.RawMessage(value), Author: from, Schema: schema, CreatedAt: at, Associate: &associate})
	}
	ackRow := func(original concrnt.SignedDocument, from, to, schema string, ackCommitID, ackedCommitID *string) string {
		t.Helper()
		id, err := original.CDID()
		require.NoError(t, err)
		require.NoError(t, db.Exec(`INSERT INTO acks ("from", "to", schema, document_id, ack_commit_id, acked_commit_id, valid, created_at) VALUES (?, ?, ?, ?, ?, ?, true, ?)`,
			from, to, schema, id, ackCommitID, ackedCommitID, st.ackAt).Error)
		return id
	}

	entity(st.alice, repairFQDN)
	entity(st.bob, repairFQDN)
	entity(st.carol, "remote.example.net")

	// alice -> bob: both sides local, unsorted value
	orig := rawAck(st.alice, st.bob, followSchema, `{"z":1,"a":2}`, st.ackAt)
	st.ackAliceBob = commit(orig, &st.alice, false)
	mirrorID, mirror := betaMirrorOf(t, orig)
	st.mirrorAliceBob = commit(mirror, &st.bob, false)
	require.Equal(t, mirrorID, st.mirrorAliceBob)
	ackRow(orig, st.alice, st.bob, followSchema, &st.ackAliceBob, &st.mirrorAliceBob)

	// bob -> alice: both sides local, sorted value
	orig = rawAck(st.bob, st.alice, followSchema, `{"a":2,"z":1}`, st.ackAt)
	st.ackBobAlice = commit(orig, &st.bob, false)
	_, mirror = betaMirrorOf(t, orig)
	st.mirrorBobAlice = commit(mirror, &st.alice, false)
	ackRow(orig, st.bob, st.alice, followSchema, &st.ackBobAlice, &st.mirrorBobAlice)

	// carol -> bob: receiving side only
	orig = rawAck(st.carol, st.bob, followSchema, `{"z":1,"a":2}`, st.ackAt)
	_, mirror = betaMirrorOf(t, orig)
	st.mirrorCarolBob = commit(mirror, &st.bob, false)
	st.ackCarolBob = ackRow(orig, st.carol, st.bob, followSchema, nil, &st.mirrorCarolBob)

	// alice -> carol: acker side only
	orig = rawAck(st.alice, st.carol, followSchema, `{"z":1,"a":2}`, st.ackAt)
	st.ackAliceCarol = commit(orig, &st.alice, false)
	ackRow(orig, st.alice, st.carol, followSchema, &st.ackAliceCarol, nil)

	// alice -> bob on a second schema: the ack commit is gone, only the
	// mirror survived
	orig = rawAck(st.alice, st.bob, lostSchema, `{"z":1,"a":2}`, st.ackAt.Add(time.Hour))
	id, err := orig.CDID()
	require.NoError(t, err)
	st.ackAliceBobLost = id
	_, mirror = betaMirrorOf(t, orig)
	st.mirrorAliceBobLost = commit(mirror, &st.bob, false)
	ackRow(orig, st.alice, st.bob, lostSchema, nil, &st.mirrorAliceBobLost)

	// a superseded mirror with no row
	orig = rawAck(st.carol, st.alice, followSchema, `{"old":true}`, st.ackAt.Add(-time.Hour))
	_, mirror = betaMirrorOf(t, orig)
	st.staleMirror = commit(mirror, &st.alice, true)

	// a pass-through record commit
	st.passThrough = commit(signed(concrnt.Document[map[string]string]{Kind: "record", Key: "cckv://" + st.carol + "/posts/1", Value: map[string]string{"body": "x"}, Author: st.carol, Schema: "https://example.com/post.json", CreatedAt: st.ackAt}), nil, false)

	return st
}

func TestRepairAnchoredAcks(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)
	ctx := context.Background()

	st := seedBetaState(t, db)
	want := repairAnchoredAcksStats{Holdings: 4, RestoredAcks: 1, RemovedRows: 1, StaleMirrors: 4, NullOwners: 1}

	// dry run reports the work without doing it
	stats, err := repairAnchoredAcks(ctx, db, repairFQDN, true)
	require.NoError(t, err)
	require.Equal(t, want, stats)
	require.True(t, db.Migrator().HasColumn("acks", "ack_commit_id"))
	require.False(t, db.Migrator().HasTable(&postgres.Acked{}))
	var count int64
	require.NoError(t, db.Model(&postgres.Ack{}).Count(&count).Error)
	require.EqualValues(t, 5, count)

	stats, err = repairAnchoredAcks(ctx, db, repairFQDN, false)
	require.NoError(t, err)
	require.Equal(t, want, stats)

	// the schema is the current one: anchors gone, foreign key back, ackeds
	// and entities.created_at in place
	require.False(t, db.Migrator().HasColumn("acks", "ack_commit_id"))
	require.False(t, db.Migrator().HasColumn("acks", "acked_commit_id"))
	require.True(t, db.Migrator().HasConstraint(&postgres.Ack{}, "fk_acks_document"))
	require.True(t, db.Migrator().HasTable(&postgres.Acked{}))
	require.True(t, db.Migrator().HasColumn(&postgres.Entity{}, "created_at"))

	commitOf := func(id string) *postgres.CommitLog {
		var log postgres.CommitLog
		err := db.Where("id = ?", id).Take(&log).Error
		if err == gorm.ErrRecordNotFound {
			return nil
		}
		require.NoError(t, err)
		return &log
	}
	holdingOf := func(from, to, schema string) postgres.Acked {
		var holding postgres.Acked
		require.NoError(t, db.Where(`"from" = ? AND "to" = ? AND schema = ?`, from, to, schema).Take(&holding).Error, "holding %s -> %s", from, to)
		return holding
	}
	requireAcked := func(holding postgres.Acked, owner string) {
		t.Helper()
		log := commitOf(holding.DocumentID)
		require.NotNil(t, log)
		require.Equal(t, owner, log.Owner)
		require.False(t, log.GcCandidate)
		var proof concrnt.Proof
		require.NoError(t, json.Unmarshal([]byte(log.Proof), &proof))
		require.Equal(t, concrnt.ProofTypeDocumentDirect, proof.Type)
		// the same derivation check verify.go applies to document-direct
		require.NotNil(t, proof.Document)
		require.NotNil(t, proof.Proof)
		embedded := concrnt.SignedDocument{Document: *proof.Document, Proof: *proof.Proof}
		expected, err := embedded.DeriveAcked()
		require.NoError(t, err)
		require.Equal(t, expected.Document, log.Document)
		require.True(t, holding.Valid)
		require.True(t, holding.CreatedAt.Equal(st.ackAt) || holding.CreatedAt.After(st.ackAt))
	}

	// acker-side rows survive for local authors only, keyed by the ack commit
	var acks []postgres.Ack
	require.NoError(t, db.Order("document_id").Find(&acks).Error)
	ids := map[string]bool{}
	for _, a := range acks {
		ids[a.DocumentID] = true
		require.NotNil(t, commitOf(a.DocumentID), "ack commit for %s", a.DocumentID)
	}
	require.Equal(t, map[string]bool{st.ackAliceBob: true, st.ackBobAlice: true, st.ackAliceCarol: true, st.ackAliceBobLost: true}, ids)

	// the restored ack commit carries the author as owner
	restored := commitOf(st.ackAliceBobLost)
	require.NotNil(t, restored)
	require.Equal(t, st.alice, restored.Owner)

	// ackee-side holdings for every local target
	requireAcked(holdingOf(st.alice, st.bob, followSchema), st.bob)
	requireAcked(holdingOf(st.bob, st.alice, followSchema), st.alice)
	requireAcked(holdingOf(st.carol, st.bob, followSchema), st.bob)
	requireAcked(holdingOf(st.alice, st.bob, lostSchema), st.bob)
	require.NoError(t, db.Model(&postgres.Acked{}).Where(`"to" = ?`, st.carol).Count(&count).Error)
	require.Zero(t, count)

	// the sorted-value mirror kept its CDID and was rewritten in place
	require.Equal(t, st.mirrorBobAlice, holdingOf(st.bob, st.alice, followSchema).DocumentID)
	// the unsorted-value mirrors hash differently and are gone with the stale one
	require.NotEqual(t, st.mirrorAliceBob, holdingOf(st.alice, st.bob, followSchema).DocumentID)
	for _, id := range []string{st.mirrorAliceBob, st.mirrorCarolBob, st.mirrorAliceBobLost, st.staleMirror} {
		require.Nil(t, commitOf(id), "legacy mirror %s must be deleted", id)
	}
	require.NoError(t, db.Model(&postgres.CommitLog{}).Where("proof LIKE ?", "%"+legacyProofTypeAckReference+"%").Count(&count).Error)
	require.Zero(t, count)

	// NULL owner spelled ''
	require.Equal(t, "", commitOf(st.passThrough).Owner)
	require.NoError(t, db.Model(&postgres.CommitLog{}).Where("owner IS NULL").Count(&count).Error)
	require.Zero(t, count)

	// re-running is a no-op
	stats, err = repairAnchoredAcks(ctx, db, repairFQDN, false)
	require.NoError(t, err)
	require.Equal(t, repairAnchoredAcksStats{}, stats)

	// the follow-up repair only has entity timestamps left to do. It runs in
	// its own process in practice; here the pooled connections still hold
	// statements prepared against the pre-drop acks columns, so recycle them.
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxIdleConns(0)
	sqlDB.SetMaxIdleConns(2)
	ownerStats, err := repairSingleOwner(ctx, db, repairFQDN, false)
	require.NoError(t, err)
	require.Equal(t, repairSingleOwnerStats{EntityTimestamps: 3, Owners: 1}, ownerStats)
	require.Equal(t, st.carol, commitOf(st.passThrough).Owner)
}
