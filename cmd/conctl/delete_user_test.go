package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/concrnt/concrnt/schemas"
)

// deleteUserState seeds two local users: alice (the target) and bob, each
// with an entity, a registration meta, a record, a subscription, and an ack
// in both directions held the single-owner way (ack owned by the acker,
// acked owned by the target).
type deleteUserState struct {
	alice, bob string
	at         time.Time
}

func seedDeleteUserState(t *testing.T, db *gorm.DB) deleteUserState {
	t.Helper()
	ctx := context.Background()

	st := deleteUserState{
		alice: "con1alice",
		bob:   "con1bob",
		at:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	commit := func(id, owner string, doc any) {
		t.Helper()
		docBytes, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.WithContext(ctx).Create(&models.CommitLog{ID: id, IP: "127.0.0.1", Document: string(docBytes), Proof: `{"type":"none"}`, Owner: owner}).Error)
	}
	user := func(ccid string) {
		t.Helper()
		entityID := "entity-" + ccid
		commit(entityID, ccid, concrnt.Document[schemas.Entity]{Kind: "entity", Value: schemas.Entity{Domain: "example.com"}, Author: ccid, CreatedAt: st.at})
		require.NoError(t, db.WithContext(ctx).Create(&models.Entity{ID: ccid, Domain: "example.com", DocumentID: entityID, CreatedAt: st.at}).Error)
		require.NoError(t, db.WithContext(ctx).Create(&models.EntityMeta{ID: ccid, Info: `{}`}).Error)

		recordID := "record-" + ccid
		key := "cckv://" + ccid + "/posts/1"
		commit(recordID, ccid, concrnt.Document[map[string]string]{Kind: "record", Key: key, Value: map[string]string{"body": "hi"}, Author: ccid, Schema: "https://example.com/post.json", CreatedAt: st.at})
		require.NoError(t, db.WithContext(ctx).Create(&models.Record{DocumentID: recordID, Owner: ccid, Author: ccid, Schema: "https://example.com/post.json", CreatedAt: st.at}).Error)
		require.NoError(t, db.WithContext(ctx).Create(&models.RecordKey{URI: key, RecordID: &recordID, RecordCreatedAt: &st.at}).Error)

		require.NoError(t, db.WithContext(ctx).Create(&models.Subscription{VendorID: "vendor", Owner: ccid, Schemas: pq.StringArray{}, Prefixes: pq.StringArray{}, Subscription: "{}"}).Error)
	}
	follow := func(from, to string) {
		t.Helper()
		associate := "cckv://" + to
		ackID := "ack-" + from + "-" + to
		commit(ackID, from, concrnt.Document[map[string]string]{Kind: "ack", Value: map[string]string{}, Author: from, Schema: "https://example.com/follow.json", CreatedAt: st.at, Associate: &associate})
		require.NoError(t, db.WithContext(ctx).Create(&models.Ack{From: from, To: to, Schema: "https://example.com/follow.json", DocumentID: ackID, Valid: true, CreatedAt: st.at}).Error)
		ackedID := "acked-" + from + "-" + to
		commit(ackedID, to, concrnt.Document[map[string]string]{Kind: "acked", Value: map[string]string{}, Author: from, Schema: "https://example.com/follow.json", CreatedAt: st.at, Associate: &associate})
		require.NoError(t, db.WithContext(ctx).Create(&models.Acked{From: from, To: to, Schema: "https://example.com/follow.json", DocumentID: ackedID, Valid: true, CreatedAt: st.at}).Error)
	}

	user(st.alice)
	user(st.bob)
	follow(st.alice, st.bob)
	follow(st.bob, st.alice)

	return st
}

func TestDeleteUser(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)
	ctx := context.Background()

	st := seedDeleteUserState(t, db)

	count := func(model any, query string, args ...any) int64 {
		t.Helper()
		var n int64
		require.NoError(t, db.Model(model).Where(query, args...).Count(&n).Error)
		return n
	}
	ids := func(model any, query string, args ...any) []string {
		t.Helper()
		var out []string
		require.NoError(t, db.Model(model).Where(query, args...).Order("document_id").Pluck("document_id", &out).Error)
		return out
	}

	// alice owns: entity, record, her ack to bob, bob's acked holding on her
	require.EqualValues(t, 4, count(&models.CommitLog{}, "owner = ?", st.alice))
	require.EqualValues(t, 4, count(&models.CommitLog{}, "owner = ?", st.bob))

	// dry run reports without touching anything
	stats, err := deleteUser(ctx, db, st.alice, nil, true)
	require.NoError(t, err)
	require.Equal(t, deleteUserStats{Commits: 4, Metas: 1, Subscriptions: 1}, stats)
	require.EqualValues(t, 8, count(&models.CommitLog{}, "1 = 1"))
	require.EqualValues(t, 2, count(&models.EntityMeta{}, "1 = 1"))
	require.EqualValues(t, 2, count(&models.Subscription{}, "1 = 1"))

	stats, err = deleteUser(ctx, db, st.alice, nil, false)
	require.NoError(t, err)
	require.Equal(t, deleteUserStats{Commits: 4, Metas: 1, Subscriptions: 1}, stats)

	// everything alice owned is gone, cascades included
	require.Zero(t, count(&models.CommitLog{}, "owner = ?", st.alice))
	require.Zero(t, count(&models.Entity{}, "id = ?", st.alice))
	require.Zero(t, count(&models.EntityMeta{}, "id = ?", st.alice))
	require.Zero(t, count(&models.Subscription{}, "owner = ?", st.alice))
	require.Zero(t, count(&models.Record{}, "owner = ?", st.alice))
	require.Zero(t, count(&models.RecordKey{}, "uri LIKE ?", "cckv://"+st.alice+"/%"))
	require.Empty(t, ids(&models.Ack{}, `"from" = ?`, st.alice), "alice's own ack")
	require.Empty(t, ids(&models.Acked{}, `"to" = ?`, st.alice), "acked holding addressed to alice")

	// bob's data is intact, including the rows that name alice as counterparty
	require.EqualValues(t, 4, count(&models.CommitLog{}, "owner = ?", st.bob))
	require.EqualValues(t, 1, count(&models.Entity{}, "id = ?", st.bob))
	require.EqualValues(t, 1, count(&models.EntityMeta{}, "id = ?", st.bob))
	require.EqualValues(t, 1, count(&models.Subscription{}, "owner = ?", st.bob))
	require.EqualValues(t, 1, count(&models.Record{}, "owner = ?", st.bob))
	require.Equal(t, []string{"ack-" + st.bob + "-" + st.alice}, ids(&models.Ack{}, `"from" = ?`, st.bob), "bob's ack to alice")
	require.Equal(t, []string{"acked-" + st.alice + "-" + st.bob}, ids(&models.Acked{}, `"to" = ?`, st.bob), "alice's acked holding on bob")

	// re-running is a no-op
	stats, err = deleteUser(ctx, db, st.alice, nil, false)
	require.NoError(t, err)
	require.Equal(t, deleteUserStats{}, stats)
	require.EqualValues(t, 4, count(&models.CommitLog{}, "1 = 1"))
}
