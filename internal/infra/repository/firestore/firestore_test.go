package firestore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

func TestAncestorsOf(t *testing.T) {
	require.Nil(t, ancestorsOf("nonsense"))
	require.Nil(t, ancestorsOf("cckv://owner"))
	require.Equal(t, []string{"cckv://owner"}, ancestorsOf("cckv://owner/a"))
	require.Equal(t, []string{"cckv://owner", "cckv://owner/a", "cckv://owner/a/b"}, ancestorsOf("cckv://owner/a/b/c"))

	parent, ok, err := parentURIOf("cckv://owner/a/b")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "cckv://owner/a", parent)
	_, ok, err = parentURIOf("cckv://owner/a")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestHashIDIsStableAndSafe(t *testing.T) {
	require.Equal(t, keyID("cckv://o/a"), keyID("cckv://o/a"))
	require.NotEqual(t, keyID("cckv://o/a"), keyID("cckv://o/b"))
	require.NotEqual(t, ackID("a", "b", "c"), ackID("a", "bc", ""))
	require.Len(t, keyID("cckv://o/a"), 32)
	require.NotContains(t, keyID("cckv://o/a"), "/")
}

// seedRecords writes n records under prefix/<name>, each one second apart.
func seedRecords(t *testing.T, ctx context.Context, repo record.Repository, owner string, keys []string, base time.Time) {
	t.Helper()
	for i, key := range keys {
		createdAt := base.Add(time.Duration(i) * time.Second)
		id := fmt.Sprintf("seed-%d-%s", i, keyID(key)[:6])
		sd := repotest.SignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      "record",
			Key:       key,
			Value:     map[string]string{"i": fmt.Sprint(i)},
			Author:    owner,
			Schema:    "https://schema.example/post.json",
			CreatedAt: createdAt,
		})
		repotest.WithCommit(t, ctx, repo, id, "127.0.0.1", sd, owner, func(tx record.RepositoryTx) error {
			applied, err := repo.CreateRecord(ctx, tx, id, key, owner, owner, "https://schema.example/post.json", nil, nil, []string{}, nil, createdAt)
			require.True(t, applied)
			return err
		})
	}
}

func urisOf(rows []record.QueryRow) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, *r.Row.CCKV)
	}
	return out
}

func TestQueryByPrefix(t *testing.T) {
	ctx := context.Background()
	repo := NewRecordRepository(newClient(t))
	owner := "con1prefix"
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	seedRecords(t, ctx, repo, owner, []string{
		"cckv://con1prefix/tl/e1",
		"cckv://con1prefix/tl/e2",
		"cckv://con1prefix/tl/f1",
		"cckv://con1prefix/tl/e1/reply",
		"cckv://con1prefix/other/x",
	}, base)

	t.Run("directory prefix lists the whole subtree newest first", func(t *testing.T) {
		rows, err := repo.QueryByPrefix(ctx, "cckv://con1prefix/tl/", "", "", nil, nil, 0, "desc")
		require.NoError(t, err)
		require.Equal(t, []string{
			"cckv://con1prefix/tl/e1/reply",
			"cckv://con1prefix/tl/f1",
			"cckv://con1prefix/tl/e2",
			"cckv://con1prefix/tl/e1",
		}, urisOf(rows))
	})

	t.Run("owner prefix lists everything", func(t *testing.T) {
		rows, err := repo.QueryByPrefix(ctx, "cckv://con1prefix/", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Len(t, rows, 5)
		require.True(t, rows[0].CreatedAt.Before(rows[4].CreatedAt))
	})

	t.Run("segment prefix keeps LIKE semantics including siblings", func(t *testing.T) {
		rows, err := repo.QueryByPrefix(ctx, "cckv://con1prefix/tl/e", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{
			"cckv://con1prefix/tl/e1",
			"cckv://con1prefix/tl/e2",
			"cckv://con1prefix/tl/e1/reply",
		}, urisOf(rows))
	})

	t.Run("limit and window apply after the post-filter", func(t *testing.T) {
		since := base.Add(1 * time.Second)
		rows, err := repo.QueryByPrefix(ctx, "cckv://con1prefix/tl/e", "", "", &since, nil, 1, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"cckv://con1prefix/tl/e2"}, urisOf(rows))
	})

	t.Run("prefix without a path is rejected", func(t *testing.T) {
		_, err := repo.QueryByPrefix(ctx, "cckv://con1", "", "", nil, nil, 0, "asc")
		var verr domain.ValidationError
		require.ErrorAs(t, err, &verr)
	})
}

func TestMaintenanceGC(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	repo := NewRecordRepository(client)
	maint := NewRecordMaintenanceRepository(client)
	in := NewInspector(client)
	owner := "con1gc"
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	// ids sort as a-, b-, c-: "b" is the cutoff so only the a- commit is old enough
	target := "cckv://con1gc/tl/post"
	seedRecords(t, ctx, repo, owner, []string{target}, base)
	recordID := fmt.Sprintf("seed-0-%s", keyID(target)[:6])

	assocSD := repotest.SignedDocument(t, concrnt.Document[map[string]string]{
		Kind: "association", Author: "con1liker", Schema: "https://schema.example/like.json", CreatedAt: base,
	})
	repotest.WithCommit(t, ctx, repo, "a-like", "127.0.0.1", assocSD, owner, func(tx record.RepositoryTx) error {
		applied, err := repo.CreateAssociation(ctx, tx, "a-like", target, owner, "con1liker", "https://schema.example/like.json", nil, "uniq-like", base)
		require.True(t, applied)
		return err
	})
	repotest.InTx(t, ctx, repo, func(tx record.RepositoryTx) {
		require.NoError(t, repo.MarkCommitLogGcCandidate(ctx, tx, "a-like"))
		require.NoError(t, repo.MarkCommitLogGcCandidate(ctx, tx, recordID))
	})

	n, err := maint.CountGcCandidates(ctx, "b")
	require.NoError(t, err)
	require.Equal(t, int64(1), n, "only ids below the cutoff count")
	n, err = maint.CountGcCandidates(ctx, "zzz")
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	ids, err := maint.ListGcCandidateIDs(ctx, "zzz", 10)
	require.NoError(t, err)
	require.Equal(t, []string{"a-like", recordID}, ids)

	// pages walk the whole log in id order
	page, err := maint.ListCommitLogs(ctx, record.CommitLogPage{Limit: 1})
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, "a-like", page[0].ID)
	page, err = maint.ListCommitLogs(ctx, record.CommitLogPage{AfterID: "a-like", Limit: 10})
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, recordID, page[0].ID)
	page, err = maint.ListCommitLogs(ctx, record.CommitLogPage{Owner: "nobody", Limit: 10})
	require.NoError(t, err)
	require.Empty(t, page)

	deleted, err := maint.DeleteCommitLogs(ctx, []string{"a-like", "missing"})
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	assoc, err := in.Association(ctx, "a-like")
	require.NoError(t, err)
	require.Nil(t, assoc)
	commit, err := in.Commit(ctx, "a-like")
	require.NoError(t, err)
	require.Nil(t, commit)
	// the record commit survives
	commit, err = in.Commit(ctx, recordID)
	require.NoError(t, err)
	require.NotNil(t, commit)

	// deleting the record's commit cascades to the record and its key
	deleted, err = maint.DeleteCommitLogs(ctx, []string{recordID})
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	rec, err := in.Record(ctx, recordID)
	require.NoError(t, err)
	require.Nil(t, rec)
	key, err := in.RecordKey(ctx, target)
	require.NoError(t, err)
	require.Nil(t, key)
}

func TestAssociationCountsByVariantOrder(t *testing.T) {
	ctx := context.Background()
	repo := NewRecordRepository(newClient(t))
	owner := "con1counts"
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	target := "cckv://con1counts/tl/post"
	seedRecords(t, ctx, repo, owner, []string{target}, base)

	schema := "https://schema.example/reaction.json"
	react := func(id, author, variant string, at time.Time) {
		sd := repotest.SignedDocument(t, concrnt.Document[map[string]string]{Kind: "association", Author: author, Schema: schema, CreatedAt: at})
		repotest.WithCommit(t, ctx, repo, id, "127.0.0.1", sd, owner, func(tx record.RepositoryTx) error {
			_, err := repo.CreateAssociation(ctx, tx, id, target, owner, author, schema, &variant, "u-"+id, at)
			return err
		})
	}
	react("r1", "con1a", "😀", base.Add(3*time.Second))
	react("r2", "con1b", "🎉", base.Add(1*time.Second))
	react("r3", "con1c", "😀", base.Add(2*time.Second))

	counts, err := repo.GetAssociatedRecordCountsByVariant(ctx, target, schema)
	require.NoError(t, err)
	require.Equal(t, int64(2), (*counts)["😀"].Value)
	require.Equal(t, int64(1), (*counts)["🎉"].Value)
	require.Less(t, (*counts)["🎉"].Order, (*counts)["😀"].Order, "the earliest-seen variant orders first")

	bySchema, err := repo.GetAssociatedRecordCountsBySchema(ctx, target)
	require.NoError(t, err)
	require.Equal(t, map[string]int64{schema: 3}, bySchema)

	rows, err := repo.GetAssociatedRecords(ctx, target, schema, "😀", "", nil, nil, 0, "desc")
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.True(t, rows[0].CreatedAt.After(rows[1].CreatedAt))

	// no target key: empty, not an error
	none, err := repo.GetAssociatedRecordCountsBySchema(ctx, "cckv://con1counts/nope")
	require.NoError(t, err)
	require.Empty(t, none)
}

func TestMetadataRepositories(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)

	t.Run("notification subscriptions upsert by vendor and owner", func(t *testing.T) {
		repo := NewNotificationRepository(client)
		sub, err := repo.Subscribe(ctx, domain.NotificationSubscription{VendorID: "v", Owner: "con1x", Schemas: []string{"a"}, Subscription: "s1"})
		require.NoError(t, err)
		require.Equal(t, "s1", sub.Subscription)
		require.False(t, sub.CDate.IsZero())

		sub2, err := repo.Subscribe(ctx, domain.NotificationSubscription{VendorID: "v", Owner: "con1x", Subscription: "s2"})
		require.NoError(t, err)
		require.Equal(t, "s2", sub2.Subscription)
		require.True(t, sub2.CDate.Equal(sub.CDate), "cDate survives the upsert")
		require.Empty(t, sub2.Schemas)

		list, err := repo.List(ctx)
		require.NoError(t, err)
		require.Len(t, list, 1)

		require.NoError(t, repo.Delete(ctx, "v", "con1x"))
		_, err = repo.Get(ctx, "v", "con1x")
		require.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("entity metas", func(t *testing.T) {
		repo := NewResidenceRepository(client)
		inviter := "con1inv"
		require.NoError(t, repo.SaveMeta(ctx, domain.EntityMeta{ID: "con1m", Inviter: &inviter, Info: "{}"}))
		require.NoError(t, repo.SaveMeta(ctx, domain.EntityMeta{ID: "con1n", Info: `{"x":1}`}))
		metas, err := repo.ListMetas(ctx, "")
		require.NoError(t, err)
		require.Len(t, metas, 2)
		metas, err = repo.ListMetas(ctx, "con1n")
		require.NoError(t, err)
		require.Len(t, metas, 1)
		require.Nil(t, metas[0].Inviter)
		metas, err = repo.ListMetas(ctx, "con1zzz")
		require.NoError(t, err)
		require.Empty(t, metas)
		require.NoError(t, repo.DeleteMeta(ctx, "con1m"))
		_, err = repo.GetMeta(ctx, "con1m")
		require.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("abuse reports append", func(t *testing.T) {
		repo := NewAbuseRepository(client)
		require.NoError(t, repo.CreateAbuseReport(ctx, &concrnt.AbuseReport{TargetURI: "cckv://x/y", Body: "spam"}, "con1r", "127.0.0.1"))
		snaps, err := client.Collection(colAbuseReports).Documents(ctx).GetAll()
		require.NoError(t, err)
		require.Len(t, snaps, 1)
	})

	t.Run("readiness tolerates a missing probe document", func(t *testing.T) {
		require.NoError(t, NewReadiness(client, time.Second).Check(ctx))
	})
}
