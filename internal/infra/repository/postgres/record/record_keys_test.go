package record

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/testutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

const recordKeysOwner = "con1rk"

type recordKeysFixture struct {
	t    *testing.T
	ctx  context.Context
	repo *Repository
}

func newRecordKeysFixture(t *testing.T) *recordKeysFixture {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)
	repo := New(db)
	require.NoError(t, repo.Migrate())
	return &recordKeysFixture{t: t, ctx: context.Background(), repo: repo}
}

func (f *recordKeysFixture) commitRecord(id, key string, createdAt time.Time, onUpdate *string, policy *concrnt.Policy) {
	f.t.Helper()
	sd := repositorySignedDocument(f.t, concrnt.Document[map[string]string]{
		Kind:      "record",
		Key:       key,
		Value:     map[string]string{"body": id},
		Author:    recordKeysOwner,
		Schema:    "https://schema.example/post.json",
		CreatedAt: createdAt,
		OnUpdate:  onUpdate,
		Policy:    policy,
	})
	var policyStr *string
	if policy != nil {
		policyStr = ptr(mustJSON(f.t, policy))
	}
	withRepositoryTx(f.t, f.ctx, f.repo, id, "127.0.0.1", sd, recordKeysOwner, func(tx ucrecord.RepositoryTx) error {
		applied, err := f.repo.CreateRecord(f.ctx, tx, id, key, recordKeysOwner, recordKeysOwner, "https://schema.example/post.json", onUpdate, policyStr, []string{}, nil, createdAt)
		require.True(f.t, applied)
		return err
	})
}

func (f *recordKeysFixture) associate(id, target string, createdAt time.Time) {
	f.t.Helper()
	sd := repositorySignedDocument(f.t, concrnt.Document[map[string]string]{
		Kind:      "association",
		Value:     map[string]string{"body": id},
		Author:    recordKeysOwner,
		Schema:    "https://schema.example/like.json",
		CreatedAt: createdAt,
		Associate: &target,
	})
	withRepositoryTx(f.t, f.ctx, f.repo, id, "127.0.0.1", sd, recordKeysOwner, func(tx ucrecord.RepositoryTx) error {
		_, err := f.repo.CreateAssociation(f.ctx, tx, id, target, recordKeysOwner, recordKeysOwner, "https://schema.example/like.json", nil, id, createdAt)
		return err
	})
}

// deleteCommit runs fn in the transaction of a delete commit, the way the
// usecase removes a record and flags what it removed.
func (f *recordKeysFixture) deleteCommit(id string, fn func(tx ucrecord.RepositoryTx) error) {
	f.t.Helper()
	sd := repositorySignedDocument(f.t, concrnt.Document[string]{Kind: "delete", Value: id, Author: recordKeysOwner, CreatedAt: time.Now()})
	withRepositoryTx(f.t, f.ctx, f.repo, id, "127.0.0.1", sd, recordKeysOwner, fn)
}

func (f *recordKeysFixture) childKeys(parent string) []string {
	f.t.Helper()
	rows, err := f.repo.QueryByParent(f.ctx, parent, "", "", nil, nil, 0, "asc")
	require.NoError(f.t, err)
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, *row.Row.CCKV)
	}
	return keys
}

func (f *recordKeysFixture) keyRow(uri string) (RecordKey, bool) {
	f.t.Helper()
	var rk RecordKey
	err := f.repo.db.Where("uri = ?", uri).Limit(1).Find(&rk).Error
	require.NoError(f.t, err)
	return rk, rk.ID != 0
}

func ccfsOf(id string) string {
	return concrnt.ComposeCCFSURI(recordKeysOwner, concrnt.CCFSTypeConcrnt, id)
}

// Removing a key that has children keeps its row as a placeholder: the
// children stay listed under it, before and after the key is written again,
// while the removed record's associations go with it.
func TestRemoveKeyWithChildrenKeepsThemListed(t *testing.T) {
	f := newRecordKeysFixture(t)
	at := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	timeline := "cckv://" + recordKeysOwner + "/tl"
	f.commitRecord("tl-1", timeline, at, nil, nil)
	f.commitRecord("child-1", timeline+"/1", at.Add(time.Second), nil, nil)
	f.associate("like-tl", timeline, at.Add(2*time.Second))

	f.deleteCommit("delete-tl", func(tx ucrecord.RepositoryTx) error {
		return f.repo.DeleteRecordByKey(f.ctx, tx, timeline)
	})

	rk, ok := f.keyRow(timeline)
	require.True(t, ok, "the key row stays as a placeholder")
	require.Nil(t, rk.RecordID)
	require.Nil(t, rk.RecordCreatedAt)
	require.Empty(t, rk.Schema)
	_, err := f.repo.GetSignedDocument(f.ctx, timeline)
	require.ErrorIs(t, err, domain.ErrNotFound)

	var associations int64
	require.NoError(t, f.repo.db.Model(&Association{}).Where("document_id = ?", "like-tl").Count(&associations).Error)
	require.Zero(t, associations, "the removed record's associations go with it")

	require.Equal(t, []string{timeline + "/1"}, f.childKeys(timeline))

	f.commitRecord("tl-2", timeline, at.Add(3*time.Second), nil, nil)
	f.commitRecord("child-2", timeline+"/2", at.Add(4*time.Second), nil, nil)
	require.Equal(t, []string{timeline + "/1", timeline + "/2"}, f.childKeys(timeline))

	rk2, _ := f.keyRow(timeline)
	require.Equal(t, rk.ID, rk2.ID, "writing the key again reuses the placeholder row")
}

// Removing the last child deletes every ancestor placeholder left empty, but
// not one that still carries associations.
func TestRemoveLastChildPrunesEmptyPlaceholders(t *testing.T) {
	f := newRecordKeysFixture(t)
	at := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)
	root := "cckv://" + recordKeysOwner

	f.commitRecord("deep", root+"/a/b/c", at, nil, nil)
	_, ok := f.keyRow(root + "/a/b")
	require.True(t, ok, "the child write materializes its ancestors")

	f.deleteCommit("delete-deep", func(tx ucrecord.RepositoryTx) error {
		return f.repo.DeleteRecordByKey(f.ctx, tx, root+"/a/b/c")
	})
	for _, uri := range []string{root + "/a/b/c", root + "/a/b", root + "/a"} {
		_, ok := f.keyRow(uri)
		require.False(t, ok, "%s must be gone", uri)
	}

	f.commitRecord("leaf", root+"/x/y", at, nil, nil)
	f.associate("like-x", root+"/x", at)
	f.deleteCommit("delete-leaf", func(tx ucrecord.RepositoryTx) error {
		return f.repo.DeleteRecordByKey(f.ctx, tx, root+"/x/y")
	})
	_, ok = f.keyRow(root + "/x")
	require.True(t, ok, "a placeholder with associations is kept")
}

// A version kept by onUpdate retain is served by ccfs, under its own policy,
// until it is deleted; a version replaced without retain is not served.
func TestRetainedVersionByCCFS(t *testing.T) {
	f := newRecordKeysFixture(t)
	at := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	key := "cckv://" + recordKeysOwner + "/profile"

	policyOf := func(url string) *concrnt.Policy {
		return &concrnt.Policy{Entries: []concrnt.PolicyEntry{{URL: ptr(url)}}}
	}

	f.commitRecord("v1", key, at, ptr("retain"), policyOf("https://policy.example/v1.json"))
	f.commitRecord("v2", key, at.Add(time.Second), nil, policyOf("https://policy.example/v2.json"))
	f.commitRecord("v3", key, at.Add(2*time.Second), nil, policyOf("https://policy.example/v3.json"))

	sd, err := f.repo.GetSignedDocument(f.ctx, ccfsOf("v1"))
	require.NoError(t, err)
	require.Nil(t, sd.CCKV, "no key points at the retained version")

	_, err = f.repo.GetSignedDocument(f.ctx, ccfsOf("v2"))
	require.ErrorIs(t, err, domain.ErrNotFound, "v2 was replaced without retain")

	sd, err = f.repo.GetSignedDocument(f.ctx, ccfsOf("v3"))
	require.NoError(t, err)
	require.Equal(t, key, *sd.CCKV)

	leafPolicy := func(uri string) string {
		t.Helper()
		stack, err := f.repo.GetHierarchicalRecordPolicies(f.ctx, uri)
		require.NoError(t, err)
		require.NotEmpty(t, stack)
		leaf := stack[len(stack)-1]
		require.Equal(t, key, leaf.Source)
		return *leaf.Entries[0].URL
	}
	require.Equal(t, "https://policy.example/v1.json", leafPolicy(ccfsOf("v1")), "a retained version is evaluated under its own policy")
	require.Equal(t, "https://policy.example/v3.json", leafPolicy(ccfsOf("v3")))
	require.Equal(t, "https://policy.example/v3.json", leafPolicy(key))

	f.deleteCommit("delete-v1", func(tx ucrecord.RepositoryTx) error {
		if err := f.repo.DeleteRecordByDocumentID(f.ctx, tx, "v1"); err != nil {
			return err
		}
		return f.repo.MarkCommitLogGcCandidate(f.ctx, tx, "v1")
	})

	_, err = f.repo.GetSignedDocument(f.ctx, ccfsOf("v1"))
	require.ErrorIs(t, err, domain.ErrNotFound, "a deleted retained version is not served")
	_, err = f.repo.GetHierarchicalRecordPolicies(f.ctx, ccfsOf("v1"))
	require.ErrorIs(t, err, domain.ErrNotFound)

	sd, err = f.repo.GetSignedDocument(f.ctx, key)
	require.NoError(t, err)
	require.Equal(t, ccfsOf("v3"), *sd.CCFS, "deleting a retained version leaves the key alone")
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

// The counts of a target are grouped by the associations' schema, not the
// target record's own (record_keys carries a schema column too).
func TestAssociatedRecordCountsBySchema(t *testing.T) {
	f := newRecordKeysFixture(t)
	post := "cckv://" + recordKeysOwner + "/posts/1"
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	f.commitRecord("counts-post", post, at, nil, nil)
	f.associate("counts-like-1", post, at.Add(time.Second))
	f.associate("counts-like-2", post, at.Add(2*time.Second))

	counts, err := f.repo.GetAssociatedRecordCountsBySchema(f.ctx, post)
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"https://schema.example/like.json": 2}, counts)
}
