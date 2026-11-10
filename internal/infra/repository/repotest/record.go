package repotest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/schemas"
)

// RunRecordSuite runs the record.Repository contract tests of records, keys,
// entities and the commit log. open must return a fresh, isolated backend
// per call; it is called once per top-level test.
func RunRecordSuite(t *testing.T, open func(t *testing.T) Backend) {
	t.Run("ApplyRecord", func(t *testing.T) { testApplyRecord(t, open(t)) })
	t.Run("DuplicateLeavesNoTrace", func(t *testing.T) { testDuplicateLeavesNoTrace(t, open(t)) })
	t.Run("OnUpdate", func(t *testing.T) { testRecordOnUpdate(t, open(t)) })
	t.Run("ApplyEntity", func(t *testing.T) { testApplyEntity(t, open(t)) })
	t.Run("ReferenceRecord", func(t *testing.T) { testReferenceRecord(t, open(t)) })
	t.Run("SubtreeQuery", func(t *testing.T) { testRecordSubtreeQuery(t, open(t)) })
	t.Run("QueryAuthorFilter", func(t *testing.T) { testRecordQueryAuthorFilter(t, open(t)) })
	t.Run("QueryByCreatedAt", func(t *testing.T) { testRecordQueryByCreatedAt(t, open(t)) })
	t.Run("HierarchicalRecordPoliciesRootFirst", func(t *testing.T) { testHierarchicalRecordPoliciesRootFirst(t, open(t)) })
	t.Run("HierarchicalRecordPoliciesEmitsPolicylessDistributingLevel", func(t *testing.T) {
		testHierarchicalRecordPoliciesEmitsPolicylessDistributingLevel(t, open(t))
	})
	t.Run("ApplyRecordOverParentPlaceholder", func(t *testing.T) { testApplyRecordOverParentPlaceholder(t, open(t)) })
	t.Run("ApplyRecordFreshKeyRace", func(t *testing.T) { testApplyRecordFreshKeyRace(t, open(t)) })
	t.Run("GetAllCommitLogsByOwner", func(t *testing.T) { testGetAllCommitLogsByOwner(t, open(t)) })
	t.Run("QueryByParentOrderByKey", func(t *testing.T) { testRecordQueryByParentOrderByKey(t, open(t)) })
	t.Run("GetSignedDocumentByCCFS", func(t *testing.T) { testGetSignedDocumentByCCFS(t, open(t)) })
	t.Run("GetSignedDocuments", func(t *testing.T) { testGetSignedDocuments(t, open(t)) })
	t.Run("HierarchicalRecordPoliciesMulti", func(t *testing.T) { testHierarchicalRecordPoliciesMulti(t, open(t)) })
	t.Run("GetEntityByCCIDAndAlias", func(t *testing.T) { testGetEntityByCCIDAndAlias(t, open(t)) })
}

// A record is stored with its commit and read back whole from its key; a
// newer document takes the key and reports what it replaced; an older or
// equal one is Stale and leaves nothing behind, not even its commit.
func testApplyRecord(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	key := "cckv://con1owner/timeline/post-1"
	createdAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	distributions := []string{"cckv://con1channel/timeline"}
	href := "cckv://con1owner/timeline/proof"

	oldRC := recordCommit(t, "record-1-old", key, createdAt, func(rc *record.RecordCommit) {
		rc.Author = "con1author"
		rc.Distributions = distributions
	})
	oldRC.Proof.Href = &href
	outcome, err := repo.ApplyRecord(ctx, oldRC)
	require.NoError(t, err)
	require.Equal(t, record.RecordOutcome{Status: record.Applied, FreshKey: true}, outcome)

	t.Run("the commit is in the log as given", func(t *testing.T) {
		entry := commitEntry(t, ctx, repo, "record-1-old")
		require.NotNil(t, entry)
		require.Equal(t, "127.0.0.1", entry.IP)
		require.Equal(t, oldRC.Document, entry.Document)
		require.Equal(t, oldRC.Proof, entry.Proof, "the proof round-trips, optional fields included")
		require.Equal(t, "con1owner", entry.Owner)
		require.False(t, entry.CDate.IsZero())
		require.False(t, gcFlagged(t, ctx, repo, "record-1-old"))
	})

	t.Run("the key resolves to the complete document", func(t *testing.T) {
		got := resolve(t, ctx, repo, key)
		require.NotNil(t, got)
		require.Equal(t, oldRC.Document, got.Document)
		require.Equal(t, oldRC.Proof, got.Proof)
		require.Equal(t, key, *got.CCKV)
		require.Equal(t, ccfsOf("con1owner", "record-1-old"), *got.CCFS)

		rows, err := repo.QueryByParent(ctx, "cckv://con1owner/timeline", postSchema, "con1author", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, got, &rows[0].Row, "a listing row is the same complete document")
		require.True(t, rows[0].CreatedAt.Equal(createdAt), "the row's sort key is the record's createdAt")

		stack, err := repo.GetHierarchicalRecordPolicies(ctx, key)
		require.NoError(t, err)
		require.Len(t, stack, 1)
		require.Equal(t, distributions, *stack[0].VirtualParents, "the distributions are stored with the record")
	})

	newCreatedAt := createdAt.Add(24 * time.Hour)
	newRC := recordCommit(t, "record-2-new", key, newCreatedAt, func(rc *record.RecordCommit) {
		rc.Author = "con1author"
		rc.Schema = "https://schema.example/post.v2.json"
	})

	t.Run("a newer document takes the key", func(t *testing.T) {
		outcome, err := repo.ApplyRecord(ctx, newRC)
		require.NoError(t, err)
		require.Equal(t, record.RecordOutcome{Status: record.Applied, PreviousID: "record-1-old"}, outcome)

		got := resolve(t, ctx, repo, key)
		require.NotNil(t, got)
		require.Equal(t, newRC.Document, got.Document)
		require.Equal(t, ccfsOf("con1owner", "record-2-new"), *got.CCFS)

		rows, err := repo.QueryByParent(ctx, "cckv://con1owner/timeline", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"record-2-new"}, rowIDs(t, rows), "the key lists once, as its current version")
		require.True(t, rows[0].CreatedAt.Equal(newCreatedAt))

		rows, err = repo.QueryByParent(ctx, "cckv://con1owner/timeline", postSchema, "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Empty(t, rows, "the replaced version's schema no longer matches")

		stack, err := repo.GetHierarchicalRecordPolicies(ctx, key)
		require.NoError(t, err)
		require.Empty(t, stack, "the new version distributes nothing")

		// onUpdate defaults to forget: the replaced commit is up for gc
		require.True(t, gcFlagged(t, ctx, repo, "record-1-old"))
		require.False(t, gcFlagged(t, ctx, repo, "record-2-new"))
	})

	// accept-if-newer: a replayed older (or equal-createdAt) document must
	// not roll the key back, and stores nothing at all
	t.Run("an older or equal document is stale and leaves no trace", func(t *testing.T) {
		for id, at := range map[string]time.Time{"record-0-stale": createdAt, "record-9-same-time": newCreatedAt} {
			outcome, err := repo.ApplyRecord(ctx, recordCommit(t, id, key, at))
			require.NoError(t, err)
			require.Equal(t, record.Stale, outcome.Status, id)
			require.False(t, commitExists(t, ctx, repo, id), "a stale write must not leave its commit")
			require.Nil(t, resolve(t, ctx, repo, ccfsOf("con1owner", id)))
		}

		got := resolve(t, ctx, repo, key)
		require.NotNil(t, got)
		require.Equal(t, newRC.Document, got.Document)
		require.False(t, gcFlagged(t, ctx, repo, "record-2-new"), "the stored version is untouched")
	})

	t.Run("a stale write creates no ancestor placeholders", func(t *testing.T) {
		deep := "cckv://con1owner/stale/a/b"
		putRecord(t, ctx, repo, "deep-2-new", deep, newCreatedAt)
		removeKeys(t, ctx, repo, "deep-delete", "con1owner", deep)
		// the key is gone; its ancestors remain as placeholders, so a stale
		// write cannot be told from one that created them. Use a tree of
		// its own: the losing write's ancestors must not appear
		lone := "cckv://con1owner/stale2/x/y"
		putRecord(t, ctx, repo, "lone-2-new", lone, newCreatedAt)
		outcome, err := repo.ApplyRecord(ctx, recordCommit(t, "lone-1-old", lone, createdAt))
		require.NoError(t, err)
		require.Equal(t, record.Stale, outcome.Status)
		rows, err := repo.QueryByParentOrderByKey(ctx, "cckv://con1owner/stale2/x", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{lone}, rowKeys(t, rows))
	})
}

// A commit id already in the log makes every Apply* a Duplicate: nothing
// the second call describes is stored and the log entry stays as first
// written.
func testDuplicateLeavesNoTrace(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	key := "cckv://con1owner/dup/first"
	first, _ := putRecord(t, ctx, repo, "dup-1", key, at)

	sameLog := func(t *testing.T) {
		t.Helper()
		entry := commitEntry(t, ctx, repo, "dup-1")
		require.NotNil(t, entry)
		require.Equal(t, first.Document, entry.Document)
		require.Equal(t, "con1owner", entry.Owner)
		require.Equal(t, "127.0.0.1", entry.IP)
		rows, err := repo.QueryCommitLogs(ctx, "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Len(t, rows, 1, "the log holds the one commit")
	}

	t.Run("record", func(t *testing.T) {
		other := recordCommit(t, "dup-1", "cckv://con1other/dup/second", at.Add(time.Hour))
		other.IP = "192.0.2.1"
		outcome, err := repo.ApplyRecord(ctx, other)
		require.NoError(t, err)
		require.Equal(t, record.Duplicate, outcome.Status)
		require.Nil(t, resolve(t, ctx, repo, "cckv://con1other/dup/second"))
		rows, err := repo.QueryByParentOrderByKey(ctx, "cckv://con1other", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Empty(t, rows, "not even a placeholder of the second key's ancestors")

		// the same document again, the redelivery proper
		outcome, err = repo.ApplyRecord(ctx, first)
		require.NoError(t, err)
		require.Equal(t, record.Duplicate, outcome.Status)
		sameLog(t)
	})

	t.Run("entity", func(t *testing.T) {
		status, err := repo.ApplyEntity(ctx, record.EntityCommit{
			Commit: record.Commit{ID: "dup-1", Document: "{}", Owner: "con1dupentity"},
			CCID:   "con1dupentity", Domain: "example.com", CreatedAt: at,
		})
		require.NoError(t, err)
		require.Equal(t, record.Duplicate, status)
		_, err = repo.GetEntityByCCID(ctx, "con1dupentity")
		require.True(t, errors.Is(err, domain.ErrNotFound))
		sameLog(t)
	})

	t.Run("association", func(t *testing.T) {
		status, err := repo.ApplyAssociation(ctx, record.AssociationCommit{
			Commit: record.Commit{ID: "dup-1", Document: "{}", Owner: "con1owner"},
			Target: key, Author: "con1author", Schema: "https://schema.example/like.json", Unique: "dup-unique", CreatedAt: at,
		})
		require.NoError(t, err)
		require.Equal(t, record.Duplicate, status)
		rows, err := repo.GetAssociatedRecords(ctx, key, "", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Empty(t, rows)
		sameLog(t)
	})

	t.Run("ack", func(t *testing.T) {
		outcome, err := repo.ApplyAck(ctx, record.AckCommit{
			Commit: record.Commit{ID: "dup-1", Document: "{}", Owner: "con1author"},
			Side:   record.AckerSide, From: "con1author", To: "con1owner", Schema: "https://schema.example/follow.json", Valid: true, CreatedAt: at,
		})
		require.NoError(t, err)
		require.Equal(t, record.Duplicate, outcome.Status)
		rows, err := repo.GetAcknowledgeRecords(ctx, "con1author", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Empty(t, rows)
		sameLog(t)
	})

	t.Run("delete", func(t *testing.T) {
		outcome, err := repo.ApplyDelete(ctx, record.DeleteCommit{
			Commit:      &record.Commit{ID: "dup-1", Document: "{}", Owner: "con1owner"},
			Removals:    []record.Removal{{Kind: record.RemoveKey, URI: key}},
			GcCommitIDs: []string{"dup-1"},
		})
		require.NoError(t, err)
		require.Equal(t, record.Duplicate, outcome.Status)
		require.Empty(t, outcome.Removed)
		require.Empty(t, outcome.Cleanups)
		require.NotNil(t, resolve(t, ctx, repo, key), "a duplicate delete removes nothing")
		require.False(t, gcFlagged(t, ctx, repo, "dup-1"), "and flags nothing")
		sameLog(t)
	})
}

// CIP-1 §5.7: omitting onUpdate means forget — replacing the key flags the
// old commit for gc. Only an explicit "retain" on the version being replaced
// keeps it. Either way the old document stays readable by its ccfs URI for
// as long as its commit is in the log, without a key pointing at it.
func testRecordOnUpdate(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	for _, tc := range []struct {
		name     string
		onUpdate *string
		forgets  bool
	}{
		{"default", nil, true},
		{"forget", ptr("forget"), true},
		{"retain", ptr("retain"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := "cckv://con1owner/timeline/onupdate-" + tc.name
			oldID := "onupdate-" + tc.name + "-1-old"
			newID := "onupdate-" + tc.name + "-2-new"
			at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			withOnUpdate := func(rc *record.RecordCommit) { rc.OnUpdate = tc.onUpdate }

			old, _ := putRecord(t, ctx, repo, oldID, key, at, withOnUpdate)
			_, outcome := putRecord(t, ctx, repo, newID, key, at.Add(time.Second), withOnUpdate)
			require.Equal(t, oldID, outcome.PreviousID)
			require.False(t, outcome.FreshKey)

			require.Equal(t, tc.forgets, gcFlagged(t, ctx, repo, oldID))
			require.False(t, gcFlagged(t, ctx, repo, newID))

			got := resolve(t, ctx, repo, ccfsOf("con1owner", oldID))
			require.NotNil(t, got, "the replaced document is still in the log")
			require.Equal(t, old.Document, got.Document)
			require.Nil(t, got.CCKV, "the key has moved on from it")

			current := resolve(t, ctx, repo, key)
			require.NotNil(t, current)
			require.Equal(t, ccfsOf("con1owner", newID), *current.CCFS)
		})
	}
}

// Entities are accept-if-newer on createdAt alone (CIP-3 §3.4) and exempt
// from onUpdate (CIP-0 §8.4): replacing an entity document keeps the old
// commit unflagged.
func testApplyEntity(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	ccid := "con1entity"
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	entityCommit := func(id, domainName string, createdAt time.Time) record.EntityCommit {
		sd := SignedDocument(t, concrnt.Document[schemas.Entity]{
			Kind:      "entity",
			Value:     schemas.Entity{Domain: domainName},
			Author:    ccid,
			Schema:    schemas.EntityURL,
			CreatedAt: createdAt,
			OnUpdate:  ptr("forget"),
		})
		return record.EntityCommit{Commit: commitOf(id, sd, ccid), CCID: ccid, Domain: domainName, CreatedAt: createdAt}
	}

	first := entityCommit("entity-1-old", "example.com", at)
	status, err := repo.ApplyEntity(ctx, first)
	require.NoError(t, err)
	require.Equal(t, record.Applied, status)

	second := entityCommit("entity-2-new", "example.org", at.Add(time.Second))
	status, err = repo.ApplyEntity(ctx, second)
	require.NoError(t, err)
	require.Equal(t, record.Applied, status)

	entity, err := repo.GetEntityByCCID(ctx, ccid)
	require.NoError(t, err)
	require.Equal(t, "example.org", entity.Domain)
	require.Equal(t, second.Document, entity.SignedDocument.Document)
	require.False(t, gcFlagged(t, ctx, repo, "entity-1-old"), "the replaced entity document is kept")
	require.True(t, commitExists(t, ctx, repo, "entity-1-old"))

	// the key is createdAt alone: an equal-createdAt document is stale
	// whatever its id, an older one too
	for id, createdAt := range map[string]time.Time{"entity-9-same-time": at.Add(time.Second), "entity-0-older": at} {
		status, err := repo.ApplyEntity(ctx, entityCommit(id, "other.example.net", createdAt))
		require.NoError(t, err)
		require.Equal(t, record.Stale, status, id)
		require.False(t, commitExists(t, ctx, repo, id), "a stale write must not leave its commit")
	}
	entity, err = repo.GetEntityByCCID(ctx, ccid)
	require.NoError(t, err)
	require.Equal(t, "example.org", entity.Domain)
	require.Equal(t, second.Document, entity.SignedDocument.Document)
}

// A reference record is listed under its target's schema and createdAt and
// stands in a chunkline body as its target; GetTimelineRemoval names the
// same item id the body serves.
func testReferenceRecord(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	timeline := "cckv://con1owner/timeline"
	plainKey := timeline + "/post-1"
	plainAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	putRecord(t, ctx, repo, "plain-record", plainKey, plainAt)

	refKey := timeline + "/ref-1"
	targetURI := "cckv://con1target/timeline/post-2"
	targetSchema := "https://schema.example/target.json"
	targetCreatedAt := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	putRecord(t, ctx, repo, "reference-record", refKey, targetCreatedAt, func(rc *record.RecordCommit) {
		rc.Schema = targetSchema
		rc.Redirect = &targetURI
	})

	rows, err := repo.QueryByParent(ctx, timeline, targetSchema, "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Equal(t, []string{refKey}, rowKeys(t, rows), "the reference lists under the schema it was stored with")
	require.True(t, rows[0].CreatedAt.Equal(targetCreatedAt))

	// plain record: the item id is the record key
	tl, itemID, err := repo.GetTimelineRemoval(ctx, plainKey)
	require.NoError(t, err)
	require.Equal(t, timeline, tl)
	require.Equal(t, plainKey, itemID)

	// reference record: the item id is the redirect target
	tl, itemID, err = repo.GetTimelineRemoval(ctx, refKey)
	require.NoError(t, err)
	require.Equal(t, timeline, tl)
	require.Equal(t, targetURI, itemID)

	// the advertised id must equal the BodyItem.ID() the body endpoint
	// serves for the same member, or readers can't match them up
	for wantID, at := range map[string]time.Time{plainKey: plainAt, targetURI: targetCreatedAt} {
		items, err := repo.LoadLocalBody(ctx, timeline, at.Unix()/600)
		require.NoError(t, err)
		ids := make([]string, 0, len(items))
		for _, item := range items {
			ids = append(ids, item.ID())
		}
		require.Contains(t, ids, wantID)
	}

	// the timeline key itself (a placeholder here) is no chunkline member
	tl, itemID, err = repo.GetTimelineRemoval(ctx, timeline)
	require.NoError(t, err)
	require.Empty(t, tl)
	require.Empty(t, itemID)

	_, _, err = repo.GetTimelineRemoval(ctx, timeline+"/no-such-key")
	require.True(t, errors.Is(err, domain.ErrNotFound), "an unknown key is not found, got %v", err)
}

// QueryRecordSubtree must match by path hierarchy — never siblings sharing a
// string prefix, never keys that only match via unescaped pattern
// metacharacters — in key order, paged by an exclusive cursor.
func testRecordSubtreeQuery(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	base := "cckv://con1owner/lists/item"
	keys := []string{
		base,                             // the base record itself
		base + "/a",                      // child
		base + "/a/b",                    // grandchild
		base + "2",                       // sibling sharing the string prefix — never in the subtree
		"cckv://con1owner/lists/it_em/x", // LIKE-metacharacter decoy: '_' matches any char unescaped
		"cckv://con1owner/lists/ITEM/y",  // case decoy: keys are case-sensitive
	}
	createdAt := time.Date(2026, 7, 8, 9, 10, 11, 0, time.UTC)
	for i, key := range keys {
		putRecord(t, ctx, repo, fmt.Sprintf("subtree-%d", i), key, createdAt)
	}

	urisOf := func(sds []concrnt.SignedDocument) []string {
		uris := make([]string, 0, len(sds))
		for _, sd := range sds {
			require.NotNil(t, sd.CCKV)
			require.NotNil(t, sd.CCFS)
			require.NotEmpty(t, sd.Document)
			uris = append(uris, *sd.CCKV)
		}
		return uris
	}

	t.Run("subtree including self", func(t *testing.T) {
		sds, err := repo.QueryRecordSubtree(ctx, base, true, "", 0)
		require.NoError(t, err)
		require.Equal(t, []string{base, base + "/a", base + "/a/b"}, urisOf(sds))
	})

	t.Run("children only", func(t *testing.T) {
		sds, err := repo.QueryRecordSubtree(ctx, base, false, "", 0)
		require.NoError(t, err)
		require.Equal(t, []string{base + "/a", base + "/a/b"}, urisOf(sds))
	})

	t.Run("metacharacters in base are escaped", func(t *testing.T) {
		// unescaped, 'it_em' would also match 'it/em', 'item', ... — it must
		// only match its own literal subtree
		sds, err := repo.QueryRecordSubtree(ctx, "cckv://con1owner/lists/it_em", true, "", 0)
		require.NoError(t, err)
		require.Equal(t, []string{"cckv://con1owner/lists/it_em/x"}, urisOf(sds))
	})

	t.Run("case differs, subtree differs", func(t *testing.T) {
		sds, err := repo.QueryRecordSubtree(ctx, "cckv://con1owner/lists/ITEM", true, "", 0)
		require.NoError(t, err)
		require.Equal(t, []string{"cckv://con1owner/lists/ITEM/y"}, urisOf(sds))
	})

	t.Run("pages in key order", func(t *testing.T) {
		// the cursor and the limit page a large subtree one write at a time
		// (the range delete jobs); the page after the last key is empty
		sds, err := repo.QueryRecordSubtree(ctx, base, true, "", 2)
		require.NoError(t, err)
		require.Equal(t, []string{base, base + "/a"}, urisOf(sds))

		sds, err = repo.QueryRecordSubtree(ctx, base, true, base+"/a", 2)
		require.NoError(t, err)
		require.Equal(t, []string{base + "/a/b"}, urisOf(sds))

		sds, err = repo.QueryRecordSubtree(ctx, base, true, base+"/a/b", 2)
		require.NoError(t, err)
		require.Empty(t, sds)

		sds, err = repo.QueryRecordSubtree(ctx, base, false, base, 0)
		require.NoError(t, err)
		require.Equal(t, []string{base + "/a", base + "/a/b"}, urisOf(sds), "the cursor is exclusive and ignores includeSelf")
	})

	t.Run("deleting the enumerated keys removes exactly the subtree", func(t *testing.T) {
		outcome := removeKeys(t, ctx, repo, "subtree-delete", "con1owner", base, base+"/a", base+"/a/b")
		require.Equal(t, []bool{true, true, true}, outcome.Removed)

		for _, uri := range []string{base, base + "/a", base + "/a/b"} {
			require.Nil(t, resolve(t, ctx, repo, uri), "key %s must no longer resolve", uri)
		}
		sds, err := repo.QueryRecordSubtree(ctx, base, true, "", 0)
		require.NoError(t, err)
		require.Empty(t, sds)

		// siblings and decoys survive
		for _, uri := range keys[3:] {
			require.NotNil(t, resolve(t, ctx, repo, uri), "key %s must survive", uri)
		}
		// commits of the deleted records stay (delete never erases history)
		for i := range 3 {
			require.True(t, commitExists(t, ctx, repo, fmt.Sprintf("subtree-%d", i)), "commit %d must survive", i)
		}
	})
}

// The query API's author filter narrows list queries to documents written by
// one author — e.g. an AP outbox enumerating only the user's own posts out of
// their timeline. All keys in a timeline live in the timeline owner's space,
// so the discriminating column is the document author (which distribute
// references preserve: the document-reference proof forces the reference
// document's author to match the target's).
func testRecordQueryAuthorFilter(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	parent := "cckv://con1owner/tl"
	entries := []struct {
		key    string
		author string
	}{
		{parent + "/e1", "con1alice"},
		{parent + "/e2", "con1bob"},
		{parent + "/e3", "con1alice"},
	}
	createdAt := time.Date(2026, 8, 14, 9, 10, 11, 0, time.UTC)
	for i, e := range entries {
		putRecord(t, ctx, repo, fmt.Sprintf("authorfilter-%d", i), e.key, createdAt.Add(time.Duration(i)*time.Second), func(rc *record.RecordCommit) {
			rc.Author = e.author
		})
	}

	t.Run("by parent with author", func(t *testing.T) {
		rows, err := repo.QueryByParent(ctx, parent, "", "con1alice", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/e1", parent + "/e3"}, rowKeys(t, rows))
	})

	t.Run("by parent without author returns all", func(t *testing.T) {
		rows, err := repo.QueryByParent(ctx, parent, "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/e1", parent + "/e2", parent + "/e3"}, rowKeys(t, rows))
	})

	t.Run("by prefix with author", func(t *testing.T) {
		rows, err := repo.QueryByPrefix(ctx, parent+"/", "", "con1bob", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/e2"}, rowKeys(t, rows))
	})

	// the prefix is a literal: "_" in it matches itself only (a raw "%" cannot
	// appear in a key, so only "_" is observable)
	t.Run("prefix metacharacters are literal", func(t *testing.T) {
		for i, key := range []string{parent + "/t_1", parent + "/tx1"} {
			putRecord(t, ctx, repo, fmt.Sprintf("prefixmeta-%d", i), key, createdAt.Add(time.Duration(10+i)*time.Second))
		}

		rows, err := repo.QueryByPrefix(ctx, parent+"/t_", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/t_1"}, rowKeys(t, rows), "underscore must not match any character")
	})
}

// The createdAt listings order by (createdAt, id) in either direction,
// window inclusively, bound the page, and a prefix reaches below direct
// children while a parent does not.
func testRecordQueryByCreatedAt(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	parent := "cckv://con1owner/feed"
	t0 := time.Date(2026, 8, 14, 9, 0, 0, 0, time.UTC)
	// two records share a createdAt: the id breaks the tie
	putRecord(t, ctx, repo, "feed-a", parent+"/k3", t0)
	putRecord(t, ctx, repo, "feed-b", parent+"/k1", t0.Add(time.Minute))
	putRecord(t, ctx, repo, "feed-c", parent+"/k2", t0.Add(time.Minute))
	putRecord(t, ctx, repo, "feed-d", parent+"/sub/k4", t0.Add(2*time.Minute))

	for name, query := range map[string]func(since, until *time.Time, limit int, order string) ([]record.QueryRow, error){
		"parent": func(since, until *time.Time, limit int, order string) ([]record.QueryRow, error) {
			return repo.QueryByParent(ctx, parent, "", "", since, until, limit, order)
		},
		"prefix": func(since, until *time.Time, limit int, order string) ([]record.QueryRow, error) {
			return repo.QueryByPrefix(ctx, parent+"/k", "", "", since, until, limit, order)
		},
	} {
		t.Run(name, func(t *testing.T) {
			rows, err := query(nil, nil, 0, "asc")
			require.NoError(t, err)
			require.Equal(t, []string{"feed-a", "feed-b", "feed-c"}, rowIDs(t, rows))

			rows, err = query(nil, nil, 2, "desc")
			require.NoError(t, err)
			require.Equal(t, []string{"feed-c", "feed-b"}, rowIDs(t, rows))

			rows, err = query(ptr(t0.Add(time.Minute)), nil, 0, "asc")
			require.NoError(t, err)
			require.Equal(t, []string{"feed-b", "feed-c"}, rowIDs(t, rows), "since is inclusive")

			rows, err = query(nil, ptr(t0), 0, "desc")
			require.NoError(t, err)
			require.Equal(t, []string{"feed-a"}, rowIDs(t, rows), "until is inclusive")
		})
	}

	rows, err := repo.QueryByPrefix(ctx, parent+"/", "", "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Equal(t, []string{"feed-a", "feed-b", "feed-c", "feed-d"}, rowIDs(t, rows), "a prefix reaches the whole subtree")
}

// CIP-12 §5.3: the hierarchical policy stack is emitted root-first with the
// target resource itself last.
func testHierarchicalRecordPoliciesRootFirst(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	policyJSON := `{"entries":[{"url":"https://example.com/p.json"}]}`
	keys := []string{
		"cckv://con1powner/l1",
		"cckv://con1powner/l1/l2",
		"cckv://con1powner/l1/l2/l3",
	}
	for i, key := range keys {
		putRecord(t, ctx, repo, fmt.Sprintf("policy-record-%d", i), key, time.Date(2026, 6, 1, 0, 0, i, 0, time.UTC), func(rc *record.RecordCommit) {
			rc.Policies = &policyJSON
		})
	}

	stack, err := repo.GetHierarchicalRecordPolicies(ctx, keys[2])
	require.NoError(t, err)

	sources := make([]string, len(stack))
	for i, layer := range stack {
		sources[i] = layer.Source
	}
	require.Equal(t, keys, sources, "layers must be emitted root-first, self last")

	// a ccfs URI is mapped to the key pointing at it
	byCCFS, err := repo.GetHierarchicalRecordPolicies(ctx, ccfsOf("con1powner", "policy-record-2"))
	require.NoError(t, err)
	require.Equal(t, stack, byCCFS)

	_, err = repo.GetHierarchicalRecordPolicies(ctx, ccfsOf("con1powner", "policy-record-none"))
	require.True(t, errors.Is(err, domain.ErrNotFound), "a ccfs URI no key points at is not found, got %v", err)
}

// A level with no policy of its own but with distributions must still be
// emitted — as an empty layer carrying its virtual parents — otherwise the
// destination timelines' policies never reach the stack (CIP-12 §5.3).
func testHierarchicalRecordPoliciesEmitsPolicylessDistributingLevel(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	policyJSON := `{"entries":[{"url":"https://example.com/p.json"}]}`
	parentKey := "cckv://con1downer/d1"
	leafKey := "cckv://con1downer/d1/post"
	timeline := "cckv://con1tl/timelines/home"

	putRecord(t, ctx, repo, "distributing-record-0", parentKey, time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC), func(rc *record.RecordCommit) {
		rc.Policies = &policyJSON
	})
	putRecord(t, ctx, repo, "distributing-record-1", leafKey, time.Date(2026, 6, 2, 0, 0, 1, 0, time.UTC), func(rc *record.RecordCommit) {
		rc.Distributions = []string{timeline}
	})

	stack, err := repo.GetHierarchicalRecordPolicies(ctx, leafKey)
	require.NoError(t, err)

	require.Len(t, stack, 2)
	require.Equal(t, parentKey, stack[0].Source)
	require.Equal(t, leafKey, stack[1].Source)
	require.Empty(t, stack[1].Entries, "a policyless level is an empty layer")
	require.NotNil(t, stack[1].VirtualParents)
	require.Equal(t, []string{timeline}, *stack[1].VirtualParents)
}

// Committing a child materialises its ancestors as placeholders: listed as
// key-only rows, resolving to nothing. Writing a record to a key that so
// far only exists as a placeholder must apply, as a fresh key.
func testApplyRecordOverParentPlaceholder(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	parentKey := "cckv://con1ph/parent"
	childKey := "cckv://con1ph/parent/child"

	putRecord(t, ctx, repo, "placeholder-child", childKey, time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC))

	require.Nil(t, resolve(t, ctx, repo, parentKey), "a placeholder resolves to nothing")
	rows, err := repo.QueryByParentOrderByKey(ctx, "cckv://con1ph", "", "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Equal(t, []string{parentKey}, rowKeys(t, rows))
	require.Empty(t, rows[0].Row.Document, "the parent exists as a placeholder")

	_, outcome := putRecord(t, ctx, repo, "placeholder-parent", parentKey, time.Date(2026, 6, 3, 0, 0, 1, 0, time.UTC))
	require.True(t, outcome.FreshKey, "a placeholder held no record")
	require.Empty(t, outcome.PreviousID)

	rows, err = repo.QueryByParentOrderByKey(ctx, "cckv://con1ph", "", "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Equal(t, []string{"placeholder-parent"}, rowIDs(t, rows))

	// the child is still the parent's child
	rows, err = repo.QueryByParent(ctx, parentKey, "", "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Equal(t, []string{childKey}, rowKeys(t, rows))
}

// Two commits racing on a key that does not exist yet: whatever the
// interleaving, the newer document holds the key afterwards, and the
// outcomes tell one consistent story (the older one either lost, or was
// applied first and then replaced).
func testApplyRecordFreshKeyRace(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	oldAt := time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)
	newAt := oldAt.Add(time.Second)

	for round := range 20 {
		key := fmt.Sprintf("cckv://con1race/fresh/%02d", round)
		oldID, newID := fmt.Sprintf("race-%02d-1-old", round), fmt.Sprintf("race-%02d-2-new", round)
		commits := []record.RecordCommit{recordCommit(t, oldID, key, oldAt), recordCommit(t, newID, key, newAt)}
		outcomes := make([]record.RecordOutcome, 2)
		errs := make([]error, 2)

		var wg sync.WaitGroup
		for i := range commits {
			wg.Add(1)
			go func() {
				defer wg.Done()
				outcomes[i], errs[i] = repo.ApplyRecord(ctx, commits[i])
			}()
		}
		wg.Wait()
		require.NoError(t, errs[0])
		require.NoError(t, errs[1])
		older, newer := outcomes[0], outcomes[1]

		got := resolve(t, ctx, repo, key)
		require.NotNil(t, got)
		require.Equal(t, ccfsOf("con1race", newID), *got.CCFS, "the newer document must hold the key")
		require.Equal(t, record.Applied, newer.Status)

		switch older.Status {
		case record.Stale:
			require.True(t, newer.FreshKey)
			require.Empty(t, newer.PreviousID)
			require.False(t, commitExists(t, ctx, repo, oldID))
		case record.Applied:
			require.True(t, older.FreshKey)
			require.False(t, newer.FreshKey, "the newer document replaced the older one")
			require.Equal(t, oldID, newer.PreviousID)
			require.True(t, gcFlagged(t, ctx, repo, oldID), "the replaced version is forgotten")
		default:
			t.Fatalf("unexpected status %v for the older document", older.Status)
		}
	}
}

// The repository dump (/repository, conctl dump-commitlog) is scoped by the
// single owner recorded on each commit log, in commit order; other owners'
// and ownerless commits are excluded.
func testGetAllCommitLogsByOwner(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	userA, userB := "con1aaaa", "con1bbbb"
	putCommit(t, ctx, repo, "a-1", userA, `{"id":"a-1"}`)
	putCommit(t, ctx, repo, "b-1", userB, `{"id":"b-1"}`)
	putCommit(t, ctx, repo, "a-2", userA, `{"id":"a-2"}`)
	putCommit(t, ctx, repo, "unowned", "", `{"id":"unowned"}`)

	logs, err := repo.GetAllCommitLogs(ctx, userA)
	require.NoError(t, err)
	require.Len(t, logs, 2)
	require.Contains(t, logs[0].Document, `"id":"a-1"`)
	require.Contains(t, logs[1].Document, `"id":"a-2"`)
	require.Equal(t, concrnt.ProofTypeNone, logs[0].Proof.Type)

	logs, err = repo.GetAllCommitLogs(ctx, userB)
	require.NoError(t, err)
	require.Len(t, logs, 1)

	logs, err = repo.GetAllCommitLogs(ctx, "con1nobody")
	require.NoError(t, err)
	require.Empty(t, logs)
}

// CIP-5 orderby=key: a parent listing in cckv order that keeps placeholder
// (intermediate) keys as key-only rows, pages by inclusive key bounds, drops
// placeholders under a record-column filter, and accepts the owner root.
func testRecordQueryByParentOrderByKey(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	owner := "con1keyorder"
	root := "cckv://" + owner
	parent := root + "/app"
	keys := []string{parent + "/b", parent + "/a", parent + "/posts/1"}
	createdAt := time.Date(2026, 9, 21, 9, 0, 0, 0, time.UTC)
	for i, key := range keys {
		putRecord(t, ctx, repo, fmt.Sprintf("keyorder-%d", i), key, createdAt.Add(time.Duration(i)*time.Second), func(rc *record.RecordCommit) {
			rc.Schema = "https://schema.example/item.json"
		})
	}

	t.Run("ascending with the placeholder as a key-only row", func(t *testing.T) {
		rows, err := repo.QueryByParentOrderByKey(ctx, parent, "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/a", parent + "/b", parent + "/posts"}, rowKeys(t, rows))
		require.NotEmpty(t, rows[0].Row.Document)
		require.NotNil(t, rows[0].Row.CCFS)
		require.Empty(t, rows[2].Row.Document, "a placeholder key carries no document")
		require.Nil(t, rows[2].Row.CCFS)
		require.Equal(t, concrnt.Proof{}, rows[2].Row.Proof)
	})

	t.Run("descending", func(t *testing.T) {
		rows, err := repo.QueryByParentOrderByKey(ctx, parent, "", "", nil, nil, 0, "desc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/posts", parent + "/b", parent + "/a"}, rowKeys(t, rows))
	})

	t.Run("since and until are inclusive key bounds", func(t *testing.T) {
		since := parent + "/b"
		rows, err := repo.QueryByParentOrderByKey(ctx, parent, "", "", &since, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/b", parent + "/posts"}, rowKeys(t, rows))

		until := parent + "/b"
		rows, err = repo.QueryByParentOrderByKey(ctx, parent, "", "", nil, &until, 0, "desc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/b", parent + "/a"}, rowKeys(t, rows))

		rows, err = repo.QueryByParentOrderByKey(ctx, parent, "", "", nil, nil, 2, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/a", parent + "/b"}, rowKeys(t, rows))
	})

	t.Run("schema filter drops placeholders", func(t *testing.T) {
		rows, err := repo.QueryByParentOrderByKey(ctx, parent, "https://schema.example/item.json", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/a", parent + "/b"}, rowKeys(t, rows))

		rows, err = repo.QueryByParentOrderByKey(ctx, parent, "", "con1someoneelse", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Empty(t, rows)
	})

	t.Run("owner root lists the top-level keys", func(t *testing.T) {
		rows, err := repo.QueryByParentOrderByKey(ctx, root, "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent}, rowKeys(t, rows))
		require.Empty(t, rows[0].Row.Document)

		rows, err = repo.QueryByParentOrderByKey(ctx, "cckv://con1keyorderx", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Empty(t, rows, "a sibling owner sharing the prefix must not match")
	})

	// The order is byte order of the key, whatever the backend's default
	// collation does with case and punctuation: "B" < "a" < "a-b" < "a_b".
	t.Run("key order is byte order", func(t *testing.T) {
		parent := root + "/bytes"
		for i, key := range []string{parent + "/a_b", parent + "/B", parent + "/a-b", parent + "/a"} {
			putRecord(t, ctx, repo, fmt.Sprintf("byteorder-%d", i), key, createdAt)
		}

		rows, err := repo.QueryByParentOrderByKey(ctx, parent, "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/B", parent + "/a", parent + "/a-b", parent + "/a_b"}, rowKeys(t, rows))

		since := parent + "/a"
		rows, err = repo.QueryByParentOrderByKey(ctx, parent, "", "", &since, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/a", parent + "/a-b", parent + "/a_b"}, rowKeys(t, rows), "the since bound is a byte-order bound too")

		sds, err := repo.QueryRecordSubtree(ctx, parent, false, "", 0)
		require.NoError(t, err)
		uris := make([]string, 0, len(sds))
		for _, sd := range sds {
			uris = append(uris, *sd.CCKV)
		}
		require.Equal(t, []string{parent + "/B", parent + "/a", parent + "/a-b", parent + "/a_b"}, uris)
	})

	t.Run("createdAt ordering also accepts the owner root", func(t *testing.T) {
		rows, err := repo.QueryByParent(ctx, root, "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Empty(t, rows, "top-level placeholder keys have no document to list")

		rows, err = repo.QueryByParent(ctx, parent, "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/b", parent + "/a"}, rowKeys(t, rows), "placeholders stay out of the createdAt listing")
	})
}

// A ccfs URI resolves through the commit log: the document comes back with
// its CCKV filled in when a key currently points at it, and without one
// otherwise (a commit that never was or no longer is a record). A cckv
// placeholder key and an unknown id are not found; a non-concrnt ccfs type
// is not found either.
func testGetSignedDocumentByCCFS(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	key := "cckv://con1owner/notes/n1"
	rc, _ := putRecord(t, ctx, repo, "ccfs-record", key, time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC))
	putCommit(t, ctx, repo, "ccfs-keyless", "con1owner", "{}")

	ccfs := ccfsOf("con1owner", "ccfs-record")
	got, err := repo.GetSignedDocument(ctx, ccfs)
	require.NoError(t, err)
	require.Equal(t, rc.Document, got.Document)
	require.Equal(t, rc.Proof, got.Proof)
	require.NotNil(t, got.CCFS)
	require.Equal(t, ccfs, *got.CCFS)
	require.NotNil(t, got.CCKV)
	require.Equal(t, key, *got.CCKV)

	// the cckv side fills in the ccfs too
	got, err = repo.GetSignedDocument(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, got.CCFS)
	require.Equal(t, ccfs, *got.CCFS)

	keyless := ccfsOf("con1owner", "ccfs-keyless")
	got, err = repo.GetSignedDocument(ctx, keyless)
	require.NoError(t, err)
	require.Equal(t, "{}", got.Document)
	require.Nil(t, got.CCKV, "a commit no key points at has no cckv")

	_, err = repo.GetSignedDocument(ctx, ccfsOf("con1owner", "ccfs-unknown"))
	require.True(t, errors.Is(err, domain.ErrNotFound), "unknown id must be not found, got %v", err)

	_, err = repo.GetSignedDocument(ctx, "cckv://con1owner/notes")
	require.True(t, errors.Is(err, domain.ErrNotFound), "a placeholder key must be not found, got %v", err)

	_, err = repo.GetSignedDocument(ctx, "cckv://con1owner/notes/none")
	require.True(t, errors.Is(err, domain.ErrNotFound), "an unknown key must be not found, got %v", err)

	_, err = repo.GetSignedDocument(ctx, "ccfs://con1owner/"+concrnt.CCFSTypeBlob+"/"+strings.Repeat("0", 64))
	require.True(t, errors.Is(err, domain.ErrNotFound), "a non-concrnt ccfs type must be not found, got %v", err)
}

// GetSignedDocuments resolves a mixed batch of cckv and ccfs URIs in one go:
// each hit carries the same CCKV / CCFS as the single lookup would, misses
// (unknown keys and ids, placeholder keys, non-concrnt ccfs types) are
// simply absent, and an empty batch is an empty map.
func testGetSignedDocuments(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	createdAt := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	rc1, _ := putRecord(t, ctx, repo, "multi-1", "cckv://con1owner/posts/p1", createdAt)
	rc2, _ := putRecord(t, ctx, repo, "multi-2", "cckv://con1owner/posts/p2", createdAt)
	putCommit(t, ctx, repo, "multi-keyless", "con1owner", "{}")

	ccfs2 := ccfsOf("con1owner", "multi-2")
	keyless := ccfsOf("con1owner", "multi-keyless")
	uris := []string{
		"cckv://con1owner/posts/p1",
		ccfs2,
		keyless,
		"cckv://con1owner/posts/none",
		"cckv://con1owner/posts", // placeholder
		ccfsOf("con1owner", "multi-none"),
		"ccfs://con1owner/" + concrnt.CCFSTypeBlob + "/" + strings.Repeat("0", 64),
	}
	got, err := repo.GetSignedDocuments(ctx, uris)
	require.NoError(t, err)
	require.Len(t, got, 3)

	p1 := got["cckv://con1owner/posts/p1"]
	require.NotNil(t, p1)
	require.Equal(t, rc1.Document, p1.Document)
	require.Equal(t, rc1.Proof, p1.Proof)
	require.Equal(t, "cckv://con1owner/posts/p1", *p1.CCKV)
	require.Equal(t, ccfsOf("con1owner", "multi-1"), *p1.CCFS)

	p2 := got[ccfs2]
	require.NotNil(t, p2)
	require.Equal(t, rc2.Document, p2.Document)
	require.Equal(t, ccfs2, *p2.CCFS)
	require.Equal(t, "cckv://con1owner/posts/p2", *p2.CCKV)

	kl := got[keyless]
	require.NotNil(t, kl)
	require.Equal(t, "{}", kl.Document)
	require.Nil(t, kl.CCKV)

	single, err := repo.GetSignedDocument(ctx, ccfs2)
	require.NoError(t, err)
	require.Equal(t, single, p2, "the batch result equals the single lookup")

	got, err = repo.GetSignedDocuments(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

// The multi variant assembles the same stacks the single lookup does, one
// per input URI, including URIs with no policies at all.
func testHierarchicalRecordPoliciesMulti(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	policyJSON := `{"entries":[{"url":"https://example.com/p.json"}]}`
	dist := []string{"cckv://con1elsewhere/tl"}
	for i, key := range []string{"cckv://con1multi/l1", "cckv://con1multi/l1/l2", "cckv://con1multi/other"} {
		putRecord(t, ctx, repo, fmt.Sprintf("multi-policy-%d", i), key, time.Date(2026, 6, 1, 0, 0, i, 0, time.UTC), func(rc *record.RecordCommit) {
			switch i {
			case 0:
				rc.Policies = ptr(policyJSON)
			case 1:
				rc.Distributions = dist
			}
		})
	}

	uris := []string{"cckv://con1multi/l1/l2", "cckv://con1multi/l1/l2/l3", "cckv://con1multi/other", "cckv://con1multi/l1/l2"}
	stacks, err := repo.GetHierarchicalRecordPoliciesMulti(ctx, uris)
	require.NoError(t, err)
	require.Len(t, stacks, 3)
	for _, uri := range uris {
		single, err := repo.GetHierarchicalRecordPolicies(ctx, uri)
		require.NoError(t, err)
		require.Equal(t, single, stacks[uri], "stack of %s", uri)
	}
	require.Len(t, stacks["cckv://con1multi/l1/l2"], 2, "l1's policy layer, then l2's distributing layer")
	require.Equal(t, "cckv://con1multi/l1", stacks["cckv://con1multi/l1/l2"][0].Source)
	require.Equal(t, "cckv://con1multi/l1/l2", stacks["cckv://con1multi/l1/l2"][1].Source)
	require.Equal(t, dist, *stacks["cckv://con1multi/l1/l2"][1].VirtualParents)
	require.Empty(t, stacks["cckv://con1multi/other"])

	_, err = repo.GetHierarchicalRecordPoliciesMulti(ctx, []string{ccfsOf("con1multi", "multi-policy-0")})
	require.Error(t, err, "the multi lookup is keyed by cckv")

	stacks, err = repo.GetHierarchicalRecordPoliciesMulti(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, stacks)
}

// An entity is read back by ccid or by alias together with the signed
// document that registered it; unknown lookups are not found.
func testGetEntityByCCIDAndAlias(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	ccid := "con1entityentityentityentityentityentityen"
	alias := "alice.example.com"
	createdAt := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)
	sd := SignedDocument(t, concrnt.Document[schemas.Entity]{
		Kind:      "entity",
		Value:     schemas.Entity{Domain: "example.com"},
		Author:    ccid,
		Schema:    schemas.EntityURL,
		CreatedAt: createdAt,
	})
	status, err := repo.ApplyEntity(ctx, record.EntityCommit{
		Commit: commitOf("entity-lookup", sd, ccid), CCID: ccid, Alias: &alias, Domain: "example.com", CreatedAt: createdAt,
	})
	require.NoError(t, err)
	require.Equal(t, record.Applied, status)

	for _, lookup := range []struct {
		name string
		get  func() (*domain.Entity, error)
	}{
		{"ccid", func() (*domain.Entity, error) { return repo.GetEntityByCCID(ctx, ccid) }},
		{"alias", func() (*domain.Entity, error) { return repo.GetEntityByAlias(ctx, alias) }},
	} {
		t.Run("by "+lookup.name, func(t *testing.T) {
			entity, err := lookup.get()
			require.NoError(t, err)
			require.Equal(t, ccid, entity.ID)
			require.Equal(t, "example.com", entity.Domain)
			require.NotNil(t, entity.Alias)
			require.Equal(t, alias, *entity.Alias)
			require.NotNil(t, entity.SignedDocument)
			require.Equal(t, sd.Document, entity.SignedDocument.Document)
			require.Equal(t, sd.Proof, entity.SignedDocument.Proof)
		})
	}

	_, err = repo.GetEntityByCCID(ctx, "con1nobodynobodynobodynobodynobodynobodyno")
	require.True(t, errors.Is(err, domain.ErrNotFound), "unknown ccid must be not found, got %v", err)
	_, err = repo.GetEntityByAlias(ctx, "nobody.example.com")
	require.True(t, errors.Is(err, domain.ErrNotFound), "unknown alias must be not found, got %v", err)
}
