package repotest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/usecase/record"
)

// RunDeleteSuite runs the ApplyDelete / Cleanup contract tests of
// record.Repository. open must return a fresh, isolated backend per call;
// it is called once per top-level test.
func RunDeleteSuite(t *testing.T, open func(t *testing.T) Backend) {
	t.Run("RemoveKey", func(t *testing.T) { testDeleteRemoveKey(t, open(t)) })
	t.Run("RemoveVersion", func(t *testing.T) { testDeleteRemoveVersion(t, open(t)) })
	t.Run("GcCommitIDs", func(t *testing.T) { testDeleteGcCommitIDs(t, open(t)) })
	t.Run("ContinuationPage", func(t *testing.T) { testDeleteContinuationPage(t, open(t)) })
	t.Run("Incarnation", func(t *testing.T) { testDeleteIncarnation(t, open(t)) })
	t.Run("CleanupIsIdempotentAndInvisible", func(t *testing.T) { testDeleteCleanup(t, open(t)) })
}

// RemoveKey removes the record a key points at and the key; the commits
// (the removed record's and the delete's) stay in the log. A key that
// holds no record (unknown, placeholder, already removed) is left alone
// and reported as not removed.
func testDeleteRemoveKey(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	at := time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC)
	key := "cckv://con1owner/timeline/delete-target"
	sibling := "cckv://con1owner/timeline/sibling"
	target, _ := putRecord(t, ctx, repo, "delete-target", key, at)
	putRecord(t, ctx, repo, "delete-sibling", sibling, at)

	outcome := removeKeys(t, ctx, repo, "delete-commit", "con1owner", key, "cckv://con1owner/timeline", "cckv://con1owner/timeline/unknown")
	require.Equal(t, []bool{true, false, false}, outcome.Removed)

	require.Nil(t, resolve(t, ctx, repo, key))
	rows, err := repo.QueryByParent(ctx, "cckv://con1owner/timeline", "", "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Equal(t, []string{sibling}, rowKeys(t, rows), "the removed key is no longer listed, its sibling is")
	_, _, err = repo.GetTimelineRemoval(ctx, sibling)
	require.NoError(t, err, "removing nothing from the placeholder parent left it in place")

	// delete never erases history: the document is still readable by id
	got := resolve(t, ctx, repo, ccfsOf("con1owner", "delete-target"))
	require.NotNil(t, got)
	require.Equal(t, target.Document, got.Document)
	require.Nil(t, got.CCKV)
	require.False(t, gcFlagged(t, ctx, repo, "delete-target"), "only GcCommitIDs flags a commit")

	entry := commitEntry(t, ctx, repo, "delete-commit")
	require.NotNil(t, entry)
	require.Equal(t, "con1owner", entry.Owner)

	t.Run("removing it again removes nothing", func(t *testing.T) {
		outcome := removeKeys(t, ctx, repo, "delete-commit-2", "con1owner", key)
		require.Equal(t, []bool{false}, outcome.Removed)
	})

	t.Run("the key can be written again, as a fresh key", func(t *testing.T) {
		// older than the removed version: accept-if-newer died with the key
		_, outcome := putRecord(t, ctx, repo, "delete-target-again", key, at.Add(-time.Hour))
		require.True(t, outcome.FreshKey)
		require.Empty(t, outcome.PreviousID)
	})
}

// RemoveVersion removes a record version by document id: the current
// version takes the key with it (as RemoveKey does), a version the key has
// moved on from leaves the key alone, an unknown one is not removed.
func testDeleteRemoveVersion(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	at := time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC)
	retain := func(rc *record.RecordCommit) { rc.OnUpdate = ptr("retain") }
	removeVersion := func(key, id string) bool {
		t.Helper()
		outcome, err := repo.ApplyDelete(ctx, record.DeleteCommit{Removals: []record.Removal{{Kind: record.RemoveVersion, URI: key, DocumentID: id}}})
		require.NoError(t, err)
		require.Equal(t, record.Applied, outcome.Status)
		require.Len(t, outcome.Removed, 1)
		return outcome.Removed[0]
	}

	t.Run("current version", func(t *testing.T) {
		key := "cckv://con1owner/versions/current"
		putRecord(t, ctx, repo, "version-current", key, at)
		putAssociation(t, ctx, repo, key, associationFixture{"version-current-like", "con1author", "https://schema.example/like.json", nil, at})

		require.True(t, removeVersion(key, "version-current"))
		require.Nil(t, resolve(t, ctx, repo, key))
		require.False(t, removeVersion(key, "version-current"), "a second removal finds nothing")

		_, outcome := putRecord(t, ctx, repo, "version-current-again", key, at.Add(time.Hour))
		require.True(t, outcome.FreshKey)
		rows, err := repo.GetAssociatedRecords(ctx, key, "", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Empty(t, rows, "the key's incarnation ended with its current version")
	})

	t.Run("retained older version", func(t *testing.T) {
		key := "cckv://con1owner/versions/retained"
		putRecord(t, ctx, repo, "version-1-old", key, at, retain)
		current, _ := putRecord(t, ctx, repo, "version-2-new", key, at.Add(time.Hour), retain)

		require.True(t, removeVersion(key, "version-1-old"))
		got := resolve(t, ctx, repo, key)
		require.NotNil(t, got, "the key stays with its current version")
		require.Equal(t, current.Document, got.Document)
		require.False(t, removeVersion(key, "version-1-old"))
	})

	t.Run("unknown version", func(t *testing.T) {
		require.False(t, removeVersion("cckv://con1owner/versions/none", "version-unknown"))
	})
}

// GcCommitIDs flags commits for gc in the same operation; ids the log does
// not hold are ignored, and flagging is idempotent.
func testDeleteGcCommitIDs(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	at := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)
	key := "cckv://con1owner/timeline/gc-flag"
	putRecord(t, ctx, repo, "gc-target", key, at)
	putRecord(t, ctx, repo, "gc-bystander", "cckv://con1owner/timeline/bystander", at)
	require.False(t, gcFlagged(t, ctx, repo, "gc-target"))

	// the shape of a single delete: remove the key, flag the removed
	// document's commit and the delete commit itself
	outcome, err := repo.ApplyDelete(ctx, record.DeleteCommit{
		Commit:      &record.Commit{ID: "gc-delete", Document: "{}", Owner: "con1owner"},
		Removals:    []record.Removal{{Kind: record.RemoveKey, URI: key}},
		GcCommitIDs: []string{"gc-target", "gc-delete", "no-such-commit"},
	})
	require.NoError(t, err)
	require.Equal(t, record.Applied, outcome.Status)
	require.Equal(t, []bool{true}, outcome.Removed)

	require.True(t, gcFlagged(t, ctx, repo, "gc-target"))
	require.True(t, gcFlagged(t, ctx, repo, "gc-delete"), "the delete commit of the same call can be flagged")
	require.False(t, gcFlagged(t, ctx, repo, "gc-bystander"))
	require.False(t, commitExists(t, ctx, repo, "no-such-commit"))

	outcome, err = repo.ApplyDelete(ctx, record.DeleteCommit{GcCommitIDs: []string{"gc-target"}})
	require.NoError(t, err)
	require.Equal(t, record.Applied, outcome.Status)
	require.True(t, gcFlagged(t, ctx, repo, "gc-target"))

	count, err := repo.CountGcCandidates(ctx, gcCutoff)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
}

// A delete without a commit is a continuation page of a range delete: it
// removes and flags like any other, adds nothing to the log, and running
// it again is harmless.
func testDeleteContinuationPage(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	at := time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC)
	base := "cckv://con1owner/range"
	keys := make([]string, 6)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s/k%d", base, i)
		putRecord(t, ctx, repo, fmt.Sprintf("range-%d", i), keys[i], at)
	}

	// first page: the commit and half the keys
	first := removeKeys(t, ctx, repo, "range-delete", "con1owner", keys[:3]...)
	require.Equal(t, []bool{true, true, true}, first.Removed)
	logged, err := repo.QueryCommitLogs(ctx, "", nil, nil, 0, "asc")
	require.NoError(t, err)

	page := record.DeleteCommit{GcCommitIDs: []string{"range-delete"}}
	for _, key := range keys[3:] {
		page.Removals = append(page.Removals, record.Removal{Kind: record.RemoveKey, URI: key})
	}
	outcome, err := repo.ApplyDelete(ctx, page)
	require.NoError(t, err)
	require.Equal(t, record.Applied, outcome.Status)
	require.Equal(t, []bool{true, true, true}, outcome.Removed)
	runCleanups(t, ctx, repo, append(first.Cleanups, outcome.Cleanups...))

	sds, err := repo.QueryRecordSubtree(ctx, base, false, "", 0)
	require.NoError(t, err)
	require.Empty(t, sds)
	require.True(t, gcFlagged(t, ctx, repo, "range-delete"))

	after, err := repo.QueryCommitLogs(ctx, "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Len(t, after, len(logged), "a continuation page writes no commit")

	// a retried job replays the page
	outcome, err = repo.ApplyDelete(ctx, page)
	require.NoError(t, err)
	require.Equal(t, record.Applied, outcome.Status)
	require.Equal(t, []bool{false, false, false}, outcome.Removed)

	outcome, err = repo.ApplyDelete(ctx, record.DeleteCommit{})
	require.NoError(t, err)
	require.Equal(t, record.Applied, outcome.Status, "an empty page is a no-op")
}

// Associations belong to the incarnation of the key they were created on
// (P4). Overwriting the record keeps the incarnation; removing the key
// ends it: from that operation on its associations are gone from every
// read and from uniqueness, and a record written to the key later starts
// with none.
func testDeleteIncarnation(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	like := "https://schema.example/like.json"
	at := time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC)
	key := "cckv://con1owner/timeline/incarnated"
	putRecord(t, ctx, repo, "incarnation-1", key, at)
	fixture := associationFixture{"like-1", "con1alice", like, ptr("👍"), at}
	putAssociation(t, ctx, repo, key, fixture)
	putAssociation(t, ctx, repo, key, associationFixture{"like-2", "con1bob", like, ptr("👍"), at.Add(time.Minute)})

	associations := func() []string {
		t.Helper()
		rows, err := repo.GetAssociatedRecords(ctx, key, "", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		return rowIDs(t, rows)
	}
	summary := func() record.AssociationSummary {
		t.Helper()
		summary, err := repo.ComputeAssociationSummary(ctx, key, 10)
		require.NoError(t, err)
		return summary
	}

	// an overwrite is the same incarnation
	putRecord(t, ctx, repo, "incarnation-1b", key, at.Add(time.Hour))
	require.Equal(t, []string{"like-1", "like-2"}, associations())
	require.EqualValues(t, 2, summary().Variants[0].Count)

	outcome := removeKeys(t, ctx, repo, "incarnation-delete", "con1owner", key)
	require.Equal(t, []bool{true}, outcome.Removed)

	// gone in the same operation, before any cleanup has run
	require.Empty(t, associations())
	require.Empty(t, summary().Variants)
	require.Empty(t, summary().Authors)

	_, recreated := putRecord(t, ctx, repo, "incarnation-2", key, at.Add(2*time.Hour))
	require.True(t, recreated.FreshKey)
	require.Empty(t, associations(), "the old incarnation's associations never reappear")
	require.Empty(t, summary().Variants)

	// the same logical association is new to the new incarnation
	again := associationCommit(t, key, associationFixture{"like-1-again", "con1alice", like, ptr("👍"), at.Add(3 * time.Hour)}, key+"#like-1")
	status, err := repo.ApplyAssociation(ctx, again)
	require.NoError(t, err)
	require.Equal(t, record.Applied, status)
	require.Equal(t, []string{"like-1-again"}, associations())

	// and within the new incarnation it is unique again
	dup := associationCommit(t, key, associationFixture{"like-1-dup", "con1alice", like, ptr("👍"), at.Add(4 * time.Hour)}, key+"#like-1")
	status, err = repo.ApplyAssociation(ctx, dup)
	require.NoError(t, err)
	require.Equal(t, record.Stale, status)

	// an association of the ended incarnation is not there to remove, even
	// while its physical removal is still pending
	stale, err := repo.ApplyDelete(ctx, record.DeleteCommit{Removals: []record.Removal{{Kind: record.RemoveAssociation, URI: key, DocumentID: "like-2"}}})
	require.NoError(t, err)
	require.Equal(t, record.Applied, stale.Status)
	require.Equal(t, []bool{false}, stale.Removed)

	// the deferred removal of the old incarnation must not touch the new one
	runCleanups(t, ctx, repo, append(outcome.Cleanups, stale.Cleanups...))
	require.Equal(t, []string{"like-1-again"}, associations())
	got := summary()
	require.Len(t, got.Variants, 1)
	require.EqualValues(t, 1, got.Variants[0].Count)
	require.True(t, got.Variants[0].Earliest.Equal(at.Add(3*time.Hour)))
	require.Equal(t, []string{"con1alice"}, got.Authors)
}

// Cleanup is the physical half of a removal: every chain ends, running a
// token again (a retried job) is harmless, and nothing a read returns
// changes, whether the chains run before or after the keys are reused.
func testDeleteCleanup(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	like := "https://schema.example/like.json"
	at := time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC)
	keys := []string{"cckv://con1owner/cleanup/a", "cckv://con1owner/cleanup/b"}
	for i, key := range keys {
		putRecord(t, ctx, repo, fmt.Sprintf("cleanup-%d", i), key, at)
		// enough dependents that a paging backend needs more than one step
		for j := range 40 {
			putAssociation(t, ctx, repo, key, associationFixture{fmt.Sprintf("cleanup-%d-like-%02d", i, j), fmt.Sprintf("con1author%02d", j), like, nil, at.Add(time.Duration(j) * time.Second)})
		}
	}
	bystander := "cckv://con1owner/cleanup/bystander"
	putRecord(t, ctx, repo, "cleanup-bystander", bystander, at)
	putAssociation(t, ctx, repo, bystander, associationFixture{"cleanup-bystander-like", "con1alice", like, nil, at})

	outcome := removeKeys(t, ctx, repo, "cleanup-delete", "con1owner", keys...)
	require.Equal(t, []bool{true, true}, outcome.Removed)

	// key a is reused before its cleanup ran, with an association whose
	// uniqueness key an old one had
	putRecord(t, ctx, repo, "cleanup-0-again", keys[0], at.Add(time.Hour))
	status, err := repo.ApplyAssociation(ctx, associationCommit(t, keys[0], associationFixture{"cleanup-0-like-again", "con1author00", like, nil, at.Add(time.Hour)}, keys[0]+"#cleanup-0-like-00"))
	require.NoError(t, err)
	require.Equal(t, record.Applied, status)

	observe := func() map[string][]string {
		t.Helper()
		seen := map[string][]string{}
		for _, key := range append(keys, bystander) {
			rows, err := repo.GetAssociatedRecords(ctx, key, "", "", "", nil, nil, 0, "asc")
			require.NoError(t, err)
			seen[key] = rowIDs(t, rows)
			summary, err := repo.ComputeAssociationSummary(ctx, key, 100)
			require.NoError(t, err)
			for _, v := range summary.Variants {
				seen[key+" count"] = append(seen[key+" count"], fmt.Sprint(v.Count))
			}
		}
		return seen
	}
	before := observe()
	require.Equal(t, []string{"cleanup-0-like-again"}, before[keys[0]])
	require.Empty(t, before[keys[1]])
	require.Equal(t, []string{"cleanup-bystander-like"}, before[bystander])

	runCleanups(t, ctx, repo, outcome.Cleanups)
	require.Equal(t, before, observe(), "cleanup changes nothing a read returns")

	// the first step of every chain again, and the chains again in full
	for _, token := range outcome.Cleanups {
		_, err := repo.Cleanup(ctx, token)
		require.NoError(t, err)
	}
	runCleanups(t, ctx, repo, outcome.Cleanups)
	require.Equal(t, before, observe())

	// key b is reused after its cleanup ran
	putRecord(t, ctx, repo, "cleanup-1-again", keys[1], at.Add(time.Hour))
	status, err = repo.ApplyAssociation(ctx, associationCommit(t, keys[1], associationFixture{"cleanup-1-like-again", "con1author00", like, nil, at.Add(time.Hour)}, keys[1]+"#cleanup-1-like-00"))
	require.NoError(t, err)
	require.Equal(t, record.Applied, status)
	rows, err := repo.GetAssociatedRecords(ctx, keys[1], "", "", "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Equal(t, []string{"cleanup-1-like-again"}, rowIDs(t, rows))
}
