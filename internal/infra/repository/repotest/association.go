package repotest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// RunAssociationSuite runs the association contract tests of
// record.Repository: the write, the listing filters of /associations and
// the summary the aggregate layer recomputes from. open must return a
// fresh, isolated backend per call; it is called once per top-level test.
func RunAssociationSuite(t *testing.T, open func(t *testing.T) Backend) {
	t.Run("Apply", func(t *testing.T) { testApplyAssociation(t, open(t)) })
	t.Run("ListFilters", func(t *testing.T) { testAssociationListFilters(t, open(t)) })
	t.Run("Summary", func(t *testing.T) { testAssociationSummary(t, open(t)) })
	t.Run("Remove", func(t *testing.T) { testAssociationRemove(t, open(t)) })
}

const associationTargetKey = "cckv://con1owner/timeline/post-1"

type associationFixture struct {
	id        string
	author    string
	schema    string
	variant   *string
	createdAt time.Time
}

// associationCommit builds the commit of an association on target whose
// uniqueness key is unique.
func associationCommit(t *testing.T, target string, f associationFixture, unique string) record.AssociationCommit {
	t.Helper()
	owner := ownerOf(t, target)
	sd := SignedDocument(t, concrnt.Document[map[string]string]{
		Kind:               "association",
		Value:              map[string]string{"id": f.id},
		Author:             f.author,
		Schema:             f.schema,
		CreatedAt:          f.createdAt,
		Associate:          &target,
		AssociationVariant: f.variant,
	})
	return record.AssociationCommit{
		Commit:    commitOf(f.id, sd, owner),
		Target:    target,
		Author:    f.author,
		Schema:    f.schema,
		Variant:   f.variant,
		Unique:    unique,
		CreatedAt: f.createdAt,
	}
}

// putAssociation applies an association that must be Applied (unique =
// target + id, so fixtures never collide).
func putAssociation(t *testing.T, ctx context.Context, repo record.Repository, target string, f associationFixture) record.AssociationCommit {
	t.Helper()
	ac := associationCommit(t, target, f, target+"#"+f.id)
	status, err := repo.ApplyAssociation(ctx, ac)
	require.NoError(t, err)
	require.Equal(t, record.Applied, status, "%s must apply", f.id)
	return ac
}

// seedAssociations writes the target record and one association per fixture
// on it.
func seedAssociations(t *testing.T, ctx context.Context, repo record.Repository, fixtures []associationFixture) {
	t.Helper()
	putRecord(t, ctx, repo, "assoc-target", associationTargetKey, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	for _, f := range fixtures {
		putAssociation(t, ctx, repo, associationTargetKey, f)
	}
}

// variantsOf indexes a summary by "schema|variant".
func variantsOf(summary record.AssociationSummary) map[string]record.AssociationVariant {
	out := make(map[string]record.AssociationVariant, len(summary.Variants))
	for _, v := range summary.Variants {
		out[v.Schema+"|"+v.Variant] = v
	}
	return out
}

// An association is stored with its commit and listed whole; the same
// logical association (same unique) under another document is Stale and
// leaves nothing; a target key that does not exist is NotFound and leaves
// nothing either, while a placeholder key is a valid target.
func testApplyAssociation(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	at := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	putRecord(t, ctx, repo, "assoc-target", associationTargetKey, at.Add(-time.Hour))

	fixture := associationFixture{"association-1", "con1author", "https://schema.example/comment.json", ptr("reply"), at}
	ac := associationCommit(t, associationTargetKey, fixture, "unique-1")
	status, err := repo.ApplyAssociation(ctx, ac)
	require.NoError(t, err)
	require.Equal(t, record.Applied, status)

	entry := commitEntry(t, ctx, repo, "association-1")
	require.NotNil(t, entry)
	require.Equal(t, "con1owner", entry.Owner, "the commit is recorded under the target's owner")

	rows, err := repo.GetAssociatedRecords(ctx, associationTargetKey, "", "", "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, ac.Document, rows[0].Row.Document)
	require.Equal(t, ac.Proof, rows[0].Row.Proof)
	require.Equal(t, ccfsOf("con1owner", "association-1"), *rows[0].Row.CCFS)
	require.True(t, rows[0].CreatedAt.Equal(at))

	t.Run("the same unique under another document is stale", func(t *testing.T) {
		retry := associationCommit(t, associationTargetKey, associationFixture{"association-1-retry", "con1author", fixture.schema, fixture.variant, at.Add(time.Minute)}, "unique-1")
		status, err := repo.ApplyAssociation(ctx, retry)
		require.NoError(t, err)
		require.Equal(t, record.Stale, status)
		require.False(t, commitExists(t, ctx, repo, "association-1-retry"), "a stale write must not leave its commit")

		rows, err := repo.GetAssociatedRecords(ctx, associationTargetKey, "", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"association-1"}, rowIDs(t, rows))
	})

	t.Run("another unique coexists", func(t *testing.T) {
		other := associationCommit(t, associationTargetKey, associationFixture{"association-2", "con1author", fixture.schema, fixture.variant, at.Add(time.Minute)}, "unique-2")
		status, err := repo.ApplyAssociation(ctx, other)
		require.NoError(t, err)
		require.Equal(t, record.Applied, status)
	})

	t.Run("a missing target key is not found and leaves no trace", func(t *testing.T) {
		missing := "cckv://con1owner/timeline/no-such-post"
		_, err := repo.ApplyAssociation(ctx, associationCommit(t, missing, associationFixture{"association-orphan", "con1author", fixture.schema, nil, at}, "unique-orphan"))
		require.True(t, errors.Is(err, domain.ErrNotFound), "expected not found, got %v", err)
		require.False(t, commitExists(t, ctx, repo, "association-orphan"))
	})

	t.Run("a placeholder key is a target, and keeps its associations when a record arrives", func(t *testing.T) {
		placeholder := "cckv://con1owner/timeline"
		putAssociation(t, ctx, repo, placeholder, associationFixture{"association-on-placeholder", "con1author", fixture.schema, nil, at})
		putRecord(t, ctx, repo, "timeline-record", placeholder, at.Add(time.Hour))

		rows, err := repo.GetAssociatedRecords(ctx, placeholder, "", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"association-on-placeholder"}, rowIDs(t, rows))
	})
}

// /associations lists the associations on one target: optional equality
// filters on schema, variant and author, an inclusive createdAt window,
// createdAt (then id) ordering in either direction, and a limit. Each row's
// CCFS is composed from the association's owner.
func testAssociationListFilters(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	like, repost := "https://schema.example/like.json", "https://schema.example/repost.json"
	t0 := time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC)
	seedAssociations(t, ctx, repo, []associationFixture{
		{"a-1", "con1alice", like, ptr("👍"), t0},
		{"a-2", "con1bob", like, ptr("👍"), t0.Add(time.Minute)},
		{"a-3", "con1alice", like, ptr("❤️"), t0.Add(2 * time.Minute)},
		{"a-4", "con1carol", repost, nil, t0.Add(3 * time.Minute)},
	})

	t.Run("all, ascending", func(t *testing.T) {
		rows, err := repo.GetAssociatedRecords(ctx, associationTargetKey, "", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1", "a-2", "a-3", "a-4"}, rowIDs(t, rows))
		for i, row := range rows {
			require.Equal(t, ccfsOf("con1owner", fmt.Sprintf("a-%d", i+1)), *row.Row.CCFS, "ccfs is composed from the association owner")
			require.True(t, row.CreatedAt.Equal(t0.Add(time.Duration(i)*time.Minute)), "QueryRow.CreatedAt is the association createdAt")
			require.NotEmpty(t, row.Row.Document)
		}
	})

	t.Run("descending with limit", func(t *testing.T) {
		rows, err := repo.GetAssociatedRecords(ctx, associationTargetKey, "", "", "", nil, nil, 2, "desc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-4", "a-3"}, rowIDs(t, rows))
	})

	t.Run("schema, variant and author filters", func(t *testing.T) {
		rows, err := repo.GetAssociatedRecords(ctx, associationTargetKey, like, "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1", "a-2", "a-3"}, rowIDs(t, rows))

		rows, err = repo.GetAssociatedRecords(ctx, associationTargetKey, like, "👍", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1", "a-2"}, rowIDs(t, rows))

		rows, err = repo.GetAssociatedRecords(ctx, associationTargetKey, "", "", "con1alice", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1", "a-3"}, rowIDs(t, rows))

		rows, err = repo.GetAssociatedRecords(ctx, associationTargetKey, repost, "👍", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Empty(t, rows)
	})

	t.Run("since and until are inclusive", func(t *testing.T) {
		rows, err := repo.GetAssociatedRecords(ctx, associationTargetKey, "", "", "", ptr(t0.Add(time.Minute)), ptr(t0.Add(2*time.Minute)), 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-2", "a-3"}, rowIDs(t, rows))
	})

	t.Run("another target lists nothing", func(t *testing.T) {
		rows, err := repo.GetAssociatedRecords(ctx, "cckv://con1owner/timeline", "", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Empty(t, rows)

		rows, err = repo.GetAssociatedRecords(ctx, "cckv://con1owner/no/such/key", "", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Empty(t, rows)
	})
}

// The summary is exact: the count and the earliest createdAt of every
// (schema, variant) (an association without a variant counts under ""),
// and every distinct author once, or AuthorsOverflow instead of a partial
// list when there are more than asked for. An absent target yields the
// zero summary.
func testAssociationSummary(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	like, repost := "https://schema.example/like.json", "https://schema.example/repost.json"
	t0 := time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC)
	// inserted out of createdAt order so the earliest has to come from the
	// data, not from insertion order
	seedAssociations(t, ctx, repo, []associationFixture{
		{"c-3", "con1alice", like, ptr("❤️"), t0.Add(2 * time.Minute)},
		{"c-2", "con1bob", like, ptr("👍"), t0.Add(time.Minute)},
		{"c-1", "con1alice", like, ptr("👍"), t0},
		{"c-6", "con1erin", like, ptr("🎉"), t0.Add(5 * time.Minute)},
		{"c-4", "con1carol", repost, nil, t0.Add(3 * time.Minute)},
		{"c-5", "con1dave", repost, nil, t0.Add(4 * time.Minute)},
	})
	authors := []string{"con1alice", "con1bob", "con1carol", "con1dave", "con1erin"}

	summary, err := repo.ComputeAssociationSummary(ctx, associationTargetKey, 10)
	require.NoError(t, err)
	variants := variantsOf(summary)
	require.Len(t, variants, 4)
	require.Len(t, summary.Variants, 4, "one entry per (schema, variant)")

	require.EqualValues(t, 2, variants[like+"|👍"].Count)
	require.True(t, variants[like+"|👍"].Earliest.Equal(t0), "a variant's earliest is its earliest association")
	require.EqualValues(t, 1, variants[like+"|❤️"].Count)
	require.True(t, variants[like+"|❤️"].Earliest.Equal(t0.Add(2*time.Minute)))
	require.EqualValues(t, 1, variants[like+"|🎉"].Count)
	require.True(t, variants[like+"|🎉"].Earliest.Equal(t0.Add(5*time.Minute)))
	require.EqualValues(t, 2, variants[repost+"|"].Count, "a variant-less association counts under the empty variant")
	require.True(t, variants[repost+"|"].Earliest.Equal(t0.Add(3*time.Minute)))

	require.False(t, summary.AuthorsOverflow)
	require.ElementsMatch(t, authors, summary.Authors, "each author once")

	t.Run("exactly maxAuthors authors is complete", func(t *testing.T) {
		summary, err := repo.ComputeAssociationSummary(ctx, associationTargetKey, len(authors))
		require.NoError(t, err)
		require.False(t, summary.AuthorsOverflow)
		require.ElementsMatch(t, authors, summary.Authors)
	})

	t.Run("more than maxAuthors authors overflows, the counts stay exact", func(t *testing.T) {
		for _, maxAuthors := range []int{len(authors) - 1, 2, 0} {
			summary, err := repo.ComputeAssociationSummary(ctx, associationTargetKey, maxAuthors)
			require.NoError(t, err)
			require.True(t, summary.AuthorsOverflow, "maxAuthors %d", maxAuthors)
			require.Empty(t, summary.Authors, "an overflow lists no partial set")
			require.Equal(t, variants, variantsOf(summary))
		}
	})

	t.Run("a target without associations is the zero summary", func(t *testing.T) {
		for _, target := range []string{"cckv://con1owner/timeline", "cckv://con1owner/no/such/key"} {
			summary, err := repo.ComputeAssociationSummary(ctx, target, 10)
			require.NoError(t, err)
			require.Empty(t, summary.Variants)
			require.Empty(t, summary.Authors)
			require.False(t, summary.AuthorsOverflow)
		}
	})
}

// A removed association leaves the summary to the associations left;
// removing one that is gone (a replay) or never existed is not an error
// and reports that it removed nothing.
func testAssociationRemove(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	like := "https://schema.example/like.json"
	t0 := time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC)
	seedAssociations(t, ctx, repo, []associationFixture{
		{"d-1", "con1alice", like, ptr("👍"), t0},
		{"d-2", "con1bob", like, ptr("👍"), t0.Add(time.Minute)},
	})

	remove := func(ids ...string) []bool {
		t.Helper()
		removals := make([]record.Removal, len(ids))
		for i, id := range ids {
			removals[i] = record.Removal{Kind: record.RemoveAssociation, URI: associationTargetKey, DocumentID: id}
		}
		outcome, err := repo.ApplyDelete(ctx, record.DeleteCommit{Removals: removals})
		require.NoError(t, err)
		require.Equal(t, record.Applied, outcome.Status)
		return outcome.Removed
	}

	require.Equal(t, []bool{true}, remove("d-1"))
	require.Equal(t, []bool{false, false}, remove("d-1", "never-existed"))

	rows, err := repo.GetAssociatedRecords(ctx, associationTargetKey, "", "", "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Equal(t, []string{"d-2"}, rowIDs(t, rows))

	summary, err := repo.ComputeAssociationSummary(ctx, associationTargetKey, 10)
	require.NoError(t, err)
	require.Len(t, summary.Variants, 1)
	require.EqualValues(t, 1, summary.Variants[0].Count)
	require.True(t, summary.Variants[0].Earliest.Equal(t0.Add(time.Minute)), "the recomputed earliest is the earliest association left")
	require.Equal(t, []string{"con1bob"}, summary.Authors)

	require.True(t, commitExists(t, ctx, repo, "d-1"), "the removed association's commit stays in the log")

	// its uniqueness key is free again
	again := associationCommit(t, associationTargetKey, associationFixture{"d-1-again", "con1alice", like, ptr("👍"), t0.Add(time.Hour)}, associationTargetKey+"#d-1")
	status, err := repo.ApplyAssociation(ctx, again)
	require.NoError(t, err)
	require.Equal(t, record.Applied, status)
}
