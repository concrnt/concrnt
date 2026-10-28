package repotest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// RunAssociationSuite runs the association contract tests of
// record.Repository: the listing filters of /associations and the counts the
// aggregate layer recomputes from. open must return a fresh, isolated backend
// per call; it is called once per top-level test.
func RunAssociationSuite(t *testing.T, open func(t *testing.T) Backend) {
	t.Run("ListFilters", func(t *testing.T) { testAssociationListFilters(t, open(t)) })
	t.Run("Counts", func(t *testing.T) { testAssociationCounts(t, open(t)) })
}

const associationTargetKey = "cckv://con1owner/timeline/post-1"

type associationFixture struct {
	id        string
	author    string
	schema    string
	variant   *string
	createdAt time.Time
}

// seedAssociations writes a target record and one association per fixture
// on it (unique = id, owner = the target owner).
func seedAssociations(t *testing.T, ctx context.Context, repo record.Repository, fixtures []associationFixture) {
	t.Helper()

	targetCreatedAt := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	targetSD := SignedDocument(t, concrnt.Document[map[string]string]{
		Kind:      "record",
		Key:       associationTargetKey,
		Value:     map[string]string{"body": "target"},
		Author:    "con1owner",
		Schema:    "https://schema.example/post.json",
		CreatedAt: targetCreatedAt,
	})
	WithCommit(t, ctx, repo, "assoc-target", "127.0.0.1", targetSD, "con1owner", func(tx record.RepositoryTx) error {
		_, err := repo.CreateRecord(ctx, tx, "assoc-target", associationTargetKey, "con1owner", "con1owner", "https://schema.example/post.json", nil, nil, []string{}, nil, targetCreatedAt)
		return err
	})

	target := associationTargetKey
	for _, f := range fixtures {
		sd := SignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      "association",
			Value:     map[string]string{"id": f.id},
			Author:    f.author,
			Schema:    f.schema,
			CreatedAt: f.createdAt,
			Associate: &target,
		})
		WithCommit(t, ctx, repo, f.id, "127.0.0.1", sd, "con1owner", func(tx record.RepositoryTx) error {
			applied, err := repo.CreateAssociation(ctx, tx, f.id, target, "con1owner", f.author, f.schema, f.variant, f.id, f.createdAt)
			if err != nil {
				return err
			}
			require.True(t, applied, "%s must apply", f.id)
			return nil
		})
	}
}

// associationIDs extracts the "id" value each association document carries.
func associationIDs(t *testing.T, rows []record.QueryRow) []string {
	t.Helper()
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		parsed, err := row.Row.ParsedDocument()
		require.NoError(t, err)
		value, ok := parsed.Value.(map[string]any)
		require.True(t, ok, "association value must be an object")
		out = append(out, value["id"].(string))
	}
	return out
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
		{"a-1", "con1alice", like, Ptr("👍"), t0},
		{"a-2", "con1bob", like, Ptr("👍"), t0.Add(time.Minute)},
		{"a-3", "con1alice", like, Ptr("❤️"), t0.Add(2 * time.Minute)},
		{"a-4", "con1carol", repost, nil, t0.Add(3 * time.Minute)},
	})

	t.Run("all, ascending", func(t *testing.T) {
		rows, err := repo.GetAssociatedRecords(ctx, associationTargetKey, "", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1", "a-2", "a-3", "a-4"}, associationIDs(t, rows))
		for i, row := range rows {
			require.NotNil(t, row.Row.CCFS)
			require.Equal(t, concrnt.ComposeCCFSURI("con1owner", concrnt.CCFSTypeConcrnt, fmt.Sprintf("a-%d", i+1)), *row.Row.CCFS, "ccfs is composed from the association owner")
			require.True(t, row.CreatedAt.Equal(t0.Add(time.Duration(i)*time.Minute)), "QueryRow.CreatedAt is the association createdAt")
		}
	})

	t.Run("descending with limit", func(t *testing.T) {
		rows, err := repo.GetAssociatedRecords(ctx, associationTargetKey, "", "", "", nil, nil, 2, "desc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-4", "a-3"}, associationIDs(t, rows))
	})

	t.Run("schema, variant and author filters", func(t *testing.T) {
		rows, err := repo.GetAssociatedRecords(ctx, associationTargetKey, like, "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1", "a-2", "a-3"}, associationIDs(t, rows))

		rows, err = repo.GetAssociatedRecords(ctx, associationTargetKey, like, "👍", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1", "a-2"}, associationIDs(t, rows))

		rows, err = repo.GetAssociatedRecords(ctx, associationTargetKey, "", "", "con1alice", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1", "a-3"}, associationIDs(t, rows))

		rows, err = repo.GetAssociatedRecords(ctx, associationTargetKey, repost, "👍", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Empty(t, rows)
	})

	t.Run("since and until are inclusive", func(t *testing.T) {
		rows, err := repo.GetAssociatedRecords(ctx, associationTargetKey, "", "", "", Ptr(t0.Add(time.Minute)), Ptr(t0.Add(2*time.Minute)), 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-2", "a-3"}, associationIDs(t, rows))
	})

	t.Run("another target lists nothing", func(t *testing.T) {
		rows, err := repo.GetAssociatedRecords(ctx, "cckv://con1owner/timeline", "", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Empty(t, rows)
	})
}

// The counts are grouped by schema over a target, and by variant within one
// schema (an association without a variant counts under ""). An absent
// target yields empty counts.
func testAssociationCounts(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	like, repost := "https://schema.example/like.json", "https://schema.example/repost.json"
	t0 := time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC)
	seedAssociations(t, ctx, repo, []associationFixture{
		{"c-1", "con1alice", like, Ptr("👍"), t0},
		{"c-2", "con1bob", like, Ptr("👍"), t0.Add(time.Minute)},
		{"c-3", "con1alice", like, Ptr("❤️"), t0.Add(2 * time.Minute)},
		{"c-4", "con1carol", repost, nil, t0.Add(3 * time.Minute)},
		{"c-5", "con1dave", repost, nil, t0.Add(4 * time.Minute)},
	})

	bySchema, err := repo.GetAssociatedRecordCountsBySchema(ctx, associationTargetKey)
	require.NoError(t, err)
	require.Equal(t, map[string]int64{like: 3, repost: 2}, bySchema)

	byVariant, err := repo.GetAssociatedRecordCountsByVariant(ctx, associationTargetKey, like)
	require.NoError(t, err)
	require.NotNil(t, byVariant)
	require.Len(t, *byVariant, 2)
	require.EqualValues(t, 2, (*byVariant)["👍"].Value)
	require.EqualValues(t, 1, (*byVariant)["❤️"].Value)

	byVariant, err = repo.GetAssociatedRecordCountsByVariant(ctx, associationTargetKey, repost)
	require.NoError(t, err)
	require.Len(t, *byVariant, 1)
	require.EqualValues(t, 2, (*byVariant)[""].Value, "a variant-less association counts under the empty variant")

	bySchema, err = repo.GetAssociatedRecordCountsBySchema(ctx, "cckv://con1owner/timeline")
	require.NoError(t, err)
	require.Empty(t, bySchema)
}
