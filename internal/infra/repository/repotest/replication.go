package repotest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// RunReplicationSuite runs the contract tests of the replication feed
// (QueryCommitLogs). open must return a fresh, isolated backend per call.
func RunReplicationSuite(t *testing.T, open func(t *testing.T) Backend) {
	t.Run("QueryCommitLogs", func(t *testing.T) { testQueryCommitLogs(t, open(t)) })
}

// The replication feed pages the commit log by server receipt time with id
// as tie-break, inclusive since/until bounds, an optional owner filter, and
// returns every commit — ownerless and gc-flagged rows included — as a
// SignedDocument carrying its ccfs URI.
func testQueryCommitLogs(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Record

	userA, userB := "con1aaaa", "con1bbbb"
	base := time.Date(2026, 9, 21, 9, 0, 0, 0, time.UTC)
	// receipt times are pinned so the ordering never depends on the store's
	// clock: a-1 < b-1 == a-2 < unowned, the tie leaving only the id to decide
	seed := func(id, owner string, received time.Time) {
		t.Helper()
		sd := SignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      "record",
			Key:       "cckv://" + owner + "/" + id,
			Value:     map[string]string{"id": id},
			Author:    owner,
			Schema:    "https://schema.example/post.json",
			CreatedAt: received,
		})
		InTx(t, ctx, repo, func(tx record.RepositoryTx) {
			require.NoError(t, repo.CreateCommitLog(ctx, tx, id, "127.0.0.1", sd.Document, sd.Proof, owner))
		})
		require.NoError(t, be.Inspect.SetCommitCDate(ctx, id, received))
	}
	seed("a-1", userA, base)
	seed("b-1", userB, base.Add(2*time.Second))
	seed("a-2", userA, base.Add(2*time.Second))
	seed("unowned", "", base.Add(3*time.Second))

	cdate := func(id string) time.Time {
		t.Helper()
		return mustCommit(t, ctx, be.Inspect, id).CDate
	}
	// ids reads each row's id back out of its ccfs URI; the ownerless row has
	// no ccfs, so it is identified by the value the seed stored instead.
	ids := func(rows []record.QueryRow) []string {
		out := make([]string, 0, len(rows))
		for _, row := range rows {
			if row.Row.CCFS == nil {
				var doc concrnt.Document[map[string]string]
				require.NoError(t, json.Unmarshal([]byte(row.Row.Document), &doc))
				out = append(out, doc.Value["id"])
				continue
			}
			parsed, err := concrnt.ParseCCURI(*row.Row.CCFS)
			require.NoError(t, err)
			out = append(out, parsed.CDID)
		}
		return out
	}

	t.Run("all owners in receipt order with id tie-break", func(t *testing.T) {
		rows, err := repo.QueryCommitLogs(ctx, "", nil, nil, 10, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1", "a-2", "b-1", "unowned"}, ids(rows))
		require.Nil(t, rows[3].Row.CCFS, "an ownerless commit has no ccfs namespace")
		for i, row := range rows {
			require.Nil(t, row.Row.CCKV)
			require.False(t, row.CreatedAt.IsZero())
			if i > 0 {
				require.False(t, row.CreatedAt.Before(rows[i-1].CreatedAt), "rows must be ordered by receipt time")
			}
		}
		require.Equal(t, "ccfs://"+userA+"/concrnt/a-1", *rows[0].Row.CCFS)
		require.True(t, cdate("a-1").Equal(rows[0].CreatedAt), "CreatedAt must carry the receipt time")
		require.Contains(t, rows[0].Row.Document, `"id":"a-1"`)
		require.Equal(t, concrnt.ProofTypeNone, rows[0].Row.Proof.Type)

		rows, err = repo.QueryCommitLogs(ctx, "", nil, nil, 10, "desc")
		require.NoError(t, err)
		require.Equal(t, []string{"unowned", "b-1", "a-2", "a-1"}, ids(rows))
	})

	t.Run("owner filter", func(t *testing.T) {
		rows, err := repo.QueryCommitLogs(ctx, userA, nil, nil, 10, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1", "a-2"}, ids(rows))

		rows, err = repo.QueryCommitLogs(ctx, "con1nobody", nil, nil, 10, "asc")
		require.NoError(t, err)
		require.Empty(t, rows)
	})

	t.Run("since and until are inclusive", func(t *testing.T) {
		since := cdate("a-2")
		rows, err := repo.QueryCommitLogs(ctx, "", &since, nil, 10, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-2", "b-1", "unowned"}, ids(rows))

		until := cdate("a-1")
		rows, err = repo.QueryCommitLogs(ctx, "", nil, &until, 10, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1"}, ids(rows))
	})

	t.Run("limit", func(t *testing.T) {
		rows, err := repo.QueryCommitLogs(ctx, "", nil, nil, 2, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1", "a-2"}, ids(rows))
	})

	t.Run("gc candidates are still replicated", func(t *testing.T) {
		InTx(t, ctx, repo, func(tx record.RepositoryTx) {
			require.NoError(t, repo.MarkCommitLogGcCandidate(ctx, tx, "a-1"))
		})
		require.True(t, mustCommit(t, ctx, be.Inspect, "a-1").GcCandidate)
		rows, err := repo.QueryCommitLogs(ctx, userA, nil, nil, 10, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1", "a-2"}, ids(rows))
	})
}
