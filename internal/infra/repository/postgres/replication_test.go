package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// The replication feed (/replication) pages the commit log by server receipt
// time (c_date) with id as tie-break, inclusive since/until bounds, an
// optional owner filter, and returns every commit — ownerless and gc-flagged
// rows included — as a SignedDocument carrying its ccfs URI.
func TestQueryCommitLogs(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := NewRecordRepository(db)

	userA, userB := "con1aaaa", "con1bbbb"
	seed := func(id, owner string) {
		t.Helper()
		sd := repositorySignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      "record",
			Key:       "cckv://" + owner + "/" + id,
			Value:     map[string]string{"id": id},
			Author:    owner,
			Schema:    "https://schema.example/post.json",
			CreatedAt: time.Now(),
		})
		tx, err := repo.BeginTx(ctx)
		require.NoError(t, err)
		require.NoError(t, repo.CreateCommitLog(ctx, tx, id, "127.0.0.1", sd.Document, sd.Proof, owner))
		require.NoError(t, tx.Commit(ctx))
	}
	seed("a-1", userA)
	seed("b-1", userB)
	seed("a-2", userA)
	seed("unowned", "")

	cdate := func(id string) time.Time {
		t.Helper()
		var cl models.CommitLog
		require.NoError(t, db.Where("id = ?", id).Take(&cl).Error)
		return cl.CDate
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

	t.Run("all owners in receipt order", func(t *testing.T) {
		rows, err := repo.QueryCommitLogs(ctx, "", nil, nil, 10, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1", "b-1", "a-2", "unowned"}, ids(rows))
		require.Nil(t, rows[3].Row.CCFS, "an ownerless commit has no ccfs namespace")
		for i, row := range rows {
			require.Nil(t, row.Row.CCKV)
			require.False(t, row.CreatedAt.IsZero())
			if i > 0 {
				require.False(t, row.CreatedAt.Before(rows[i-1].CreatedAt), "rows must be ordered by c_date")
			}
		}
		require.Equal(t, "ccfs://"+userA+"/concrnt/a-1", *rows[0].Row.CCFS)
		require.True(t, cdate("a-1").Equal(rows[0].CreatedAt), "CreatedAt must carry c_date")
		require.Contains(t, rows[0].Row.Document, `"id":"a-1"`)
		require.Equal(t, concrnt.ProofTypeNone, rows[0].Row.Proof.Type)
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
		require.Equal(t, []string{"a-2", "unowned"}, ids(rows))

		until := cdate("b-1")
		rows, err = repo.QueryCommitLogs(ctx, "", nil, &until, 10, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1", "b-1"}, ids(rows))
	})

	t.Run("limit", func(t *testing.T) {
		rows, err := repo.QueryCommitLogs(ctx, "", nil, nil, 2, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1", "b-1"}, ids(rows))
	})

	t.Run("gc candidates are still replicated", func(t *testing.T) {
		require.NoError(t, db.Model(&models.CommitLog{}).Where("id = ?", "a-1").Update("gc_candidate", true).Error)
		rows, err := repo.QueryCommitLogs(ctx, userA, nil, nil, 10, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1", "a-2"}, ids(rows))
	})

	t.Run("desc with id tie-break", func(t *testing.T) {
		// give b-1 and a-2 the same receipt time so only the id decides
		tie := cdate("a-2")
		require.NoError(t, db.Model(&models.CommitLog{}).Where("id = ?", "b-1").Update("c_date", tie).Error)

		rows, err := repo.QueryCommitLogs(ctx, "", nil, nil, 10, "desc")
		require.NoError(t, err)
		require.Equal(t, []string{"unowned", "b-1", "a-2", "a-1"}, ids(rows))

		rows, err = repo.QueryCommitLogs(ctx, "", nil, nil, 10, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{"a-1", "a-2", "b-1", "unowned"}, ids(rows))
	})
}
