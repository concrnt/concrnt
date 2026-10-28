package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
	"github.com/concrnt/concrnt/internal/testutil"
)

// The backend-neutral chunkline tests live in repotest (see suite_test.go);
// this file keeps the Postgres-only ones.

// LoadLocalBody must judge window coverage and emit timestamps by the
// record_created_at column — the column its queries filter and order by — not
// the joined records.created_at, which can diverge (e.g. backdated records).
func TestChunklineLoadLocalBodySurvivesRecordCreatedAtDivergence(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	recordRepo := NewRecordRepository(db)
	chunklineRepo := NewChunklineRepository(db)

	parentURI := "cckv://con2owner/timeline"

	// more records than defaultChunkSize inside chunk 20's window, so the
	// limited first query cannot cover the window on its own
	count := defaultChunkSize + 1
	for i := range count {
		createdAt := time.Unix(20*600+int64(i)+1, 0).UTC()
		id := fmt.Sprintf("div-record-%02d", i)
		repotest.CreateChunklineRecord(t, ctx, recordRepo, id, fmt.Sprintf("%s/%s", parentURI, id), createdAt)
	}

	// Backdate the joined records.created_at of the limited query's last
	// member (the oldest of the top defaultChunkSize records) to before the
	// window start. The window-coverage guard must ignore this diverged
	// column, or the oldest record in the window gets dropped.
	backdated := time.Unix(18*600, 0).UTC()
	require.NoError(t, db.Model(&models.Record{}).Where("document_id = ?", "div-record-01").Update("created_at", backdated).Error)

	body, err := chunklineRepo.LoadLocalBody(ctx, parentURI, 20)
	require.NoError(t, err)
	require.Len(t, body, count)

	// emitted timestamps must come from record_created_at as well
	for _, item := range body {
		require.True(t, item.Timestamp.After(time.Unix(20*600, 0)), "timestamp %v must be record_created_at, not the backdated created_at", item.Timestamp)
	}
}
