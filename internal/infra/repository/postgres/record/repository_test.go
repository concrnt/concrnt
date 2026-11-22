package record

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/testutil"
)

// Half of the connection pool may run transactions at once; the next
// BeginTx waits until one of them ends.
func TestBeginTxIsCappedAtHalfThePool(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(4)

	repo := New(db)
	ctx := context.Background()

	tx1, err := repo.BeginTx(ctx)
	require.NoError(t, err)
	tx2, err := repo.BeginTx(ctx)
	require.NoError(t, err)

	waitCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	_, err = repo.BeginTx(waitCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	require.NoError(t, tx1.Rollback(ctx))
	tx3, err := repo.BeginTx(ctx)
	require.NoError(t, err)

	require.NoError(t, tx2.Rollback(ctx))
	require.NoError(t, tx3.Rollback(ctx))
}
