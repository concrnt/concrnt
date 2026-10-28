package record

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// While one transaction holds the writer lock longer than the busy timeout,
// a raw BEGIN IMMEDIATE fails with SQLITE_BUSY (the retryable error), and
// RunInTx keeps retrying it until the lock is free and the write lands.
func TestRunInTxRetriesOnBusy(t *testing.T) {
	ctx := context.Background()
	db, err := sqlitedb.Open(filepath.Join(t.TempDir(), "busy.db"), "busy_timeout(20)")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	require.NoError(t, sqlitedb.Migrate(ctx, db))
	repo := New(db, nil, domain.Config{})

	holding := make(chan struct{})
	released := make(chan struct{})
	go func() {
		defer close(released)
		_ = repo.RunInTx(ctx, func(tx ucrecord.RepositoryTx) error {
			if err := repo.CreateCommitLog(ctx, tx, "busy-holder", "127.0.0.1", "{}", concrnt.Proof{Type: concrnt.ProofTypeNone}, "con1owner"); err != nil {
				return err
			}
			close(holding)
			time.Sleep(300 * time.Millisecond)
			return nil
		})
	}()
	<-holding

	_, err = db.BeginTx(ctx, nil)
	require.Error(t, err, "a second writer cannot begin while the lock is held")
	require.True(t, isRetryableTxError(err), "the busy error must be the retryable kind, got %v", err)

	start := time.Now()
	require.NoError(t, repo.RunInTx(ctx, func(tx ucrecord.RepositoryTx) error {
		return repo.CreateCommitLog(ctx, tx, "busy-waiter", "127.0.0.1", "{}", concrnt.Proof{Type: concrnt.ProofTypeNone}, "con1owner")
	}))
	require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond, "RunInTx must have waited through retries for the holder")
	<-released

	for _, id := range []string{"busy-holder", "busy-waiter"} {
		has, err := repo.HasCommitLog(ctx, id)
		require.NoError(t, err)
		require.True(t, has, "%s must be committed", id)
	}
}
