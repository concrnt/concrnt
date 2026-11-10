package record

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// While one transaction holds the writer lock longer than the busy timeout,
// a raw BEGIN IMMEDIATE fails with SQLITE_BUSY (the retryable error), and
// an Apply* call keeps retrying until the lock is free and the write lands.
func TestApplyRetriesOnBusy(t *testing.T) {
	ctx := context.Background()
	db, err := sqlitedb.Open(filepath.Join(t.TempDir(), "busy.db"), "busy_timeout(20)")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	require.NoError(t, sqlitedb.Migrate(ctx, db))
	repo := New(db.DB).(*Repository)

	commit := func(id string) ucrecord.Commit {
		return ucrecord.Commit{ID: id, IP: "127.0.0.1", Document: "{}", Proof: concrnt.Proof{Type: concrnt.ProofTypeNone}, Owner: "con1owner"}
	}

	holding := make(chan struct{})
	released := make(chan struct{})
	go func() {
		defer close(released)
		_, _ = repo.apply(ctx, func(tx *gorm.DB) (ucrecord.ApplyStatus, error) {
			if _, err := insertCommit(tx, commit("busy-holder")); err != nil {
				return ucrecord.Applied, err
			}
			close(holding)
			time.Sleep(300 * time.Millisecond)
			return ucrecord.Applied, nil
		})
	}()
	<-holding

	_, err = db.SQL().BeginTx(ctx, nil)
	require.Error(t, err, "a second writer cannot begin while the lock is held")
	require.True(t, isRetryableTxError(err), "the busy error must be the retryable kind, got %v", err)

	start := time.Now()
	waiter := commit("busy-waiter")
	outcome, err := repo.ApplyDelete(ctx, ucrecord.DeleteCommit{Commit: &waiter})
	require.NoError(t, err)
	require.Equal(t, ucrecord.Applied, outcome.Status)
	require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond, "the write must have waited through retries for the holder")
	<-released

	entries, err := repo.ListCommitLogs(ctx, ucrecord.CommitLogPage{Limit: 10})
	require.NoError(t, err)
	require.Len(t, entries, 2, "both writes must be committed")
}
