package record

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// Two transactions lock the same two rows in opposite order; Postgres kills
// one with deadlock_detected (40P01) and RunInTx must re-run it to success.
func TestRunInTxRetriesDeadlock(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	defer cleanup()
	repo := New(db, nil, domain.Config{})
	ctx := context.Background()

	for _, uri := range []string{"cckv://con1dead/a", "cckv://con1dead/b"} {
		require.NoError(t, db.Create(&models.RecordKey{URI: uri}).Error)
	}

	lock := func(tx record.RepositoryTx, uri string) error {
		return tx.(*recordTx).tx.Exec("SELECT id FROM record_keys WHERE uri = ? FOR UPDATE", uri).Error
	}

	var attempts atomic.Int32
	aLockedFirst := make(chan struct{})
	bLockedFirst := make(chan struct{})
	run := func(first, second string, lockedFirst chan<- struct{}, peerLocked <-chan struct{}) error {
		var local int32
		return repo.RunInTx(ctx, func(tx record.RepositoryTx) error {
			attempts.Add(1)
			local++
			if err := lock(tx, first); err != nil {
				return err
			}
			if local == 1 {
				close(lockedFirst)
				<-peerLocked
			}
			return lock(tx, second)
		})
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errs[0] = run("cckv://con1dead/a", "cckv://con1dead/b", aLockedFirst, bLockedFirst)
	}()
	go func() {
		defer wg.Done()
		errs[1] = run("cckv://con1dead/b", "cckv://con1dead/a", bLockedFirst, aLockedFirst)
	}()
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.Equal(t, int32(3), attempts.Load(), "exactly one side is retried once")
}
