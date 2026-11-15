package record

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
	"github.com/concrnt/concrnt/internal/testutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// Two transactions lock the same two rows in opposite order; Postgres kills
// one with deadlock_detected (40P01) and apply must re-run it to success.
func TestApplyRetriesDeadlock(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	defer cleanup()
	repo := New(db).(*Repository)
	ctx := context.Background()

	for _, uri := range []string{"cckv://con1dead/a", "cckv://con1dead/b"} {
		require.NoError(t, db.Create(&postgres.RecordKey{URI: uri}).Error)
	}

	lock := func(tx *gorm.DB, uri string) error {
		return tx.Exec("SELECT id FROM record_keys WHERE uri = ? FOR UPDATE", uri).Error
	}

	var attempts atomic.Int32
	aLockedFirst := make(chan struct{})
	bLockedFirst := make(chan struct{})
	run := func(first, second string, lockedFirst chan<- struct{}, peerLocked <-chan struct{}) error {
		var local int32
		_, err := repo.apply(ctx, func(tx *gorm.DB) (ucrecord.ApplyStatus, error) {
			attempts.Add(1)
			local++
			if err := lock(tx, first); err != nil {
				return ucrecord.Applied, err
			}
			if local == 1 {
				close(lockedFirst)
				<-peerLocked
			}
			return ucrecord.Applied, lock(tx, second)
		})
		return err
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

// A write that ends Stale or Duplicate is rolled back whole and reported
// without an error.
func TestApplyRollsBackUnappliedWrites(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	defer cleanup()
	repo := New(db).(*Repository)
	ctx := context.Background()

	for _, want := range []ucrecord.ApplyStatus{ucrecord.Stale, ucrecord.Duplicate} {
		status, err := repo.apply(ctx, func(tx *gorm.DB) (ucrecord.ApplyStatus, error) {
			if err := tx.Create(&postgres.RecordKey{URI: "cckv://con1rollback/" + want.String()}).Error; err != nil {
				return ucrecord.Applied, err
			}
			return want, nil
		})
		require.NoError(t, err)
		require.Equal(t, want, status)
	}

	var count int64
	require.NoError(t, db.Model(&postgres.RecordKey{}).Count(&count).Error)
	require.Zero(t, count)
}

// With a bounded pool, transactions take at most half of it: the one that
// would go beyond waits without a connection, so reads still get one.
func TestApplyLeavesConnectionsToReads(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	defer cleanup()
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(4)
	repo := New(db).(*Repository)
	ctx := context.Background()

	inside := make(chan struct{})
	finish := make(chan struct{})
	var held sync.WaitGroup
	hold := func() {
		defer held.Done()
		_, err := repo.apply(ctx, func(tx *gorm.DB) (ucrecord.ApplyStatus, error) {
			inside <- struct{}{}
			<-finish
			return ucrecord.Applied, nil
		})
		if err != nil {
			t.Error(err)
		}
	}
	held.Add(2)
	go hold()
	go hold()
	<-inside
	<-inside

	// the third transaction never starts while the two are open
	waiting, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	_, err = repo.apply(waiting, func(tx *gorm.DB) (ucrecord.ApplyStatus, error) {
		t.Error("a transaction ran beyond the slots")
		return ucrecord.Applied, nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 2, sqlDB.Stats().InUse, "the waiting transaction holds no connection")

	// and the other half of the pool serves reads meanwhile
	_, err = repo.GetSignedDocument(ctx, "cckv://con1slots/missing")
	require.ErrorIs(t, err, domain.ErrNotFound)

	close(finish)
	held.Wait()
	status, err := repo.apply(ctx, func(tx *gorm.DB) (ucrecord.ApplyStatus, error) { return ucrecord.Applied, nil })
	require.NoError(t, err)
	require.Equal(t, ucrecord.Applied, status)
}
