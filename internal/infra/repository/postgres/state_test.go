package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/testutil"
)

// A second Lock on the same name waits for the first holder's unlock, gives
// up with its context, and does not contend with other names.
func TestStateLock(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := NewStateRepository(db)

	unlock, err := repo.Lock(ctx, "a")
	require.NoError(t, err)

	// a different name is independent
	unlockB, err := repo.Lock(ctx, "b")
	require.NoError(t, err)
	unlockB()

	// the same name blocks until the context ends
	shortCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	_, err = repo.Lock(shortCtx, "a")
	cancel()
	require.Error(t, err)

	// and is granted once the holder releases it
	acquired := make(chan func(), 1)
	go func() {
		u, err := repo.Lock(ctx, "a")
		if err != nil {
			close(acquired)
			return
		}
		acquired <- u
	}()

	select {
	case <-acquired:
		t.Fatal("lock granted while still held")
	case <-time.After(200 * time.Millisecond):
	}

	unlock()

	select {
	case u, ok := <-acquired:
		require.True(t, ok, "waiting Lock failed")
		u()
	case <-time.After(5 * time.Second):
		t.Fatal("lock not granted after unlock")
	}
}
