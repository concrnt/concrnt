package lease

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/testutil"
)

// A second Acquire of the same name waits for the first holder's release, gives
// up with its context, and does not contend with other names.
func TestLeaseAcquire(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	l := NewLease(db)

	release, err := l.Acquire(ctx, "a")
	require.NoError(t, err)

	// a different name is independent
	releaseB, err := l.Acquire(ctx, "b")
	require.NoError(t, err)
	releaseB()

	// the same name blocks until the context ends
	shortCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	_, err = l.Acquire(shortCtx, "a")
	cancel()
	require.Error(t, err)

	// and is granted once the holder releases it
	acquired := make(chan func(), 1)
	go func() {
		u, err := l.Acquire(ctx, "a")
		if err != nil {
			close(acquired)
			return
		}
		acquired <- u
	}()

	select {
	case <-acquired:
		t.Fatal("lease granted while still held")
	case <-time.After(200 * time.Millisecond):
	}

	release()

	select {
	case u, ok := <-acquired:
		require.True(t, ok, "waiting Acquire failed")
		u()
	case <-time.After(5 * time.Second):
		t.Fatal("lease not granted after release")
	}
}
