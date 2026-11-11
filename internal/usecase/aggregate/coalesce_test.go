package aggregate_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/aggregate/storetest"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

// gatedStore holds every Get after it has read the entry until the test
// lets it return: a read in flight that carries the value it found when it
// started.
type gatedStore struct {
	aggregate.Store
	gets atomic.Int64
	// entered receives one value per Get that has read and is waiting.
	entered chan struct{}
	// release lets one waiting Get return per value.
	release chan struct{}
}

func newGatedStore() *gatedStore {
	return &gatedStore{Store: storetest.NewInMemory(), entered: make(chan struct{}, 1024), release: make(chan struct{}, 1024)}
}

func (s *gatedStore) Get(ctx context.Context, key string) (*aggregate.Counters, bool, error) {
	s.gets.Add(1)
	entry, ok, err := s.Store.Get(ctx, key)
	s.entered <- struct{}{}
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
	return entry, ok, err
}

// settle gives goroutines just started the time to reach the point where
// they wait for a read.
func settle() { time.Sleep(100 * time.Millisecond) }

// Readers of one entry that arrive while a read is in flight share the one
// read that follows it.
func TestGetSharesConcurrentReads(t *testing.T) {
	ctx := context.Background()
	store := newGatedStore()
	uc := aggregate.New(store, 0, 0)
	d := newCountingDef()
	require.NoError(t, uc.Seed(ctx, d.def, "t1", aggregate.Counters{Counts: map[string]int64{"like": 1}}))

	const readers = 50
	results := make(chan map[string]int64, readers+1)
	var wg sync.WaitGroup
	read := func() {
		defer wg.Done()
		got, err := uc.Get(ctx, d.def, "t1")
		if err == nil {
			results <- got.Counts
		}
	}

	wg.Add(1)
	go read()
	<-store.entered
	wg.Add(readers)
	for range readers {
		go read()
	}
	settle()
	store.release <- struct{}{}
	<-store.entered
	store.release <- struct{}{}
	wg.Wait()

	require.EqualValues(t, 2, store.gets.Load(), "the read in flight and one for everyone who waited")
	require.Len(t, results, readers+1)
	for range readers + 1 {
		require.Equal(t, map[string]int64{"like": 1}, <-results)
	}
	require.Zero(t, d.computes.Load())
}

// A reader that arrives after a write was folded in never gets the result
// of a read that was already in flight, which may predate the write.
func TestGetAfterTouchDoesNotJoinAnEarlierRead(t *testing.T) {
	ctx := context.Background()
	store := newGatedStore()
	uc := aggregate.New(store, 0, 0)
	d := newCountingDef()
	require.NoError(t, uc.Seed(ctx, d.def, "t1", aggregate.Counters{Counts: map[string]int64{"like": 1}}))

	early := make(chan map[string]int64, 1)
	go func() {
		got, _ := uc.Get(ctx, d.def, "t1")
		early <- got.Counts
	}()
	<-store.entered

	require.NoError(t, uc.Touch(ctx, d.def, "t1", inc("like", 1), time.Now()))

	late := make(chan map[string]int64, 1)
	go func() {
		got, _ := uc.Get(ctx, d.def, "t1")
		late <- got.Counts
	}()
	settle()
	store.release <- struct{}{}
	store.release <- struct{}{}

	require.Equal(t, map[string]int64{"like": 1}, <-early, "the read in flight keeps what it found")
	require.Equal(t, map[string]int64{"like": 2}, <-late, "the reader that came after the write sees it")
}

// A caller that gives up while it leads the next read does not take the
// callers that joined it down with it.
func TestGetSurvivesACancelledLeader(t *testing.T) {
	ctx := context.Background()
	store := newGatedStore()
	uc := aggregate.New(store, 0, 0)
	d := newCountingDef()
	require.NoError(t, uc.Seed(ctx, d.def, "t1", aggregate.Counters{Counts: map[string]int64{"like": 1}}))

	go func() { _, _ = uc.Get(ctx, d.def, "t1") }()
	<-store.entered

	leaderCtx, cancel := context.WithCancel(ctx)
	leaderErr := make(chan error, 1)
	go func() {
		_, err := uc.Get(leaderCtx, d.def, "t1")
		leaderErr <- err
	}()
	settle()
	follower := make(chan map[string]int64, 1)
	go func() {
		got, err := uc.Get(ctx, d.def, "t1")
		if err == nil {
			follower <- got.Counts
		}
	}()
	settle()

	cancel()
	require.ErrorIs(t, <-leaderErr, context.Canceled)
	store.release <- struct{}{}
	store.release <- struct{}{}
	require.Equal(t, map[string]int64{"like": 1}, <-follower)

	// the key is free again: a later read goes straight to the store
	store.release <- struct{}{}
	got, err := uc.Get(ctx, d.def, "t1")
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"like": 1}, got.Counts)
}
