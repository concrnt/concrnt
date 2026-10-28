// Package storetest is the conformance suite of aggregate.Store plus an
// in-memory reference implementation for tests (C10: nothing here is
// imported by production code).
package storetest

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

// InMemory is the reference aggregate.Store: a mutex-guarded map with lazy
// expiry.
type InMemory struct {
	mu      sync.Mutex
	entries map[string]inMemoryEntry
}

type inMemoryEntry struct {
	aggregate.Entry
	expiresAt time.Time
}

func NewInMemory() *InMemory {
	return &InMemory{entries: map[string]inMemoryEntry{}}
}

func (s *InMemory) live(key string) (inMemoryEntry, bool) {
	e, ok := s.entries[key]
	if !ok {
		return e, false
	}
	if !time.Now().Before(e.expiresAt) {
		delete(s.entries, key)
		return e, false
	}
	return e, true
}

func (s *InMemory) Get(_ context.Context, key string) (*aggregate.Entry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.live(key)
	if !ok {
		return nil, false, nil
	}
	entry := e.Entry
	entry.Value = append([]byte(nil), e.Value...)
	return &entry, true, nil
}

func (s *InMemory) PutIfAbsent(_ context.Context, key string, entry aggregate.Entry, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.live(key); ok {
		return false, nil
	}
	entry.Value = append([]byte(nil), entry.Value...)
	s.entries[key] = inMemoryEntry{Entry: entry, expiresAt: time.Now().Add(ttl)}
	return true, nil
}

func (s *InMemory) Apply(_ context.Context, key string, fn func(old []byte) ([]byte, error)) (*aggregate.Entry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.live(key)
	if !ok {
		return nil, false, nil
	}
	value, err := fn(append([]byte(nil), e.Value...))
	if err != nil {
		return nil, false, err
	}
	e.Value = value
	e.Deltas++
	s.entries[key] = e
	entry := e.Entry
	entry.Value = append([]byte(nil), value...)
	return &entry, true, nil
}

func (s *InMemory) Invalidate(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, key)
	return nil
}

func (s *InMemory) InvalidateAll(_ context.Context, prefix string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.entries {
		if strings.HasPrefix(key, prefix) {
			delete(s.entries, key)
		}
	}
	return nil
}

// Run runs the conformance suite; open must return a fresh, isolated store
// per call.
func Run(t *testing.T, open func(t *testing.T) aggregate.Store) {
	t.Run("MissAndPut", func(t *testing.T) { testMissAndPut(t, open(t)) })
	t.Run("PutIfAbsentLoses", func(t *testing.T) { testPutIfAbsentLoses(t, open(t)) })
	t.Run("ApplyAbsent", func(t *testing.T) { testApplyAbsent(t, open(t)) })
	t.Run("ApplyCountsDeltasKeepsAsOf", func(t *testing.T) { testApplyCountsDeltasKeepsAsOf(t, open(t)) })
	t.Run("ApplyIsAtomic", func(t *testing.T) { testApplyIsAtomic(t, open(t)) })
	t.Run("ApplyErrorLeavesEntry", func(t *testing.T) { testApplyErrorLeavesEntry(t, open(t)) })
	t.Run("Invalidate", func(t *testing.T) { testInvalidate(t, open(t)) })
	t.Run("InvalidateAllByPrefix", func(t *testing.T) { testInvalidateAllByPrefix(t, open(t)) })
	t.Run("Expiry", func(t *testing.T) { testExpiry(t, open(t)) })
	t.Run("AwkwardKeys", func(t *testing.T) { testAwkwardKeys(t, open(t)) })
}

var asOf = time.Date(2026, 6, 1, 12, 0, 0, 123456000, time.UTC)

func put(t *testing.T, s aggregate.Store, key, value string) {
	t.Helper()
	won, err := s.PutIfAbsent(context.Background(), key, aggregate.Entry{Value: []byte(value), AsOf: asOf}, time.Hour)
	require.NoError(t, err)
	require.True(t, won, "put of %s must win on an empty store", key)
}

func get(t *testing.T, s aggregate.Store, key string) (*aggregate.Entry, bool) {
	t.Helper()
	entry, ok, err := s.Get(context.Background(), key)
	require.NoError(t, err)
	return entry, ok
}

func testMissAndPut(t *testing.T, s aggregate.Store) {
	_, ok := get(t, s, "counts:a")
	require.False(t, ok)

	put(t, s, "counts:a", `{"x":1}`)

	entry, ok := get(t, s, "counts:a")
	require.True(t, ok)
	require.JSONEq(t, `{"x":1}`, string(entry.Value))
	require.True(t, entry.AsOf.Equal(asOf), "AsOf must round-trip to the microsecond, got %v", entry.AsOf)
	require.Equal(t, 0, entry.Deltas)
}

func testPutIfAbsentLoses(t *testing.T, s aggregate.Store) {
	ctx := context.Background()
	put(t, s, "counts:a", `1`)
	won, err := s.PutIfAbsent(ctx, "counts:a", aggregate.Entry{Value: []byte(`2`), AsOf: asOf}, time.Hour)
	require.NoError(t, err)
	require.False(t, won)
	entry, _ := get(t, s, "counts:a")
	require.Equal(t, `1`, string(entry.Value), "the loser must not overwrite")
}

func testApplyAbsent(t *testing.T, s aggregate.Store) {
	called := false
	_, ok, err := s.Apply(context.Background(), "counts:none", func(old []byte) ([]byte, error) {
		called = true
		return old, nil
	})
	require.NoError(t, err)
	require.False(t, ok)
	require.False(t, called, "fn must not run without an entry")
	_, ok = get(t, s, "counts:none")
	require.False(t, ok, "Apply must not create an entry")
}

func testApplyCountsDeltasKeepsAsOf(t *testing.T, s aggregate.Store) {
	ctx := context.Background()
	put(t, s, "counts:a", `1`)
	for i := 1; i <= 3; i++ {
		entry, ok, err := s.Apply(ctx, "counts:a", func(old []byte) ([]byte, error) {
			require.Equal(t, string(rune('0'+i)), string(old))
			return []byte(string(rune('0' + i + 1))), nil
		})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, i, entry.Deltas)
		require.Equal(t, string(rune('0'+i+1)), string(entry.Value))
		require.True(t, entry.AsOf.Equal(asOf), "Apply keeps AsOf")
	}
	entry, _ := get(t, s, "counts:a")
	require.Equal(t, `4`, string(entry.Value))
	require.Equal(t, 3, entry.Deltas)
}

func testApplyIsAtomic(t *testing.T, s aggregate.Store) {
	ctx := context.Background()
	put(t, s, "counts:a", `0`)
	const workers, each = 16, 25
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				_, ok, err := s.Apply(ctx, "counts:a", func(old []byte) ([]byte, error) {
					n := 0
					for _, c := range old {
						n = n*10 + int(c-'0')
					}
					return []byte(itoa(n + 1)), nil
				})
				if err != nil {
					errs <- err
					return
				}
				if !ok {
					errs <- context.Canceled
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	entry, _ := get(t, s, "counts:a")
	require.Equal(t, itoa(workers*each), string(entry.Value), "every Apply must land exactly once")
	require.Equal(t, workers*each, entry.Deltas)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func testApplyErrorLeavesEntry(t *testing.T, s aggregate.Store) {
	ctx := context.Background()
	put(t, s, "counts:a", `1`)
	_, _, err := s.Apply(ctx, "counts:a", func(old []byte) ([]byte, error) {
		return nil, context.DeadlineExceeded
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	entry, ok := get(t, s, "counts:a")
	require.True(t, ok)
	require.Equal(t, `1`, string(entry.Value))
	require.Equal(t, 0, entry.Deltas)
}

func testInvalidate(t *testing.T, s aggregate.Store) {
	ctx := context.Background()
	require.NoError(t, s.Invalidate(ctx, "counts:none"), "invalidating a missing entry is fine")
	put(t, s, "counts:a", `1`)
	require.NoError(t, s.Invalidate(ctx, "counts:a"))
	_, ok := get(t, s, "counts:a")
	require.False(t, ok)
	// the key is free again
	put(t, s, "counts:a", `2`)
}

func testInvalidateAllByPrefix(t *testing.T, s aggregate.Store) {
	ctx := context.Background()
	put(t, s, "a:x", `1`)
	put(t, s, "a:x s", `1`)
	put(t, s, "a:y", `1`)
	put(t, s, "ab:x", `1`)
	put(t, s, "b:x", `1`)

	require.NoError(t, s.InvalidateAll(ctx, "a:x"))
	for key, want := range map[string]bool{"a:x": false, "a:x s": false, "a:y": true, "ab:x": true, "b:x": true} {
		_, ok := get(t, s, key)
		require.Equal(t, want, ok, "after InvalidateAll(a:x): %s", key)
	}

	require.NoError(t, s.InvalidateAll(ctx, "a:"))
	for key, want := range map[string]bool{"a:y": false, "ab:x": true, "b:x": true} {
		_, ok := get(t, s, key)
		require.Equal(t, want, ok, "after InvalidateAll(a:): %s", key)
	}

	require.NoError(t, s.InvalidateAll(ctx, ""))
	for _, key := range []string{"ab:x", "b:x"} {
		_, ok := get(t, s, key)
		require.False(t, ok, "after InvalidateAll(\"\"): %s", key)
	}
}

func testExpiry(t *testing.T, s aggregate.Store) {
	ctx := context.Background()
	won, err := s.PutIfAbsent(ctx, "counts:a", aggregate.Entry{Value: []byte(`1`), AsOf: asOf}, time.Second)
	require.NoError(t, err)
	require.True(t, won)
	time.Sleep(1200 * time.Millisecond)
	_, ok := get(t, s, "counts:a")
	require.False(t, ok, "an expired entry reads as absent")
	_, ok, err = s.Apply(ctx, "counts:a", func(old []byte) ([]byte, error) { return old, nil })
	require.NoError(t, err)
	require.False(t, ok, "an expired entry cannot be applied to")
	won, err = s.PutIfAbsent(ctx, "counts:a", aggregate.Entry{Value: []byte(`2`), AsOf: asOf}, time.Hour)
	require.NoError(t, err)
	require.True(t, won, "an expired entry counts as absent for PutIfAbsent")
}

func testAwkwardKeys(t *testing.T, s aggregate.Store) {
	key := "name:cckv://con1owner/" + strings.Repeat("k", 300) + " https://schema.example/x_y%z*?[]"
	put(t, s, key, `{"👍":1}`)
	entry, ok := get(t, s, key)
	require.True(t, ok)
	require.JSONEq(t, `{"👍":1}`, string(entry.Value))
	require.NoError(t, s.InvalidateAll(context.Background(), "name:cckv://con1owner/"+strings.Repeat("k", 300)+" "))
	_, ok = get(t, s, key)
	require.False(t, ok)
}
