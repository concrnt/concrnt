// Package storetest is the conformance suite of aggregate.Store plus an
// in-memory reference implementation for tests (C10: nothing here is
// imported by production code).
package storetest

import (
	"context"
	"fmt"
	"maps"
	"slices"
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
	aggregate.Counters
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

func clone(c aggregate.Counters) aggregate.Counters {
	c.Counts, c.Minima, c.Members = maps.Clone(c.Counts), maps.Clone(c.Minima), slices.Clone(c.Members)
	return c
}

func (s *InMemory) Get(_ context.Context, key string) (*aggregate.Counters, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.live(key)
	if !ok {
		return nil, false, nil
	}
	entry := clone(e.Counters)
	return &entry, true, nil
}

func (s *InMemory) PutIfAbsent(_ context.Context, key string, entry aggregate.Counters, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.live(key); ok {
		return false, nil
	}
	entry = clone(entry)
	entry.Deltas = 0
	s.entries[key] = inMemoryEntry{Counters: entry, expiresAt: time.Now().Add(ttl)}
	return true, nil
}

func (s *InMemory) Add(_ context.Context, key string, delta aggregate.Delta) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.live(key)
	if !ok {
		return false, nil
	}
	e.Counters.Add(delta)
	s.entries[key] = e
	return true, nil
}

func (s *InMemory) Invalidate(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, key)
	return nil
}

func (s *InMemory) InvalidateDefinition(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.entries {
		if aggregate.NameOf(key) == name {
			delete(s.entries, key)
		}
	}
	return nil
}

func (s *InMemory) InvalidateAll(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.entries)
	return nil
}

// Run runs the conformance suite; open must return a fresh, isolated store
// per call.
func Run(t *testing.T, open func(t *testing.T) aggregate.Store) {
	t.Run("MissAndPut", func(t *testing.T) { testMissAndPut(t, open(t)) })
	t.Run("PutIfAbsentLoses", func(t *testing.T) { testPutIfAbsentLoses(t, open(t)) })
	t.Run("AddAbsent", func(t *testing.T) { testAddAbsent(t, open(t)) })
	t.Run("AddFoldsDelta", func(t *testing.T) { testAddFoldsDelta(t, open(t)) })
	t.Run("AddCountsDeltasKeepsAsOf", func(t *testing.T) { testAddCountsDeltasKeepsAsOf(t, open(t)) })
	t.Run("ConcurrentAddsLoseNothing", func(t *testing.T) { testConcurrentAddsLoseNothing(t, open(t)) })
	t.Run("Invalidate", func(t *testing.T) { testInvalidate(t, open(t)) })
	t.Run("InvalidateDefinitionAndAll", func(t *testing.T) { testInvalidateDefinitionAndAll(t, open(t)) })
	t.Run("Expiry", func(t *testing.T) { testExpiry(t, open(t)) })
	t.Run("AwkwardKeys", func(t *testing.T) { testAwkwardKeys(t, open(t)) })
	t.Run("AwkwardNames", func(t *testing.T) { testAwkwardNames(t, open(t)) })
}

var asOf = time.Date(2026, 6, 1, 12, 0, 0, 123456000, time.UTC)

// early is a createdAt in unix nanoseconds: above 2^53, so a store that
// takes a minimum through a float loses the low digits.
const early = int64(1780000000123456789)

func counts(kv ...any) map[string]int64 {
	out := map[string]int64{}
	for i := 0; i < len(kv); i += 2 {
		switch n := kv[i+1].(type) {
		case int:
			out[kv[i].(string)] = int64(n)
		case int64:
			out[kv[i].(string)] = n
		}
	}
	return out
}

func put(t *testing.T, s aggregate.Store, key string, entry aggregate.Counters) {
	t.Helper()
	entry.AsOf = asOf
	won, err := s.PutIfAbsent(context.Background(), key, entry, time.Hour)
	require.NoError(t, err)
	require.True(t, won, "put of %s must win on an empty store", key)
}

func get(t *testing.T, s aggregate.Store, key string) (*aggregate.Counters, bool) {
	t.Helper()
	entry, ok, err := s.Get(context.Background(), key)
	require.NoError(t, err)
	return entry, ok
}

func add(t *testing.T, s aggregate.Store, key string, delta aggregate.Delta) {
	t.Helper()
	ok, err := s.Add(context.Background(), key, delta)
	require.NoError(t, err)
	require.True(t, ok, "add to %s must find the entry", key)
}

// requireCounters compares what a store holds under key with want: empty
// and nil read the same, the members in any order.
func requireCounters(t *testing.T, s aggregate.Store, key string, want aggregate.Counters) {
	t.Helper()
	got, ok := get(t, s, key)
	require.True(t, ok, "%s must be stored", key)
	if want.Counts == nil {
		want.Counts = map[string]int64{}
	}
	if want.Minima == nil {
		want.Minima = map[string]int64{}
	}
	if got.Counts == nil {
		got.Counts = map[string]int64{}
	}
	if got.Minima == nil {
		got.Minima = map[string]int64{}
	}
	require.Equal(t, want.Counts, got.Counts, "counts of %s", key)
	require.Equal(t, want.Minima, got.Minima, "minima of %s", key)
	require.ElementsMatch(t, want.Members, got.Members, "members of %s", key)
	require.True(t, got.AsOf.Equal(asOf), "AsOf must round-trip to the microsecond and survive Add, got %v", got.AsOf)
	require.Equal(t, want.Deltas, got.Deltas, "deltas of %s", key)
}

func testMissAndPut(t *testing.T, s aggregate.Store) {
	_, ok := get(t, s, "counts:a")
	require.False(t, ok)

	entry := aggregate.Counters{
		Counts:  counts("x", 1, "y", 0, "z", -2),
		Minima:  counts("x", early, "w", -5),
		Members: []string{"con1alice", "con1bob"},
	}
	put(t, s, "counts:a", entry)
	requireCounters(t, s, "counts:a", entry)

	// an entry without anything in it is still an entry
	put(t, s, "counts:empty", aggregate.Counters{})
	requireCounters(t, s, "counts:empty", aggregate.Counters{})
}

func testPutIfAbsentLoses(t *testing.T, s aggregate.Store) {
	ctx := context.Background()
	put(t, s, "counts:a", aggregate.Counters{Counts: counts("x", 1), Members: []string{"m1"}})
	won, err := s.PutIfAbsent(ctx, "counts:a", aggregate.Counters{Counts: counts("x", 2, "y", 2), Minima: counts("x", 1), Members: []string{"m2"}, AsOf: asOf.Add(time.Hour)}, time.Hour)
	require.NoError(t, err)
	require.False(t, won)
	requireCounters(t, s, "counts:a", aggregate.Counters{Counts: counts("x", 1), Members: []string{"m1"}})

	// the stored entry starts without deltas whatever the caller passed
	put(t, s, "counts:b", aggregate.Counters{Counts: counts("x", 1), Deltas: 7})
	requireCounters(t, s, "counts:b", aggregate.Counters{Counts: counts("x", 1)})
}

func testAddAbsent(t *testing.T, s aggregate.Store) {
	ok, err := s.Add(context.Background(), "counts:none", aggregate.Delta{Inc: counts("x", 1), Min: counts("x", 1), AddMembers: []string{"m"}})
	require.NoError(t, err)
	require.False(t, ok)
	_, ok = get(t, s, "counts:none")
	require.False(t, ok, "Add must not create an entry")
}

func testAddFoldsDelta(t *testing.T, s aggregate.Store) {
	put(t, s, "counts:a", aggregate.Counters{
		Counts:  counts("x", 5),
		Minima:  counts("x", early),
		Members: []string{"m1"},
	})

	// existing and new counters, a lower, a higher and a new minimum, a
	// known and a new member
	add(t, s, "counts:a", aggregate.Delta{
		Inc:        counts("x", 2, "y", 1),
		Min:        counts("x", early-1, "y", 10),
		AddMembers: []string{"m1", "m2"},
	})
	add(t, s, "counts:a", aggregate.Delta{
		Inc:        counts("x", -7, "z", -1),
		Min:        counts("x", early+1, "y", 11, "z", -3),
		AddMembers: []string{"m2", "m3", "m3"},
	})
	requireCounters(t, s, "counts:a", aggregate.Counters{
		// a counter that came back to zero stays
		Counts:  counts("x", 0, "y", 1, "z", -1),
		Minima:  counts("x", early-1, "y", 10, "z", -3),
		Members: []string{"m1", "m2", "m3"},
		Deltas:  2,
	})

	// each part works alone, and a delta without any still counts
	add(t, s, "counts:a", aggregate.Delta{Inc: counts("y", 1)})
	add(t, s, "counts:a", aggregate.Delta{Min: counts("y", 9)})
	add(t, s, "counts:a", aggregate.Delta{AddMembers: []string{"m4"}})
	add(t, s, "counts:a", aggregate.Delta{})
	requireCounters(t, s, "counts:a", aggregate.Counters{
		Counts:  counts("x", 0, "y", 2, "z", -1),
		Minima:  counts("x", early-1, "y", 9, "z", -3),
		Members: []string{"m1", "m2", "m3", "m4"},
		Deltas:  6,
	})

	// an entry that started empty takes all three
	put(t, s, "counts:empty", aggregate.Counters{})
	add(t, s, "counts:empty", aggregate.Delta{Inc: counts("x", 1), Min: counts("x", early), AddMembers: []string{"m1"}})
	requireCounters(t, s, "counts:empty", aggregate.Counters{Counts: counts("x", 1), Minima: counts("x", early), Members: []string{"m1"}, Deltas: 1})
}

func testAddCountsDeltasKeepsAsOf(t *testing.T, s aggregate.Store) {
	put(t, s, "counts:a", aggregate.Counters{Counts: counts("x", 1)})
	put(t, s, "counts:b", aggregate.Counters{Counts: counts("x", 1)})
	for i := 1; i <= 3; i++ {
		add(t, s, "counts:a", aggregate.Delta{Inc: counts("x", 1)})
		// requireCounters checks AsOf
		requireCounters(t, s, "counts:a", aggregate.Counters{Counts: counts("x", 1+i), Deltas: int64(i)})
	}
	requireCounters(t, s, "counts:b", aggregate.Counters{Counts: counts("x", 1)})
}

// Every Add of a burst on one key must land: the counters add up, the
// lowest minimum wins and no member is missing, without any caller
// retrying. Readers run alongside, since a store may reorganise an entry
// when it is read.
func testConcurrentAddsLoseNothing(t *testing.T, s aggregate.Store) {
	ctx := context.Background()
	put(t, s, "counts:a", aggregate.Counters{Counts: counts("hits", 1), Minima: counts("hits", early), Members: []string{"seed"}})

	const workers, each = 16, 25
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			own := fmt.Sprintf("worker-%d", w)
			for i := range each {
				n := int64(w*each + i)
				ok, err := s.Add(ctx, "counts:a", aggregate.Delta{
					Inc:        counts("hits", 1, own, 1, "down", -2),
					Min:        counts("hits", early-n, own, early+int64(i)),
					AddMembers: []string{"shared", fmt.Sprintf("member-%d", n)},
				})
				if err == nil && !ok {
					err = fmt.Errorf("add %d of %s found no entry", i, own)
				}
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	writing := make(chan struct{})
	readErrs := make(chan error, 2)
	for range cap(readErrs) {
		go func() {
			for {
				select {
				case <-writing:
					readErrs <- nil
					return
				default:
				}
				_, ok, err := s.Get(ctx, "counts:a")
				if err == nil && !ok {
					err = fmt.Errorf("the entry disappeared under its writers")
				}
				if err != nil {
					readErrs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(writing)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	for range cap(readErrs) {
		require.NoError(t, <-readErrs)
	}

	want := aggregate.Counters{
		Counts:  counts("hits", 1+workers*each, "down", -2*workers*each),
		Minima:  counts("hits", early-(workers*each-1)),
		Members: []string{"seed", "shared"},
		Deltas:  workers * each,
	}
	for w := range workers {
		own := fmt.Sprintf("worker-%d", w)
		want.Counts[own] = each
		want.Minima[own] = early
		for i := range each {
			want.Members = append(want.Members, fmt.Sprintf("member-%d", w*each+i))
		}
	}
	requireCounters(t, s, "counts:a", want)
}

func testInvalidate(t *testing.T, s aggregate.Store) {
	ctx := context.Background()
	require.NoError(t, s.Invalidate(ctx, "counts:none"), "invalidating a missing entry is fine")
	put(t, s, "counts:a", aggregate.Counters{Counts: counts("x", 1), Minima: counts("x", 1), Members: []string{"m1"}})
	add(t, s, "counts:a", aggregate.Delta{Inc: counts("y", 1), AddMembers: []string{"m2"}})
	require.NoError(t, s.Invalidate(ctx, "counts:a"))
	_, ok := get(t, s, "counts:a")
	require.False(t, ok)
	ok, err := s.Add(ctx, "counts:a", aggregate.Delta{Inc: counts("x", 1)})
	require.NoError(t, err)
	require.False(t, ok, "a dropped entry cannot be added to")
	// the key is free again and nothing of the old entry is left
	put(t, s, "counts:a", aggregate.Counters{Counts: counts("z", 2)})
	requireCounters(t, s, "counts:a", aggregate.Counters{Counts: counts("z", 2)})
}

func testInvalidateDefinitionAndAll(t *testing.T, s aggregate.Store) {
	ctx := context.Background()
	one := aggregate.Counters{Counts: counts("x", 1)}
	put(t, s, "a:x", one)
	put(t, s, "a:x s", one)
	put(t, s, "a:y", one)
	put(t, s, "ab:x", one)
	put(t, s, "b:x", one)
	add(t, s, "a:x", aggregate.Delta{Inc: counts("x", 1), AddMembers: []string{"m"}})

	require.NoError(t, s.InvalidateDefinition(ctx, "a"))
	for key, want := range map[string]bool{"a:x": false, "a:x s": false, "a:y": false, "ab:x": true, "b:x": true} {
		_, ok := get(t, s, key)
		require.Equal(t, want, ok, "after InvalidateDefinition(a): %s", key)
	}
	// the definition is usable again
	put(t, s, "a:x", one)
	requireCounters(t, s, "a:x", one)

	require.NoError(t, s.InvalidateAll(ctx))
	for _, key := range []string{"a:x", "ab:x", "b:x"} {
		_, ok := get(t, s, key)
		require.False(t, ok, "after InvalidateAll: %s", key)
	}
	put(t, s, "b:x", one)
}

func testExpiry(t *testing.T, s aggregate.Store) {
	ctx := context.Background()
	won, err := s.PutIfAbsent(ctx, "counts:a", aggregate.Counters{Counts: counts("x", 1), Members: []string{"old"}, AsOf: asOf}, time.Second)
	require.NoError(t, err)
	require.True(t, won)
	time.Sleep(1200 * time.Millisecond)
	_, ok := get(t, s, "counts:a")
	require.False(t, ok, "an expired entry reads as absent")
	// a blind Add may or may not notice the expiry; either way it revives
	// nothing and leaves nothing behind for the next entry
	_, err = s.Add(ctx, "counts:a", aggregate.Delta{Inc: counts("x", 1, "stale", 1), Min: counts("stale", 1), AddMembers: []string{"stale"}})
	require.NoError(t, err)
	_, ok = get(t, s, "counts:a")
	require.False(t, ok, "an Add does not revive an expired entry")
	won, err = s.PutIfAbsent(ctx, "counts:a", aggregate.Counters{Counts: counts("x", 2), AsOf: asOf}, time.Hour)
	require.NoError(t, err)
	require.True(t, won, "an expired entry counts as absent for PutIfAbsent")
	requireCounters(t, s, "counts:a", aggregate.Counters{Counts: counts("x", 2)})
}

func testAwkwardKeys(t *testing.T, s aggregate.Store) {
	ctx := context.Background()
	key := "name:cckv://con1owner/" + strings.Repeat("k", 300) + " https://schema.example/x_y%z*?[]"
	put(t, s, key, aggregate.Counters{Counts: counts("x", 1)})
	add(t, s, key, aggregate.Delta{Inc: counts("x", 1)})
	requireCounters(t, s, key, aggregate.Counters{Counts: counts("x", 2), Deltas: 1})
	require.NoError(t, s.Invalidate(ctx, key))
	_, ok := get(t, s, key)
	require.False(t, ok)

	// a definition name with pattern metacharacters drops only itself
	put(t, s, "a%_*?[]:x", aggregate.Counters{})
	put(t, s, "ab:x", aggregate.Counters{})
	require.NoError(t, s.InvalidateDefinition(ctx, "a%_*?[]"))
	_, ok = get(t, s, "a%_*?[]:x")
	require.False(t, ok)
	_, ok = get(t, s, "ab:x")
	require.True(t, ok)
}

// Counter names are schema URLs joined to user-chosen variants and members
// are arbitrary too: nothing in them may be read as syntax.
func testAwkwardNames(t *testing.T, s aggregate.Store) {
	names := []string{
		"https://schema.example/like.json\x1f",
		"https://schema.example/reaction.json\x1f👍",
		"https://schema.example/reaction.json\x1fhttps://emoji.example/a.b/c?d=e&f=`g`#h",
		"a.b", "a/b", "a b", "a`b", `a\b`, `a"b'c`, "a:b", "a,b", "a\nb", "{a}", "[a]", "$a", "~a", "*", "%_?",
		"__name__", "0", "-1", "", strings.Repeat("long", 500),
	}
	stored, delta, want := aggregate.Counters{Counts: map[string]int64{}, Minima: map[string]int64{}}, aggregate.Delta{Inc: map[string]int64{}, Min: map[string]int64{}}, aggregate.Counters{Counts: map[string]int64{}, Minima: map[string]int64{}, Deltas: 2}
	for i, name := range names {
		n := int64(i + 1)
		// half of them exist before the Add, half are created by it
		if i%2 == 0 {
			stored.Counts[name], stored.Minima[name] = n, early+n
			stored.Members = append(stored.Members, name)
		}
		delta.Inc[name], delta.Min[name] = n, early-n
		delta.AddMembers = append(delta.AddMembers, name)
		want.Counts[name], want.Minima[name] = stored.Counts[name]+2*n, early-n
		want.Members = append(want.Members, name)
	}
	put(t, s, "counts:a", stored)
	add(t, s, "counts:a", delta)
	add(t, s, "counts:a", delta)
	requireCounters(t, s, "counts:a", want)
}
