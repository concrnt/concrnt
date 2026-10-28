// Package aggregate keeps derived values (counts) as a cache in front of
// the primary data (CONTRACT.md C2). An entry is recomputed from the
// primary data whenever it is missing, adjusted in place by a delta when it
// exists, and dropped whenever a write is too complex to express as a delta.
// The store is interchangeable (Postgres table, Redis, Firestore ...) and
// never the source of truth: deleting everything in it is always safe.
package aggregate

import (
	"context"
	"time"
)

// Entry is one cached value. AsOf is when the value was computed from the
// primary data; it is not touched by Apply, so a later writer can tell
// whether the computation could have seen its own item. Deltas counts the
// Apply calls since the computation.
type Entry struct {
	Value  []byte
	AsOf   time.Time
	Deltas int
}

// Store is the cache backend. Keys are opaque strings (Key builds them);
// every operation is atomic per key. A backend that cannot run fn inside
// its own transaction (Firestore, Redis) implements Apply as a bounded
// compare-and-swap loop.
type Store interface {
	// Get returns the entry under key; false when absent or expired.
	Get(ctx context.Context, key string) (*Entry, bool, error)
	// PutIfAbsent stores entry under key with ttl only when no live entry
	// exists and reports whether it did (the loser of a concurrent
	// computation reads the winner's value instead).
	PutIfAbsent(ctx context.Context, key string, entry Entry, ttl time.Duration) (bool, error)
	// Apply replaces the value of an existing entry with fn(old) atomically,
	// increments Deltas and keeps AsOf and the ttl; it returns the updated
	// entry, or false when there is no live entry to apply to.
	Apply(ctx context.Context, key string, fn func(old []byte) ([]byte, error)) (*Entry, bool, error)
	// Invalidate drops the entry under key; a missing entry is fine.
	Invalidate(ctx context.Context, key string) error
	// InvalidateAll drops every entry whose key starts with prefix ("" drops
	// everything). Key prefixes are a definition name followed by ":", so
	// a whole definition, or one target's family of entries, can be dropped
	// at once.
	InvalidateAll(ctx context.Context, prefix string) error
}

// Key is the store key of a definition's entry for target.
func Key(name, target string) string {
	return name + ":" + target
}
