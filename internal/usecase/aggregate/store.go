// Package aggregate keeps derived values (counts) as a cache in front of
// the primary data (CONTRACT.md C2). An entry is a set of named counters:
// it is recomputed from the primary data whenever it is missing, adjusted
// in place by a blind, commutative delta when it exists, and dropped
// whenever a write is too complex to express as a delta. The store is
// interchangeable (Postgres table, Redis, Firestore ...) and never the
// source of truth: deleting everything in it is always safe.
package aggregate

import (
	"context"
	"slices"
	"strings"
	"time"
)

// Counters is one cached entry.
type Counters struct {
	// Counts maps a counter name to its value.
	Counts map[string]int64
	// Minima maps a counter name to the smallest value offered for it.
	Minima map[string]int64
	// Members is a set of distinct strings, in no particular order.
	Members []string
	// AsOf is when the entry was computed from the primary data. Add does
	// not touch it, so a writer can tell whether the computation could have
	// seen its own item.
	AsOf time.Time
	// Deltas counts the Add calls since the computation.
	Deltas int64
}

// Delta is one write folded into an entry. Every part is commutative, so
// deltas land in any order with the same result.
type Delta struct {
	// Inc is added to Counts (may be negative); a missing counter starts
	// at zero.
	Inc map[string]int64
	// Min lowers Minima: Minima[k] = min(existing, v), a missing one is set.
	Min map[string]int64
	// AddMembers joins Members (set union).
	AddMembers []string
}

// Add folds d into c the way Store.Add does, for the stores that keep an
// entry as one value.
func (c *Counters) Add(d Delta) {
	if len(d.Inc) > 0 && c.Counts == nil {
		c.Counts = make(map[string]int64, len(d.Inc))
	}
	for name, n := range d.Inc {
		c.Counts[name] += n
	}
	if len(d.Min) > 0 && c.Minima == nil {
		c.Minima = make(map[string]int64, len(d.Min))
	}
	for name, v := range d.Min {
		if cur, ok := c.Minima[name]; !ok || v < cur {
			c.Minima[name] = v
		}
	}
	for _, m := range d.AddMembers {
		if !slices.Contains(c.Members, m) {
			c.Members = append(c.Members, m)
		}
	}
	c.Deltas++
}

// Store is the cache backend. Keys are opaque strings (Key builds them);
// counter names and members are arbitrary strings too. Empty maps and an
// empty member set may read back as nil.
type Store interface {
	// Get returns the entry under key; false when absent or expired.
	Get(ctx context.Context, key string) (*Counters, bool, error)
	// PutIfAbsent stores entry under key with ttl only when no live entry
	// exists and reports whether it did (the loser of a concurrent
	// computation reads the winner's value instead). The stored entry
	// starts with Deltas 0.
	PutIfAbsent(ctx context.Context, key string, entry Counters, ttl time.Duration) (bool, error)
	// Add folds delta into the entry under key, increments Deltas and
	// keeps AsOf and the ttl; false when there is no entry, and nothing is
	// created then. It is blind: concurrent Adds on one key all land, in
	// any order, without the caller retrying and without a read of the
	// entry. A store that cannot see expiry without reading may report true
	// for an entry that has expired but is not collected yet; the entry
	// stays absent for Get and PutIfAbsent replaces it whole.
	Add(ctx context.Context, key string, delta Delta) (bool, error)
	// Invalidate drops the entry under key; a missing entry is fine.
	Invalidate(ctx context.Context, key string) error
	// InvalidateDefinition drops every entry of definition name (every key
	// Key(name, ...) built).
	InvalidateDefinition(ctx context.Context, name string) error
	// InvalidateAll drops every entry.
	InvalidateAll(ctx context.Context) error
}

// Key is the store key of a definition's entry for target: the name, ":"
// and the target. A name contains no ":", so a store can tell the
// definition of a key by cutting it at the first one (NameOf).
func Key(name, target string) string {
	return name + ":" + target
}

// NameOf is the definition name of a key Key built.
func NameOf(key string) string {
	name, _, _ := strings.Cut(key, ":")
	return name
}
