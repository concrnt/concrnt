package aggregate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

const (
	DefaultTTL       = 24 * time.Hour
	DefaultMaxDeltas = 10000
)

// Definition describes one aggregate: how to compute a target's counters
// from the primary data (expensive). A write is folded into a cached entry
// with Touch when it has a delta form and drops the entry with Invalidate
// when it has none.
type Definition struct {
	Name    string
	Compute func(ctx context.Context, target string) (Counters, error)
}

type Usecase struct {
	store     Store
	ttl       time.Duration
	maxDeltas int64
}

// New wraps store; a zero ttl or maxDeltas takes the default.
func New(store Store, ttl time.Duration, maxDeltas int) *Usecase {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	if maxDeltas <= 0 {
		maxDeltas = DefaultMaxDeltas
	}
	return &Usecase{store: store, ttl: ttl, maxDeltas: int64(maxDeltas)}
}

// Get is the read-through: the cached entry, or a fresh computation that is
// stored for the next reader. A concurrent computation that stored first
// wins and its entry is returned. Writers add their deltas blind, so it is
// the reader that bounds the drift of an entry: one that has absorbed more
// than MaxDeltas deltas is dropped and recomputed here.
func (uc *Usecase) Get(ctx context.Context, def *Definition, target string) (Counters, error) {
	key := Key(def.Name, target)

	entry, ok, err := uc.store.Get(ctx, key)
	if err != nil {
		return Counters{}, err
	}
	if ok && entry.Deltas <= uc.maxDeltas {
		return normalized(*entry), nil
	}
	if ok {
		if err := uc.store.Invalidate(ctx, key); err != nil {
			return Counters{}, err
		}
	}

	computed, won, err := uc.computeAndPut(ctx, def, target)
	if err != nil || won {
		return computed, err
	}
	entry, ok, err = uc.store.Get(ctx, key)
	if err != nil || !ok {
		// the winner is already gone again: our own computation is as good
		return computed, err
	}
	return normalized(*entry), nil
}

// Touch folds a committed write into the cache: the delta is added to an
// existing entry without reading it, or the entry is recomputed (the write
// is committed, so the computation includes it). receivedAt is when the
// write became part of the primary data; a concurrent computation older
// than that may have missed it and gets the delta as well. Errors are
// returned for logging only: the write is already committed and the cache
// recovers by recomputation.
func (uc *Usecase) Touch(ctx context.Context, def *Definition, target string, delta Delta, receivedAt time.Time) error {
	key := Key(def.Name, target)

	ok, err := uc.store.Add(ctx, key, delta)
	if err != nil {
		return errors.Join(err, uc.store.Invalidate(ctx, key))
	}
	if ok {
		return nil
	}

	_, won, err := uc.computeAndPut(ctx, def, target)
	if err != nil || won {
		return err
	}
	entry, ok, err := uc.store.Get(ctx, key)
	if err != nil || !ok {
		return err
	}
	if entry.AsOf.Before(receivedAt) {
		if _, err := uc.store.Add(ctx, key, delta); err != nil {
			return errors.Join(err, uc.store.Invalidate(ctx, key))
		}
	}
	return nil
}

// Seed stores counters for target when the cache has no entry, for a writer
// that knows the value without computing it (a record just created has no
// associations). A concurrent computation that stored first wins.
func (uc *Usecase) Seed(ctx context.Context, def *Definition, target string, counters Counters) error {
	counters.AsOf = time.Now()
	_, err := uc.store.PutIfAbsent(ctx, Key(def.Name, target), counters, uc.ttl)
	return err
}

// Invalidate drops one entry.
func (uc *Usecase) Invalidate(ctx context.Context, name, target string) error {
	return uc.store.Invalidate(ctx, Key(name, target))
}

// InvalidateDefinition drops every entry of definition name.
func (uc *Usecase) InvalidateDefinition(ctx context.Context, name string) error {
	return uc.store.InvalidateDefinition(ctx, name)
}

// InvalidateAll drops every entry.
func (uc *Usecase) InvalidateAll(ctx context.Context) error {
	return uc.store.InvalidateAll(ctx)
}

// Cached returns the stored entry for target without computing it; false
// when the cache has none (verification tooling).
func (uc *Usecase) Cached(ctx context.Context, def *Definition, target string) (Counters, bool, error) {
	entry, ok, err := uc.store.Get(ctx, Key(def.Name, target))
	if err != nil || !ok {
		return Counters{}, false, err
	}
	return normalized(*entry), true, nil
}

// computeAndPut computes the counters for target and offers them to the
// store; won is false when another entry got there first.
func (uc *Usecase) computeAndPut(ctx context.Context, def *Definition, target string) (Counters, bool, error) {
	asOf := time.Now()
	computed, err := def.Compute(ctx, target)
	if err != nil {
		return Counters{}, false, fmt.Errorf("aggregate %s: compute %q: %w", def.Name, target, err)
	}
	computed.AsOf, computed.Deltas = asOf, 0
	computed = normalized(computed)
	won, err := uc.store.PutIfAbsent(ctx, Key(def.Name, target), computed, uc.ttl)
	if err != nil {
		slog.Warn("aggregate: store rejected a computed value", slog.String("name", def.Name), slog.String("error", err.Error()))
		return computed, false, err
	}
	return computed, won, nil
}

// normalized gives the callers maps they can index and serialise without
// telling an empty entry from one a store read back as nil.
func normalized(c Counters) Counters {
	if c.Counts == nil {
		c.Counts = map[string]int64{}
	}
	if c.Minima == nil {
		c.Minima = map[string]int64{}
	}
	return c
}
