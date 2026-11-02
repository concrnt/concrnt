package aggregate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

const (
	DefaultTTL       = 24 * time.Hour
	DefaultMaxDeltas = 10000
)

// Definition describes one aggregate: how to compute its value for a target
// from the primary data (expensive) and how to apply a write's delta to a
// cached value (cheap). Apply nil means the aggregate has no delta form and
// every write invalidates instead.
type Definition[V any, D any] struct {
	Name    string
	Compute func(ctx context.Context, target string) (V, error)
	Apply   func(cur V, delta D) V
	// TTL and MaxDeltas override the usecase defaults when non-zero.
	TTL       time.Duration
	MaxDeltas int
}

type Usecase struct {
	store     Store
	ttl       time.Duration
	maxDeltas int
}

// New wraps store; a zero ttl or maxDeltas takes the default.
func New(store Store, ttl time.Duration, maxDeltas int) *Usecase {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	if maxDeltas <= 0 {
		maxDeltas = DefaultMaxDeltas
	}
	return &Usecase{store: store, ttl: ttl, maxDeltas: maxDeltas}
}

func (uc *Usecase) ttlOf(ttl time.Duration) time.Duration {
	if ttl > 0 {
		return ttl
	}
	return uc.ttl
}

func (uc *Usecase) maxDeltasOf(n int) int {
	if n > 0 {
		return n
	}
	return uc.maxDeltas
}

// Get is the read-through: the cached value, or a fresh computation that is
// stored for the next reader. A concurrent computation that stored first
// wins and its value is returned.
func Get[V, D any](ctx context.Context, uc *Usecase, def *Definition[V, D], target string) (V, error) {
	key := Key(def.Name, target)

	entry, ok, err := uc.store.Get(ctx, key)
	if err != nil {
		return *new(V), err
	}
	if ok {
		return decode[V](entry.Value)
	}

	value, won, err := computeAndPut(ctx, uc, def, target)
	if err != nil || won {
		return value, err
	}
	entry, ok, err = uc.store.Get(ctx, key)
	if err != nil || !ok {
		// the winner is already gone again: our own computation is as good
		return value, err
	}
	return decode[V](entry.Value)
}

// Touch folds a committed write into the cache: the delta is applied to an
// existing entry, or the value is recomputed (the write is committed, so the
// computation includes it). receivedAt is when the write became part of the
// primary data; a concurrent computation older than that may have missed
// it and gets the delta as well. The entry is dropped once it has absorbed
// more than MaxDeltas deltas, so drift from lost races is bounded by the
// next recomputation. Errors are returned for logging only: the write is
// already committed and the cache recovers by recomputation.
func Touch[V, D any](ctx context.Context, uc *Usecase, def *Definition[V, D], target string, delta D, receivedAt time.Time) error {
	key := Key(def.Name, target)
	if def.Apply == nil {
		return uc.store.Invalidate(ctx, key)
	}

	apply := func(old []byte) ([]byte, error) {
		cur, err := decode[V](old)
		if err != nil {
			return nil, err
		}
		return json.Marshal(def.Apply(cur, delta))
	}

	entry, ok, err := uc.store.Apply(ctx, key, apply)
	if err != nil {
		return errors.Join(err, uc.store.Invalidate(ctx, key))
	}
	if ok {
		if entry.Deltas > uc.maxDeltasOf(def.MaxDeltas) {
			return uc.store.Invalidate(ctx, key)
		}
		return nil
	}

	_, won, err := computeAndPut(ctx, uc, def, target)
	if err != nil || won {
		return err
	}
	entry, ok, err = uc.store.Get(ctx, key)
	if err != nil || !ok {
		return err
	}
	if entry.AsOf.Before(receivedAt) {
		if _, _, err := uc.store.Apply(ctx, key, apply); err != nil {
			return errors.Join(err, uc.store.Invalidate(ctx, key))
		}
	}
	return nil
}

// Invalidate drops one entry.
func (uc *Usecase) Invalidate(ctx context.Context, name, target string) error {
	return uc.store.Invalidate(ctx, Key(name, target))
}

// InvalidateAll drops every entry under prefix: "" for all of them,
// Key(name, "") for one definition, Key(name, targetPrefix) for a family.
func (uc *Usecase) InvalidateAll(ctx context.Context, prefix string) error {
	return uc.store.InvalidateAll(ctx, prefix)
}

// Cached returns the stored value for target without computing it; false
// when the cache has none (verification tooling).
func Cached[V, D any](ctx context.Context, uc *Usecase, def *Definition[V, D], target string) (V, bool, error) {
	entry, ok, err := uc.store.Get(ctx, Key(def.Name, target))
	if err != nil || !ok {
		return *new(V), false, err
	}
	value, err := decode[V](entry.Value)
	return value, err == nil, err
}

// computeAndPut computes the value for target and offers it to the store;
// won is false when another entry got there first.
func computeAndPut[V, D any](ctx context.Context, uc *Usecase, def *Definition[V, D], target string) (V, bool, error) {
	asOf := time.Now()
	value, err := def.Compute(ctx, target)
	if err != nil {
		return value, false, fmt.Errorf("aggregate %s: compute %q: %w", def.Name, target, err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return value, false, err
	}
	won, err := uc.store.PutIfAbsent(ctx, Key(def.Name, target), Entry{Value: encoded, AsOf: asOf}, uc.ttlOf(def.TTL))
	if err != nil {
		slog.Warn("aggregate: store rejected a computed value", slog.String("name", def.Name), slog.String("error", err.Error()))
		return value, false, err
	}
	return value, won, nil
}

func decode[V any](raw []byte) (V, error) {
	var value V
	if err := json.Unmarshal(raw, &value); err != nil {
		return value, fmt.Errorf("aggregate: corrupt cached value: %w", err)
	}
	return value, nil
}
