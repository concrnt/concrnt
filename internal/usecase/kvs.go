package usecase

import (
	"context"
	"time"
)

// KVS is a store of TTL'd string sets and flags, used to hold per-timeline
// removed-item advertisements (written by record on delete, served by
// chunkline) and throttle markers (record's hint-triggered entity recheck).
// Implemented by internal/infra/kvs.Redis; nil in offline tooling/tests.
type KVS interface {
	SetAdd(ctx context.Context, key string, value string, ttl time.Duration) error
	SetMembers(ctx context.Context, key string) ([]string, error)
	// SetNX stores value under key with ttl only when key is absent, and
	// reports whether it did. Atomic, so concurrent callers see one winner.
	SetNX(ctx context.Context, key string, value string, ttl time.Duration) (bool, error)
}

// RemovedItemsTTL bounds how long a timeline advertises a removed item;
// mirrors v1's 2-day retracted window. Readers only use the list to filter
// already-cached chunks, so entries outliving their usefulness are harmless.
const RemovedItemsTTL = 48 * time.Hour

// RemovedItemsKey namespaces the KVS set of item IDs recently removed from a
// timeline, written by record on delete and served via /chunkline/removed.
func RemovedItemsKey(timeline string) string { return "removed:" + timeline }
