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
