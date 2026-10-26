package kvs_test

import (
	"context"
	"testing"
	"time"

	"github.com/concrnt/concrnt/internal/infra/kvs"
	"github.com/concrnt/concrnt/internal/testutil"
)

// SetNX is the throttle primitive behind record's hint-triggered entity
// recheck: the first caller for a key wins and the key expires on its own.
func TestRedisSetNX(t *testing.T) {
	rdb, cleanup := testutil.CreateRDB()
	defer cleanup()

	store := kvs.NewRedis(rdb)
	ctx := context.Background()

	first, err := store.SetNX(ctx, "entity-recheck:con1test@remote.example.net", "1", time.Hour)
	if err != nil {
		t.Fatalf("SetNX: %v", err)
	}
	if !first {
		t.Fatalf("first SetNX = false, want true")
	}

	second, err := store.SetNX(ctx, "entity-recheck:con1test@remote.example.net", "1", time.Hour)
	if err != nil {
		t.Fatalf("second SetNX: %v", err)
	}
	if second {
		t.Fatalf("second SetNX = true, want false (key already present)")
	}

	ttl, err := rdb.TTL(ctx, "entity-recheck:con1test@remote.example.net").Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl <= 0 || ttl > time.Hour {
		t.Fatalf("TTL = %v, want (0, 1h]", ttl)
	}

	other, err := store.SetNX(ctx, "entity-recheck:con1test@other.example.net", "1", time.Hour)
	if err != nil {
		t.Fatalf("SetNX other key: %v", err)
	}
	if !other {
		t.Fatalf("SetNX on a different key = false, want true")
	}
}
