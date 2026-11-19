package kvs_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/concrnt/concrnt/internal/infra/kvs"
	"github.com/concrnt/concrnt/internal/usecase/ratelimit"
)

var _ ratelimit.Store = (*kvs.Redis)(nil)

func testRedisCounter(t *testing.T, rdb *redis.Client) {
	store := kvs.NewRedis(rdb)
	ctx := context.Background()
	const key = "ratelimit:test:1"

	missing, err := store.GetInt(ctx, key)
	if err != nil {
		t.Fatalf("GetInt on a missing key: %v", err)
	}
	if missing != 0 {
		t.Fatalf("missing key = %d, want 0", missing)
	}

	for want := int64(1); want <= 3; want++ {
		got, err := store.IncrExpire(ctx, key, time.Hour)
		if err != nil {
			t.Fatalf("IncrExpire: %v", err)
		}
		if got != want {
			t.Fatalf("IncrExpire = %d, want %d", got, want)
		}
	}

	ttl, err := rdb.TTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl <= 0 || ttl > time.Hour {
		t.Fatalf("TTL = %v, want (0, 1h]", ttl)
	}

	decremented, err := store.Decr(ctx, key)
	if err != nil {
		t.Fatalf("Decr: %v", err)
	}
	if decremented != 2 {
		t.Fatalf("Decr = %d, want 2", decremented)
	}

	// through the real store, concurrent callers never get more than the
	// limit between them
	limiter := ratelimit.New(store)
	var wg sync.WaitGroup
	var mu sync.Mutex
	passed := 0
	for range 200 {
		wg.Go(func() {
			result, err := limiter.Allow(ctx, "concurrent", ratelimit.Limit{Count: 20, Window: time.Hour})
			if err != nil {
				t.Errorf("Allow: %v", err)
				return
			}
			if result.Allowed {
				mu.Lock()
				passed++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if passed != 20 {
		t.Fatalf("passed %d of 200 concurrent events, want 20", passed)
	}
}
