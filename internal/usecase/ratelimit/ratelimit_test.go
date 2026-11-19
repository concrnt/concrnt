package ratelimit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// memStore is a Store in memory. Expiry is not modelled: the limiter never
// reads a window older than the previous one.
type memStore struct {
	mu       sync.Mutex
	counters map[string]int64
	ttls     map[string]time.Duration
	err      error
}

func newMemStore() *memStore {
	return &memStore{counters: map[string]int64{}, ttls: map[string]time.Duration{}}
}

func (s *memStore) IncrExpire(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return 0, s.err
	}
	s.counters[key]++
	s.ttls[key] = ttl
	return s.counters[key], nil
}

func (s *memStore) Decr(ctx context.Context, key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counters[key]--
	return s.counters[key], nil
}

func (s *memStore) GetInt(ctx context.Context, key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counters[key], s.err
}

// fixture runs a limiter on a clock the test moves by hand. The clock starts
// on a window boundary.
type fixture struct {
	t       *testing.T
	store   *memStore
	limiter *Limiter
	now     time.Time
}

func newFixture(t *testing.T) *fixture {
	f := &fixture{t: t, store: newMemStore(), now: time.Unix(1_800_000_000, 0).Truncate(time.Minute)}
	f.limiter = New(f.store)
	f.limiter.now = func() time.Time { return f.now }
	return f
}

func (f *fixture) allow(key string, limit Limit) Result {
	f.t.Helper()
	result, err := f.limiter.Allow(context.Background(), key, limit)
	if err != nil {
		f.t.Fatalf("Allow: %v", err)
	}
	return result
}

// allowed counts how many of n events get through.
func (f *fixture) allowed(key string, limit Limit, n int) int {
	f.t.Helper()
	passed := 0
	for range n {
		if f.allow(key, limit).Allowed {
			passed++
		}
	}
	return passed
}

func TestAllowUpToLimitThenRejects(t *testing.T) {
	f := newFixture(t)
	limit := PerMinute(5)

	for i := range 5 {
		result := f.allow("k", limit)
		if !result.Allowed {
			t.Fatalf("event %d rejected, want allowed", i+1)
		}
		if want := 4 - i; result.Remaining != want {
			t.Fatalf("event %d: Remaining = %d, want %d", i+1, result.Remaining, want)
		}
	}

	result := f.allow("k", limit)
	if result.Allowed {
		t.Fatalf("6th event allowed, want rejected")
	}
	if result.RetryAfter <= 0 || result.RetryAfter > time.Minute {
		t.Fatalf("RetryAfter = %v, want within (0, 1m]", result.RetryAfter)
	}
}

func TestBucketsAreIndependent(t *testing.T) {
	f := newFixture(t)
	limit := PerMinute(2)

	if got := f.allowed("a", limit, 5); got != 2 {
		t.Fatalf("bucket a passed %d, want 2", got)
	}
	if got := f.allowed("b", limit, 5); got != 2 {
		t.Fatalf("bucket b passed %d, want 2 (a's use must not count)", got)
	}
}

// The previous window keeps counting, weighted by how much of it the trailing
// window still covers: there is no fresh full allowance at a window boundary.
func TestPreviousWindowSlidesOut(t *testing.T) {
	f := newFixture(t)
	limit := PerMinute(10)

	if got := f.allowed("k", limit, 10); got != 10 {
		t.Fatalf("first window passed %d, want 10", got)
	}

	f.now = f.now.Add(time.Minute) // boundary: previous window fully covered
	if f.allow("k", limit).Allowed {
		t.Fatalf("event at the window boundary allowed, want rejected")
	}

	f.now = f.now.Add(30 * time.Second) // half of the previous window left
	if got := f.allowed("k", limit, 10); got != 5 {
		t.Fatalf("halfway through passed %d, want 5", got)
	}

	f.now = f.now.Add(90 * time.Second) // the full window is two windows back
	if got := f.allowed("k", limit, 20); got != 10 {
		t.Fatalf("after the window slid out passed %d, want 10", got)
	}
}

// A sender running over its limit still gets the limit through: rejected
// events do not count against it.
func TestRejectedEventsAreNotCounted(t *testing.T) {
	f := newFixture(t)
	limit := PerMinute(10)

	passed := 0
	for range 10 * 60 { // 100 events a second for ten minutes
		passed += f.allowed("k", limit, 100)
		f.now = f.now.Add(time.Second)
	}
	// 10 a minute over ten minutes; the sliding estimate may run a few short
	if passed < 90 || passed > 100 {
		t.Fatalf("passed %d over ten minutes, want about 100", passed)
	}
}

// RetryAfter is when the rejected event would fit, not just the window end.
func TestRetryAfterIsWhenTheEventFits(t *testing.T) {
	f := newFixture(t)
	limit := PerMinute(10)

	f.allowed("k", limit, 10)
	f.now = f.now.Add(time.Minute + 30*time.Second)
	f.allowed("k", limit, 5) // now full: 10*0.5 carried + 5

	result := f.allow("k", limit)
	if result.Allowed {
		t.Fatalf("event allowed, want rejected")
	}
	// one more fits once carried <= 4, i.e. 36s into the window: 6s from now
	if result.RetryAfter != 6*time.Second {
		t.Fatalf("RetryAfter = %v, want 6s", result.RetryAfter)
	}

	f.now = f.now.Add(result.RetryAfter)
	if !f.allow("k", limit).Allowed {
		t.Fatalf("event after RetryAfter rejected, want allowed")
	}
}

func TestConcurrentCallersNeverExceedLimit(t *testing.T) {
	f := newFixture(t)
	limit := PerMinute(50)

	var wg sync.WaitGroup
	var mu sync.Mutex
	passed := 0
	for range 500 {
		wg.Go(func() {
			result, err := f.limiter.Allow(context.Background(), "k", limit)
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

	if passed > 50 {
		t.Fatalf("passed %d, want at most 50", passed)
	}
	if passed == 0 {
		t.Fatalf("passed 0, want some")
	}
}

func TestNonPositiveLimitIsUnlimited(t *testing.T) {
	f := newFixture(t)

	if got := f.allowed("k", Limit{}, 1000); got != 1000 {
		t.Fatalf("passed %d, want 1000", got)
	}
	if len(f.store.counters) != 0 {
		t.Fatalf("unlimited bucket touched the store: %v", f.store.counters)
	}
}

func TestCounterOutlivesItsWindowByOne(t *testing.T) {
	f := newFixture(t)

	f.allow("k", PerMinute(5))
	for key, ttl := range f.store.ttls {
		if ttl != 2*time.Minute {
			t.Fatalf("ttl of %s = %v, want 2m", key, ttl)
		}
	}
}

func TestStoreErrorIsReturned(t *testing.T) {
	f := newFixture(t)
	f.store.err = errors.New("store down")

	if _, err := f.limiter.Allow(context.Background(), "k", PerMinute(5)); err == nil {
		t.Fatalf("Allow returned no error, want the store's")
	}
}
