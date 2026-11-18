// Package ratelimit is a sliding-window-counter rate limiter over a shared
// counter store. It knows nothing about HTTP or commits: callers name a
// bucket, pass the limit that applies to it right now, and get a verdict —
// so it can back the commit ingest limit today and a middleware later.
package ratelimit

import (
	"context"
	"math"
	"strconv"
	"time"
)

// Store is the counter primitive the limiter needs. Every operation is a
// single atomic counter command on the backing store (redis INCR/DECR/GET,
// memcached incr/decr/get), so the limiter needs no scripting and no
// read-modify-write cycle.
type Store interface {
	// IncrExpire increments key (created at 1 when missing), makes it expire
	// ttl from now, and returns the new value.
	IncrExpire(ctx context.Context, key string, ttl time.Duration) (int64, error)
	// Decr decrements key and returns the new value.
	Decr(ctx context.Context, key string) (int64, error)
	// GetInt reads key; a missing key reads as 0.
	GetInt(ctx context.Context, key string) (int64, error)
}

// Limit allows Count events per Window.
type Limit struct {
	Count  int
	Window time.Duration
}

// PerMinute is the limit of n events per minute.
func PerMinute(n int) Limit {
	return Limit{Count: n, Window: time.Minute}
}

type Result struct {
	Allowed bool
	// Remaining is how many more events the bucket takes right now.
	Remaining int
	// RetryAfter is how long until the bucket takes another event; zero when
	// Allowed.
	RetryAfter time.Duration
}

type Limiter struct {
	store Store
	now   func() time.Time
}

func New(store Store) *Limiter {
	return &Limiter{store: store, now: time.Now}
}

// Allow counts one event against key and reports whether it fits limit.
//
// Time is cut into fixed windows, each with its own counter. The rate over
// the trailing Window is estimated as the current window's count plus the
// previous window's count weighted by how much of it the trailing Window
// still covers. A rejected event is taken back out of the counter, so a
// sender running over its limit keeps getting Count events per Window through
// rather than being shut out for as long as it keeps trying.
//
// A non-positive limit means unlimited.
func (l *Limiter) Allow(ctx context.Context, key string, limit Limit) (Result, error) {
	if limit.Count <= 0 || limit.Window <= 0 {
		return Result{Allowed: true, Remaining: math.MaxInt}, nil
	}

	now := l.now().UnixNano()
	window := limit.Window.Nanoseconds()
	index := now / window
	elapsed := now - index*window
	prevWeight := 1 - float64(elapsed)/float64(window)

	currKey := windowKey(key, index)

	prev, err := l.store.GetInt(ctx, windowKey(key, index-1))
	if err != nil {
		return Result{}, err
	}
	// the counter is read as the previous window for one more Window after
	// its own ends
	curr, err := l.store.IncrExpire(ctx, currKey, 2*limit.Window)
	if err != nil {
		return Result{}, err
	}

	carried := float64(prev) * prevWeight
	room := float64(limit.Count) - carried - float64(curr)
	if room >= 0 {
		return Result{Allowed: true, Remaining: int(room)}, nil
	}

	if _, err := l.store.Decr(ctx, currKey); err != nil {
		return Result{}, err
	}

	// The estimate only falls as the previous window slides out: this event
	// fits once carried <= Count - curr. If the current window alone is
	// already full, nothing fits before it ends.
	retryAfter := time.Duration(window - elapsed)
	if free := float64(limit.Count) - float64(curr); free >= 0 && prev > 0 {
		fitsAt := int64(math.Ceil(float64(window) * (1 - free/float64(prev))))
		if wait := fitsAt - elapsed; wait > 0 && wait < window-elapsed {
			retryAfter = time.Duration(wait)
		}
	}
	return Result{Allowed: false, RetryAfter: retryAfter}, nil
}

func windowKey(key string, index int64) string {
	return "ratelimit:" + key + ":" + strconv.FormatInt(index, 10)
}
