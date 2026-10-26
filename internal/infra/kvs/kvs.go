package kvs

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis is a small store of TTL'd string sets over redis. Higher layers build
// specific stores (e.g. removed-item advertisements) on top of it.
type Redis struct {
	rdb *redis.Client
}

func NewRedis(rdb *redis.Client) *Redis {
	return &Redis{rdb: rdb}
}

// SetAdd adds a member to a set and refreshes the whole set's TTL.
func (r *Redis) SetAdd(ctx context.Context, key string, value string, ttl time.Duration) error {
	if err := r.rdb.SAdd(ctx, key, value).Err(); err != nil {
		return err
	}
	return r.rdb.Expire(ctx, key, ttl).Err()
}

func (r *Redis) SetMembers(ctx context.Context, key string) ([]string, error) {
	return r.rdb.SMembers(ctx, key).Result()
}

// SetNX stores value under key with ttl only when key is absent (one SET NX
// PX command, so it is atomic) and reports whether it did.
func (r *Redis) SetNX(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	return r.rdb.SetNX(ctx, key, value, ttl).Result()
}

// Incr increments an integer counter and returns the new value. The key is
// created at 1 when missing and never expires.
func (r *Redis) Incr(ctx context.Context, key string) (int64, error) {
	return r.rdb.Incr(ctx, key).Result()
}

// GetInt reads an integer counter; a missing key reads as 0.
func (r *Redis) GetInt(ctx context.Context, key string) (int64, error) {
	value, err := r.rdb.Get(ctx, key).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return value, err
}

func (r *Redis) Delete(ctx context.Context, key string) error {
	return r.rdb.Del(ctx, key).Err()
}
