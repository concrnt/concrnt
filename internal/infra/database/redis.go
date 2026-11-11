package database

import (
	"runtime"

	"github.com/redis/go-redis/v9"
)

func NewRedis(addr string, password string, db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
}

// NewRedisServing is NewRedis for the server process. Each job queue
// consumer parks one connection in a blocking stream read for as long as the
// queue is idle, so the pool is the client's default (10 per CPU) plus one
// per consumer: otherwise the consumers alone hold most of it and requests
// (publish, enqueue, the KVS) queue for what is left.
func NewRedisServing(addr string, password string, db int, jobQueueConsumers int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
		PoolSize: 10*runtime.GOMAXPROCS(0) + jobQueueConsumers,
	})
}

// NewRedisProbe opens a client for the readiness check alone. The serving
// client's pool is shared by every request and by the job queue consumers,
// each of which parks a connection in a blocking read, so a ping through it
// waits behind the load and measures the pool, not Redis. This one keeps a
// single connection of its own and honours the check's deadline.
func NewRedisProbe(addr string, password string, db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:                  addr,
		Password:              password,
		DB:                    db,
		PoolSize:              1,
		ContextTimeoutEnabled: true,
	})
}
