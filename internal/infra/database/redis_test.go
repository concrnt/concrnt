package database_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/database"
	"github.com/concrnt/concrnt/internal/testutil"
)

// Every job queue consumer parks one pooled connection in a blocking read
// while the queue is idle, and the requests share what is left. With the
// pool exhausted Redis still answers at once, but a ping through that pool
// does not get a connection: that is the readiness failure the probe
// client exists to avoid.
func TestRedisProbeIsIndependentOfTheServingPool(t *testing.T) {
	rdb, cleanup := testutil.CreateRDB()
	defer cleanup()
	addr := rdb.Options().Addr

	const poolSize = 4
	serving := redis.NewClient(&redis.Options{Addr: addr, Password: rdb.Options().Password, PoolSize: poolSize})
	var consumers sync.WaitGroup
	defer consumers.Wait()
	// closing the client is what ends the blocking reads
	defer serving.Close()
	for range poolSize {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			serving.BLPop(context.Background(), 30*time.Second, "readiness-test:never-pushed")
		}()
	}
	require.Eventually(t, func() bool {
		stats := serving.PoolStats()
		return stats.TotalConns == poolSize && stats.IdleConns == 0
	}, 5*time.Second, 10*time.Millisecond, "the consumers hold the whole pool")

	deadline, cancelDeadline := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancelDeadline()
	require.Error(t, serving.Ping(deadline).Err(), "a ping through the serving pool waits for a connection")

	probe := database.NewRedisProbe(addr, rdb.Options().Password, 0)
	defer probe.Close()
	for range 3 {
		deadline, cancelDeadline := context.WithTimeout(context.Background(), 300*time.Millisecond)
		require.NoError(t, probe.Ping(deadline).Err(), "redis itself answers")
		cancelDeadline()
	}
	require.EqualValues(t, 1, probe.PoolStats().TotalConns, "the probe keeps one connection")
}
