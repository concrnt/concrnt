package redis_test

import (
	"testing"

	"github.com/concrnt/concrnt/internal/infra/aggregate/redis"
	"github.com/concrnt/concrnt/internal/infra/aggregate/storetest"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

func TestRedisStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) aggregate.Store {
		rdb, cleanup := testutil.CreateRDB()
		t.Cleanup(cleanup)
		return redis.New(rdb)
	})
}
