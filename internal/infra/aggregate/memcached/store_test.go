package memcached_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/aggregate/memcached"
	"github.com/concrnt/concrnt/internal/infra/aggregate/storetest"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

func TestMemcachedStore(t *testing.T) {
	mc, cleanup := testutil.CreateMC()
	t.Cleanup(cleanup)

	// one memcached, a namespace per store keeps the cases apart
	var n atomic.Int64
	open := func(t *testing.T) aggregate.Store {
		return memcached.New(mc, "t"+strconv.FormatInt(n.Add(1), 10)+":")
	}
	storetest.Run(t, open)

	// a store that dropped everything is seen by another store (another
	// process) once its generation cache has been refreshed; a fresh store
	// reads the generations straight away
	t.Run("InvalidateAllReachesOtherProcesses", func(t *testing.T) {
		ctx := context.Background()
		a := memcached.New(mc, "shared:")
		won, err := a.PutIfAbsent(ctx, "counts:x", aggregate.Entry{Value: []byte(`1`)}, time.Hour)
		require.NoError(t, err)
		require.True(t, won)

		require.NoError(t, memcached.New(mc, "shared:").InvalidateDefinition(ctx, "counts"))

		_, ok, err := memcached.New(mc, "shared:").Get(ctx, "counts:x")
		require.NoError(t, err)
		require.False(t, ok)
	})
}
