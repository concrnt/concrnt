package spanner_test

import (
	"os"
	"testing"

	"github.com/concrnt/concrnt/internal/infra/aggregate/spanner"
	"github.com/concrnt/concrnt/internal/infra/aggregate/storetest"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testutil.PurgeSpanner()
	os.Exit(code)
}

func TestSpannerStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) aggregate.Store {
		client, _ := testutil.CreateSpanner(t)
		return spanner.New(client)
	})
}
