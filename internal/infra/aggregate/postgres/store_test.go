package postgres_test

import (
	"testing"

	"github.com/concrnt/concrnt/internal/infra/aggregate/postgres"
	"github.com/concrnt/concrnt/internal/infra/aggregate/storetest"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

func TestPostgresStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) aggregate.Store {
		db, cleanup := testutil.CreateDB()
		t.Cleanup(cleanup)
		return postgres.New(db)
	})
}
