package record

import (
	"testing"

	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/concrnt/concrnt/internal/usecase/record/ciptest"
)

func TestPostgresCIP(t *testing.T) {
	ciptest.Run(t, func(t *testing.T) ciptest.Backend {
		db, cleanup := testutil.CreateDB()
		t.Cleanup(cleanup)
		return ciptest.Backend{
			Record:     New(db),
			EntityMeta: postgres.NewEntityMetaRepository(db),
		}
	})
}
