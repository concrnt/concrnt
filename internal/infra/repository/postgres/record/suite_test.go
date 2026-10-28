package record

import (
	"testing"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
	"github.com/concrnt/concrnt/internal/testutil"
)

// openBackend spins up a fresh Postgres and wires the repositories under test
// into a repotest.Backend.
func openBackend(t *testing.T) repotest.Backend {
	t.Helper()
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)
	return repotest.Backend{
		Record:     New(db, nil, domain.Config{}),
		EntityMeta: postgres.NewEntityMetaRepository(db),
		Inspect:    newInspector(db),
	}
}

func TestPostgresRecordSuite(t *testing.T)      { repotest.RunRecordSuite(t, openBackend) }
func TestPostgresAckSuite(t *testing.T)         { repotest.RunAckSuite(t, openBackend) }
func TestPostgresChunklineSuite(t *testing.T)   { repotest.RunChunklineSuite(t, openBackend) }
func TestPostgresResidenceSuite(t *testing.T)   { repotest.RunResidenceSuite(t, openBackend) }
func TestPostgresReplicationSuite(t *testing.T) { repotest.RunReplicationSuite(t, openBackend) }
func TestPostgresMaintenanceSuite(t *testing.T) { repotest.RunMaintenanceSuite(t, openBackend) }
