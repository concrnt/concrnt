package postgres

import (
	"testing"

	"github.com/concrnt/concrnt/internal/domain"
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
		Record:    NewRecordRepository(db),
		Residence: NewResidenceRepository(db, nil, domain.Config{}),
		Chunkline: NewChunklineRepository(db),
		Inspect:   NewInspector(db),
	}
}

func TestPostgresRecordSuite(t *testing.T)    { repotest.RunRecordSuite(t, openBackend) }
func TestPostgresAckSuite(t *testing.T)       { repotest.RunAckSuite(t, openBackend) }
func TestPostgresChunklineSuite(t *testing.T) { repotest.RunChunklineSuite(t, openBackend) }
func TestPostgresResidenceSuite(t *testing.T) { repotest.RunResidenceSuite(t, openBackend) }
