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
	cl, _ := repotest.PeerClient(t)
	return repotest.Backend{
		Record:       New(db, nil, domain.Config{}),
		EntityMeta:   postgres.NewEntityMetaRepository(db),
		Server:       postgres.NewServerRepository(&domain.Config{FQDN: "local.example"}, db, cl),
		Notification: postgres.NewNotificationRepository(db),
		Abuse:        postgres.NewAbuseRepository(db),
		Inspect:      newInspector(db),
	}
}

func TestPostgresRecordSuite(t *testing.T)      { repotest.RunRecordSuite(t, openBackend) }
func TestPostgresAssociationSuite(t *testing.T) { repotest.RunAssociationSuite(t, openBackend) }
func TestPostgresAckSuite(t *testing.T)         { repotest.RunAckSuite(t, openBackend) }
func TestPostgresChunklineSuite(t *testing.T)   { repotest.RunChunklineSuite(t, openBackend) }
func TestPostgresResidenceSuite(t *testing.T)   { repotest.RunResidenceSuite(t, openBackend) }
func TestPostgresReplicationSuite(t *testing.T) { repotest.RunReplicationSuite(t, openBackend) }
func TestPostgresMaintenanceSuite(t *testing.T) { repotest.RunMaintenanceSuite(t, openBackend) }
func TestPostgresWorldSuite(t *testing.T)       { repotest.RunWorldSuite(t, openBackend) }
