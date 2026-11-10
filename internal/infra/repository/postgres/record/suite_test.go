package record

import (
	"testing"

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
	cl := repotest.PeerClient(t)
	return repotest.Backend{
		Record:       New(db),
		EntityMeta:   postgres.NewEntityMetaRepository(db),
		Server:       postgres.NewServerRepository(db, cl),
		Notification: postgres.NewNotificationRepository(db),
		Abuse:        postgres.NewAbuseRepository(db),
		Inspect:      newInspector(db),
	}
}

func TestPostgresSuite(t *testing.T) { repotest.Run(t, openBackend) }
