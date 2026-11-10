//go:build contractv1

package record

import (
	"os"
	"testing"

	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
	spannerdb "github.com/concrnt/concrnt/internal/infra/repository/spanner"
	"github.com/concrnt/concrnt/internal/testutil"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testutil.PurgeSpanner()
	os.Exit(code)
}

// openBackend wires the repositories under test into a repotest.Backend
// over an emptied test database (one Spanner Omni per test process).
func openBackend(t *testing.T) repotest.Backend {
	t.Helper()
	client, _ := testutil.CreateSpanner(t)
	cl := repotest.PeerClient(t)
	return repotest.Backend{
		Record:       New(client),
		EntityMeta:   spannerdb.NewEntityMetaRepository(client),
		Server:       spannerdb.NewServerRepository(client, cl),
		Notification: spannerdb.NewNotificationRepository(client),
		Abuse:        spannerdb.NewAbuseRepository(client),
		Inspect:      newInspector(client),
	}
}

func TestSpannerSuite(t *testing.T) { repotest.Run(t, openBackend) }
