package record

import (
	"os"
	"testing"

	fsrepo "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
	"github.com/concrnt/concrnt/internal/testutil"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testutil.PurgeFirestore()
	os.Exit(code)
}

// openBackend wires the repositories under test into a repotest.Backend
// over a fresh emulator project (one emulator per test process).
func openBackend(t *testing.T) repotest.Backend {
	t.Helper()
	client := testutil.CreateFirestore(t)
	cl := repotest.PeerClient(t)
	return repotest.Backend{
		Record:       New(client),
		EntityMeta:   fsrepo.NewEntityMetaRepository(client),
		Server:       fsrepo.NewServerRepository(client, cl),
		Notification: fsrepo.NewNotificationRepository(client),
		Abuse:        fsrepo.NewAbuseRepository(client),
		Inspect:      newInspector(client),
	}
}

func TestFirestoreSuite(t *testing.T) { repotest.Run(t, openBackend) }
