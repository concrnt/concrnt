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

func TestFirestoreRecordSuite(t *testing.T)      { repotest.RunRecordSuite(t, openBackend) }
func TestFirestoreAssociationSuite(t *testing.T) { repotest.RunAssociationSuite(t, openBackend) }
func TestFirestoreAckSuite(t *testing.T)         { repotest.RunAckSuite(t, openBackend) }
func TestFirestoreChunklineSuite(t *testing.T)   { repotest.RunChunklineSuite(t, openBackend) }
func TestFirestoreResidenceSuite(t *testing.T)   { repotest.RunResidenceSuite(t, openBackend) }
func TestFirestoreReplicationSuite(t *testing.T) { repotest.RunReplicationSuite(t, openBackend) }
func TestFirestoreMaintenanceSuite(t *testing.T) { repotest.RunMaintenanceSuite(t, openBackend) }
func TestFirestoreWorldSuite(t *testing.T)       { repotest.RunWorldSuite(t, openBackend) }
