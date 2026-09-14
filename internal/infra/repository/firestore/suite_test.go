package firestore

import (
	"os"
	"testing"

	"cloud.google.com/go/firestore"

	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
	"github.com/concrnt/concrnt/internal/testutil"
)

// newClient hands out an isolated emulator client per test; the emulator
// container is shared by the package.
var newClient func(t testing.TB) *firestore.Client

func TestMain(m *testing.M) {
	var cleanup func()
	newClient, cleanup = testutil.CreateFirestore()
	code := m.Run()
	cleanup()
	os.Exit(code)
}

func openBackend(t *testing.T) repotest.Backend {
	t.Helper()
	client := newClient(t)
	return repotest.Backend{
		Record:    NewRecordRepository(client),
		Residence: NewResidenceRepository(client),
		Chunkline: NewChunklineRepository(client),
		Inspect:   NewInspector(client),
	}
}

func TestFirestoreRecordSuite(t *testing.T)    { repotest.RunRecordSuite(t, openBackend) }
func TestFirestoreAckSuite(t *testing.T)       { repotest.RunAckSuite(t, openBackend) }
func TestFirestoreChunklineSuite(t *testing.T) { repotest.RunChunklineSuite(t, openBackend) }
func TestFirestoreResidenceSuite(t *testing.T) { repotest.RunResidenceSuite(t, openBackend) }
