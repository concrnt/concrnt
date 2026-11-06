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

func TestSpannerRecordSuite(t *testing.T)      { repotest.RunRecordSuite(t, openBackend) }
func TestSpannerAssociationSuite(t *testing.T) { repotest.RunAssociationSuite(t, openBackend) }
func TestSpannerAckSuite(t *testing.T)         { repotest.RunAckSuite(t, openBackend) }
func TestSpannerChunklineSuite(t *testing.T)   { repotest.RunChunklineSuite(t, openBackend) }
func TestSpannerResidenceSuite(t *testing.T)   { repotest.RunResidenceSuite(t, openBackend) }
func TestSpannerReplicationSuite(t *testing.T) { repotest.RunReplicationSuite(t, openBackend) }
func TestSpannerMaintenanceSuite(t *testing.T) { repotest.RunMaintenanceSuite(t, openBackend) }
func TestSpannerWorldSuite(t *testing.T)       { repotest.RunWorldSuite(t, openBackend) }
