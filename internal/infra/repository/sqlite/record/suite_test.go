package record

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
)

// openBackend opens a fresh database file (no Docker) and wires the
// repositories under test into a repotest.Backend.
func openBackend(t *testing.T) repotest.Backend {
	t.Helper()
	db, err := sqlitedb.Open(filepath.Join(t.TempDir(), "concrnt.db"))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	require.NoError(t, sqlitedb.Migrate(context.Background(), db))
	cl, _ := repotest.PeerClient(t)
	return repotest.Backend{
		Record:       New(db, nil, domain.Config{}),
		EntityMeta:   sqlitedb.NewEntityMetaRepository(db),
		Server:       sqlitedb.NewServerRepository(&domain.Config{FQDN: "local.example"}, db, cl),
		Notification: sqlitedb.NewNotificationRepository(db),
		Abuse:        sqlitedb.NewAbuseRepository(db),
		Inspect:      newInspector(db),
	}
}

func TestSQLiteRecordSuite(t *testing.T)      { repotest.RunRecordSuite(t, openBackend) }
func TestSQLiteAssociationSuite(t *testing.T) { repotest.RunAssociationSuite(t, openBackend) }
func TestSQLiteAckSuite(t *testing.T)         { repotest.RunAckSuite(t, openBackend) }
func TestSQLiteChunklineSuite(t *testing.T)   { repotest.RunChunklineSuite(t, openBackend) }
func TestSQLiteResidenceSuite(t *testing.T)   { repotest.RunResidenceSuite(t, openBackend) }
func TestSQLiteReplicationSuite(t *testing.T) { repotest.RunReplicationSuite(t, openBackend) }
func TestSQLiteMaintenanceSuite(t *testing.T) { repotest.RunMaintenanceSuite(t, openBackend) }
func TestSQLiteWorldSuite(t *testing.T)       { repotest.RunWorldSuite(t, openBackend) }
