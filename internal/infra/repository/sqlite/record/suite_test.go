package record

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
)

// openDB opens a fresh, migrated database file (no Docker) that lives for
// the test.
func openDB(t *testing.T) *sqlitedb.DB {
	t.Helper()
	db, err := sqlitedb.Open(filepath.Join(t.TempDir(), "concrnt.db"))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	require.NoError(t, sqlitedb.Migrate(context.Background(), db))
	return db
}

// openBackend wires the repositories under test into a repotest.Backend.
func openBackend(t *testing.T) repotest.Backend {
	t.Helper()
	db := openDB(t).DB
	cl := repotest.PeerClient(t)
	return repotest.Backend{
		Record:       New(db),
		EntityMeta:   sqlitedb.NewEntityMetaRepository(db),
		Server:       sqlitedb.NewServerRepository(db, cl),
		Notification: sqlitedb.NewNotificationRepository(db),
		Abuse:        sqlitedb.NewAbuseRepository(db),
		Inspect:      newInspector(db),
	}
}

func TestSQLiteSuite(t *testing.T) { repotest.Run(t, openBackend) }
