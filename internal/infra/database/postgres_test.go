package database_test

import (
	"strings"
	"testing"

	"github.com/concrnt/concrnt/internal/infra/database"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/stretchr/testify/require"
)

// MigratePostgres must succeed on a fresh database and be re-runnable on an
// already-migrated one (the server runs it at every start). testutil.CreateDB
// discards the migration error, so this is the explicit check that every
// table — including the ones listed after models.Acked — exists.
func TestMigratePostgresIsIdempotent(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	require.NoError(t, database.MigratePostgres(db))

	for _, model := range []any{
		&models.CommitLog{}, &models.Record{}, &models.RecordKey{}, &models.Association{},
		&models.Ack{}, &models.Acked{}, &models.Server{}, &models.Entity{}, &models.EntityMeta{},
		&models.Subscription{}, &models.AbuseReport{},
	} {
		require.True(t, db.Migrator().HasTable(model), "table for %T must exist after migration", model)
	}

	// Index names are unique per schema in Postgres, and gorm's HasIndex check
	// is satisfied by a same-named index on another table — so an index a
	// model shares by name with another model is silently never created. The
	// acked upsert relies on the unique (from, to, schema) index existing on
	// ackeds itself.
	type indexRow struct{ Indexname, Indexdef string }
	var indexes []indexRow
	require.NoError(t, db.Raw("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'ackeds'").Scan(&indexes).Error)
	var hasUnique bool
	for _, index := range indexes {
		if strings.Contains(index.Indexdef, "UNIQUE") && strings.Contains(index.Indexdef, `("from", "to", schema)`) {
			hasUnique = true
		}
	}
	require.True(t, hasUnique, "ackeds must have its own unique (from, to, schema) index, got %+v", indexes)

	// The replication feed orders by (c_date, id) without an owner filter; the
	// composite index that backs that scan must exist on commit_logs itself.
	indexes = nil
	require.NoError(t, db.Raw("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'commit_logs'").Scan(&indexes).Error)
	var hasCDateID bool
	for _, index := range indexes {
		if index.Indexname == "idx_commit_logs_c_date_id" && strings.Contains(index.Indexdef, "(c_date, id)") {
			hasCDateID = true
		}
	}
	require.True(t, hasCDateID, "commit_logs must have idx_commit_logs_c_date_id on (c_date, id), got %+v", indexes)
}
