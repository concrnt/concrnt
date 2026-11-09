package sqlite

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type indexRow struct {
	Table, Name, SQL string
}

// listIndexes reads the named indexes (the inline UNIQUE/PRIMARY KEY
// autoindexes have no DDL) with gorm's backtick quoting stripped.
func listIndexes(t *testing.T, db *DB) []indexRow {
	t.Helper()
	var rows []struct{ TblName, Name, SQL string }
	require.NoError(t, db.Raw("SELECT tbl_name, name, sql FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex_%'").Scan(&rows).Error)
	out := make([]indexRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, indexRow{r.TblName, r.Name, strings.NewReplacer("`", "", `"`, "").Replace(r.SQL)})
	}
	return out
}

// Migrate is idempotent: a second run records no step and leaves the schema
// (pinned below) untouched. Any change to the models or indexes must show up
// in expectedIndexes.
func TestMigrateIsIdempotent(t *testing.T) {
	ctx := context.Background()
	db, err := Open(filepath.Join(t.TempDir(), "concrnt.db"))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	require.NoError(t, Migrate(ctx, db))
	for _, model := range []any{&CommitLog{}, &Record{}, &RecordKey{}, &Association{}, &Ack{}, &Acked{}, &Entity{}, &EntityMeta{}, &Server{}, &Subscription{}, &AbuseReport{}} {
		require.True(t, db.Migrator().HasTable(model), "%T", model)
	}
	require.ElementsMatch(t, expectedIndexes, listIndexes(t, db))

	var tables []string
	require.NoError(t, db.Raw("SELECT sql FROM sqlite_master WHERE type = 'table' ORDER BY name").Scan(&tables).Error)
	var steps int64
	require.NoError(t, db.Raw("SELECT COUNT(*) FROM schema_migrations").Scan(&steps).Error)
	require.Equal(t, int64(len(migrations)), steps)

	require.NoError(t, Migrate(ctx, db))
	var stepsAfter int64
	require.NoError(t, db.Raw("SELECT COUNT(*) FROM schema_migrations").Scan(&stepsAfter).Error)
	require.Equal(t, steps, stepsAfter)
	var tablesAfter []string
	require.NoError(t, db.Raw("SELECT sql FROM sqlite_master WHERE type = 'table' ORDER BY name").Scan(&tablesAfter).Error)
	require.Equal(t, tables, tablesAfter, "a second AutoMigrate must not rebuild a table")
	require.ElementsMatch(t, expectedIndexes, listIndexes(t, db))
}

var expectedIndexes = []indexRow{
	{"ackeds", "idx_acked_from_to_schema", "CREATE UNIQUE INDEX idx_acked_from_to_schema ON ackeds(from,to,schema)"},
	{"ackeds", "idx_acked_to_valid_created_at", "CREATE INDEX idx_acked_to_valid_created_at ON ackeds(to,valid,created_at)"},
	{"acks", "idx_ack_from_to_schema", "CREATE UNIQUE INDEX idx_ack_from_to_schema ON acks(from,to,schema)"},
	{"acks", "idx_acks_from_valid_created_at", "CREATE INDEX idx_acks_from_valid_created_at ON acks(from,valid,created_at)"},
	{"associations", "idx_associations_target_id", "CREATE INDEX idx_associations_target_id ON associations(target_id)"},
	{"associations", "idx_associations_target_schema_variant_author", "CREATE INDEX idx_associations_target_schema_variant_author ON associations(target_id,schema,variant,author)"},
	{"associations", "idx_associations_target_schema_variant_created_at", "CREATE INDEX idx_associations_target_schema_variant_created_at ON associations(target_id,schema,variant,created_at)"},
	{"commit_logs", "idx_commit_logs_c_date_id", "CREATE INDEX idx_commit_logs_c_date_id ON commit_logs(c_date,id)"},
	{"commit_logs", "idx_commit_logs_gc_candidate", "CREATE INDEX idx_commit_logs_gc_candidate ON commit_logs(gc_candidate) WHERE gc_candidate"},
	{"commit_logs", "idx_commit_logs_owner_c_date", "CREATE INDEX idx_commit_logs_owner_c_date ON commit_logs(owner,c_date)"},
	{"entities", "idx_entities_alias", "CREATE INDEX idx_entities_alias ON entities(alias)"},
	{"record_keys", "idx_record_keys_parent_created_at_record_id", "CREATE INDEX idx_record_keys_parent_created_at_record_id ON record_keys(parent_id,record_created_at desc,record_id) WHERE parent_id IS NOT NULL AND record_created_at IS NOT NULL"},
	{"record_keys", "idx_record_keys_parent_id_record_id", "CREATE INDEX idx_record_keys_parent_id_record_id ON record_keys(parent_id,record_id)"},
	{"record_keys", "idx_record_keys_parent_id_uri", "CREATE INDEX idx_record_keys_parent_id_uri ON record_keys(parent_id,uri)"},
	{"record_keys", "idx_record_keys_record_id", "CREATE UNIQUE INDEX idx_record_keys_record_id ON record_keys(record_id)"},
	{"records", "idx_records_author_created_at_document_id", "CREATE INDEX idx_records_author_created_at_document_id ON records(author,created_at,document_id)"},
	{"records", "idx_records_document_id_created_at", "CREATE INDEX idx_records_document_id_created_at ON records(document_id,created_at)"},
	{"records", "idx_records_schema_created_at_document_id", "CREATE INDEX idx_records_schema_created_at_document_id ON records(schema,created_at,document_id)"},
	{"servers", "idx_servers_cs_id", "CREATE INDEX idx_servers_cs_id ON servers(cs_id)"},
}
