package sqlite_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/repository/sqlite"
)

type indexRow struct{ Table, Name, SQL string }

// Migrate creates every index schema.go declares, as sqlite_master reports
// it (order, direction, predicate; identifier quotes stripped), and is
// re-runnable.
func TestMigrateCreatesIndexes(t *testing.T) {
	ctx := context.Background()
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "concrnt.db"))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	require.NoError(t, sqlite.Migrate(ctx, db))
	require.NoError(t, sqlite.Migrate(ctx, db), "migrate is idempotent")

	rows, err := db.QueryContext(ctx, "SELECT tbl_name, name, sql FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex_%'")
	require.NoError(t, err)
	defer rows.Close()
	var got []indexRow
	for rows.Next() {
		var row indexRow
		require.NoError(t, rows.Scan(&row.Table, &row.Name, &row.SQL))
		row.SQL = strings.ReplaceAll(row.SQL, `"`, "")
		got = append(got, row)
	}
	require.NoError(t, rows.Err())
	require.ElementsMatch(t, expectedIndexes, got)
}

var expectedIndexes = []indexRow{
	{"ackeds", "idx_acked_from_valid_created_at", "CREATE INDEX idx_acked_from_valid_created_at ON ackeds (from, valid, created_at)"},
	{"ackeds", "idx_acked_schema_valid_created_at", "CREATE INDEX idx_acked_schema_valid_created_at ON ackeds (schema, valid, created_at)"},
	{"ackeds", "idx_acked_to_valid_created_at", "CREATE INDEX idx_acked_to_valid_created_at ON ackeds (to, valid, created_at)"},
	{"acks", "idx_acks_from_valid_created_at", "CREATE INDEX idx_acks_from_valid_created_at ON acks (from, valid, created_at)"},
	{"acks", "idx_acks_schema_valid_created_at", "CREATE INDEX idx_acks_schema_valid_created_at ON acks (schema, valid, created_at)"},
	{"acks", "idx_acks_to_valid_created_at", "CREATE INDEX idx_acks_to_valid_created_at ON acks (to, valid, created_at)"},
	{"associations", "idx_associations_target_id", "CREATE INDEX idx_associations_target_id ON associations (target_id)"},
	{"associations", "idx_associations_target_schema_variant_author", "CREATE INDEX idx_associations_target_schema_variant_author ON associations (target_id, schema, variant, author)"},
	{"associations", "idx_associations_target_schema_variant_created_at", "CREATE INDEX idx_associations_target_schema_variant_created_at ON associations (target_id, schema, variant, created_at)"},
	{"commit_logs", "idx_commit_logs_c_date_id", "CREATE INDEX idx_commit_logs_c_date_id ON commit_logs (c_date, id)"},
	{"commit_logs", "idx_commit_logs_gc_candidate", "CREATE INDEX idx_commit_logs_gc_candidate ON commit_logs (gc_candidate)"},
	{"commit_logs", "idx_commit_logs_owner_c_date", "CREATE INDEX idx_commit_logs_owner_c_date ON commit_logs (owner, c_date)"},
	{"entities", "idx_entities_alias", "CREATE INDEX idx_entities_alias ON entities (alias)"},
	{"record_keys", "idx_record_keys_parent_created_at_record_id", "CREATE INDEX idx_record_keys_parent_created_at_record_id ON record_keys (parent_id, record_created_at DESC, record_id) WHERE parent_id IS NOT NULL AND record_created_at IS NOT NULL"},
	{"record_keys", "idx_record_keys_parent_id_record_id", "CREATE INDEX idx_record_keys_parent_id_record_id ON record_keys (parent_id, record_id)"},
	{"record_keys", "idx_record_keys_parent_id_uri", "CREATE INDEX idx_record_keys_parent_id_uri ON record_keys (parent_id, uri)"},
	{"records", "idx_records_author_created_at_document_id", "CREATE INDEX idx_records_author_created_at_document_id ON records (author, created_at, document_id)"},
	{"records", "idx_records_document_id_created_at", "CREATE INDEX idx_records_document_id_created_at ON records (document_id, created_at)"},
	{"records", "idx_records_schema_created_at_document_id", "CREATE INDEX idx_records_schema_created_at_document_id ON records (schema, created_at, document_id)"},
	{"servers", "idx_servers_cs_id", "CREATE INDEX idx_servers_cs_id ON servers (cs_id)"},
}
