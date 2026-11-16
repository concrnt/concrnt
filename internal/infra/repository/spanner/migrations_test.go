package spanner_test

import (
	"context"
	"os"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"

	spannerrepo "github.com/concrnt/concrnt/internal/infra/repository/spanner"
	"github.com/concrnt/concrnt/internal/testutil"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testutil.PurgeSpanner()
	os.Exit(code)
}

// indexRow is one declared index as INFORMATION_SCHEMA reports it, with its
// key columns in order and direction. The indexes Spanner manages itself to
// back a foreign key (IDX_<table>_<cols>_<hash>) are left out: their names
// are derived and they follow the constraint, not the DDL.
type indexRow struct {
	Table, Name, Type    string
	Unique, NullFiltered bool
	Columns              string
}

func listIndexes(t *testing.T, client *spanner.Client) []indexRow {
	t.Helper()
	ctx := context.Background()
	it := client.Single().Query(ctx, spanner.Statement{SQL: `
		SELECT i.TABLE_NAME, i.INDEX_NAME, i.INDEX_TYPE, i.IS_UNIQUE, i.IS_NULL_FILTERED,
		  ARRAY_TO_STRING(ARRAY(
		    SELECT CONCAT(c.COLUMN_NAME, IF(c.COLUMN_ORDERING IS NULL, '', CONCAT(' ', c.COLUMN_ORDERING)))
		    FROM INFORMATION_SCHEMA.INDEX_COLUMNS c
		    WHERE c.TABLE_SCHEMA = i.TABLE_SCHEMA AND c.TABLE_NAME = i.TABLE_NAME AND c.INDEX_NAME = i.INDEX_NAME
		    ORDER BY c.ORDINAL_POSITION), ', ')
		FROM INFORMATION_SCHEMA.INDEXES i
		WHERE i.TABLE_SCHEMA = '' AND i.TABLE_NAME <> 'schema_migrations' AND NOT i.SPANNER_IS_MANAGED
		ORDER BY i.TABLE_NAME, i.INDEX_NAME`})
	defer it.Stop()
	var rows []indexRow
	for {
		row, err := it.Next()
		if err == iterator.Done {
			break
		}
		require.NoError(t, err)
		var r indexRow
		require.NoError(t, row.Columns(&r.Table, &r.Name, &r.Type, &r.Unique, &r.NullFiltered, &r.Columns))
		rows = append(rows, r)
	}
	return rows
}

func countMigrations(t *testing.T, client *spanner.Client) int64 {
	t.Helper()
	row, err := client.Single().Query(context.Background(), spanner.Statement{SQL: "SELECT COUNT(*) FROM schema_migrations"}).Next()
	require.NoError(t, err)
	var n int64
	require.NoError(t, row.Column(0, &n))
	return n
}

// Migrate must succeed on a fresh database (CreateSpanner ran it once) and
// be re-runnable on a migrated one: the second run records no batch and the
// index set, pinned below, is unchanged. An index added to schema.go must
// land here, and a batch that forgets its index fails here.
func TestMigrateIsIdempotent(t *testing.T) {
	client, admin := testutil.CreateSpanner(t)
	ctx := context.Background()

	steps := countMigrations(t, client)
	require.Equal(t, int64(7), steps, "baseline, the ack index drop, the count cache tables, the association incarnations, the dropped cascade, the dropped count column and the listing indexes")
	require.ElementsMatch(t, expectedIndexes, listIndexes(t, client))

	require.NoError(t, spannerrepo.Migrate(ctx, admin, client))
	require.Equal(t, steps, countMigrations(t, client))
	require.ElementsMatch(t, expectedIndexes, listIndexes(t, client))

	// a start that finds batches applied but not recorded (the process died
	// in between) runs them again: the IF [NOT] EXISTS statements of
	// batches 4 and 7 change nothing, and batches 5 and 6, whose DROP
	// CONSTRAINT and DROP COLUMN cannot be repeated, see through their
	// Pending queries that the constraint and the column are gone
	_, err := client.Apply(ctx, []*spanner.Mutation{spanner.Delete("schema_migrations", spanner.KeySets(spanner.Key{4}, spanner.Key{5}, spanner.Key{6}, spanner.Key{7}))})
	require.NoError(t, err)
	require.NoError(t, spannerrepo.Migrate(ctx, admin, client))
	require.Equal(t, steps, countMigrations(t, client))
	require.ElementsMatch(t, expectedIndexes, listIndexes(t, client))
	require.Equal(t, []string{"fk_entities_commit", "fk_record_keys_record"}, listForeignKeys(t, client),
		"a key's associations no longer cascade from it")
}

func listForeignKeys(t *testing.T, client *spanner.Client) []string {
	t.Helper()
	it := client.Single().Query(context.Background(), spanner.Statement{SQL: `
		SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
		WHERE TABLE_SCHEMA = '' AND CONSTRAINT_TYPE = 'FOREIGN KEY' ORDER BY CONSTRAINT_NAME`})
	defer it.Stop()
	var names []string
	for {
		row, err := it.Next()
		if err == iterator.Done {
			return names
		}
		require.NoError(t, err)
		var name string
		require.NoError(t, row.Column(0, &name))
		names = append(names, name)
	}
}

var expectedIndexes = []indexRow{
	{"abuse_reports", "PRIMARY_KEY", "PRIMARY_KEY", true, false, "id ASC"},
	{"ackeds", "PRIMARY_KEY", "PRIMARY_KEY", true, false, "id ASC"},
	{"ackeds", "idx_acked_from_to_schema", "INDEX", true, false, "from_ccid ASC, to_ccid ASC, schema ASC"},
	{"ackeds", "idx_acked_to_valid_created_at", "INDEX", false, false, "to_ccid ASC, valid ASC, created_at ASC"},
	{"acks", "PRIMARY_KEY", "PRIMARY_KEY", true, false, "id ASC"},
	{"acks", "idx_ack_from_to_schema", "INDEX", true, false, "from_ccid ASC, to_ccid ASC, schema ASC"},
	{"acks", "idx_acks_from_valid_created_at", "INDEX", false, false, "from_ccid ASC, valid ASC, created_at ASC"},
	{"aggregate_deltas", "PRIMARY_KEY", "PRIMARY_KEY", true, false, "key ASC, id ASC"},
	{"aggregate_entries", "PRIMARY_KEY", "PRIMARY_KEY", true, false, "key ASC"},
	{"associations", "PRIMARY_KEY", "PRIMARY_KEY", true, false, "id ASC"},
	{"associations", "idx_associations_target_gen_author_created_at_desc", "INDEX", false, false, "target_uri ASC, target_gen ASC, author ASC, created_at DESC, id DESC"},
	{"associations", "idx_associations_target_gen_created_at_desc", "INDEX", false, false, "target_uri ASC, target_gen ASC, created_at DESC, id DESC"},
	{"associations", "idx_associations_target_gen_schema_variant_created_at_desc", "INDEX", false, false, "target_uri ASC, target_gen ASC, schema ASC, variant ASC, created_at DESC, id DESC"},
	{"associations", "uni_associations_target_gen_unique", "INDEX", true, false, "target_uri ASC, target_gen ASC, unique_key ASC"},
	{"commit_logs", "PRIMARY_KEY", "PRIMARY_KEY", true, false, "id ASC"},
	{"commit_logs", "idx_commit_logs_c_date_id", "INDEX", false, false, "c_date ASC, id ASC"},
	{"commit_logs", "idx_commit_logs_gc_candidate", "INDEX", false, false, "gc_candidate ASC"},
	{"commit_logs", "idx_commit_logs_owner_c_date", "INDEX", false, false, "owner ASC, c_date ASC"},
	{"entities", "PRIMARY_KEY", "PRIMARY_KEY", true, false, "ccid ASC"},
	{"entities", "idx_entities_alias", "INDEX", false, false, "alias ASC"},
	{"entity_meta", "PRIMARY_KEY", "PRIMARY_KEY", true, false, "ccid ASC"},
	{"record_keys", "PRIMARY_KEY", "PRIMARY_KEY", true, false, "uri ASC"},
	{"record_keys", "idx_record_keys_parent_created_at_record_id", "INDEX", false, true, "parent_uri ASC, record_created_at DESC, record_id ASC"},
	{"record_keys", "idx_record_keys_parent_uri_uri", "INDEX", false, false, "parent_uri ASC, uri ASC"},
	{"record_keys", "idx_record_keys_record_id", "INDEX", true, true, "record_id ASC"},
	{"records", "PRIMARY_KEY", "PRIMARY_KEY", true, false, "id ASC"},
	{"records", "idx_records_author_created_at_id", "INDEX", false, false, "author ASC, created_at ASC, id ASC"},
	{"records", "idx_records_schema_created_at_id", "INDEX", false, false, "schema ASC, created_at ASC, id ASC"},
	{"servers", "PRIMARY_KEY", "PRIMARY_KEY", true, false, "fqdn ASC"},
	{"servers", "idx_servers_cs_id", "INDEX", false, false, "cs_id ASC"},
	{"subscriptions", "PRIMARY_KEY", "PRIMARY_KEY", true, false, "vendor_id ASC, owner ASC"},
}
