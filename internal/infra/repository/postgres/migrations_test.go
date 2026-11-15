package postgres_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
	"github.com/concrnt/concrnt/internal/testutil"
)

// Migrate must succeed on a fresh database and be re-runnable on an
// already-migrated one (the server runs it at every start): the second run
// finds every step recorded and applies nothing.
func TestMigrateIsIdempotent(t *testing.T) {
	ctx := context.Background()
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	require.NoError(t, postgres.Migrate(ctx, db))

	for _, model := range []any{
		&postgres.CommitLog{}, &postgres.Record{}, &postgres.RecordKey{}, &postgres.Association{},
		&postgres.Ack{}, &postgres.Acked{}, &postgres.Server{}, &postgres.Entity{}, &postgres.EntityMeta{},
		&postgres.Subscription{}, &postgres.AbuseReport{},
	} {
		require.True(t, db.Migrator().HasTable(model), "table for %T must exist after migration", model)
	}

	type migrationRow struct {
		Version int
		Name    string
	}
	var rows []migrationRow
	require.NoError(t, db.Raw("SELECT version, name FROM schema_migrations ORDER BY version").Scan(&rows).Error)
	require.NotEmpty(t, rows)
	require.Equal(t, 1, rows[0].Version)
	for i, row := range rows {
		require.Equal(t, i+1, row.Version, "steps are numbered contiguously from 1")
	}

	require.ElementsMatch(t, expectedIndexes, listIndexes(t, db))

	// a second run finds every step recorded and AutoMigrate nothing to add
	require.NoError(t, postgres.Migrate(ctx, db))
	var again []migrationRow
	require.NoError(t, db.Raw("SELECT version, name FROM schema_migrations ORDER BY version").Scan(&again).Error)
	require.Equal(t, rows, again)
	require.ElementsMatch(t, expectedIndexes, listIndexes(t, db))
}

type indexRow struct{ Tablename, Indexname, Indexdef string }

func listIndexes(t *testing.T, db *gorm.DB) []indexRow {
	t.Helper()
	var rows []indexRow
	require.NoError(t, db.Raw("SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' AND indexname <> 'schema_migrations_pkey'").Scan(&rows).Error)
	return rows
}

// expectedIndexes pins the index set of a migrated database: every index the
// model tags declare, as pg_indexes reports it (order, direction, collation,
// predicate). An access path added to CONTRACT.md lands here with its tag.
var expectedIndexes = []indexRow{
	{"abuse_reports", "abuse_reports_pkey", "CREATE UNIQUE INDEX abuse_reports_pkey ON public.abuse_reports USING btree (id)"},
	{"ackeds", "ackeds_pkey", "CREATE UNIQUE INDEX ackeds_pkey ON public.ackeds USING btree (document_id)"},
	{"ackeds", "idx_acked_from_to_schema", "CREATE UNIQUE INDEX idx_acked_from_to_schema ON public.ackeds USING btree (\"from\", \"to\", schema)"},
	{"ackeds", "idx_acked_to_valid_created_at", "CREATE INDEX idx_acked_to_valid_created_at ON public.ackeds USING btree (\"to\", valid, created_at)"},
	{"acks", "acks_pkey", "CREATE UNIQUE INDEX acks_pkey ON public.acks USING btree (document_id)"},
	{"acks", "idx_ack_from_to_schema", "CREATE UNIQUE INDEX idx_ack_from_to_schema ON public.acks USING btree (\"from\", \"to\", schema)"},
	{"acks", "idx_acks_from_valid_created_at", "CREATE INDEX idx_acks_from_valid_created_at ON public.acks USING btree (\"from\", valid, created_at)"},
	{"aggregate_entries", "aggregate_entries_pkey", "CREATE UNIQUE INDEX aggregate_entries_pkey ON public.aggregate_entries USING btree (key)"},
	{"associations", "associations_pkey", "CREATE UNIQUE INDEX associations_pkey ON public.associations USING btree (document_id)"},
	{"associations", "idx_associations_target_id", "CREATE INDEX idx_associations_target_id ON public.associations USING btree (target_id)"},
	{"associations", "idx_associations_target_schema_variant_author", "CREATE INDEX idx_associations_target_schema_variant_author ON public.associations USING btree (target_id, schema, variant, author)"},
	{"associations", "idx_associations_target_schema_variant_created_at", "CREATE INDEX idx_associations_target_schema_variant_created_at ON public.associations USING btree (target_id, schema, variant, created_at)"},
	{"associations", "uni_associations_unique", "CREATE UNIQUE INDEX uni_associations_unique ON public.associations USING btree (\"unique\")"},
	{"commit_logs", "commit_logs_pkey", "CREATE UNIQUE INDEX commit_logs_pkey ON public.commit_logs USING btree (id)"},
	{"commit_logs", "idx_commit_logs_c_date_id", "CREATE INDEX idx_commit_logs_c_date_id ON public.commit_logs USING btree (c_date, id)"},
	{"commit_logs", "idx_commit_logs_gc_candidate", "CREATE INDEX idx_commit_logs_gc_candidate ON public.commit_logs USING btree (gc_candidate) WHERE gc_candidate"},
	{"commit_logs", "idx_commit_logs_owner_c_date", "CREATE INDEX idx_commit_logs_owner_c_date ON public.commit_logs USING btree (owner, c_date)"},
	{"entities", "entities_pkey", "CREATE UNIQUE INDEX entities_pkey ON public.entities USING btree (id)"},
	{"entities", "idx_entities_alias", "CREATE INDEX idx_entities_alias ON public.entities USING btree (alias)"},
	{"entity_meta", "entity_meta_pkey", "CREATE UNIQUE INDEX entity_meta_pkey ON public.entity_meta USING btree (id)"},
	{"record_keys", "idx_record_keys_parent_created_at_record_id", "CREATE INDEX idx_record_keys_parent_created_at_record_id ON public.record_keys USING btree (parent_id, record_created_at DESC, record_id) WHERE ((parent_id IS NOT NULL) AND (record_created_at IS NOT NULL))"},
	{"record_keys", "idx_record_keys_parent_id_record_id", "CREATE INDEX idx_record_keys_parent_id_record_id ON public.record_keys USING btree (parent_id, record_id)"},
	{"record_keys", "idx_record_keys_parent_id_uri", "CREATE INDEX idx_record_keys_parent_id_uri ON public.record_keys USING btree (parent_id, uri COLLATE \"C\")"},
	{"record_keys", "idx_record_keys_record_id", "CREATE UNIQUE INDEX idx_record_keys_record_id ON public.record_keys USING btree (record_id)"},
	{"record_keys", "idx_record_keys_uri_c", "CREATE INDEX idx_record_keys_uri_c ON public.record_keys USING btree (uri COLLATE \"C\")"},
	{"record_keys", "record_keys_pkey", "CREATE UNIQUE INDEX record_keys_pkey ON public.record_keys USING btree (id)"},
	{"record_keys", "uni_record_keys_uri", "CREATE UNIQUE INDEX uni_record_keys_uri ON public.record_keys USING btree (uri)"},
	{"records", "idx_records_author_created_at_document_id", "CREATE INDEX idx_records_author_created_at_document_id ON public.records USING btree (author, created_at, document_id)"},
	{"records", "idx_records_document_id_created_at", "CREATE INDEX idx_records_document_id_created_at ON public.records USING btree (document_id, created_at)"},
	{"records", "idx_records_schema_created_at_document_id", "CREATE INDEX idx_records_schema_created_at_document_id ON public.records USING btree (schema, created_at, document_id)"},
	{"records", "records_pkey", "CREATE UNIQUE INDEX records_pkey ON public.records USING btree (document_id)"},
	{"servers", "idx_servers_cs_id", "CREATE INDEX idx_servers_cs_id ON public.servers USING btree (cs_id)"},
	{"servers", "servers_pkey", "CREATE UNIQUE INDEX servers_pkey ON public.servers USING btree (id)"},
	{"subscriptions", "subscriptions_pkey", "CREATE UNIQUE INDEX subscriptions_pkey ON public.subscriptions USING btree (vendor_id, owner)"},
}
