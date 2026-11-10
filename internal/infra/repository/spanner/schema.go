// Package spanner is the Spanner backend of the repository contract: the
// native cloud.google.com/go/spanner client over GoogleSQL, with its own
// DDL (schema.go), migrations (migrate.go) and the stores that are not the
// record store. The record store is the record subpackage.
package spanner

// migration is one numbered DDL batch. Batches are applied in order and
// recorded in schema_migrations; every statement is written to be
// re-runnable (IF NOT EXISTS) because DDL is not transactional and two
// replicas may start at once. A schema change is a new batch, never an edit
// of an applied one.
//
// Pending is for the statements GoogleSQL gives no re-runnable form (DROP
// CONSTRAINT has no IF EXISTS): a query that returns a row for as long as
// the batch still has to run. When it returns none the batch is recorded
// without being applied, which is what a start after a batch that ran but
// was not recorded finds.
type migration struct {
	Version    int
	Name       string
	Statements []string
	Pending    string
}

// The tables mirror the Postgres ones column for column
// (internal/infra/repository/postgres/models.go); what differs is forced by
// Spanner:
//   - no serial ids (C3): record_keys is keyed by uri and the parent link is
//     parent_uri; associations reference the target by uri.
//   - the tables keyed by a commit id (records, acks, ackeds, associations)
//     are interleaved in commit_logs with ON DELETE CASCADE, so deleting a
//     commit log takes its state rows with it; entities and record_keys have
//     other keys and cascade through foreign keys instead.
//   - a key's associations do not cascade from it (one commit holds 80,000
//     mutations at most): record_keys.gen names the incarnation of the key,
//     written when the row is created, and an association carries the
//     target_gen it was created on. Only associations whose target_gen is
//     the gen of the key row now at target_uri are read; the rest wait for
//     Repository.Cleanup.
//   - commit_logs.c_date is a commit timestamp (monotonic within the
//     database), written with PENDING_COMMIT_TIMESTAMP().
//   - "from", "to" and "unique" are reserved words: from_ccid, to_ccid,
//     unique_key.
//   - text[] columns are ARRAY<STRING(MAX)>; JSON documents stay STRING so
//     they round-trip byte for byte in dumps.
//   - the partial index of Postgres is a NULL_FILTERED index.
//
// Indexes (per table, with the access path they serve):
//   - commit_logs: PK (id); (owner, c_date) GetAllCommitLogs, QueryCommitLogs
//     by owner, MarkCommitLogsGcCandidateByOwner; (c_date, id) QueryCommitLogs;
//     (gc_candidate) CountGcCandidates / ListGcCandidates.
//   - records: PK (id) interleaved; (schema, created_at, id) and
//     (author, created_at, id) for the schema / author filters of
//     QueryByPrefix and QueryByParent.
//   - record_keys: PK (uri) resolution, prefix scans (STARTS_WITH on the key)
//     and the owner-root branch of QueryByParent / QueryByParentOrderByKey;
//     idx_record_keys_parent_created_at_record_id NULL_FILTERED
//     (parent_uri, record_created_at DESC, record_id) chunkline scans and
//     QueryByParent; idx_record_keys_record_id UNIQUE NULL_FILTERED reverse
//     ccfs-to-cckv lookups; idx_record_keys_parent_uri_uri (parent_uri, uri)
//     QueryByParentOrderByKey including placeholders.
//   - associations: PK (id) interleaved; every index starts with the
//     incarnation (target_uri, target_gen): (.., created_at)
//     GetAssociatedRecords in its own order and the Cleanup pages;
//     (.., schema, variant, author) ComputeAssociationSummary and the
//     filtered listings, (.., schema, variant, created_at) the filtered
//     listings in order; UNIQUE (.., unique_key) one logical association
//     per incarnation.
//   - acks / ackeds: PK (id) interleaved; UNIQUE (from_ccid, to_ccid, schema)
//     the accept-if-newer state key; acks (from_ccid, valid, created_at) and
//     ackeds (to_ccid, valid, created_at) the list and count lookups (acks
//     are read by from, ackeds by to; schema only narrows).
//   - entities: PK (ccid); (alias) GetEntityByAlias.
//   - entity_meta: PK (ccid). servers: PK (fqdn); (cs_id). subscriptions:
//     PK (vendor_id, owner). abuse_reports: PK (id). aggregate_entries:
//     PK (key), rows past expires_at are dropped by the row deletion policy;
//     aggregate_deltas: PK (key, id), interleaved in aggregate_entries and
//     deleted with its row.
var migrations = []migration{
	{1, "baseline_tables", []string{
		`CREATE TABLE IF NOT EXISTS commit_logs (
			id STRING(MAX) NOT NULL,
			ip STRING(MAX) NOT NULL,
			document STRING(MAX) NOT NULL,
			proof STRING(MAX) NOT NULL,
			owner STRING(MAX) NOT NULL,
			gc_candidate BOOL NOT NULL,
			c_date TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true)
		) PRIMARY KEY (id)`,
		`CREATE INDEX IF NOT EXISTS idx_commit_logs_owner_c_date ON commit_logs (owner, c_date)`,
		`CREATE INDEX IF NOT EXISTS idx_commit_logs_c_date_id ON commit_logs (c_date, id)`,
		`CREATE INDEX IF NOT EXISTS idx_commit_logs_gc_candidate ON commit_logs (gc_candidate)`,

		`CREATE TABLE IF NOT EXISTS records (
			id STRING(MAX) NOT NULL,
			owner STRING(MAX) NOT NULL,
			author STRING(MAX) NOT NULL,
			redirect STRING(MAX),
			schema STRING(MAX) NOT NULL,
			policies STRING(MAX),
			distributions ARRAY<STRING(MAX)> NOT NULL,
			created_at TIMESTAMP NOT NULL,
			c_date TIMESTAMP NOT NULL
		) PRIMARY KEY (id), INTERLEAVE IN PARENT commit_logs ON DELETE CASCADE`,
		`CREATE INDEX IF NOT EXISTS idx_records_schema_created_at_id ON records (schema, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS idx_records_author_created_at_id ON records (author, created_at, id)`,

		`CREATE TABLE IF NOT EXISTS record_keys (
			uri STRING(MAX) NOT NULL,
			parent_uri STRING(MAX),
			record_id STRING(MAX),
			record_created_at TIMESTAMP,
			redirect STRING(MAX),
			clean_on_update BOOL NOT NULL,
			CONSTRAINT fk_record_keys_record FOREIGN KEY (record_id) REFERENCES records (id) ON DELETE CASCADE
		) PRIMARY KEY (uri)`,
		`CREATE NULL_FILTERED INDEX IF NOT EXISTS idx_record_keys_parent_created_at_record_id ON record_keys (parent_uri, record_created_at DESC, record_id)`,
		`CREATE UNIQUE NULL_FILTERED INDEX IF NOT EXISTS idx_record_keys_record_id ON record_keys (record_id)`,
		`CREATE INDEX IF NOT EXISTS idx_record_keys_parent_uri_uri ON record_keys (parent_uri, uri)`,

		`CREATE TABLE IF NOT EXISTS associations (
			id STRING(MAX) NOT NULL,
			target_uri STRING(MAX) NOT NULL,
			owner STRING(MAX) NOT NULL,
			author STRING(MAX) NOT NULL,
			schema STRING(MAX) NOT NULL,
			variant STRING(MAX),
			unique_key STRING(MAX) NOT NULL,
			created_at TIMESTAMP NOT NULL,
			c_date TIMESTAMP NOT NULL,
			CONSTRAINT fk_associations_target FOREIGN KEY (target_uri) REFERENCES record_keys (uri) ON DELETE CASCADE
		) PRIMARY KEY (id), INTERLEAVE IN PARENT commit_logs ON DELETE CASCADE`,
		`CREATE INDEX IF NOT EXISTS idx_associations_target_uri ON associations (target_uri)`,
		`CREATE INDEX IF NOT EXISTS idx_associations_target_schema_variant_author ON associations (target_uri, schema, variant, author)`,
		`CREATE INDEX IF NOT EXISTS idx_associations_target_schema_variant_created_at ON associations (target_uri, schema, variant, created_at)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uni_associations_unique ON associations (unique_key)`,

		`CREATE TABLE IF NOT EXISTS acks (
			id STRING(MAX) NOT NULL,
			from_ccid STRING(MAX) NOT NULL,
			to_ccid STRING(MAX) NOT NULL,
			schema STRING(MAX) NOT NULL,
			valid BOOL NOT NULL,
			created_at TIMESTAMP NOT NULL,
			c_date TIMESTAMP NOT NULL
		) PRIMARY KEY (id), INTERLEAVE IN PARENT commit_logs ON DELETE CASCADE`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_ack_from_to_schema ON acks (from_ccid, to_ccid, schema)`,
		`CREATE INDEX IF NOT EXISTS idx_acks_from_valid_created_at ON acks (from_ccid, valid, created_at)`,

		`CREATE TABLE IF NOT EXISTS ackeds (
			id STRING(MAX) NOT NULL,
			from_ccid STRING(MAX) NOT NULL,
			to_ccid STRING(MAX) NOT NULL,
			schema STRING(MAX) NOT NULL,
			valid BOOL NOT NULL,
			created_at TIMESTAMP NOT NULL,
			c_date TIMESTAMP NOT NULL
		) PRIMARY KEY (id), INTERLEAVE IN PARENT commit_logs ON DELETE CASCADE`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_acked_from_to_schema ON ackeds (from_ccid, to_ccid, schema)`,
		`CREATE INDEX IF NOT EXISTS idx_acked_to_valid_created_at ON ackeds (to_ccid, valid, created_at)`,

		`CREATE TABLE IF NOT EXISTS entities (
			ccid STRING(MAX) NOT NULL,
			alias STRING(MAX),
			domain STRING(MAX) NOT NULL,
			tag STRING(MAX) NOT NULL,
			document_id STRING(MAX) NOT NULL,
			created_at TIMESTAMP NOT NULL,
			c_date TIMESTAMP NOT NULL,
			m_date TIMESTAMP NOT NULL,
			CONSTRAINT fk_entities_commit FOREIGN KEY (document_id) REFERENCES commit_logs (id) ON DELETE CASCADE
		) PRIMARY KEY (ccid)`,
		`CREATE INDEX IF NOT EXISTS idx_entities_alias ON entities (alias)`,

		`CREATE TABLE IF NOT EXISTS entity_meta (
			ccid STRING(MAX) NOT NULL,
			inviter STRING(MAX),
			info STRING(MAX) NOT NULL,
			c_date TIMESTAMP NOT NULL,
			m_date TIMESTAMP NOT NULL
		) PRIMARY KEY (ccid)`,

		`CREATE TABLE IF NOT EXISTS servers (
			fqdn STRING(MAX) NOT NULL,
			cs_id STRING(MAX) NOT NULL,
			tag STRING(MAX) NOT NULL,
			layer STRING(MAX) NOT NULL,
			well_known STRING(MAX) NOT NULL,
			c_date TIMESTAMP NOT NULL,
			m_date TIMESTAMP NOT NULL,
			last_scraped TIMESTAMP
		) PRIMARY KEY (fqdn)`,
		`CREATE INDEX IF NOT EXISTS idx_servers_cs_id ON servers (cs_id)`,

		`CREATE TABLE IF NOT EXISTS subscriptions (
			vendor_id STRING(MAX) NOT NULL,
			owner STRING(MAX) NOT NULL,
			schemas ARRAY<STRING(MAX)> NOT NULL,
			prefixes ARRAY<STRING(MAX)> NOT NULL,
			subscription STRING(MAX) NOT NULL,
			c_date TIMESTAMP NOT NULL,
			m_date TIMESTAMP NOT NULL
		) PRIMARY KEY (vendor_id, owner)`,

		`CREATE TABLE IF NOT EXISTS abuse_reports (
			id STRING(36) NOT NULL,
			ip STRING(MAX) NOT NULL,
			reporter STRING(MAX) NOT NULL,
			target_uri STRING(MAX) NOT NULL,
			body STRING(MAX) NOT NULL,
			c_date TIMESTAMP NOT NULL
		) PRIMARY KEY (id)`,

		`CREATE TABLE IF NOT EXISTS aggregate_entries (
			key STRING(MAX) NOT NULL,
			value BYTES(MAX) NOT NULL,
			as_of TIMESTAMP NOT NULL,
			deltas INT64 NOT NULL,
			expires_at TIMESTAMP NOT NULL
		) PRIMARY KEY (key), ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 0 DAY))`,
	}, ""},
	// acks are read by from and ackeds by to: the other side's indexes and
	// the schema ones were never scanned; IF EXISTS because the baseline
	// does not create them
	{2, "drop_unused_ack_indexes", []string{
		`DROP INDEX IF EXISTS idx_acks_to_valid_created_at`,
		`DROP INDEX IF EXISTS idx_acks_schema_valid_created_at`,
		`DROP INDEX IF EXISTS idx_acked_from_valid_created_at`,
		`DROP INDEX IF EXISTS idx_acked_schema_valid_created_at`,
	}, ""},
	// the count cache holds counters and takes its writes as delta rows
	// (internal/infra/aggregate/spanner). It is a cache, so the old rows
	// are dropped with the table rather than converted
	{3, "aggregate_entries_counters", []string{
		`DROP TABLE IF EXISTS aggregate_deltas`,
		`DROP TABLE IF EXISTS aggregate_entries`,
		`CREATE TABLE aggregate_entries (
			key STRING(MAX) NOT NULL,
			value BYTES(MAX) NOT NULL,
			as_of TIMESTAMP NOT NULL,
			deltas INT64 NOT NULL,
			expires_at TIMESTAMP NOT NULL
		) PRIMARY KEY (key), ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 0 DAY))`,
		`CREATE TABLE aggregate_deltas (
			key STRING(MAX) NOT NULL,
			id STRING(36) NOT NULL,
			delta BYTES(MAX) NOT NULL
		) PRIMARY KEY (key, id), INTERLEAVE IN PARENT aggregate_entries ON DELETE CASCADE`,
	}, ""},
	// an association is bound to the incarnation of its target key
	// (record_keys.gen) instead of cascading from the key row, so removing
	// a key no longer deletes its associations in the same commit. The
	// columns default to '' on the rows already there: those associations
	// are all on live keys (the foreign key saw to it), and every key row
	// written from now on gets a generation of its own
	{4, "association_incarnations", []string{
		`ALTER TABLE record_keys ADD COLUMN IF NOT EXISTS gen STRING(36) NOT NULL DEFAULT ('')`,
		`ALTER TABLE associations ADD COLUMN IF NOT EXISTS target_gen STRING(36) NOT NULL DEFAULT ('')`,
		`CREATE INDEX IF NOT EXISTS idx_associations_target_gen_created_at ON associations (target_uri, target_gen, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_associations_target_gen_schema_variant_author ON associations (target_uri, target_gen, schema, variant, author)`,
		`CREATE INDEX IF NOT EXISTS idx_associations_target_gen_schema_variant_created_at ON associations (target_uri, target_gen, schema, variant, created_at)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uni_associations_target_gen_unique ON associations (target_uri, target_gen, unique_key)`,
		`DROP INDEX IF EXISTS idx_associations_target_uri`,
		`DROP INDEX IF EXISTS idx_associations_target_schema_variant_author`,
		`DROP INDEX IF EXISTS idx_associations_target_schema_variant_created_at`,
		`DROP INDEX IF EXISTS uni_associations_unique`,
	}, ""},
	// the cascade from a key row to its associations is what put a whole
	// key's associations into one commit (80,000 mutations at most); the
	// generation columns of batch 4 replace it
	{5, "drop_association_target_cascade", []string{
		`ALTER TABLE associations DROP CONSTRAINT fk_associations_target`,
	}, `SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
		WHERE TABLE_SCHEMA = '' AND TABLE_NAME = 'associations' AND CONSTRAINT_NAME = 'fk_associations_target'`},
}
