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
type migration struct {
	Version    int
	Name       string
	Statements []string
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
//     and the owner-root branch of QueryByParent*;
//     idx_record_keys_parent_created_at_record_id NULL_FILTERED
//     (parent_uri, record_created_at DESC, record_id) chunkline scans and
//     QueryByParent; idx_record_keys_record_id UNIQUE NULL_FILTERED reverse
//     ccfs-to-cckv lookups; idx_record_keys_parent_uri_uri (parent_uri, uri)
//     QueryByParentOrderByKey including placeholders.
//   - associations: PK (id) interleaved; (target_uri) GetAssociatedRecords
//     and the Compute* counts; (target_uri, schema, variant, author) and
//     (target_uri, schema, variant, created_at) for the filtered variants;
//     UNIQUE (unique_key) idempotent association writes.
//   - acks / ackeds: PK (id) interleaved; UNIQUE (from_ccid, to_ccid, schema)
//     the accept-if-newer state key; acks (from_ccid, valid, created_at) and
//     ackeds (to_ccid, valid, created_at) the list and count lookups (acks
//     are read by from, ackeds by to; schema only narrows).
//   - entities: PK (ccid); (alias) GetEntityByAlias.
//   - entity_meta: PK (ccid). servers: PK (fqdn); (cs_id). subscriptions:
//     PK (vendor_id, owner). abuse_reports: PK (id). aggregate_entries:
//     PK (key), rows past expires_at are dropped by the row deletion policy.
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
	}},
	// acks are read by from and ackeds by to: the other side's indexes and
	// the schema ones were never scanned. A fresh database never had them
	// (the baseline above no longer creates them), hence IF EXISTS.
	{2, "drop_unused_ack_indexes", []string{
		`DROP INDEX IF EXISTS idx_acks_to_valid_created_at`,
		`DROP INDEX IF EXISTS idx_acks_schema_valid_created_at`,
		`DROP INDEX IF EXISTS idx_acked_from_valid_created_at`,
		`DROP INDEX IF EXISTS idx_acked_schema_valid_created_at`,
	}},
}
