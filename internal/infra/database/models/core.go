package models

import (
	"time"

	"github.com/lib/pq"
)

// Indexes:
//   - PRIMARY KEY (id): canonical commit/document lookup; used by record,
//     entity, ack, association foreign keys and direct ccfs lookups in
//     postgres.RecordRepository.GetSignedDocument/Delete.
//   - idx_commit_logs_owner_c_date (owner, c_date): owner-scoped dump and
//     replication scans; used by postgres.RecordRepository.GetAllCommitLogs,
//     postgres.RecordRepository.QueryCommitLogs (owner filter) and
//     postgres.ResidenceRepository.MarkCommitLogsGcCandidateByOwner.
//   - idx_commit_logs_c_date_id (c_date, id): replication cursor scan without an
//     owner filter, ordered by receipt time with id as tie-break; used by
//     postgres.RecordRepository.QueryCommitLogs.
//   - idx_commit_logs_gc_candidate (gc_candidate): marks commits for later GC
//     scans; set by postgres.RecordRepository.CreateRecord when replacing a key
//     and by postgres.ResidenceRepository.MarkCommitLogsGcCandidateByOwner on
//     unregister.
type CommitLog struct {
	ID          string    `json:"id" gorm:"primaryKey;type:text;index:idx_commit_logs_c_date_id,priority:2"`
	IP          string    `json:"ip" gorm:"type:text"`
	Document    string    `json:"document" gorm:"type:text"`
	Proof       string    `json:"proof" gorm:"type:text"`
	Owner       string    `json:"owner" gorm:"type:text;index:idx_commit_logs_owner_c_date,priority:1"`
	GcCandidate bool      `json:"gcCandidate" gorm:"type:boolean;not null;default:false;index"`
	CDate       time.Time `json:"cdate" gorm:"type:timestamp with time zone;not null;default:clock_timestamp();index:idx_commit_logs_owner_c_date,priority:2;index:idx_commit_logs_c_date_id,priority:1"`
}

// Indexes:
//   - PRIMARY KEY (id): internal key identity; used by parent_id references and
//     associations.target_id joins in postgres.RecordRepository.GetAssociated*.
//   - uni_record_keys_uri (uri): URI resolution and upsert target; used by
//     postgres.GetRecordKeyByURI, getOrCreateParentRecordKey, CreateRecord,
//     GetSignedDocument, ChunklineRepository.GetChunklineManifest/LookupLocalItrs,
//     and association target lookups.
//   - idx_record_keys_record_id UNIQUE (record_id): reverse ccfs-to-cckv lookup
//     and record joins; used by postgres.RecordRepository.GetSignedDocument and
//     GetHierarchicalRecordPolicies.
//   - idx_record_keys_parent_id_record_id (parent_id, record_id): parent member
//     lookup and record preload joins; used by postgres.RecordRepository.QueryByParent.
//   - idx_record_keys_parent_created_at_record_id
//     (parent_id, record_created_at DESC, record_id) WHERE parent_id IS NOT NULL
//     AND record_created_at IS NOT NULL: timeline/chunkline range scans without
//     joining records; used by postgres.ChunklineRepository.GetChunklineManifest,
//     LookupLocalItrs, and LoadLocalBody.
//   - idx_record_keys_parent_id_uri (parent_id, uri): key-ordered child listing
//     including placeholder keys (no record_created_at predicate); used by
//     postgres.RecordRepository.QueryByParentOrderByKey.
type RecordKey struct {
	ID              int64      `json:"id" gorm:"primaryKey;autoIncrement"`
	ParentID        *int64     `json:"parentID" gorm:"index:idx_record_keys_parent_id_record_id,priority:1;index:idx_record_keys_parent_created_at_record_id,priority:1,where:parent_id IS NOT NULL AND record_created_at IS NOT NULL;index:idx_record_keys_parent_id_uri,priority:1"`
	URI             string     `json:"uri" gorm:"type:text;unique;index:idx_record_keys_parent_id_uri,priority:2"`
	RecordID        *string    `json:"recordID" gorm:"type:text;uniqueIndex;index:idx_record_keys_parent_id_record_id,priority:2;index:idx_record_keys_parent_created_at_record_id,priority:3,where:parent_id IS NOT NULL AND record_created_at IS NOT NULL"`
	Record          Record     `json:"record" gorm:"foreignKey:RecordID;references:DocumentID;constraint:OnDelete:CASCADE;"`
	RecordCreatedAt *time.Time `json:"recordCreatedAt,omitempty" gorm:"type:timestamp with time zone;index:idx_record_keys_parent_created_at_record_id,priority:2,sort:desc,where:parent_id IS NOT NULL AND record_created_at IS NOT NULL"`
	CleanOnUpdate   bool       `json:"cleanOnUpdate" gorm:"type:boolean;not null;default:false"`
}

// Indexes:
//   - PRIMARY KEY (document_id): record payload lookup and foreign-key target;
//     used by preloads from RecordKey.Record and direct deletes in
//     postgres.RecordRepository.CreateRecord/Delete.
//   - idx_records_schema_created_at_document_id (schema, created_at, document_id):
//     schema/time-ordered record searches; used by postgres.RecordRepository
//     QueryByPrefix and QueryByParent when schema/time filters are present.
//   - idx_records_author_created_at_document_id (author, created_at, document_id):
//     author/time-ordered record searches; used by postgres.RecordRepository
//     QueryByPrefix and QueryByParent when the author filter is present.
//   - idx_records_document_id_created_at (document_id, created_at): record join
//     plus timestamp predicate support for record_keys-to-records joins that still
//     need records.created_at.
type Record struct {
	DocumentID string    `json:"id" gorm:"primaryKey;type:text;index:idx_records_schema_created_at_document_id,priority:3;index:idx_records_author_created_at_document_id,priority:3;index:idx_records_document_id_created_at,priority:1"`
	Document   CommitLog `json:"documnet" gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`
	Owner      string    `json:"owner" gorm:"type:text"`
	// document author; NULL only on rows written before the column existed
	// (backfilled by conctl op repair-record-authors)
	Author        string         `json:"author" gorm:"type:text;index:idx_records_author_created_at_document_id,priority:1"`
	Redirect      *string        `json:"redirect" gorm:"type:text"`
	Schema        string         `json:"schema" gorm:"type:text;index:idx_records_schema_created_at_document_id,priority:1"`
	Policies      *string        `json:"policies" gorm:"type:text"`
	Distributions pq.StringArray `json:"distributions" gorm:"type:text[]"`
	// user-provided creation time
	CreatedAt time.Time `json:"createdAt" gorm:"type:timestamp with time zone;not null;index:idx_records_schema_created_at_document_id,priority:2;index:idx_records_author_created_at_document_id,priority:2;index:idx_records_document_id_created_at,priority:2"`
	// record creation time in the system
	CDate time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
}

// Indexes:
//   - PRIMARY KEY (document_id): one ack state per commit document and commit-log
//     foreign-key target.
//   - idx_ack_from_to_schema UNIQUE (from, to, schema): idempotent ack state
//     upsert; used by postgres.RecordRepository.saveAck and filtered by
//     GetAcknowledgeRecords/GetAcknowledgeRecordCounts.
//   - idx_acks_from_valid_created_at (from, valid, created_at): from-filtered
//     ack list/count lookup; used by postgres.RecordRepository.GetAcknowledge*.
//   - idx_acks_to_valid_created_at (to, valid, created_at): to-filtered ack
//     list/count lookup; used by postgres.RecordRepository.GetAcknowledge*.
//   - idx_acks_schema_valid_created_at (schema, valid, created_at):
//     schema-filtered ack list/count lookup; used by
//     postgres.RecordRepository.GetAcknowledge*.
type Ack struct {
	From   string `json:"from" gorm:"type:text;index:idx_ack_from_to_schema,unique;index:idx_acks_from_valid_created_at,priority:1"`
	To     string `json:"to" gorm:"type:text;index:idx_ack_from_to_schema,unique;index:idx_acks_to_valid_created_at,priority:1"`
	Schema string `json:"schema" gorm:"type:text;index:idx_ack_from_to_schema,unique;index:idx_acks_schema_valid_created_at,priority:1"`

	DocumentID string    `json:"id" gorm:"primaryKey;type:text"`
	Document   CommitLog `json:"-" gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`

	Valid bool `json:"valid" gorm:"type:boolean;not null;default:true;index:idx_acks_from_valid_created_at,priority:2;index:idx_acks_to_valid_created_at,priority:2;index:idx_acks_schema_valid_created_at,priority:2"`

	CreatedAt time.Time `json:"createdAt" gorm:"type:timestamp with time zone;not null;index:idx_acks_from_valid_created_at,priority:3;index:idx_acks_to_valid_created_at,priority:3;index:idx_acks_schema_valid_created_at,priority:3"` // user-provided creation time
	CDate     time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
}

// Indexes:
//   - PRIMARY KEY (document_id): one ack state per commit document and commit-log
//     foreign-key target.
//   - idx_acked_from_to_schema UNIQUE (from, to, schema): idempotent ack state
//     upsert; used by postgres.RecordRepository.saveAck and filtered by
//     GetAcknowledgeRecords/GetAcknowledgeRecordCounts.
//   - idx_acked_from_valid_created_at (from, valid, created_at): from-filtered
//     ack list/count lookup; used by postgres.RecordRepository.GetAcknowledge*.
//   - idx_acked_to_valid_created_at (to, valid, created_at): to-filtered ack
//     list/count lookup; used by postgres.RecordRepository.GetAcknowledge*.
//   - idx_acked_schema_valid_created_at (schema, valid, created_at):
//     schema-filtered ack list/count lookup; used by
//     postgres.RecordRepository.GetAcknowledge*.
type Acked struct {
	From   string `json:"from" gorm:"type:text;index:idx_acked_from_to_schema,unique;index:idx_acked_from_valid_created_at,priority:1"`
	To     string `json:"to" gorm:"type:text;index:idx_acked_from_to_schema,unique;index:idx_acked_to_valid_created_at,priority:1"`
	Schema string `json:"schema" gorm:"type:text;index:idx_acked_from_to_schema,unique;index:idx_acked_schema_valid_created_at,priority:1"`

	DocumentID string    `json:"id" gorm:"primaryKey;type:text"`
	Document   CommitLog `json:"-" gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`

	Valid bool `json:"valid" gorm:"type:boolean;not null;default:true;index:idx_acked_from_valid_created_at,priority:2;index:idx_acked_to_valid_created_at,priority:2;index:idx_acked_schema_valid_created_at,priority:2"`

	CreatedAt time.Time `json:"createdAt" gorm:"type:timestamp with time zone;not null;index:idx_acked_from_valid_created_at,priority:3;index:idx_acked_to_valid_created_at,priority:3;index:idx_acked_schema_valid_created_at,priority:3"` // user-provided creation time
	CDate     time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
}

// Indexes:
//   - PRIMARY KEY (document_id): one association per commit document and
//     commit-log foreign-key target.
//   - idx_associations_target_id (target_id): association lookup by target record
//     key; used by postgres.RecordRepository.GetAssociatedRecords,
//     GetAssociatedRecordCountsBySchema, and GetAssociatedRecordCountsByVariant.
//   - idx_associations_target_schema_variant_author
//     (target_id, schema, variant, author): target lookup with optional filters;
//     used by postgres.RecordRepository.GetAssociatedRecords.
//   - idx_associations_target_schema_variant_created_at
//     (target_id, schema, variant, created_at): target/schema counts by variant
//     and earliest association ordering; used by
//     postgres.RecordRepository.GetAssociatedRecordCountsByVariant.
//   - uni_associations_unique (unique): idempotency key for association writes;
//     used by postgres.RecordRepository.CreateAssociation.
type Association struct {
	DocumentID string    `json:"id" gorm:"primaryKey;type:text"`
	Document   CommitLog `json:"-" gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`

	TargetID int64     `json:"targetID" gorm:"type:bigint;index:idx_associations_target_id;index:idx_associations_target_schema_variant_author,priority:1;index:idx_associations_target_schema_variant_created_at,priority:1"`
	Target   RecordKey `json:"-" gorm:"foreignKey:TargetID;references:ID;constraint:OnDelete:CASCADE;"`

	Owner  string `json:"owner" gorm:"type:text"`
	Author string `json:"author" gorm:"type:text;index:idx_associations_target_schema_variant_author,priority:4"`

	Schema  string  `json:"schema" gorm:"type:text;index:idx_associations_target_schema_variant_author,priority:2;index:idx_associations_target_schema_variant_created_at,priority:2"`
	Variant *string `json:"variant" gorm:"type:text;index:idx_associations_target_schema_variant_author,priority:3;index:idx_associations_target_schema_variant_created_at,priority:3"`
	Unique  string  `json:"unique" gorm:"type:text;unique"`

	CreatedAt time.Time `json:"createdAt" gorm:"type:timestamp with time zone;not null;index:idx_associations_target_schema_variant_created_at,priority:4"` // user-provided creation time
	CDate     time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
}

// Indexes:
//   - PRIMARY KEY (id): FQDN lookup and upsert target; used by
//     postgres.ServerRepository.GetAndCacheByFQDN and cache writes.
//   - idx_servers_cs_id (cs_id): CSID cache lookup; used by
//     postgres.ServerRepository.GetAndCacheByCSID.
type Server struct {
	ID          string    `json:"fqdn" gorm:"type:text;primaryKey"` // FQDN
	CSID        string    `json:"csid" gorm:"type:text;index:idx_servers_cs_id"`
	Tag         string    `json:"tag" gorm:"type:text"`
	Layer       string    `json:"layer" gorm:"type:text"`
	WellKnown   string    `json:"wellKnown" gorm:"type:jsonb"`
	CDate       time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
	MDate       time.Time `json:"mdate" gorm:"autoUpdateTime"`
	LastScraped time.Time `json:"lastScraped" gorm:"type:timestamp with time zone"`
}

// Indexes:
//   - PRIMARY KEY (id): ccid lookup and upsert target; used by
//     postgres.RecordRepository.CreateEntity and
//     postgres.ResidenceRepository.GetEntityByCCID.
//   - idx_entities_alias (alias): alias lookup; used by
//     postgres.ResidenceRepository.GetEntityByAlias.
type Entity struct {
	ID     string  `json:"ccid" gorm:"type:text;primaryKey"`
	Alias  *string `json:"alias,omitempty" gorm:"type:text;index"`
	Domain string  `json:"domain" gorm:"type:text"`
	Tag    string  `json:"tag" gorm:"type:text;"`

	DocumentID string    `json:"id" gorm:"type:text"`
	Document   CommitLog `json:"-" gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`
	// document createdAt: the accept-if-newer key (CIP-3 §3.4). Rows written
	// before the column existed hold the epoch until backfilled by
	// `conctl op repair-single-owner`, so any real document replaces them.
	CreatedAt time.Time `json:"createdAt" gorm:"type:timestamp with time zone;not null;default:'epoch'"`

	CDate time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
	MDate time.Time `json:"mdate" gorm:"autoUpdateTime"`
}

// Indexes:
//   - PRIMARY KEY (id): entity metadata lookup and upsert target; used by
//     postgres.ResidenceRepository.SaveMeta and GetMeta.
type EntityMeta struct {
	ID      string    `json:"ccid" gorm:"type:text;primaryKey"`
	Inviter *string   `json:"inviter" gorm:"type:text"`
	Info    string    `json:"info" gorm:"type:jsonb;default:'null'"`
	CDate   time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
	MDate   time.Time `json:"mdate" gorm:"autoUpdateTime"`
}

// Indexes:
//   - PRIMARY KEY (id): report identity. Abuse reports are currently append-only;
//     postgres.AbuseRepository.CreateAbuseReport does not issue read queries.
type AbuseReport struct {
	ID        int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	IP        string    `json:"ip" gorm:"type:text"`
	Reporter  string    `json:"reporter" gorm:"type:text"`
	TargetURI string    `json:"target" gorm:"type:text"`
	Body      string    `json:"body" gorm:"type:text"`
	CDate     time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
}
