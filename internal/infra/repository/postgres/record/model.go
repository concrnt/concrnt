package record

import (
	"time"

	"github.com/lib/pq"
)

// Indexes:
//   - PRIMARY KEY (id): canonical commit/document lookup; used by record,
//     entity, ack, association foreign keys and direct ccfs lookups in
//     pgrecord.Repository.GetSignedDocument/Delete.
//   - idx_commit_logs_owner_c_date (owner, c_date): owner-scoped dump and
//     replication scans; used by pgrecord.Repository.GetAllCommitLogs,
//     pgrecord.Repository.QueryCommitLogs (owner filter) and
//     pgrecord.Repository.MarkCommitLogsGcCandidateByOwner.
//   - idx_commit_logs_c_date_id (c_date, id): replication cursor scan without an
//     owner filter, ordered by receipt time with id as tie-break; used by
//     pgrecord.Repository.QueryCommitLogs.
//   - idx_commit_logs_gc_candidate (gc_candidate): marks commits for later GC
//     scans; set by pgrecord.Repository.CreateRecord when replacing a key
//     (unless the replaced version asked to be retained), by
//     pgrecord.Repository.MarkCommitLogGcCandidate on delete and by
//     pgrecord.Repository.MarkCommitLogsGcCandidateByOwner on unregister.
//     A flagged commit is no longer served by ccfs
//     (pgrecord.Repository.GetSignedDocument).
type CommitLog struct {
	ID          string    `json:"id" gorm:"primaryKey;type:text;index:idx_commit_logs_c_date_id,priority:2"`
	IP          string    `json:"ip" gorm:"type:text"`
	Document    string    `json:"document" gorm:"type:text"`
	Proof       string    `json:"proof" gorm:"type:text"`
	Owner       string    `json:"owner" gorm:"type:text;index:idx_commit_logs_owner_c_date,priority:1"`
	GcCandidate bool      `json:"gcCandidate" gorm:"type:boolean;not null;default:false;index"`
	CDate       time.Time `json:"cdate" gorm:"type:timestamp with time zone;not null;default:clock_timestamp();index:idx_commit_logs_owner_c_date,priority:2;index:idx_commit_logs_c_date_id,priority:1"`
}

// RecordKey is one cckv slot and, while it holds one, the record written to
// it: the record's columns live on the key row itself, so a listing reads one
// table (plus commit_logs for the signed document). A placeholder key (an
// ancestor materialized by a child write, or a removed key that still has
// children) has RecordID NULL and every record column NULL. A version the
// key has moved on from is not on any row: when it asked to be retained
// (onUpdate "retain") it stays readable through its commit log by ccfs,
// otherwise its commit log is flagged gc_candidate.
//
// Indexes:
//   - PRIMARY KEY (id): internal key identity; used by parent_id references and
//     associations.target_id joins in pgrecord.Repository.GetAssociated*.
//   - uni_record_keys_uri (uri): URI resolution and upsert target; used by
//     pgrecord.getRecordKeyByURI, getOrCreateParentRecordKey, CreateRecord,
//     GetSignedDocument, ChunklineRepository.GetChunklineManifest/LookupLocalItrs,
//     and association target lookups.
//   - idx_record_keys_record_id UNIQUE (record_id): reverse ccfs-to-cckv lookup;
//     used by pgrecord.Repository.GetSignedDocument,
//     GetHierarchicalRecordPolicies and DeleteRecordByDocumentID.
//   - idx_record_keys_uri_pattern (uri text_pattern_ops): URI prefix
//     matches (uri LIKE 'prefix%'), which the unique index cannot serve
//     under a non-C collation; used by pgrecord.Repository.QueryByPrefix,
//     QueryRecordSubtree and the owner-root branches of QueryByParent and
//     QueryByParentOrderByKey.
//   - idx_record_keys_parent_created_at_record_id
//     (parent_id, record_created_at DESC, record_id) WHERE parent_id IS NOT NULL
//     AND record_created_at IS NOT NULL: parent listings and timeline/chunkline
//     range scans; used by pgrecord.Repository.QueryByParent and
//     pgrecord.ChunklineRepository.GetChunklineManifest, LookupLocalItrs, and
//     LoadLocalBody.
//   - idx_record_keys_parent_id_uri (parent_id, uri): key-ordered child listing
//     including placeholder keys (no record_created_at predicate) and the
//     child-existence checks of a key removal; used by
//     pgrecord.Repository.QueryByParentOrderByKey and removeRecordKey.
//   - idx_record_keys_schema_created_at_record_id
//     (schema, record_created_at, record_id): schema/time-ordered record
//     searches; used by pgrecord.Repository.QueryByPrefix and QueryByParent
//     when a schema filter is present.
//   - idx_record_keys_author_created_at_record_id
//     (author, record_created_at, record_id): author/time-ordered record
//     searches; used by pgrecord.Repository.QueryByPrefix and QueryByParent
//     when the author filter is present.
type RecordKey struct {
	ID       int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	ParentID *int64    `json:"parentID" gorm:"index:idx_record_keys_parent_created_at_record_id,priority:1,where:parent_id IS NOT NULL AND record_created_at IS NOT NULL;index:idx_record_keys_parent_id_uri,priority:1"`
	URI      string    `json:"uri" gorm:"type:text;unique;index:idx_record_keys_parent_id_uri,priority:2;index:idx_record_keys_uri_pattern,expression:uri text_pattern_ops"`
	RecordID *string   `json:"recordID" gorm:"type:text;uniqueIndex;index:idx_record_keys_parent_created_at_record_id,priority:3,where:parent_id IS NOT NULL AND record_created_at IS NOT NULL;index:idx_record_keys_schema_created_at_record_id,priority:3;index:idx_record_keys_author_created_at_record_id,priority:3"`
	Document CommitLog `json:"-" gorm:"foreignKey:RecordID;references:ID;constraint:OnDelete:CASCADE;"`

	// The record columns below are NULL on a placeholder key. Owner, Author
	// and Schema carry `default:null` so that gorm leaves them out of the
	// INSERT of a placeholder instead of writing "".
	Owner         string         `json:"owner" gorm:"type:text;default:null"`
	Author        string         `json:"author" gorm:"type:text;default:null;index:idx_record_keys_author_created_at_record_id,priority:1"`
	Schema        string         `json:"schema" gorm:"type:text;default:null;index:idx_record_keys_schema_created_at_record_id,priority:1"`
	Redirect      *string        `json:"redirect" gorm:"type:text"`
	Policies      *string        `json:"policies" gorm:"type:text"`
	Distributions pq.StringArray `json:"distributions" gorm:"type:text[]"`
	// the record's user-provided creation time (the document's createdAt, or
	// the referenced document's for a reference record)
	RecordCreatedAt *time.Time `json:"recordCreatedAt,omitempty" gorm:"type:timestamp with time zone;index:idx_record_keys_parent_created_at_record_id,priority:2,sort:desc,where:parent_id IS NOT NULL AND record_created_at IS NOT NULL;index:idx_record_keys_schema_created_at_record_id,priority:2;index:idx_record_keys_author_created_at_record_id,priority:2"`
	CleanOnUpdate   bool       `json:"cleanOnUpdate" gorm:"type:boolean;not null;default:false"`
}

// Indexes:
//   - PRIMARY KEY (document_id): one ack state per commit document and commit-log
//     foreign-key target.
//   - idx_ack_from_to_schema UNIQUE (from, to, schema): idempotent ack state
//     upsert; used by pgrecord.Repository.saveAck and filtered by
//     GetAcknowledgeRecords/GetAcknowledgeRecordCounts.
//   - idx_acks_from_valid_created_at (from, valid, created_at): from-filtered
//     ack list/count lookup; used by pgrecord.Repository.GetAcknowledge*.
//   - idx_acks_to_valid_created_at (to, valid, created_at): to-filtered ack
//     list/count lookup; used by pgrecord.Repository.GetAcknowledge*.
//   - idx_acks_schema_valid_created_at (schema, valid, created_at):
//     schema-filtered ack list/count lookup; used by
//     pgrecord.Repository.GetAcknowledge*.
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
//     upsert; used by pgrecord.Repository.saveAck and filtered by
//     GetAcknowledgeRecords/GetAcknowledgeRecordCounts.
//   - idx_acked_from_valid_created_at (from, valid, created_at): from-filtered
//     ack list/count lookup; used by pgrecord.Repository.GetAcknowledge*.
//   - idx_acked_to_valid_created_at (to, valid, created_at): to-filtered ack
//     list/count lookup; used by pgrecord.Repository.GetAcknowledge*.
//   - idx_acked_schema_valid_created_at (schema, valid, created_at):
//     schema-filtered ack list/count lookup; used by
//     pgrecord.Repository.GetAcknowledge*.
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
//     key; used by pgrecord.Repository.GetAssociatedRecords,
//     GetAssociatedRecordCountsBySchema, and GetAssociatedRecordCountsByVariant.
//   - idx_associations_target_schema_variant_author
//     (target_id, schema, variant, author): target lookup with optional filters;
//     used by pgrecord.Repository.GetAssociatedRecords.
//   - idx_associations_target_schema_variant_created_at
//     (target_id, schema, variant, created_at): target/schema counts by variant
//     and earliest association ordering; used by
//     pgrecord.Repository.GetAssociatedRecordCountsByVariant.
//   - uni_associations_unique (unique): idempotency key for association writes;
//     used by pgrecord.Repository.CreateAssociation.
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
//   - PRIMARY KEY (id): ccid lookup and upsert target; used by
//     pgrecord.Repository.CreateEntity and
//     pgrecord.Repository.GetEntityByCCID.
//   - idx_entities_alias (alias): alias lookup; used by
//     pgrecord.Repository.GetEntityByAlias.
type Entity struct {
	ID     string  `json:"ccid" gorm:"type:text;primaryKey"`
	Alias  *string `json:"alias,omitempty" gorm:"type:text;index"`
	Domain string  `json:"domain" gorm:"type:text"`
	Tag    string  `json:"tag" gorm:"type:text;"`

	// IsTrustSeed marks the entities the operator trusts outright (set with
	// `conctl op trust-seed`); trust reaches every other entity through
	// acknowledgements. Trust is the entity's score from the latest trust
	// computation (see the trust usecase), 0 to 1: a result kept for
	// inspection and for adding up per domain, never an input.
	//   - idx_entities_is_trust_seed (partial, is_trust_seed): seed lookup.
	//   - idx_entities_trust (partial, trust > 0): per-domain trust totals.
	IsTrustSeed bool    `json:"isTrustSeed" gorm:"type:boolean;not null;default:false;index:idx_entities_is_trust_seed,where:is_trust_seed"`
	Trust       float64 `json:"trust" gorm:"type:double precision;not null;default:0;index:idx_entities_trust,where:trust > 0"`

	DocumentID string    `json:"id" gorm:"type:text"`
	Document   CommitLog `json:"-" gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`
	// document createdAt: the accept-if-newer key (CIP-3 §3.4). Rows written
	// before the column existed hold the epoch until backfilled by
	// `conctl op repair-single-owner`, so any real document replaces them.
	CreatedAt time.Time `json:"createdAt" gorm:"type:timestamp with time zone;not null;default:'epoch'"`

	CDate time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
	MDate time.Time `json:"mdate" gorm:"autoUpdateTime"`
}
