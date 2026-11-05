package sqlite

import "gorm.io/gorm"

// The gorm models of the SQLite backend: table for table the Postgres ones
// (internal/infra/repository/postgres/models.go), AutoMigrated at every start
// (migrations.go). The differences are the storage types: timestamps are
// INTEGER Unix microseconds (conv.go), text[] columns are JSON text
// (JSONStrings), and the key-ordered index needs no collation because
// SQLite's default BINARY collation already is byte order. SQLite has no
// clock_timestamp() default, so the hooks below stamp c_date and m_date.

// Indexes:
//   - PRIMARY KEY (id): canonical commit/document lookup; used by record,
//     entity, ack, association foreign keys and direct ccfs lookups in
//     record.Repository.GetSignedDocument/HasCommitLog/DeleteCommitLogs, and
//     as the cursor of ListCommitLogs/ListGcCandidates (id < cutoff, id > after).
//   - idx_commit_logs_owner_c_date (owner, c_date): owner-scoped dump and
//     replication scans; used by record.Repository.GetAllCommitLogs,
//     QueryCommitLogs (owner filter) and MarkCommitLogsGcCandidateByOwner.
//   - idx_commit_logs_c_date_id (c_date, id): replication cursor scan without an
//     owner filter, ordered by receipt time with id as tie-break; used by
//     record.Repository.QueryCommitLogs.
//   - idx_commit_logs_gc_candidate (gc_candidate): marks commits for later GC
//     scans; set by record.Repository.CreateRecord when replacing a key,
//     MarkCommitLogGcCandidate and MarkCommitLogsGcCandidateByOwner on
//     unregister, read by CountGcCandidates/ListGcCandidates.
type CommitLog struct {
	ID          string `json:"id" gorm:"primaryKey;type:text;index:idx_commit_logs_c_date_id,priority:2"`
	IP          string `json:"ip" gorm:"type:text"`
	Document    string `json:"document" gorm:"type:text"`
	Proof       string `json:"proof" gorm:"type:text"`
	Owner       string `json:"owner" gorm:"type:text;index:idx_commit_logs_owner_c_date,priority:1"`
	GcCandidate bool   `json:"gcCandidate" gorm:"type:integer;not null;default:false;index:idx_commit_logs_gc_candidate"`
	CDate       int64  `json:"cdate" gorm:"type:integer;not null;index:idx_commit_logs_owner_c_date,priority:2;index:idx_commit_logs_c_date_id,priority:1"`
}

func (m *CommitLog) BeforeCreate(*gorm.DB) error {
	if m.CDate == 0 {
		m.CDate = nowMicros()
	}
	return nil
}

// Indexes:
//   - PRIMARY KEY (id): internal key identity; used by parent_id references and
//     associations.target_id joins in record.Repository.GetAssociatedRecords and
//     ComputeAssociationCounts*.
//   - uni_record_keys_uri (uri): URI resolution and upsert target; used by
//     record.GetRecordKeyByURI, getOrCreateParentRecordKey, CreateRecord,
//     GetSignedDocument(s), DeleteRecordByKey, GetTimelineRemoval,
//     GetChunklineManifest/LookupLocalItrs/LoadLocalBody, and association target
//     lookups. Prefix scans (QueryByPrefix, QueryRecordSubtree, the parent_id IS
//     NULL branch of QueryByParent*) also walk it with LIKE.
//   - idx_record_keys_record_id UNIQUE (record_id): reverse ccfs-to-cckv lookup
//     and record joins; used by record.Repository.GetHierarchicalRecordPolicies,
//     policiesFor and signedDocumentsByID.
//   - idx_record_keys_parent_id_record_id (parent_id, record_id): parent member
//     lookup and record preload joins; used by record.Repository.QueryByParent.
//   - idx_record_keys_parent_created_at_record_id
//     (parent_id, record_created_at DESC, record_id) WHERE parent_id IS NOT NULL
//     AND record_created_at IS NOT NULL: timeline/chunkline range scans without
//     joining records; used by record.Repository.GetChunklineManifest,
//     LookupLocalItrs, and LoadLocalBody.
//   - idx_record_keys_parent_id_uri (parent_id, uri): key-ordered child listing
//     including placeholder keys (no record_created_at predicate); used by
//     record.Repository.QueryByParentOrderByKey.
type RecordKey struct {
	ID              int64   `json:"id" gorm:"primaryKey;autoIncrement"`
	ParentID        *int64  `json:"parentID" gorm:"type:integer;index:idx_record_keys_parent_id_record_id,priority:1;index:idx_record_keys_parent_created_at_record_id,priority:1,where:parent_id IS NOT NULL AND record_created_at IS NOT NULL;index:idx_record_keys_parent_id_uri,priority:1"`
	URI             string  `json:"uri" gorm:"type:text;not null;unique;index:idx_record_keys_parent_id_uri,priority:2"`
	RecordID        *string `json:"recordID" gorm:"type:text;uniqueIndex;index:idx_record_keys_parent_id_record_id,priority:2;index:idx_record_keys_parent_created_at_record_id,priority:3"`
	Record          Record  `json:"record" gorm:"foreignKey:RecordID;references:DocumentID;constraint:OnDelete:CASCADE;"`
	RecordCreatedAt *int64  `json:"recordCreatedAt,omitempty" gorm:"type:integer;index:idx_record_keys_parent_created_at_record_id,priority:2,sort:desc"`
	// Redirect is the record's redirect, denormalised so chunkline bodies and
	// removal lookups never join records
	Redirect      *string `json:"redirect,omitempty" gorm:"type:text"`
	CleanOnUpdate bool    `json:"cleanOnUpdate" gorm:"type:integer;not null;default:false"`
}

// Indexes:
//   - PRIMARY KEY (document_id): record payload lookup and foreign-key target;
//     used by the record_keys-to-records joins and direct deletes in
//     record.Repository.CreateRecord/DeleteRecordByKey/DeleteRecordByDocumentID.
//   - idx_records_schema_created_at_document_id (schema, created_at, document_id):
//     schema/time-ordered record searches; used by record.Repository
//     QueryByPrefix and QueryByParent when schema/time filters are present.
//   - idx_records_author_created_at_document_id (author, created_at, document_id):
//     author/time-ordered record searches; used by record.Repository
//     QueryByPrefix and QueryByParent when the author filter is present.
//   - idx_records_document_id_created_at (document_id, created_at): record join
//     plus timestamp predicate support for record_keys-to-records joins that still
//     need records.created_at.
type Record struct {
	DocumentID    string      `json:"id" gorm:"primaryKey;type:text;index:idx_records_schema_created_at_document_id,priority:3;index:idx_records_author_created_at_document_id,priority:3;index:idx_records_document_id_created_at,priority:1"`
	Document      CommitLog   `json:"documnet" gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`
	Owner         string      `json:"owner" gorm:"type:text"`
	Author        string      `json:"author" gorm:"type:text;index:idx_records_author_created_at_document_id,priority:1"`
	Redirect      *string     `json:"redirect" gorm:"type:text"`
	Schema        string      `json:"schema" gorm:"type:text;index:idx_records_schema_created_at_document_id,priority:1"`
	Policies      *string     `json:"policies" gorm:"type:text"`
	Distributions JSONStrings `json:"distributions" gorm:"type:text;not null"`
	// user-provided creation time
	CreatedAt int64 `json:"createdAt" gorm:"type:integer;not null;index:idx_records_schema_created_at_document_id,priority:2;index:idx_records_author_created_at_document_id,priority:2;index:idx_records_document_id_created_at,priority:2"`
	// record creation time in the system
	CDate int64 `json:"cdate" gorm:"->;<-:create;type:integer;not null"`
}

func (m *Record) BeforeCreate(*gorm.DB) error {
	if m.CDate == 0 {
		m.CDate = nowMicros()
	}
	return nil
}

// Indexes:
//   - PRIMARY KEY (document_id): one ack state per commit document and commit-log
//     foreign-key target.
//   - idx_ack_from_to_schema UNIQUE (from, to, schema): idempotent ack state
//     upsert; used by record.Repository.processAck and filtered by
//     getAckRecords/getAckRecordCounts.
//   - idx_acks_from_valid_created_at (from, valid, created_at): from-filtered
//     ack list/count lookup; used by record.Repository.getAckRecords and
//     getAckRecordCounts (via GetAcknowledgeRecords/ComputeAcknowledgeCounts).
//   - idx_acks_to_valid_created_at (to, valid, created_at): to-filtered ack
//     list/count lookup; same callers.
//   - idx_acks_schema_valid_created_at (schema, valid, created_at):
//     schema-filtered ack list/count lookup; same callers.
type Ack struct {
	From   string `json:"from" gorm:"type:text;index:idx_ack_from_to_schema,unique,priority:1;index:idx_acks_from_valid_created_at,priority:1"`
	To     string `json:"to" gorm:"type:text;index:idx_ack_from_to_schema,unique,priority:2;index:idx_acks_to_valid_created_at,priority:1"`
	Schema string `json:"schema" gorm:"type:text;index:idx_ack_from_to_schema,unique,priority:3;index:idx_acks_schema_valid_created_at,priority:1"`

	DocumentID string    `json:"id" gorm:"primaryKey;type:text"`
	Document   CommitLog `json:"-" gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`

	// no default: gorm would swap a false for it on insert, and an
	// unacknowledge may be the first state a (from, to, schema) ever sees
	Valid bool `json:"valid" gorm:"type:integer;not null;index:idx_acks_from_valid_created_at,priority:2;index:idx_acks_to_valid_created_at,priority:2;index:idx_acks_schema_valid_created_at,priority:2"`

	CreatedAt int64 `json:"createdAt" gorm:"type:integer;not null;index:idx_acks_from_valid_created_at,priority:3;index:idx_acks_to_valid_created_at,priority:3;index:idx_acks_schema_valid_created_at,priority:3"` // user-provided creation time
	CDate     int64 `json:"cdate" gorm:"->;<-:create;type:integer;not null"`
}

func (m *Ack) BeforeCreate(*gorm.DB) error {
	if m.CDate == 0 {
		m.CDate = nowMicros()
	}
	return nil
}

// Indexes:
//   - PRIMARY KEY (document_id): one ack state per commit document and commit-log
//     foreign-key target.
//   - idx_acked_from_to_schema UNIQUE (from, to, schema): idempotent ack state
//     upsert; used by record.Repository.processAcked and filtered by
//     getAckedRecords/getAckedRecordCounts.
//   - idx_acked_from_valid_created_at (from, valid, created_at): from-filtered
//     ack list/count lookup; used by record.Repository.getAckedRecords and
//     getAckedRecordCounts (via GetAcknowledgeRecords/ComputeAcknowledgeCounts).
//   - idx_acked_to_valid_created_at (to, valid, created_at): to-filtered ack
//     list/count lookup; same callers.
//   - idx_acked_schema_valid_created_at (schema, valid, created_at):
//     schema-filtered ack list/count lookup; same callers.
type Acked struct {
	From   string `json:"from" gorm:"type:text;index:idx_acked_from_to_schema,unique,priority:1;index:idx_acked_from_valid_created_at,priority:1"`
	To     string `json:"to" gorm:"type:text;index:idx_acked_from_to_schema,unique,priority:2;index:idx_acked_to_valid_created_at,priority:1"`
	Schema string `json:"schema" gorm:"type:text;index:idx_acked_from_to_schema,unique,priority:3;index:idx_acked_schema_valid_created_at,priority:1"`

	DocumentID string    `json:"id" gorm:"primaryKey;type:text"`
	Document   CommitLog `json:"-" gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`

	Valid bool `json:"valid" gorm:"type:integer;not null;index:idx_acked_from_valid_created_at,priority:2;index:idx_acked_to_valid_created_at,priority:2;index:idx_acked_schema_valid_created_at,priority:2"`

	CreatedAt int64 `json:"createdAt" gorm:"type:integer;not null;index:idx_acked_from_valid_created_at,priority:3;index:idx_acked_to_valid_created_at,priority:3;index:idx_acked_schema_valid_created_at,priority:3"` // user-provided creation time
	CDate     int64 `json:"cdate" gorm:"->;<-:create;type:integer;not null"`
}

func (m *Acked) BeforeCreate(*gorm.DB) error {
	if m.CDate == 0 {
		m.CDate = nowMicros()
	}
	return nil
}

// Indexes:
//   - PRIMARY KEY (document_id): one association per commit document and
//     commit-log foreign-key target; used by record.Repository.DeleteAssociation.
//   - idx_associations_target_id (target_id): association lookup by target record
//     key; used by record.Repository.GetAssociatedRecords.
//   - idx_associations_target_schema_variant_author
//     (target_id, schema, variant, author): target lookup with optional filters;
//     used by record.Repository.GetAssociatedRecords.
//   - idx_associations_target_schema_variant_created_at
//     (target_id, schema, variant, created_at): a target's counts by schema and
//     variant and each variant's earliest association, read from the index
//     alone; used by record.Repository.ComputeAssociationCounts.
//   - uni_associations_unique (unique): idempotency key for association writes;
//     used by record.Repository.CreateAssociation.
type Association struct {
	DocumentID string    `json:"id" gorm:"primaryKey;type:text"`
	Document   CommitLog `json:"-" gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`

	TargetID int64     `json:"targetID" gorm:"type:integer;not null;index:idx_associations_target_id;index:idx_associations_target_schema_variant_author,priority:1;index:idx_associations_target_schema_variant_created_at,priority:1"`
	Target   RecordKey `json:"-" gorm:"foreignKey:TargetID;references:ID;constraint:OnDelete:CASCADE;"`

	Owner  string `json:"owner" gorm:"type:text"`
	Author string `json:"author" gorm:"type:text;index:idx_associations_target_schema_variant_author,priority:4"`

	Schema  string  `json:"schema" gorm:"type:text;index:idx_associations_target_schema_variant_author,priority:2;index:idx_associations_target_schema_variant_created_at,priority:2"`
	Variant *string `json:"variant" gorm:"type:text;index:idx_associations_target_schema_variant_author,priority:3;index:idx_associations_target_schema_variant_created_at,priority:3"`
	Unique  string  `json:"unique" gorm:"type:text;not null;unique"`

	CreatedAt int64 `json:"createdAt" gorm:"type:integer;not null;index:idx_associations_target_schema_variant_created_at,priority:4"` // user-provided creation time
	CDate     int64 `json:"cdate" gorm:"->;<-:create;type:integer;not null"`
}

func (m *Association) BeforeCreate(*gorm.DB) error {
	if m.CDate == 0 {
		m.CDate = nowMicros()
	}
	return nil
}

// Indexes:
//   - PRIMARY KEY (id): FQDN lookup and upsert target; used by
//     ServerRepository.GetAndCacheByFQDN and cache writes.
//   - idx_servers_cs_id (cs_id): CSID cache lookup; used by
//     ServerRepository.GetAndCacheByCSID.
type Server struct {
	ID          string `json:"fqdn" gorm:"type:text;primaryKey"` // FQDN
	CSID        string `json:"csid" gorm:"type:text;index:idx_servers_cs_id"`
	Tag         string `json:"tag" gorm:"type:text"`
	Layer       string `json:"layer" gorm:"type:text"`
	WellKnown   string `json:"wellKnown" gorm:"type:text"`
	CDate       int64  `json:"cdate" gorm:"->;<-:create;type:integer;not null"`
	MDate       int64  `json:"mdate" gorm:"type:integer;not null"`
	LastScraped *int64 `json:"lastScraped" gorm:"type:integer"`
}

func (m *Server) BeforeCreate(*gorm.DB) error {
	now := nowMicros()
	if m.CDate == 0 {
		m.CDate = now
	}
	m.MDate = now
	return nil
}

func (m *Server) BeforeUpdate(tx *gorm.DB) error {
	tx.Statement.SetColumn("m_date", nowMicros())
	return nil
}

// Indexes:
//   - PRIMARY KEY (id): ccid lookup and upsert target; used by
//     record.Repository.CreateEntity and GetEntityByCCID.
//   - idx_entities_alias (alias): alias lookup; used by
//     record.Repository.GetEntityByAlias.
type Entity struct {
	ID     string  `json:"ccid" gorm:"type:text;primaryKey"`
	Alias  *string `json:"alias,omitempty" gorm:"type:text;index:idx_entities_alias"`
	Domain string  `json:"domain" gorm:"type:text"`
	Tag    string  `json:"tag" gorm:"type:text"`

	DocumentID string    `json:"id" gorm:"type:text;not null"`
	Document   CommitLog `json:"-" gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`
	// document createdAt: the accept-if-newer key (CIP-3 §3.4). Postgres holds
	// the epoch on rows written before the column existed; 0 is the same
	// instant here, so any real document replaces them.
	CreatedAt int64 `json:"createdAt" gorm:"type:integer;not null;default:0"`

	CDate int64 `json:"cdate" gorm:"->;<-:create;type:integer;not null"`
	MDate int64 `json:"mdate" gorm:"type:integer;not null"`
}

func (m *Entity) BeforeCreate(*gorm.DB) error {
	now := nowMicros()
	if m.CDate == 0 {
		m.CDate = now
	}
	m.MDate = now
	return nil
}

func (m *Entity) BeforeUpdate(tx *gorm.DB) error {
	tx.Statement.SetColumn("m_date", nowMicros())
	return nil
}

// Indexes:
//   - PRIMARY KEY (id): entity metadata lookup and upsert target; used by
//     EntityMetaRepository.SaveMeta, GetMeta, UpdateMetaInfo and DeleteMeta.
type EntityMeta struct {
	ID      string  `json:"ccid" gorm:"type:text;primaryKey"`
	Inviter *string `json:"inviter" gorm:"type:text"`
	// JSON; "null" when nothing was stored (Postgres' default:'null')
	Info  string `json:"info" gorm:"type:text;not null"`
	CDate int64  `json:"cdate" gorm:"->;<-:create;type:integer;not null"`
	MDate int64  `json:"mdate" gorm:"type:integer;not null"`
}

func (m *EntityMeta) BeforeCreate(*gorm.DB) error {
	now := nowMicros()
	if m.CDate == 0 {
		m.CDate = now
	}
	m.MDate = now
	if m.Info == "" {
		m.Info = "null"
	}
	return nil
}

func (m *EntityMeta) BeforeUpdate(tx *gorm.DB) error {
	tx.Statement.SetColumn("m_date", nowMicros())
	return nil
}

// Indexes:
//   - PRIMARY KEY (id): report identity. Abuse reports are currently append-only;
//     AbuseRepository.CreateAbuseReport does not issue read queries.
type AbuseReport struct {
	ID        int64  `json:"id" gorm:"primaryKey;autoIncrement"`
	IP        string `json:"ip" gorm:"type:text"`
	Reporter  string `json:"reporter" gorm:"type:text"`
	TargetURI string `json:"target" gorm:"type:text"`
	Body      string `json:"body" gorm:"type:text"`
	CDate     int64  `json:"cdate" gorm:"->;<-:create;type:integer;not null"`
}

func (m *AbuseReport) BeforeCreate(*gorm.DB) error {
	if m.CDate == 0 {
		m.CDate = nowMicros()
	}
	return nil
}

// Indexes:
//   - PRIMARY KEY (vendor_id, owner): one subscription per vendor and owner;
//     used by NotificationRepository.Subscribe/Get/Delete (List scans the table).
type Subscription struct {
	VendorID     string      `json:"vendorID" gorm:"primaryKey;type:text"`
	Owner        string      `json:"owner" gorm:"primaryKey;type:text"`
	Schemas      JSONStrings `json:"schemas" gorm:"type:text;not null"`
	Prefixes     JSONStrings `json:"prefixes" gorm:"type:text;not null"`
	Subscription string      `json:"subscription" gorm:"type:text"`
	CDate        int64       `json:"cdate" gorm:"type:integer;not null"`
	MDate        int64       `json:"mdate" gorm:"type:integer;not null"`
}

func (m *Subscription) BeforeSave(*gorm.DB) error {
	now := nowMicros()
	if m.CDate == 0 {
		m.CDate = now
	}
	if m.MDate == 0 {
		m.MDate = now
	}
	return nil
}
