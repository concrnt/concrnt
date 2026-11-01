package postgres

import (
	"time"

	"github.com/lib/pq"
)

// The gorm models of the Postgres backend. AutoMigrate derives the tables,
// constraints and indexes from the tags at every start (migrations.go); the
// access path each index serves is listed in CONTRACT.md.

type CommitLog struct {
	ID          string    `json:"id" gorm:"primaryKey;type:text;index:idx_commit_logs_c_date_id,priority:2"`
	IP          string    `json:"ip" gorm:"type:text"`
	Document    string    `json:"document" gorm:"type:text"`
	Proof       string    `json:"proof" gorm:"type:text"`
	Owner       string    `json:"owner" gorm:"type:text;index:idx_commit_logs_owner_c_date,priority:1"`
	GcCandidate bool      `json:"gcCandidate" gorm:"type:boolean;not null;default:false;index:idx_commit_logs_gc_candidate"`
	CDate       time.Time `json:"cdate" gorm:"type:timestamp with time zone;not null;default:clock_timestamp();index:idx_commit_logs_owner_c_date,priority:2;index:idx_commit_logs_c_date_id,priority:1"`
}

type RecordKey struct {
	ID       int64  `json:"id" gorm:"primaryKey;autoIncrement"`
	ParentID *int64 `json:"parentID" gorm:"index:idx_record_keys_parent_id_record_id,priority:1;index:idx_record_keys_parent_created_at_record_id,priority:1,where:parent_id IS NOT NULL AND record_created_at IS NOT NULL;index:idx_record_keys_parent_id_uri,priority:1"`
	// key order is byte order (CONTRACT.md "Key order"), hence COLLATE "C"
	URI             string     `json:"uri" gorm:"type:text;unique;index:idx_record_keys_parent_id_uri,priority:2,collate:\"C\""`
	RecordID        *string    `json:"recordID" gorm:"type:text;uniqueIndex;index:idx_record_keys_parent_id_record_id,priority:2;index:idx_record_keys_parent_created_at_record_id,priority:3"`
	Record          Record     `json:"record" gorm:"foreignKey:RecordID;references:DocumentID;constraint:OnDelete:CASCADE;"`
	RecordCreatedAt *time.Time `json:"recordCreatedAt,omitempty" gorm:"type:timestamp with time zone;index:idx_record_keys_parent_created_at_record_id,priority:2,sort:desc"`
	// Redirect is the record's redirect, denormalised so chunkline bodies and
	// removal lookups never join records
	Redirect      *string `json:"redirect,omitempty" gorm:"type:text"`
	CleanOnUpdate bool    `json:"cleanOnUpdate" gorm:"type:boolean;not null;default:false"`
}

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

type Ack struct {
	From   string `json:"from" gorm:"type:text;index:idx_ack_from_to_schema,unique,priority:1;index:idx_acks_from_valid_created_at,priority:1"`
	To     string `json:"to" gorm:"type:text;index:idx_ack_from_to_schema,unique,priority:2;index:idx_acks_to_valid_created_at,priority:1"`
	Schema string `json:"schema" gorm:"type:text;index:idx_ack_from_to_schema,unique,priority:3;index:idx_acks_schema_valid_created_at,priority:1"`

	DocumentID string    `json:"id" gorm:"primaryKey;type:text"`
	Document   CommitLog `json:"-" gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`

	Valid bool `json:"valid" gorm:"type:boolean;not null;default:true;index:idx_acks_from_valid_created_at,priority:2;index:idx_acks_to_valid_created_at,priority:2;index:idx_acks_schema_valid_created_at,priority:2"`

	CreatedAt time.Time `json:"createdAt" gorm:"type:timestamp with time zone;not null;index:idx_acks_from_valid_created_at,priority:3;index:idx_acks_to_valid_created_at,priority:3;index:idx_acks_schema_valid_created_at,priority:3"` // user-provided creation time
	CDate     time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
}

type Acked struct {
	From   string `json:"from" gorm:"type:text;index:idx_acked_from_to_schema,unique,priority:1;index:idx_acked_from_valid_created_at,priority:1"`
	To     string `json:"to" gorm:"type:text;index:idx_acked_from_to_schema,unique,priority:2;index:idx_acked_to_valid_created_at,priority:1"`
	Schema string `json:"schema" gorm:"type:text;index:idx_acked_from_to_schema,unique,priority:3;index:idx_acked_schema_valid_created_at,priority:1"`

	DocumentID string    `json:"id" gorm:"primaryKey;type:text"`
	Document   CommitLog `json:"-" gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`

	Valid bool `json:"valid" gorm:"type:boolean;not null;default:true;index:idx_acked_from_valid_created_at,priority:2;index:idx_acked_to_valid_created_at,priority:2;index:idx_acked_schema_valid_created_at,priority:2"`

	CreatedAt time.Time `json:"createdAt" gorm:"type:timestamp with time zone;not null;index:idx_acked_from_valid_created_at,priority:3;index:idx_acked_to_valid_created_at,priority:3;index:idx_acked_schema_valid_created_at,priority:3"` // user-provided creation time
	CDate     time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
}

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

type Entity struct {
	ID     string  `json:"ccid" gorm:"type:text;primaryKey"`
	Alias  *string `json:"alias,omitempty" gorm:"type:text;index:idx_entities_alias"`
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

type EntityMeta struct {
	ID      string    `json:"ccid" gorm:"type:text;primaryKey"`
	Inviter *string   `json:"inviter" gorm:"type:text"`
	Info    string    `json:"info" gorm:"type:jsonb;default:'null'"`
	CDate   time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
	MDate   time.Time `json:"mdate" gorm:"autoUpdateTime"`
}

type AbuseReport struct {
	ID        int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	IP        string    `json:"ip" gorm:"type:text"`
	Reporter  string    `json:"reporter" gorm:"type:text"`
	TargetURI string    `json:"target" gorm:"type:text"`
	Body      string    `json:"body" gorm:"type:text"`
	CDate     time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
}

type Subscription struct {
	VendorID     string         `json:"vendorID" gorm:"primaryKey;type:text"`
	Owner        string         `json:"owner" gorm:"primaryKey;type:text"`
	Schemas      pq.StringArray `json:"schemas" gorm:"type:text[]"`
	Prefixes     pq.StringArray `json:"prefixes" gorm:"type:text[]"`
	Subscription string         `json:"subscription" gorm:"type:text"`
	CDate        time.Time      `json:"cdate" gorm:"type:timestamp with time zone;not null;default:clock_timestamp()"`
	MDate        time.Time      `json:"mdate" gorm:"autoUpdateTime"`
}
