package models

import (
	"time"

	"github.com/lib/pq"
)

// Indexes: see shapes.All (internal/infra/repository/shapes); unique
// constraints and primary keys stay on the tags.
type CommitLog struct {
	ID          string    `json:"id" gorm:"primaryKey;type:text"`
	IP          string    `json:"ip" gorm:"type:text"`
	Document    string    `json:"document" gorm:"type:text"`
	Proof       string    `json:"proof" gorm:"type:text"`
	Owner       string    `json:"owner" gorm:"type:text"`
	GcCandidate bool      `json:"gcCandidate" gorm:"type:boolean;not null;default:false"`
	CDate       time.Time `json:"cdate" gorm:"type:timestamp with time zone;not null;default:clock_timestamp()"`
}

// Indexes: see shapes.All (internal/infra/repository/shapes); unique
// constraints and primary keys stay on the tags.
type RecordKey struct {
	ID              int64      `json:"id" gorm:"primaryKey;autoIncrement"`
	ParentID        *int64     `json:"parentID"`
	URI             string     `json:"uri" gorm:"type:text;unique"`
	RecordID        *string    `json:"recordID" gorm:"type:text;uniqueIndex"`
	Record          Record     `json:"record" gorm:"foreignKey:RecordID;references:DocumentID;constraint:OnDelete:CASCADE;"`
	RecordCreatedAt *time.Time `json:"recordCreatedAt,omitempty" gorm:"type:timestamp with time zone"`
	CleanOnUpdate   bool       `json:"cleanOnUpdate" gorm:"type:boolean;not null;default:false"`
}

// Indexes: see shapes.All (internal/infra/repository/shapes); unique
// constraints and primary keys stay on the tags.
type Record struct {
	DocumentID string    `json:"id" gorm:"primaryKey;type:text"`
	Document   CommitLog `json:"documnet" gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`
	Owner      string    `json:"owner" gorm:"type:text"`
	// document author; NULL only on rows written before the column existed
	// (backfilled by conctl op repair-record-authors)
	Author        string         `json:"author" gorm:"type:text"`
	Redirect      *string        `json:"redirect" gorm:"type:text"`
	Schema        string         `json:"schema" gorm:"type:text"`
	Policies      *string        `json:"policies" gorm:"type:text"`
	Distributions pq.StringArray `json:"distributions" gorm:"type:text[]"`
	// user-provided creation time
	CreatedAt time.Time `json:"createdAt" gorm:"type:timestamp with time zone;not null"`
	// record creation time in the system
	CDate time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
}

// Indexes: see shapes.All (internal/infra/repository/shapes); unique
// constraints and primary keys stay on the tags.
type Ack struct {
	From   string `json:"from" gorm:"type:text;index:idx_ack_from_to_schema,unique"`
	To     string `json:"to" gorm:"type:text;index:idx_ack_from_to_schema,unique"`
	Schema string `json:"schema" gorm:"type:text;index:idx_ack_from_to_schema,unique"`

	DocumentID string    `json:"id" gorm:"primaryKey;type:text"`
	Document   CommitLog `json:"-" gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`

	Valid bool `json:"valid" gorm:"type:boolean;not null;default:true"`

	CreatedAt time.Time `json:"createdAt" gorm:"type:timestamp with time zone;not null"` // user-provided creation time
	CDate     time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
}

// Indexes: see shapes.All (internal/infra/repository/shapes); unique
// constraints and primary keys stay on the tags.
type Acked struct {
	From   string `json:"from" gorm:"type:text;index:idx_acked_from_to_schema,unique"`
	To     string `json:"to" gorm:"type:text;index:idx_acked_from_to_schema,unique"`
	Schema string `json:"schema" gorm:"type:text;index:idx_acked_from_to_schema,unique"`

	DocumentID string    `json:"id" gorm:"primaryKey;type:text"`
	Document   CommitLog `json:"-" gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`

	Valid bool `json:"valid" gorm:"type:boolean;not null;default:true"`

	CreatedAt time.Time `json:"createdAt" gorm:"type:timestamp with time zone;not null"` // user-provided creation time
	CDate     time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
}

// Indexes: see shapes.All (internal/infra/repository/shapes); unique
// constraints and primary keys stay on the tags.
type Association struct {
	DocumentID string    `json:"id" gorm:"primaryKey;type:text"`
	Document   CommitLog `json:"-" gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`

	TargetID int64     `json:"targetID" gorm:"type:bigint"`
	Target   RecordKey `json:"-" gorm:"foreignKey:TargetID;references:ID;constraint:OnDelete:CASCADE;"`

	Owner  string `json:"owner" gorm:"type:text"`
	Author string `json:"author" gorm:"type:text"`

	Schema  string  `json:"schema" gorm:"type:text"`
	Variant *string `json:"variant" gorm:"type:text"`
	Unique  string  `json:"unique" gorm:"type:text;unique"`

	CreatedAt time.Time `json:"createdAt" gorm:"type:timestamp with time zone;not null"` // user-provided creation time
	CDate     time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
}

// Indexes: see shapes.All (internal/infra/repository/shapes); unique
// constraints and primary keys stay on the tags.
type Server struct {
	ID          string    `json:"fqdn" gorm:"type:text;primaryKey"` // FQDN
	CSID        string    `json:"csid" gorm:"type:text"`
	Tag         string    `json:"tag" gorm:"type:text"`
	Layer       string    `json:"layer" gorm:"type:text"`
	WellKnown   string    `json:"wellKnown" gorm:"type:jsonb"`
	CDate       time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
	MDate       time.Time `json:"mdate" gorm:"autoUpdateTime"`
	LastScraped time.Time `json:"lastScraped" gorm:"type:timestamp with time zone"`
}

// Indexes: see shapes.All (internal/infra/repository/shapes); unique
// constraints and primary keys stay on the tags.
type Entity struct {
	ID     string  `json:"ccid" gorm:"type:text;primaryKey"`
	Alias  *string `json:"alias,omitempty" gorm:"type:text"`
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

// Indexes: see shapes.All (internal/infra/repository/shapes); unique
// constraints and primary keys stay on the tags.
type EntityMeta struct {
	ID      string    `json:"ccid" gorm:"type:text;primaryKey"`
	Inviter *string   `json:"inviter" gorm:"type:text"`
	Info    string    `json:"info" gorm:"type:jsonb;default:'null'"`
	CDate   time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
	MDate   time.Time `json:"mdate" gorm:"autoUpdateTime"`
}

// Indexes: see shapes.All (internal/infra/repository/shapes); unique
// constraints and primary keys stay on the tags.
type AbuseReport struct {
	ID        int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	IP        string    `json:"ip" gorm:"type:text"`
	Reporter  string    `json:"reporter" gorm:"type:text"`
	TargetURI string    `json:"target" gorm:"type:text"`
	Body      string    `json:"body" gorm:"type:text"`
	CDate     time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
}
