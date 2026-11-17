package server

import (
	"time"
)

// Indexes:
//   - PRIMARY KEY (id): FQDN lookup and upsert target; used by
//     pgserver.Repository.GetAndCacheByFQDN and cache writes.
//   - idx_servers_cs_id (cs_id): CSID cache lookup; used by
//     pgserver.Repository.GetAndCacheByCSID.
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
