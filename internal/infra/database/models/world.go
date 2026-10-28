package models

import (
	"time"

	"github.com/lib/pq"
)

type Subscription struct {
	VendorID     string         `json:"vendorID" gorm:"primaryKey;type:text"`
	Owner        string         `json:"owner" gorm:"primaryKey;type:text"`
	Schemas      pq.StringArray `json:"schemas" gorm:"type:text[]"`
	Prefixes     pq.StringArray `json:"prefixes" gorm:"type:text[]"`
	Subscription string         `json:"subscription" gorm:"type:text"`
	CDate        time.Time      `json:"cdate" gorm:"type:timestamp with time zone;not null;default:clock_timestamp()"`
	MDate        time.Time      `json:"mdate" gorm:"autoUpdateTime"`
}
