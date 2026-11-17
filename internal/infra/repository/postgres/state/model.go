package state

import (
	"time"
)

type State struct {
	Name  string    `json:"name" gorm:"type:text;primaryKey"`
	Value string    `json:"value" gorm:"type:text"`
	CDate time.Time `json:"cdate" gorm:"->;<-:create;type:timestamp with time zone;not null;default:clock_timestamp()"`
	MDate time.Time `json:"mdate" gorm:"autoUpdateTime"`
}
