package record

import (
	"context"
	"time"

	"gorm.io/gorm"

	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
)

// inspector is the SQLite repotest.Inspector, for tests only.
type inspector struct {
	db *gorm.DB
}

func newInspector(db *gorm.DB) repotest.Inspector {
	return &inspector{db: db}
}

func (i *inspector) SetCommitCDate(ctx context.Context, id string, cDate time.Time) error {
	return i.db.WithContext(ctx).Model(&sqlitedb.CommitLog{}).Where("id = ?", id).Update("c_date", sqlitedb.Micros(cDate)).Error
}

func (i *inspector) AbuseReportCount(ctx context.Context) (int64, error) {
	var count int64
	err := i.db.WithContext(ctx).Model(&sqlitedb.AbuseReport{}).Count(&count).Error
	return count, err
}
