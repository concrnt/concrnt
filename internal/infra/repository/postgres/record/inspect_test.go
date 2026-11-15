package record

import (
	"context"
	"time"

	"gorm.io/gorm"

	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
)

// inspector is the Postgres repotest.Inspector, for tests only.
type inspector struct {
	db *gorm.DB
}

func newInspector(db *gorm.DB) repotest.Inspector {
	return &inspector{db: db}
}

func (i *inspector) SetCommitCDate(ctx context.Context, id string, cDate time.Time) error {
	return i.db.WithContext(ctx).Model(&postgres.CommitLog{}).Where("id = ?", id).Update("c_date", cDate).Error
}

func (i *inspector) AbuseReportCount(ctx context.Context) (int64, error) {
	var count int64
	err := i.db.WithContext(ctx).Model(&postgres.AbuseReport{}).Count(&count).Error
	return count, err
}
