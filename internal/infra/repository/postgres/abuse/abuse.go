package abuse

import (
	"context"

	"go.opentelemetry.io/otel"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt"
)

var tracer = otel.Tracer("repository")

type Repository struct {
	db *gorm.DB
}

func New(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Migrate() error {
	return r.db.AutoMigrate(&AbuseReport{})
}

func (r *Repository) CreateAbuseReport(ctx context.Context, report *concrnt.AbuseReport, reporter, reposterIP string) error {
	ctx, span := tracer.Start(ctx, "Repository.Abuse.CreateAbuseReport")
	defer span.End()

	abuseReport := &AbuseReport{
		IP:        reposterIP,
		Reporter:  reporter,
		TargetURI: report.TargetURI,
		Body:      report.Body,
	}

	if err := r.db.WithContext(ctx).Create(abuseReport).Error; err != nil {
		return err
	}
	return nil
}
