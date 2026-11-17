package abuse

import (
	"context"

	"go.opentelemetry.io/otel"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/usecase/abuse"
)

var tracer = otel.Tracer("repository")

type AbuseRepository struct {
	db *gorm.DB
}

func NewAbuseRepository(db *gorm.DB) abuse.Repository {
	return &AbuseRepository{db: db}
}

func (r *AbuseRepository) CreateAbuseReport(ctx context.Context, report *concrnt.AbuseReport, reporter, reposterIP string) error {
	ctx, span := tracer.Start(ctx, "AbuseRepository.CreateAbuseReport")
	defer span.End()

	abuseReport := &models.AbuseReport{
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
