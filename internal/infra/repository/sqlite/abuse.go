package sqlite

import (
	"context"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/usecase/abuse"
)

type AbuseRepository struct {
	db *DB
}

func NewAbuseRepository(db *DB) abuse.Repository {
	return &AbuseRepository{db: db}
}

func (r *AbuseRepository) CreateAbuseReport(ctx context.Context, report *concrnt.AbuseReport, reporter, reposterIP string) error {
	ctx, span := tracer.Start(ctx, "AbuseRepository.CreateAbuseReport")
	defer span.End()

	_, err := r.db.ExecContext(ctx, "INSERT INTO abuse_reports (ip, reporter, target_uri, body, c_date) VALUES (?, ?, ?, ?, ?)",
		reposterIP, reporter, report.TargetURI, report.Body, nowMicros())
	return err
}
