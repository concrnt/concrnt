package firestore

import (
	"context"

	"cloud.google.com/go/firestore"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/usecase/abuse"
)

type AbuseRepository struct {
	client *firestore.Client
}

func NewAbuseRepository(client *firestore.Client) abuse.Repository {
	return &AbuseRepository{client: client}
}

func (r *AbuseRepository) CreateAbuseReport(ctx context.Context, report *concrnt.AbuseReport, reporter, reposterIP string) error {
	ctx, span := tracer.Start(ctx, "AbuseRepository.CreateAbuseReport")
	defer span.End()

	_, err := r.client.Collection(colAbuseReports).NewDoc().Set(ctx, map[string]any{
		"ip":        reposterIP,
		"reporter":  reporter,
		"targetURI": report.TargetURI,
		"body":      report.Body,
		"cDate":     firestore.ServerTimestamp,
	})
	if err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}
