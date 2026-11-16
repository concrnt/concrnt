package spanner

import (
	"context"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/usecase/abuse"
)

type AbuseRepository struct {
	client *spanner.Client
}

func NewAbuseRepository(client *spanner.Client) abuse.Repository {
	return &AbuseRepository{client: client}
}

// CreateAbuseReport appends a report under a random id (C3: no sequence).
func (r *AbuseRepository) CreateAbuseReport(ctx context.Context, report *concrnt.AbuseReport, reporter, reposterIP string) error {
	ctx, span := tracer.Start(ctx, "AbuseRepository.CreateAbuseReport")
	defer span.End()

	_, err := r.client.Apply(ctx, []*spanner.Mutation{spanner.Insert("abuse_reports",
		[]string{"id", "ip", "reporter", "target_uri", "body", "c_date"},
		[]any{uuid.NewString(), reposterIP, reporter, report.TargetURI, report.Body, now()})})
	if err != nil {
		span.RecordError(err)
	}
	return err
}
