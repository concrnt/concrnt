package record

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"

	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
)

// inspector is the Firestore repotest.Inspector, for tests only.
type inspector struct {
	client *firestore.Client
}

func newInspector(client *firestore.Client) repotest.Inspector {
	return &inspector{client: client}
}

func (i *inspector) SetCommitCDate(ctx context.Context, id string, cDate time.Time) error {
	_, err := i.client.Collection(fs.ColCommits).Doc(id).Update(ctx, []firestore.Update{{Path: "cDate", Value: cDate}})
	return err
}

func (i *inspector) AbuseReportCount(ctx context.Context) (int64, error) {
	res, err := i.client.Collection(fs.ColAbuseReports).NewAggregationQuery().WithCount("n").Get(ctx)
	if err != nil {
		return 0, err
	}
	return aggregationCount(res)
}
