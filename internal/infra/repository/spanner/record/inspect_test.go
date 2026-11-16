package record

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
)

// inspector is the Spanner repotest.Inspector, for tests only.
type inspector struct {
	client *spanner.Client
}

func newInspector(client *spanner.Client) repotest.Inspector {
	return &inspector{client: client}
}

// SetCommitCDate writes an explicit (past) timestamp into the commit
// timestamp column, which allow_commit_timestamp permits.
func (i *inspector) SetCommitCDate(ctx context.Context, id string, cDate time.Time) error {
	_, err := i.client.Apply(ctx, []*spanner.Mutation{
		spanner.Update("commit_logs", []string{"id", "c_date"}, []any{id, cDate}),
	})
	return err
}

func (i *inspector) AbuseReportCount(ctx context.Context) (int64, error) {
	return scalar[int64](ctx, i.client.Single(), spanner.Statement{SQL: "SELECT COUNT(*) FROM abuse_reports"})
}
