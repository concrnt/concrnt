// Package record is the Spanner implementation of record.Repository.
//
// Every Apply* method is one ReadWriteTransaction with the same shape: one
// statement reads everything the decision depends on (the commit id, the
// rows named by the arguments), the method decides, and the writes are
// buffered as mutations that go out with the commit (ApplyRecord replacing
// a version it forgets is the one that needs DML, see there). The reads
// take locks, so two transactions that decide on the same rows cannot both
// commit: one is ABORTED and re-run by the client from its read, which is
// the contract's contention retry. Nothing is written before the decision,
// so a Stale or Duplicate transaction has nothing to leave behind. Reads
// outside a transaction are single-use strong reads.
//
// Dependents (CONTRACT.md C11): a key row carries the generation (gen) of
// its incarnation and an association the target_gen it was created on.
// Removing a key deletes the key row only, which makes its associations
// unreadable at once (no key row with their generation is left); their
// rows go later, a page per Cleanup call. A key's associations therefore
// never count against the mutation limit of the commit that removes it.
package record

import (
	"context"
	"errors"

	"cloud.google.com/go/spanner"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"

	spannerdb "github.com/concrnt/concrnt/internal/infra/repository/spanner"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

type Repository struct {
	client *spanner.Client
}

func New(client *spanner.Client) ucrecord.Repository {
	return &Repository{client: client}
}

var tracer = otel.Tracer("repository")

// mutationCountKey is the span attribute carrying the mutation count
// Spanner reports for a commit (rows, index entries and cascaded rows, the
// number its 80,000 per commit limit is about).
const mutationCountKey = attribute.Key("spanner.mutation_count")

// errNotStored rolls back a transaction whose write ended Stale or
// Duplicate: neither leaves a trace, not even the commit log entry.
var errNotStored = errors.New("not stored")

// write runs fn as one read-write transaction. The client re-runs fn from
// scratch when the transaction is ABORTED (lock contention), so fn must
// reset whatever it reports through captured variables at its start. The
// commit's mutation count goes on the span of ctx.
func (r *Repository) write(ctx context.Context, fn func(ctx context.Context, tx *spanner.ReadWriteTransaction) error) error {
	resp, err := r.client.ReadWriteTransactionWithOptions(ctx, fn, spanner.TransactionOptions{
		CommitOptions: spanner.CommitOptions{ReturnCommitStats: true},
	})
	if err != nil {
		return err
	}
	if resp.CommitStats != nil {
		trace.SpanFromContext(ctx).SetAttributes(mutationCountKey.Int64(resp.CommitStats.MutationCount))
	}
	return nil
}

// apply runs fn as the one transaction of an Apply* method and returns its
// status. fn reads, decides, and buffers its writes only when the status is
// Applied; anything else rolls the transaction back.
func (r *Repository) apply(ctx context.Context, fn func(ctx context.Context, tx *spanner.ReadWriteTransaction) (ucrecord.ApplyStatus, error)) (ucrecord.ApplyStatus, error) {
	status := ucrecord.Applied
	err := r.write(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var err error
		status, err = fn(ctx, tx)
		if err == nil && status != ucrecord.Applied {
			return errNotStored
		}
		return err
	})
	if err == nil || errors.Is(err, errNotStored) {
		return status, nil
	}
	return status, err
}

// readSet runs the single statement an Apply* method reads with. Such a
// statement is built from scalar and ARRAY subqueries, so it always returns
// exactly one row.
func readSet[T any](ctx context.Context, tx *spanner.ReadWriteTransaction, stmt spanner.Statement) (T, error) {
	v, found, err := spannerdb.QueryOne[T](ctx, tx, stmt)
	if err == nil && !found {
		err = errors.New("the read statement returned no row")
	}
	return v, err
}

func isNotFound(err error) bool {
	return spanner.ErrCode(err) == codes.NotFound
}

// scalar runs a single-column, single-row statement (a count).
func scalar[T any](ctx context.Context, q spannerdb.Reader, stmt spanner.Statement) (T, error) {
	var v T
	it := q.Query(ctx, stmt)
	defer it.Stop()
	row, err := it.Next()
	if err != nil {
		return v, err
	}
	return v, row.Column(0, &v)
}
