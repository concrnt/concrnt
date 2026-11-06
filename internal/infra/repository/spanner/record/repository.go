// Package record is the Spanner implementation of record.Repository, file
// for file and method for method the same as the Postgres and SQLite ones.
// Writes are DML statements inside a ReadWriteTransaction, so a method sees
// what earlier statements of the same transaction wrote (what the
// conditional upserts and placeholder creation rely on); reads outside a
// transaction are single-use strong reads. Spanner retries an ABORTED
// transaction itself, which is RunInTx's contention retry.
package record

import (
	"context"
	"errors"

	"cloud.google.com/go/spanner"
	"go.opentelemetry.io/otel"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"

	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

type Repository struct {
	client *spanner.Client
}

func New(client *spanner.Client) ucrecord.Repository {
	return &Repository{client: client}
}

var tracer = otel.Tracer("repository")

type recordTx struct {
	tx *spanner.ReadWriteTransaction
}

func (*recordTx) IsRepositoryTx() {}

// RunInTx runs fn in a read-write transaction. The client re-runs the
// closure on ABORTED (lock contention); any other error fn returns rolls
// the transaction back and is returned as is.
func (r *Repository) RunInTx(ctx context.Context, fn func(tx ucrecord.RepositoryTx) error) error {
	_, err := r.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		return fn(&recordTx{tx: tx})
	})
	return err
}

func getRecordTx(tx ucrecord.RepositoryTx) (*spanner.ReadWriteTransaction, error) {
	recordTx, ok := tx.(*recordTx)
	if !ok || recordTx == nil || recordTx.tx == nil {
		return nil, errors.New("invalid record repository transaction")
	}
	return recordTx.tx, nil
}

// reader is what a single-use read-only transaction and a read-write
// transaction have in common.
type reader interface {
	Query(ctx context.Context, stmt spanner.Statement) *spanner.RowIterator
	ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error)
}

func isNotFound(err error) bool {
	return spanner.ErrCode(err) == codes.NotFound
}

// queryAll runs stmt and decodes every row into a T by column name.
func queryAll[T any](ctx context.Context, q reader, stmt spanner.Statement) ([]T, error) {
	it := q.Query(ctx, stmt)
	defer it.Stop()
	var out []T
	for {
		row, err := it.Next()
		if err == iterator.Done {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		var v T
		if err := row.ToStruct(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
}

// queryOne runs stmt and decodes its first row; found is false when there
// is none.
func queryOne[T any](ctx context.Context, q reader, stmt spanner.Statement) (v T, found bool, err error) {
	it := q.Query(ctx, stmt)
	defer it.Stop()
	row, err := it.Next()
	if err == iterator.Done {
		return v, false, nil
	}
	if err != nil {
		return v, false, err
	}
	return v, true, row.ToStruct(&v)
}

// scalar runs a single-column, single-row statement (a count).
func scalar[T any](ctx context.Context, q reader, stmt spanner.Statement) (T, error) {
	var v T
	it := q.Query(ctx, stmt)
	defer it.Stop()
	row, err := it.Next()
	if err != nil {
		return v, err
	}
	return v, row.Column(0, &v)
}
