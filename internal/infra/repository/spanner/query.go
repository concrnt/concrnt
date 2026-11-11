package spanner

import (
	"context"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// Reader is what a single-use read-only transaction and a read-write
// transaction have in common.
type Reader interface {
	Query(ctx context.Context, stmt spanner.Statement) *spanner.RowIterator
}

// QueryAll runs stmt and decodes every row into a T by column name.
func QueryAll[T any](ctx context.Context, q Reader, stmt spanner.Statement) ([]T, error) {
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

// QueryOne runs stmt and decodes its first row; found is false when there
// is none.
func QueryOne[T any](ctx context.Context, q Reader, stmt spanner.Statement) (v T, found bool, err error) {
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
