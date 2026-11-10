package record

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"
	"gorm.io/gorm"

	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// Repository is the Postgres implementation of the usecase record.Repository: commit log,
// records and keys, associations, acks, entities with their registration
// state, and the chunkline views, all over one connection pool.
//
// Every Apply* method is one transaction on one pooled connection, and the
// reads the usecase issues happen before it, so a commit never holds a
// connection while waiting for another.
//
// Dependents (CONTRACT.md P4) are removed by ON DELETE CASCADE inside the
// transaction that removes what they hang off: an association references
// the row of its target key, and a key recreated later is a new row, so it
// starts without any. Nothing is left for Cleanup.
type Repository struct {
	db *gorm.DB
}

func New(db *gorm.DB) ucrecord.Repository {
	return &Repository{db: db}
}

var tracer = otel.Tracer("repository")

const (
	txMaxAttempts  = 8
	txBackoffStart = 10 * time.Millisecond
	txBackoffMax   = 500 * time.Millisecond
)

// errNotStored rolls back a transaction whose write ended Stale or
// Duplicate: neither leaves a trace, not even the commit log entry.
var errNotStored = errors.New("not stored")

// errRetryTx restarts a transaction whose view of a row was overtaken by a
// concurrent writer before it could lock it.
var errRetryTx = errors.New("retry transaction")

// apply runs fn as one transaction and returns its status. fn is re-run
// from scratch on serialization_failure / deadlock_detected (the two
// outcomes Postgres asks the client to retry) and on errRetryTx, so it must
// reset whatever it reports through captured variables at its start.
// Anything but Applied rolls the transaction back.
func (r *Repository) apply(ctx context.Context, fn func(tx *gorm.DB) (ucrecord.ApplyStatus, error)) (ucrecord.ApplyStatus, error) {
	backoff := txBackoffStart
	for attempt := 1; ; attempt++ {
		status := ucrecord.Applied
		err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var err error
			status, err = fn(tx)
			if err == nil && status != ucrecord.Applied {
				return errNotStored
			}
			return err
		})
		if err == nil || errors.Is(err, errNotStored) {
			return status, nil
		}
		var pgErr *pgconn.PgError
		retryable := errors.Is(err, errRetryTx) || (errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01"))
		if !retryable || attempt >= txMaxAttempts {
			return status, err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return status, errors.Join(ctx.Err(), err)
		case <-timer.C:
		}
		backoff = min(backoff*2, txBackoffMax)
	}
}

// Cleanup has nothing to do: ApplyDelete removes dependents itself (ON
// DELETE CASCADE) and issues no token.
func (r *Repository) Cleanup(ctx context.Context, token string) (string, error) {
	return "", nil
}
