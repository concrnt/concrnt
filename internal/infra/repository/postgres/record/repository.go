package record

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
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
// Dependents (CONTRACT.md C11) are removed by ON DELETE CASCADE inside the
// transaction that removes what they hang off: an association references
// the row of its target key, and a key recreated later is a new row, so it
// starts without any. Nothing is left for Cleanup.
type Repository struct {
	db *gorm.DB
	// txSlots bounds the Apply* transactions open at once, nil when the
	// pool is unbounded. A transaction keeps its connection for several
	// round trips and a read for one, and the pool gives a freed
	// connection to any waiter: without the bound a burst of commits
	// takes every connection and each read waits among them.
	txSlots chan struct{}
}

// New builds the repository over db. Half of a bounded pool is the most
// its transactions take at once; the rest stays with the reads.
func New(db *gorm.DB) ucrecord.Repository {
	r := &Repository{db: db}
	if sqlDB, err := db.DB(); err == nil {
		if maxOpen := sqlDB.Stats().MaxOpenConnections; maxOpen > 1 {
			r.txSlots = make(chan struct{}, maxOpen/2)
		}
	}
	return r
}

var tracer = otel.Tracer("repository")

var txSlotWait = promauto.NewHistogram(prometheus.HistogramOpts{
	Namespace: "concrnt",
	Subsystem: "record",
	Name:      "tx_slot_wait_seconds",
	Help:      "Time an Apply* transaction waited for one of the transaction slots (half the connection pool).",
	Buckets:   []float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
})

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
	retryable := func(err error) bool {
		var pgErr *pgconn.PgError
		return errors.Is(err, errRetryTx) || (errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01"))
	}
	status := ucrecord.Applied
	err := repoutil.RetryTx(ctx, retryable, func() error {
		// the wait for a slot holds no connection
		if r.txSlots != nil {
			start := time.Now()
			select {
			case r.txSlots <- struct{}{}:
				txSlotWait.Observe(time.Since(start).Seconds())
				defer func() { <-r.txSlots }()
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var err error
			status, err = fn(tx)
			if err == nil && status != ucrecord.Applied {
				return errNotStored
			}
			return err
		})
	})
	if errors.Is(err, errNotStored) {
		return status, nil
	}
	return status, err
}

// Cleanup has nothing to do: ApplyDelete removes dependents itself (ON
// DELETE CASCADE) and issues no token.
func (r *Repository) Cleanup(ctx context.Context, token string) (string, error) {
	return "", nil
}
