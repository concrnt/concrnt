package record

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

var tracer = otel.Tracer("repository")

type Repository struct {
	db *gorm.DB
}

type recordTx struct {
	tx   *gorm.DB
	done bool // slot released (Commit or Rollback ran)
}

// MaxConcurrentTx caps the transactions one process runs at once. A
// transaction pins one pooled connection and the reads issued inside it go
// through r.db, i.e. a second connection of the same pool; with
// MaxConcurrentTx >= pool size every transaction can be left waiting for a
// connection another transaction holds. Half the pool (database.MaxOpenConns)
// keeps one free connection per transaction.
const MaxConcurrentTx = 8

var txSlots = make(chan struct{}, MaxConcurrentTx)

func (tx *recordTx) release() {
	if !tx.done {
		tx.done = true
		<-txSlots
	}
}

func New(db *gorm.DB) ucrecord.Repository {
	return &Repository{db: db}
}

func (r *Repository) BeginTx(ctx context.Context) (ucrecord.RepositoryTx, error) {
	select {
	case txSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	tx := r.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		<-txSlots
		return nil, tx.Error
	}
	return &recordTx{tx: tx}, nil
}

func (tx *recordTx) Commit(ctx context.Context) error {
	defer tx.release()
	return tx.tx.WithContext(ctx).Commit().Error
}

func (tx *recordTx) Rollback(ctx context.Context) error {
	defer tx.release()
	return tx.tx.WithContext(ctx).Rollback().Error
}

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
		select {
		case txSlots <- struct{}{}:
			defer func() { <-txSlots }()
		case <-ctx.Done():
			return ctx.Err()
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

func getRecordTx(ctx context.Context, tx ucrecord.RepositoryTx) (*gorm.DB, error) {
	recordTx, ok := tx.(*recordTx)
	if !ok || recordTx == nil || recordTx.tx == nil {
		return nil, errors.New("invalid record repository transaction")
	}
	return recordTx.tx.WithContext(ctx), nil
}
