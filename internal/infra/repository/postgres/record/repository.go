package record

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"gorm.io/gorm"

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

func getRecordTx(ctx context.Context, tx ucrecord.RepositoryTx) (*gorm.DB, error) {
	recordTx, ok := tx.(*recordTx)
	if !ok || recordTx == nil || recordTx.tx == nil {
		return nil, errors.New("invalid record repository transaction")
	}
	return recordTx.tx.WithContext(ctx), nil
}
