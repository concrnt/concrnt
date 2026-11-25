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
	// txSlots caps the transactions run at once. A transaction pins one
	// pooled connection and the reads issued inside it go through r.db, i.e.
	// a second connection of the same pool; with as many transactions as
	// connections every transaction can be left waiting for a connection
	// another transaction holds. Half the pool keeps one free connection per
	// transaction.
	txSlots chan struct{}
}

type recordTx struct {
	tx    *gorm.DB
	slots chan struct{}
	done  bool // slot released (Commit or Rollback ran)
}

func (tx *recordTx) release() {
	if !tx.done {
		tx.done = true
		<-tx.slots
	}
}

var _ ucrecord.Repository = (*Repository)(nil)

// New takes a db whose pool is bounded (database.NewPostgres sets it) and
// lets half of that pool run transactions.
func New(db *gorm.DB) *Repository {
	sqlDB, err := db.DB()
	if err != nil {
		panic(err)
	}
	return &Repository{db: db, txSlots: make(chan struct{}, sqlDB.Stats().MaxOpenConnections/2)}
}

func (r *Repository) Migrate() error {
	return r.db.AutoMigrate(
		&CommitLog{},
		&RecordKey{},
		&Association{},
		&Ack{},
		&Acked{},
		&Entity{},
	)
}

func (r *Repository) BeginTx(ctx context.Context) (ucrecord.RepositoryTx, error) {
	select {
	case r.txSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	tx := r.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		<-r.txSlots
		return nil, tx.Error
	}
	return &recordTx{tx: tx, slots: r.txSlots}, nil
}

func (tx *recordTx) Commit(ctx context.Context) error {
	defer tx.release()
	return tx.tx.WithContext(ctx).Commit().Error
}

func (tx *recordTx) Rollback(ctx context.Context) error {
	defer tx.release()
	return tx.tx.WithContext(ctx).Rollback().Error
}

// getRecordTx unwraps the transaction handle. The statements run on it go
// through gorm.G, which binds the context per call.
func getRecordTx(tx ucrecord.RepositoryTx) (*gorm.DB, error) {
	recordTx, ok := tx.(*recordTx)
	if !ok || recordTx == nil || recordTx.tx == nil {
		return nil, errors.New("invalid record repository transaction")
	}
	return recordTx.tx, nil
}
