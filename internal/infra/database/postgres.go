package database

import (
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/XSAM/otelsql"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/plugin/opentelemetry/tracing"

	"github.com/concrnt/concrnt/internal/infra/database/models"
)

// MaxOpenConns is the size of the connection pool of one process.
const MaxOpenConns = 16

func NewPostgres(dsn string) (*gorm.DB, error) {

	gormLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             300 * time.Millisecond, // Slow SQL threshold
			LogLevel:                  logger.Warn,            // Log level
			IgnoreRecordNotFoundError: true,                   // Ignore ErrRecordNotFound error for logger
			Colorful:                  true,                   // Enable color
		},
	)

	sqlDB, err := otelsql.Open("pgx", dsn,
		otelsql.WithAttributes(semconv.DBSystemPostgreSQL),
		otelsql.WithSpanOptions(otelsql.SpanOptions{
			OmitConnResetSession: true,
			OmitConnPrepare:      true,
			OmitConnQuery:        true,
			OmitRows:             true,
			OmitConnectorConnect: true,
		}),
	)
	if err != nil {
		return nil, err
	}

	// DBStatsをメトリクスとして記録（コネクションプール待ちを可視化）
	if _, err = otelsql.RegisterDBStatsMetrics(sqlDB,
		otelsql.WithAttributes(semconv.DBSystemPostgreSQL),
	); err != nil {
		return nil, err
	}

	// A transaction holds one connection while the reads issued inside it
	// (through the repository's plain db handle) take another one from this
	// same pool, so the pool must be larger than the number of transactions
	// allowed at once (postgres.MaxConcurrentTx); otherwise that many
	// concurrent commits block each other until their context expires.
	sqlDB.SetMaxOpenConns(MaxOpenConns)
	sqlDB.SetMaxIdleConns(MaxOpenConns)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)
	sqlDB.SetConnMaxIdleTime(0)

	db, err := gorm.Open(
		postgres.New(postgres.Config{Conn: sqlDB}),
		&gorm.Config{
			TranslateError: true,
			Logger:         gormLogger,
		},
	)
	if err != nil {
		return nil, err
	}

	if err = db.Use(tracing.NewPlugin(
		tracing.WithDBSystem("postgresql"),
	)); err != nil {
		slog.Error("failed to enable tracing plugin for postgres", slog.String("error", err.Error()))
	}

	return db, nil
}

// migrationLockID serializes AutoMigrate across replicas starting simultaneously.
const migrationLockID int64 = 0x636f6e63726e74 // "concrnt"

func MigratePostgres(db *gorm.DB) error {
	// session-level advisory lock must be taken and released on the same connection
	return db.Connection(func(conn *gorm.DB) error {
		if err := conn.Exec("SELECT pg_advisory_lock(?)", migrationLockID).Error; err != nil {
			return err
		}
		defer conn.Exec("SELECT pg_advisory_unlock(?)", migrationLockID)

		return conn.AutoMigrate(
			&models.CommitLog{},
			&models.Record{},
			&models.RecordKey{},
			&models.Association{},
			&models.Ack{},
			&models.Acked{},
			&models.Server{},
			&models.Entity{},
			&models.EntityMeta{},
			&models.Subscription{},
			&models.AbuseReport{},
		)
	})
}
