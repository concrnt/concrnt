package database

import (
	"fmt"
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

// A schema change is a new step, never an edit of an applied one: a step
// runs once per database.
type migration struct {
	Version int
	Name    string
	Up      func(tx *gorm.DB) error
}

var migrations = []migration{
	{1, "baseline_tables", autoMigrateTables},
	// AutoMigrate does not rebuild an existing index whose collation changed
	{2, "rebuild_parent_key_index_in_byte_order", func(tx *gorm.DB) error {
		return tx.Exec("DROP INDEX IF EXISTS idx_record_keys_parent_id_uri").Error
	}},
	{4, "record_keys_redirect", func(tx *gorm.DB) error {
		for _, stmt := range []string{
			`ALTER TABLE record_keys ADD COLUMN IF NOT EXISTS redirect text`,
			`UPDATE record_keys rk SET redirect = r.redirect FROM records r WHERE rk.record_id = r.document_id AND r.redirect IS NOT NULL AND rk.redirect IS NULL`,
			`UPDATE record_keys rk SET record_created_at = r.created_at FROM records r WHERE rk.record_id = r.document_id AND rk.record_created_at IS NULL`,
		} {
			if err := tx.Exec(stmt).Error; err != nil {
				return err
			}
		}
		return nil
	}},
	{5, "drop_unused_ack_indexes_and_partial_gc_index", func(tx *gorm.DB) error {
		for _, name := range []string{
			"idx_acks_to_valid_created_at", "idx_acks_schema_valid_created_at",
			"idx_acked_from_valid_created_at", "idx_acked_schema_valid_created_at",
			"idx_commit_logs_gc_candidate",
		} {
			if err := tx.Exec("DROP INDEX IF EXISTS " + name).Error; err != nil {
				return err
			}
		}
		return nil
	}},
}

// Not a numbered step alone: AutoMigrate only adds, so it also runs at every
// start and a new column or index needs nothing but its tag. Changing an
// existing index needs a step that drops it by name first.
func autoMigrateTables(db *gorm.DB) error {
	return db.AutoMigrate(
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
}

// migrationLockID serializes AutoMigrate across replicas starting simultaneously.
const migrationLockID int64 = 0x636f6e63726e74 // "concrnt"

// Not a session-level advisory lock around the steps: without a transaction
// a failed step would leave the database half migrated.
func MigratePostgres(db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", migrationLockID).Error; err != nil {
			return err
		}
		if err := tx.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
			version integer PRIMARY KEY,
			name text NOT NULL,
			applied_at timestamp with time zone NOT NULL DEFAULT now()
		)`).Error; err != nil {
			return err
		}

		var versions []int
		if err := tx.Raw("SELECT version FROM schema_migrations").Scan(&versions).Error; err != nil {
			return err
		}
		applied := make(map[int]bool, len(versions))
		for _, v := range versions {
			applied[v] = true
		}

		for _, m := range migrations {
			if applied[m.Version] {
				continue
			}
			if err := m.Up(tx); err != nil {
				return fmt.Errorf("migration %d (%s): %w", m.Version, m.Name, err)
			}
			if err := tx.Exec("INSERT INTO schema_migrations (version, name) VALUES (?, ?)", m.Version, m.Name).Error; err != nil {
				return err
			}
		}
		return autoMigrateTables(tx)
	})
}
