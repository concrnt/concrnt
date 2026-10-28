package postgres

import (
	"context"
	"fmt"

	"gorm.io/gorm"

	aggpg "github.com/concrnt/concrnt/internal/infra/aggregate/postgres"
	"github.com/concrnt/concrnt/internal/infra/database/models"
)

// migration is one numbered, idempotent schema step. Steps are applied in
// order and recorded in schema_migrations, so a step runs once per database;
// a schema change is a new step, never an edit of an applied one.
type migration struct {
	Version int
	Name    string
	Up      func(tx *gorm.DB) error
}

var migrations = []migration{
	{1, "baseline_tables", AutoMigrateTables},
	// the key-ordered child listing moved to byte order (CONTRACT.md "Key
	// order"); the index under this name was built on the database collation
	// and is rebuilt by ensureIndexes with COLLATE "C"
	{2, "rebuild_parent_key_index_in_byte_order", func(tx *gorm.DB) error {
		return tx.Exec("DROP INDEX IF EXISTS idx_record_keys_parent_id_uri").Error
	}},
	{3, "aggregate_entries", func(tx *gorm.DB) error {
		return tx.Exec(aggpg.Schema).Error
	}},
	// chunkline reads come off the key row alone (C1): redirect moves onto
	// record_keys and record_created_at is filled where it was never set
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
}

// AutoMigrateTables creates or extends every table from its model (tables,
// primary keys, unique constraints, foreign keys). Migrate runs it once as
// the baseline; `conctl op repair-anchored-acks` runs it again after it has
// reshaped a pre-baseline schema by hand.
func AutoMigrateTables(db *gorm.DB) error {
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

// migrationLockID serializes Migrate across replicas starting simultaneously.
const migrationLockID int64 = 0x636f6e63726e74 // "concrnt"

// Migrate brings the schema up to date: the pending numbered steps, then the
// shape indexes that do not exist yet. Everything runs in one transaction
// under a transaction-scoped advisory lock, so concurrent starters queue up
// and a failed step leaves the database untouched.
func Migrate(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
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
		return ensureIndexes(tx)
	})
}
