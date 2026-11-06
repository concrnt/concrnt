package sqlite

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// autoMigrateTables creates the tables, constraints and indexes the models
// declare. It is additive, so Migrate runs it at every start after the
// numbered steps: a new column or index only needs its tag. Changing an
// existing index or column needs a step that drops it by name first.
func autoMigrateTables(db *gorm.DB) error {
	return db.AutoMigrate(
		&CommitLog{},
		&Record{},
		&RecordKey{},
		&Association{},
		&Ack{},
		&Acked{},
		&Entity{},
		&EntityMeta{},
		&Server{},
		&Subscription{},
		&AbuseReport{},
	)
}

type migration struct {
	Version int
	Name    string
	Up      func(tx *gorm.DB) error
}

// The numbered steps are what AutoMigrate cannot express (backfills, index
// rebuilds, raw DDL). SQLite shipped on AutoMigrate alone, so the first real
// step gets version 1.
var migrations = []migration{
	// acks are read by from and ackeds by to: the other side's indexes and
	// the schema ones were never scanned, and the gc flag index is rebuilt
	// by AutoMigrate as a partial index
	{1, "drop_unused_ack_indexes_and_partial_gc_index", func(tx *gorm.DB) error {
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

// Migrate applies the pending numbered steps, then autoMigrateTables, in one
// IMMEDIATE transaction (the writer lock serialises concurrent starters).
func Migrate(ctx context.Context, db *DB) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
			version integer PRIMARY KEY,
			name text NOT NULL,
			applied_at integer NOT NULL
		)`).Error; err != nil {
			return err
		}
		var versions []int
		if err := tx.Raw("SELECT version FROM schema_migrations").Scan(&versions).Error; err != nil {
			return err
		}
		applied := map[int]bool{}
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
			if err := tx.Exec("INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)", m.Version, m.Name, nowMicros()).Error; err != nil {
				return err
			}
		}

		return autoMigrateTables(tx)
	})
}
