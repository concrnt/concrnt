package migrations

import (
	"context"
	"fmt"
	"log/slog"

	"gorm.io/gorm"
)

func init() {
	migrations["v1.12.0"] = foldRecordsIntoRecordKeys
}

// foldRecordsBatchSize bounds one backfill UPDATE, so the migration of a
// large records table does not hold one huge transaction.
const foldRecordsBatchSize = 5000

// foldRecordsIntoRecordKeys migrates a database from the layout where a
// record's columns lived in a separate records table (keyed by document id,
// pointed at by record_keys.record_id) to the one where they live on the key
// row. It runs after the repositories' AutoMigrate has added those columns
// to record_keys.
//
// The records table itself is left in place, no longer read or written; the
// operator drops it when convenient. What goes is record_keys' foreign key to
// it, which the key rows written from now on would violate. Dropping that
// key is the last step, and its presence is what marks the migration as not
// done: an interrupted run is finished by the next start, a finished one (or
// a database created without records) is a no-op.
//
// The servers of the old version must be stopped first: they still write to
// records and expect the key rows to point there. The database must have run
// v1.11.11 before: that release sets net.concrnt.version, and without it
// Migrator.Do takes the database for a fresh one and skips this migration.
//
// A server without a postgres database (no PostgresKey in ctx) has nothing
// to migrate.
func foldRecordsIntoRecordKeys(ctx context.Context) error {
	db, ok := ctx.Value(PostgresKey).(*gorm.DB)
	if !ok || db == nil {
		return nil
	}
	db = db.WithContext(ctx)

	if !db.Migrator().HasTable("records") {
		return nil
	}
	var legacyForeignKeys []string
	err := db.Raw(`
SELECT conname FROM pg_constraint
WHERE contype = 'f'
  AND conrelid = 'record_keys'::regclass
  AND confrelid = 'records'::regclass`).
		Scan(&legacyForeignKeys).Error
	if err != nil {
		return fmt.Errorf("find record_keys foreign keys to records: %w", err)
	}
	if len(legacyForeignKeys) == 0 {
		return nil
	}
	slog.InfoContext(ctx, "migrating records into record_keys")

	// 1. copy the record columns onto the key rows that point at them. A key
	// row whose schema is still NULL has not been copied yet (the copy
	// writes "" for a record without one, so every batch makes progress).
	total := int64(0)
	for {
		result := db.Exec(`
UPDATE record_keys k
SET owner = r.owner,
    author = r.author,
    schema = COALESCE(r.schema, ''),
    redirect = r.redirect,
    policies = r.policies,
    distributions = r.distributions,
    record_created_at = r.created_at
FROM records r
WHERE r.document_id = k.record_id
  AND k.id IN (
    SELECT k2.id FROM record_keys k2
    JOIN records r2 ON r2.document_id = k2.record_id
    WHERE k2.schema IS NULL
    LIMIT ?
  )`, foldRecordsBatchSize)
		if result.Error != nil {
			return fmt.Errorf("copy records into record_keys: %w", result.Error)
		}
		total += result.RowsAffected
		if result.RowsAffected == 0 {
			break
		}
		slog.InfoContext(ctx, "copied records into record_keys", "rows", total)
	}

	// 2. flag what ccfs must not serve any more. A record commit that no key
	// points at and that has no records row was deleted after asking to be
	// retained (a retained version the key moved on from keeps its records
	// row, everything else was flagged when it was replaced or deleted).
	// Deletes no longer keep retained history, so those versions and every
	// delete commit not flagged yet (the ones that deleted a retained
	// version) become gc candidates like any other deleted document.
	result := db.Exec(`
UPDATE commit_logs c
SET gc_candidate = true
WHERE NOT c.gc_candidate
  AND (
    (c.document::jsonb->>'kind' = 'record'
      AND NOT EXISTS (SELECT 1 FROM record_keys k WHERE k.record_id = c.id)
      AND NOT EXISTS (SELECT 1 FROM records r WHERE r.document_id = c.id))
    OR c.document::jsonb->>'kind' = 'delete'
  )`)
	if result.Error != nil {
		return fmt.Errorf("flag deleted retained versions: %w", result.Error)
	}
	slog.InfoContext(ctx, "flagged deleted retained versions and their deletes for gc", "rows", result.RowsAffected)

	// 3. drop the record_keys index that only served the join to records,
	// then record_keys' foreign key to records (last: see above).
	if err := db.Exec(`DROP INDEX IF EXISTS idx_record_keys_parent_id_record_id`).Error; err != nil {
		return fmt.Errorf("drop idx_record_keys_parent_id_record_id: %w", err)
	}
	for _, name := range legacyForeignKeys {
		if err := db.Exec(fmt.Sprintf(`ALTER TABLE record_keys DROP CONSTRAINT %q`, name)).Error; err != nil {
			return fmt.Errorf("drop record_keys foreign key %s: %w", name, err)
		}
	}

	slog.InfoContext(ctx, "migrated records into record_keys")
	return nil
}
