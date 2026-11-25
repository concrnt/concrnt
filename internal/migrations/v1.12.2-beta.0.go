package migrations

import (
	"context"

	"gorm.io/gorm"
)

func init() {
	migrations["v1.12.2-beta.0"] = dropRecordKeysURIPatternIndex
}

// dropRecordKeysURIPatternIndex drops idx_record_keys_uri_pattern, which
// idx_record_keys_uri_pattern_include (created by AutoMigrate before the
// versioned migrations run) replaces.
func dropRecordKeysURIPatternIndex(ctx context.Context) error {
	db, ok := ctx.Value(PostgresKey).(*gorm.DB)
	if !ok || db == nil {
		return nil
	}
	return gorm.G[any](db).Exec(ctx, `DROP INDEX IF EXISTS idx_record_keys_uri_pattern`)
}
