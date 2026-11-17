package migrations

import (
	"context"
	"log/slog"
)

func init() {
	migrations["v1.12.0-beta.0"] = func(ctx context.Context) error {
		slog.InfoContext(ctx, "This is Test Migrator!!!")
		return nil
	}
}
