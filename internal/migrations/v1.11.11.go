package migrations

import (
	"context"
	"log/slog"
)

func init() {
	migrations["v1.11.11"] = func(ctx context.Context) error {
		slog.InfoContext(ctx, "This is Test Migrator!!!")
		return nil
	}
}
