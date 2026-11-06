package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/iterator"
)

// Migrate applies the pending numbered batches to the client's database:
// schema_migrations is created when missing, the applied versions are
// read, and every later batch goes through UpdateDatabaseDdl and is then
// recorded. DDL is not transactional, so a batch interrupted half way is
// re-run next start; the IF NOT EXISTS statements make that a no-op.
func Migrate(ctx context.Context, admin *database.DatabaseAdminClient, client *spanner.Client) error {
	dbPath := client.DatabaseName()

	probe := client.Single().Query(ctx, spanner.Statement{
		SQL: "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '' AND TABLE_NAME = 'schema_migrations'",
	})
	_, err := probe.Next()
	probe.Stop()
	if err != nil && err != iterator.Done {
		return err
	}
	if err == iterator.Done {
		if err := applyDDL(ctx, admin, dbPath, []string{`CREATE TABLE IF NOT EXISTS schema_migrations (
			version INT64 NOT NULL,
			name STRING(MAX) NOT NULL,
			applied_at TIMESTAMP NOT NULL
		) PRIMARY KEY (version)`}); err != nil {
			return err
		}
	}

	applied := map[int]bool{}
	it := client.Single().Query(ctx, spanner.Statement{SQL: "SELECT version FROM schema_migrations"})
	err = it.Do(func(row *spanner.Row) error {
		var version int64
		if err := row.Column(0, &version); err != nil {
			return err
		}
		applied[int(version)] = true
		return nil
	})
	if err != nil {
		return err
	}

	for _, m := range migrations {
		if applied[m.Version] {
			continue
		}
		if err := applyDDL(ctx, admin, dbPath, m.Statements); err != nil {
			return fmt.Errorf("migration %d (%s): %w", m.Version, m.Name, err)
		}
		_, err := client.Apply(ctx, []*spanner.Mutation{spanner.InsertOrUpdate("schema_migrations",
			[]string{"version", "name", "applied_at"},
			[]any{int64(m.Version), m.Name, time.Now()})})
		if err != nil {
			return err
		}
	}
	return nil
}

func applyDDL(ctx context.Context, admin *database.DatabaseAdminClient, dbPath string, statements []string) error {
	op, err := admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{Database: dbPath, Statements: statements})
	if err != nil {
		return err
	}
	return op.Wait(ctx)
}
