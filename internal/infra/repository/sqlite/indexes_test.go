package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/repository/shapes"
	"github.com/concrnt/concrnt/internal/infra/repository/sqlite"
)

func TestEveryShapeIsRendered(t *testing.T) {
	for _, s := range shapes.All {
		_, ok := sqlite.Tables[s.Collection]
		require.True(t, ok, "shape %s: collection %s has no table", s.Name, s.Collection)
		_, err := sqlite.IndexColumns(s)
		require.NoError(t, err)
	}
	_, err := sqlite.IndexDDL()
	require.NoError(t, err)
}

// After Migrate, sqlite_master holds an index for every shape under the
// shape's name, on its table, with exactly the shape's columns, directions
// and predicate; Migrate is re-runnable.
func TestMigratedIndexesCoverShapes(t *testing.T) {
	ctx := context.Background()
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "concrnt.db"))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	require.NoError(t, sqlite.Migrate(ctx, db))
	require.NoError(t, sqlite.Migrate(ctx, db), "migrate is idempotent")

	for _, s := range shapes.All {
		var table, ddl string
		err := db.QueryRowContext(ctx, "SELECT tbl_name, sql FROM sqlite_master WHERE type = 'index' AND name = ?", s.Name).Scan(&table, &ddl)
		require.NoError(t, err, "shape %s: no index named %s", s.Name, s.Name)
		require.Equal(t, sqlite.Tables[s.Collection], table, "shape %s is on the wrong table", s.Name)

		// column order and direction from index_xinfo (key columns only)
		rows, err := db.QueryContext(ctx, `SELECT name, "desc" FROM pragma_index_xinfo(?) WHERE key = 1 ORDER BY seqno`, s.Name)
		require.NoError(t, err)
		var got []string
		for rows.Next() {
			var name string
			var desc int
			require.NoError(t, rows.Scan(&name, &desc))
			col := `"` + name + `"`
			if desc == 1 {
				col += " DESC"
			}
			got = append(got, col)
		}
		rows.Close()
		want, err := sqlite.IndexColumns(s)
		require.NoError(t, err)
		require.Equal(t, want, got, "shape %s: columns of %q", s.Name, ddl)

		if s.Partial != "" {
			require.Contains(t, ddl, " WHERE "+sqlite.Partials[s.Partial], "shape %s: predicate", s.Name)
		} else {
			require.NotContains(t, ddl, " WHERE ", "shape %s: unexpected predicate", s.Name)
		}
	}
}
