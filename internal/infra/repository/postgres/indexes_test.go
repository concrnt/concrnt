package postgres_test

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
	"github.com/concrnt/concrnt/internal/infra/repository/shapes"
	"github.com/concrnt/concrnt/internal/testutil"
)

// Every shape maps onto known columns (a new shape with an unmapped field
// fails here, without Docker).
func TestEveryShapeIsRendered(t *testing.T) {
	for _, s := range shapes.All {
		_, ok := postgres.Tables[s.Collection]
		require.True(t, ok, "shape %s: collection %s has no table", s.Name, s.Collection)
		_, err := postgres.IndexColumns(s)
		require.NoError(t, err)
	}
	_, err := postgres.IndexDDL()
	require.NoError(t, err)
}

var indexDefRe = regexp.MustCompile(`^CREATE (?:UNIQUE )?INDEX (\S+) ON \S+ USING btree \((.*?)\)(?: WHERE \((.*)\))?$`)

// After Migrate, pg_indexes holds an index for every shape under the shape's
// name with exactly the shape's columns (order, direction, collation) and
// predicate — on the shape's own table (index names are unique per schema,
// so a same-named index on another table would otherwise hide a missing one).
func TestMigratedIndexesCoverShapes(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)
	require.NoError(t, postgres.Migrate(context.Background(), db))

	type indexRow struct{ Tablename, Indexname, Indexdef string }
	var rows []indexRow
	require.NoError(t, db.Raw("SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = 'public'").Scan(&rows).Error)
	byName := map[string]indexRow{}
	for _, row := range rows {
		byName[row.Indexname] = row
	}

	for _, s := range shapes.All {
		row, ok := byName[s.Name]
		require.True(t, ok, "shape %s: no index named %s", s.Name, s.Name)
		require.Equal(t, postgres.Tables[s.Collection], row.Tablename, "shape %s is on the wrong table", s.Name)

		m := indexDefRe.FindStringSubmatch(row.Indexdef)
		require.NotNil(t, m, "shape %s: unparsed indexdef %q", s.Name, row.Indexdef)

		want, err := postgres.IndexColumns(s)
		require.NoError(t, err)
		require.Equal(t, strings.Join(want, ", "), normalizeColumns(m[2]), "shape %s: columns of %q", s.Name, row.Indexdef)

		wantWhere := ""
		if s.Partial != "" {
			wantWhere = postgres.Partials[s.Partial]
		}
		require.Equal(t, strings.ReplaceAll(wantWhere, "(", ""), strings.ReplaceAll(strings.ReplaceAll(m[3], "(", ""), ")", ""), "shape %s: predicate of %q", s.Name, row.Indexdef)
	}
}

// pg_indexes quotes only the identifiers that need it; the shape renders
// every column quoted, so quote the bare ones before comparing.
func normalizeColumns(def string) string {
	parts := strings.Split(def, ", ")
	for i, p := range parts {
		if !strings.HasPrefix(p, `"`) {
			name, rest, _ := strings.Cut(p, " ")
			p = `"` + name + `"`
			if rest != "" {
				p += " " + rest
			}
		}
		parts[i] = p
	}
	return strings.Join(parts, ", ")
}
