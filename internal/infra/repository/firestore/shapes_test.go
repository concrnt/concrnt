package firestore

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var updateIndexes = flag.Bool("update-indexes", false, "rewrite deploy/firestore/firestore.indexes.json from the query shapes")

const indexesPath = "../../../../deploy/firestore/firestore.indexes.json"

// TestIndexesFileMatchesShapes keeps the deployable index file in lock-step
// with the queries: the emulator does not enforce composite indexes, so a
// query shape without one would only fail in production.
func TestIndexesFileMatchesShapes(t *testing.T) {
	want := requiredIndexFile()
	wantJSON, err := json.MarshalIndent(want, "", "  ")
	require.NoError(t, err)
	wantJSON = append(wantJSON, '\n')

	if *updateIndexes {
		require.NoError(t, os.MkdirAll(filepath.Dir(indexesPath), 0o755))
		require.NoError(t, os.WriteFile(indexesPath, wantJSON, 0o644))
	}

	got, err := os.ReadFile(indexesPath)
	require.NoError(t, err, "run: go test ./internal/infra/repository/firestore -run TestIndexesFileMatchesShapes -args -update-indexes")
	require.JSONEq(t, string(wantJSON), string(got), "deploy/firestore/firestore.indexes.json is stale; regenerate with -args -update-indexes")
}

// TestShapesDoNotUseExemptFields guards against querying a field whose
// single-field index was disabled.
func TestShapesDoNotUseExemptFields(t *testing.T) {
	for _, s := range shapes {
		exempt := map[string]bool{}
		for _, f := range exemptFields[s.Collection] {
			exempt[f] = true
		}
		fields := append(append([]string{}, s.Equalities...), s.Optional...)
		fields = append(fields, s.ArrayContains, s.Range)
		for _, o := range s.OrderBy {
			fields = append(fields, o.Field)
		}
		for _, f := range fields {
			require.False(t, exempt[f], "shape %s queries exempt field %s", s.Name, f)
		}
	}
}
