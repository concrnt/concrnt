package aggregate_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The Compute* repository methods are the cache-miss path (C2): only the
// aggregate definitions, the backends and their tests, and the verification
// tool may call them. Everything else reads counts through the aggregate
// layer.
func TestComputeIsCalledOnlyFromTheAggregateLayer(t *testing.T) {
	root, err := os.Getwd()
	require.NoError(t, err)
	for !fileExists(filepath.Join(root, "go.mod")) {
		parent := filepath.Dir(root)
		require.NotEqual(t, parent, root, "module root not found")
		root = parent
	}

	allowed := []string{
		"internal/usecase/record/aggregates.go",
		"internal/usecase/record/query.go", // the fallback when no store is configured
		"internal/infra/repository/",
		"internal/infra/aggregate/",
		"cmd/conctl/aggregate",
	}
	call := regexp.MustCompile(`\.Compute(AssociationCountsBySchema|AssociationCountsByVariant|AcknowledgeCounts)\(`)

	var offenders []string
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		for _, prefix := range allowed {
			if strings.HasPrefix(rel, prefix) {
				return nil
			}
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if call.Match(src) {
			offenders = append(offenders, rel)
		}
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, offenders, "Compute* must only be called from the aggregate layer")
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
