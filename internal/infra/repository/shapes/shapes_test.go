package shapes_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/repository/shapes"
)

func TestRegistryIsWellFormed(t *testing.T) {
	names := map[string]bool{}
	known := map[shapes.Partial]bool{}
	for _, p := range shapes.Partials {
		known[p] = true
	}
	for _, s := range shapes.All {
		require.NotEmpty(t, s.Name)
		require.False(t, names[s.Name], "duplicate shape name %s", s.Name)
		names[s.Name] = true
		require.NotEmpty(t, s.Collection, "%s: collection", s.Name)
		require.NotEmpty(t, s.Fields(), "%s: a shape references at least one field", s.Name)

		seen := map[shapes.Field]bool{}
		for _, f := range s.Fields() {
			require.NotEmpty(t, f, "%s: empty field", s.Name)
			require.False(t, seen[f], "%s: field %s listed twice", s.Name, f)
			seen[f] = true
		}
		if s.Range == "" {
			require.False(t, s.RangeDesc, "%s: RangeDesc without a Range", s.Name)
			require.False(t, s.Binary, "%s: Binary marks the Range field", s.Name)
		}
		if s.Binary {
			require.Equal(t, shapes.Key, s.Range, "%s: only key order is byte order", s.Name)
		}
		if s.Partial != "" {
			require.True(t, known[s.Partial], "%s: unknown partial %q", s.Name, s.Partial)
		}
	}
}
