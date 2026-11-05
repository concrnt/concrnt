package record

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func variantOrder(t *testing.T, vc variantCounts) string {
	t.Helper()
	encoded, err := json.Marshal(vc.ordered())
	require.NoError(t, err)
	return string(encoded)
}

// A variant keeps the place where it first appeared while it has any
// association left: A, B, A then removing the first A stays A, B.
func TestApplyAssociationDeltaKeepsFirstAppearance(t *testing.T) {
	const like, repost = "https://schema.example/like.json", "https://schema.example/repost.json"
	t0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	add := func(c associationCounts, schema, variant string, at time.Time) associationCounts {
		return applyAssociationDelta(c, associationDelta{Schema: schema, Variant: variant, N: 1, CreatedAt: at})
	}
	remove := func(c associationCounts, schema, variant string, at time.Time) associationCounts {
		return applyAssociationDelta(c, associationDelta{Schema: schema, Variant: variant, N: -1, CreatedAt: at})
	}

	var c associationCounts
	c = add(c, like, "A", t0)
	c = add(c, like, "B", t0.Add(time.Minute))
	c = add(c, like, "A", t0.Add(2*time.Minute))
	c = add(c, repost, "", t0.Add(3*time.Minute))
	require.Equal(t, `{"A":2,"B":1}`, variantOrder(t, c[like]))
	require.Equal(t, map[string]int64{like: 3, repost: 1}, c.bySchema())

	c = remove(c, like, "A", t0)
	require.Equal(t, `{"A":1,"B":1}`, variantOrder(t, c[like]), "removing the first A keeps A in front")

	c = remove(c, like, "A", t0.Add(2*time.Minute))
	require.Equal(t, `{"B":1}`, variantOrder(t, c[like]), "a variant at zero is gone")

	c = add(c, like, "A", t0.Add(4*time.Minute))
	require.Equal(t, `{"B":1,"A":1}`, variantOrder(t, c[like]), "a variant that comes back starts at the end")

	c = remove(c, repost, "", t0.Add(3*time.Minute))
	require.NotContains(t, c, repost, "a schema without associations is gone")
	require.Equal(t, map[string]int64{like: 2}, c.bySchema())

	require.Equal(t, `{}`, variantOrder(t, c[repost]), "an absent schema reads as no variants")
}

// Verification compares only the counts: the order may differ between a
// cached entry and a recomputation.
func TestAssociationCountsOnlyIgnoresOrder(t *testing.T) {
	cached := associationCounts{"s": {"A": {Count: 1, First: 1}, "B": {Count: 1, First: 2}}}
	computed := associationCounts{"s": {"A": {Count: 1, First: 3}, "B": {Count: 1, First: 2}}}
	require.Equal(t, cached.countsOnly(), computed.countsOnly())
}
