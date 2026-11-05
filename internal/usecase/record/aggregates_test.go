package record

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/utils"
)

func variantOrder(t *testing.T, vc variantCounts) string {
	t.Helper()
	encoded, err := json.Marshal(vc.ordered())
	require.NoError(t, err)
	return string(encoded)
}

// A variant keeps its place while it has any association left: A, B, A
// then removing an A stays A, B.
func TestApplyAssociationDeltaKeepsPlace(t *testing.T) {
	const like, repost = "https://schema.example/like.json", "https://schema.example/repost.json"
	add := func(c associationCounts, schema, variant string) associationCounts {
		return applyAssociationDelta(c, associationDelta{Schema: schema, Variant: variant, Author: "con1" + variant, N: 1})
	}
	remove := func(c associationCounts, schema, variant string) associationCounts {
		return applyAssociationDelta(c, associationDelta{Schema: schema, Variant: variant, Author: "con1" + variant, N: -1})
	}

	var c associationCounts
	c = add(c, like, "A")
	c = add(c, like, "B")
	c = add(c, like, "A")
	c = add(c, repost, "")
	require.Equal(t, `{"A":2,"B":1}`, variantOrder(t, c.Schemas[like]))
	require.Equal(t, map[string]int64{like: 3, repost: 1}, c.bySchema())

	c = remove(c, like, "A")
	require.Equal(t, `{"A":1,"B":1}`, variantOrder(t, c.Schemas[like]), "a removal keeps A in front")

	c = remove(c, like, "A")
	require.Equal(t, `{"B":1}`, variantOrder(t, c.Schemas[like]), "a variant at zero is gone")

	c = add(c, like, "A")
	require.Equal(t, `{"B":1,"A":1}`, variantOrder(t, c.Schemas[like]), "a variant that comes back starts at the end")

	c = remove(c, like, "C")
	require.Equal(t, `{"B":1,"A":1}`, variantOrder(t, c.Schemas[like]), "removing an absent variant changes nothing")

	c = remove(c, repost, "")
	require.NotContains(t, c.Schemas, repost, "a schema without associations is gone")
	require.Equal(t, map[string]int64{like: 2}, c.bySchema())

	require.Equal(t, `{}`, variantOrder(t, c.Schemas[repost]), "an absent schema reads as no variants")
}

// A computed value is ordered by each variant's earliest association, and
// variants that tie are ordered the same way every time.
func TestFromComputedOrder(t *testing.T) {
	c := fromComputed(map[string]utils.OrderedKVMap[int64]{
		"s": {
			"late":  {Value: 1, Order: 30},
			"b-tie": {Value: 2, Order: 10},
			"a-tie": {Value: 3, Order: 10},
			"early": {Value: 4, Order: 5},
		},
	}, nil, false)
	require.Equal(t, `{"early":4,"a-tie":3,"b-tie":2,"late":1}`, variantOrder(t, c.Schemas["s"]))
}

// Verification compares only the counts: the order may differ between a
// cached entry and a recomputation.
func TestAssociationCountsOnlyIgnoresOrder(t *testing.T) {
	cached := associationCounts{Schemas: map[string]variantCounts{"s": {{Variant: "A", Count: 1}, {Variant: "B", Count: 1}}}, AuthorsUntracked: true}
	computed := associationCounts{Schemas: map[string]variantCounts{"s": {{Variant: "B", Count: 1}, {Variant: "A", Count: 1}}}, Authors: []string{"con1a"}}
	require.Equal(t, cached.countsOnly(), computed.countsOnly())
}

func TestAssociationCountsMayMatch(t *testing.T) {
	counts := associationCounts{
		Schemas: map[string]variantCounts{
			"like":  {{Variant: "", Count: 2}},
			"react": {{Variant: "👍", Count: 1}},
		},
		Authors: []string{"con1alice", "con1bob"},
	}
	untracked := counts
	untracked.Authors, untracked.AuthorsUntracked = nil, true
	cases := []struct {
		name                    string
		counts                  associationCounts
		schema, variant, author string
		want                    bool
	}{
		{"empty entry", associationCounts{}, "", "", "", false},
		{"no filter", counts, "", "", "", true},
		{"schema present", counts, "like", "", "", true},
		{"schema absent", counts, "reply", "", "", false},
		{"schema and variant present", counts, "react", "👍", "", true},
		{"schema and variant absent", counts, "react", "👎", "", false},
		{"empty variant filter never narrows", counts, "react", "", "", true},
		{"variant across schemas present", counts, "", "👍", "", true},
		{"variant across schemas absent", counts, "", "👎", "", false},
		{"author present", counts, "like", "", "con1bob", true},
		{"author absent", counts, "like", "", "con1carol", false},
		{"author absent but untracked", untracked, "like", "", "con1carol", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.counts.mayMatch(tc.schema, tc.variant, tc.author))
		})
	}
}

// The author set follows the deltas: additions join it until it is full,
// a removal leaves it untracked.
func TestApplyAssociationDeltaAuthors(t *testing.T) {
	var c associationCounts
	c = applyAssociationDelta(c, associationDelta{Schema: "s", Author: "con1a", N: 1})
	c = applyAssociationDelta(c, associationDelta{Schema: "s", Author: "con1b", N: 1})
	c = applyAssociationDelta(c, associationDelta{Schema: "s", Author: "con1a", N: 1})
	require.Equal(t, []string{"con1a", "con1b"}, c.Authors)
	require.False(t, c.AuthorsUntracked)

	removed := applyAssociationDelta(c, associationDelta{Schema: "s", Author: "con1b", N: -1})
	require.Nil(t, removed.Authors)
	require.True(t, removed.AuthorsUntracked)
	require.True(t, removed.mayMatch("s", "", "con1zed"))

	full := associationCounts{}
	for i := range maxTrackedAuthors + 1 {
		full = applyAssociationDelta(full, associationDelta{Schema: "s", Author: fmt.Sprintf("con1%d", i), N: 1})
	}
	require.True(t, full.AuthorsUntracked, "one author over the limit untracks the set")
	require.Nil(t, full.Authors)
}
