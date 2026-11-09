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

// Variants keep the order in which they arrived: A, B, A reads A, B.
func TestApplyAssociationDeltaOrder(t *testing.T) {
	const like, repost = "https://schema.example/like.json", "https://schema.example/repost.json"
	add := func(c associationCounts, schema, variant string) associationCounts {
		return applyAssociationDelta(c, associationDelta{Schema: schema, Variant: variant, Author: "con1" + variant})
	}

	var c associationCounts
	c = add(c, like, "A")
	c = add(c, like, "B")
	c = add(c, like, "A")
	c = add(c, repost, "")
	require.Equal(t, `{"A":2,"B":1}`, variantOrder(t, c.Schemas[like]))
	require.Equal(t, map[string]int64{like: 3, repost: 1}, c.bySchema())
	require.Equal(t, `{}`, variantOrder(t, c.Schemas["absent"]), "an absent schema reads as no variants")
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

// Authors join the set once each until it is full.
func TestApplyAssociationDeltaAuthors(t *testing.T) {
	var c associationCounts
	c = applyAssociationDelta(c, associationDelta{Schema: "s", Author: "con1a"})
	c = applyAssociationDelta(c, associationDelta{Schema: "s", Author: "con1b"})
	c = applyAssociationDelta(c, associationDelta{Schema: "s", Author: "con1a"})
	require.Equal(t, []string{"con1a", "con1b"}, c.Authors)
	require.False(t, c.AuthorsUntracked)

	full := associationCounts{}
	for i := range maxTrackedAuthors + 1 {
		full = applyAssociationDelta(full, associationDelta{Schema: "s", Author: fmt.Sprintf("con1%d", i)})
	}
	require.True(t, full.mayMatch("s", "", "con1zed"), "an untracked set never rules an author out")
	require.True(t, full.AuthorsUntracked, "one author over the limit untracks the set")
	require.Nil(t, full.Authors)
}
