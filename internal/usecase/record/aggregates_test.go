package record

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

func variantOrder(t *testing.T, vc variantCounts) string {
	t.Helper()
	encoded, err := json.Marshal(vc.ordered())
	require.NoError(t, err)
	return string(encoded)
}

// Deltas land in any order, so the variants are ordered by the createdAt of
// their associations and not by arrival: B, A, A reads A, B when A's
// earliest association is the older one.
func TestAssociationDeltaOrder(t *testing.T) {
	const like, repost = "https://schema.example/like.json", "https://schema.example/repost.json"
	at := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	var c aggregate.Counters
	c.Add(associationDelta(like, "B", "con1b", at.Add(2*time.Second)))
	c.Add(associationDelta(like, "A", "con1a", at.Add(3*time.Second)))
	c.Add(associationDelta(like, "A", "con1c", at.Add(time.Second)))
	c.Add(associationDelta(repost, "", "con1a", at))

	counts := associationCountsOf(c)
	require.Equal(t, `{"A":2,"B":1}`, variantOrder(t, counts.Schemas[like]))
	require.Equal(t, map[string]int64{like: 3, repost: 1}, counts.bySchema())
	require.Equal(t, `{}`, variantOrder(t, counts.Schemas["absent"]), "an absent schema reads as no variants")
	require.ElementsMatch(t, []string{"con1a", "con1b", "con1c"}, counts.Authors)
	require.False(t, counts.AuthorsUntracked)
}

// A computed value is ordered by each variant's earliest association, and
// variants that tie are ordered the same way every time.
func TestComputedAssociationCountersOrder(t *testing.T) {
	at := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	c := associationCountsOf(associationCounters(AssociationSummary{
		Variants: []AssociationVariant{
			{Schema: "s", Variant: "late", Count: 1, Earliest: at.Add(30 * time.Second)},
			{Schema: "s", Variant: "b-tie", Count: 2, Earliest: at.Add(10 * time.Second)},
			{Schema: "s", Variant: "a-tie", Count: 3, Earliest: at.Add(10 * time.Second)},
			{Schema: "s", Variant: "early", Count: 4, Earliest: at.Add(5 * time.Second)},
		},
		Authors: []string{"con1a"},
	}))
	require.Equal(t, `{"early":4,"a-tie":3,"b-tie":2,"late":1}`, variantOrder(t, c.Schemas["s"]))
	require.Equal(t, []string{"con1a"}, c.Authors)
}

// A variant may hold the separator itself: only the first one splits.
func TestAssociationCounterNames(t *testing.T) {
	c := associationCountsOf(aggregate.Counters{Counts: map[string]int64{
		associationCounter("s", ""):                       1,
		associationCounter("s", "a"+counterSeparator+"b"): 2,
		authorsUntrackedCounter:                           1,
	}})
	require.Equal(t, map[string]map[string]int64{"s": {"": 1, "a" + counterSeparator + "b": 2}}, c.countsOnly(), "the entry's own flag is not a schema")
	require.True(t, c.AuthorsUntracked)
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
		{"seeded entry", associationCountsOf(aggregate.Counters{}), "", "", "", false},
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

// A blind decrement can take a counter to zero or below (an entry computed
// after the removal never counted the item). Such a counter is left out of
// every reported count, but a listing still goes to the primary data for it:
// a wrong "no rows" would hide associations, a wrong "maybe" costs one query.
func TestAssociationCountsHideNonPositiveCounters(t *testing.T) {
	counts := associationCountsOf(aggregate.Counters{Counts: map[string]int64{
		associationCounter("like", ""):   0,
		associationCounter("react", "👍"): -1,
		associationCounter("react", "🎉"): 2,
	}})

	require.Equal(t, map[string]int64{"react": 2}, counts.bySchema())
	require.Equal(t, `{"🎉":2}`, variantOrder(t, counts.Schemas["react"]))
	require.Equal(t, `{}`, variantOrder(t, counts.Schemas["like"]))
	require.Equal(t, map[string]map[string]int64{"react": {"🎉": 2}}, counts.countsOnly())

	require.True(t, counts.mayMatch("", "", ""))
	require.True(t, counts.mayMatch("like", "", ""), "a schema whose only counter is at zero may still have rows")
	require.True(t, counts.mayMatch("react", "👍", ""), "a variant below zero may still have rows")
	require.True(t, counts.mayMatch("", "👍", ""))
	require.False(t, counts.mayMatch("react", "👎", ""), "a variant without a counter has none")

	onlyGone := associationCountsOf(aggregate.Counters{Counts: map[string]int64{associationCounter("like", ""): 0}})
	require.Empty(t, onlyGone.bySchema())
	require.Empty(t, onlyGone.countsOnly())
	require.True(t, onlyGone.mayMatch("", "", ""), "an entry holding only spent counters is not an empty one")
}

// The ack counts report the schemas with at least one valid state: a counter
// decremented to zero or below is dropped.
func TestAckCountsDropNonPositiveCounters(t *testing.T) {
	require.Equal(t, map[string]int64{"follow": 2}, ackCountsOf(aggregate.Counters{Counts: map[string]int64{
		"follow": 2,
		"mute":   0,
		"block":  -1,
	}}))
	require.Empty(t, ackCountsOf(aggregate.Counters{}))
}

// Authors join the set once each; one over the limit untracks it, whether
// deltas grew it that far or the computation did not list them.
func TestAssociationAuthorsTracking(t *testing.T) {
	at := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	var c aggregate.Counters
	for _, author := range []string{"con1a", "con1b", "con1a"} {
		c.Add(associationDelta("s", "", author, at))
	}
	counts := associationCountsOf(c)
	require.Equal(t, []string{"con1a", "con1b"}, counts.Authors)
	require.False(t, counts.AuthorsUntracked)

	var full aggregate.Counters
	for i := range maxTrackedAuthors {
		full.Add(associationDelta("s", "", fmt.Sprintf("con1%d", i), at))
	}
	require.False(t, associationCountsOf(full).AuthorsUntracked, "the limit itself is still tracked")
	require.False(t, associationCountsOf(full).mayMatch("s", "", "con1zed"))
	full.Add(associationDelta("s", "", "con1over", at))
	counts = associationCountsOf(full)
	require.True(t, counts.AuthorsUntracked, "one author over the limit untracks the set")
	require.Nil(t, counts.Authors)
	require.True(t, counts.mayMatch("s", "", "con1zed"), "an untracked set never rules an author out")

	counts = associationCountsOf(associationCounters(AssociationSummary{
		Variants:        []AssociationVariant{{Schema: "s", Count: maxTrackedAuthors + 1, Earliest: at}},
		AuthorsOverflow: true,
	}))
	require.True(t, counts.AuthorsUntracked)
	require.True(t, counts.mayMatch("s", "", "con1zed"))
	require.Equal(t, map[string]int64{"s": maxTrackedAuthors + 1}, counts.bySchema())
}
