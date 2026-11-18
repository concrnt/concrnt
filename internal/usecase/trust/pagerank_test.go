package trust

import (
	"fmt"
	"math"
	"testing"
)

func requireScore(t *testing.T, got map[string]float64, name string, want float64) {
	t.Helper()
	if math.Abs(got[name]-want) > 1e-6 {
		t.Fatalf("score of %s = %v, want %v", name, got[name], want)
	}
}

func TestSeedsAreFullyTrusted(t *testing.T) {
	got := scores([]string{"seed"}, nil)
	requireScore(t, got, "seed", 1)
	if len(got) != 1 {
		t.Fatalf("scores = %v, want only the seed", got)
	}
}

func TestNoSeedsNoTrust(t *testing.T) {
	got := scores(nil, []Edge{{"a", "b"}})
	if len(got) != 0 {
		t.Fatalf("scores = %v, want none", got)
	}
}

// A seed's trust is split between the accounts it acknowledges, and fades
// with every hop.
func TestTrustIsSplitAndFadesWithDistance(t *testing.T) {
	got := scores([]string{"seed"}, []Edge{
		{"seed", "a"},
		{"seed", "b"},
		{"a", "c"},
	})
	requireScore(t, got, "seed", 1)
	requireScore(t, got, "a", damping/2)
	requireScore(t, got, "b", damping/2)
	requireScore(t, got, "c", damping*damping/2)
}

// Acknowledging more accounts does not create more trust: it spreads the
// same trust thinner.
func TestAcknowledgingManyDoesNotCreateTrust(t *testing.T) {
	var edges []Edge
	for i := range 1000 {
		edges = append(edges, Edge{"seed", fmt.Sprintf("a%d", i)})
	}
	got := scores([]string{"seed"}, edges)

	total := 0.0
	for name, score := range got {
		if name != "seed" {
			total += score
		}
	}
	if math.Abs(total-damping) > 1e-6 {
		t.Fatalf("trust handed out = %v, want %v whatever the number of acknowledgements", total, damping)
	}
}

// Accounts no seed leads to hold no trust, however much they acknowledge
// each other or anyone else.
func TestAccountsUnreachableFromSeedsHoldNoTrust(t *testing.T) {
	edges := []Edge{{"seed", "honest"}}
	for i := range 50 {
		sybil := fmt.Sprintf("sybil%d", i)
		edges = append(edges, Edge{sybil, "spammer"}, Edge{"spammer", sybil}, Edge{sybil, "seed"})
	}
	got := scores([]string{"seed"}, edges)

	if _, ok := got["spammer"]; ok {
		t.Fatalf("spammer scored %v, want no trust", got["spammer"])
	}
	requireScore(t, got, "honest", damping)
}

func TestDuplicateAndSelfAcknowledgementsAreIgnored(t *testing.T) {
	got := scores([]string{"seed"}, []Edge{
		{"seed", "a"},
		{"seed", "a"}, // the same pair under another schema
		{"seed", "seed"},
		{"seed", "b"},
	})
	requireScore(t, got, "a", damping/2)
	requireScore(t, got, "b", damping/2)
}

func TestSeveralSeedsEachCountInFull(t *testing.T) {
	got := scores([]string{"s1", "s2"}, []Edge{
		{"s1", "a"},
		{"s2", "a"},
		{"s2", "b"},
	})
	requireScore(t, got, "s1", 1)
	requireScore(t, got, "s2", 1)
	requireScore(t, got, "a", 1) // damping + damping/2, capped
	requireScore(t, got, "b", damping/2)
}

// Trust flowing back to a seed does not lift it, or what it hands out, above
// a seed's worth.
func TestScoresAreCappedAtOne(t *testing.T) {
	got := scores([]string{"seed"}, []Edge{{"seed", "a"}, {"a", "seed"}})
	requireScore(t, got, "seed", 1)
	if got["a"] > 1 {
		t.Fatalf("score of a = %v, want at most 1", got["a"])
	}
}
