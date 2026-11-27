package trust

import "math"

const (
	// damping is the share of a node's trust that it passes on to the nodes
	// it acknowledges; the rest stays with the seeds. It is what makes trust
	// fade with distance from a seed.
	damping = 0.85

	maxIterations = 100
	tolerance     = 1e-9
)

// Edge says that From acknowledges To.
type Edge struct {
	From string
	To   string
}

// scores computes how much trust each node holds, as a personalized PageRank
// rooted at the seeds: trust starts at the seeds, and every node passes its
// trust on in equal parts to the nodes it acknowledges. Because a node's
// trust is split between its acknowledgements rather than copied to each,
// acknowledging more accounts does not create more trust, and accounts no
// seed leads to hold none however much they acknowledge each other.
//
// The result is scaled so that a seed holds 1: a score reads as "this
// fraction of a seed's trust". Scores are capped at 1. Nodes without trust
// are left out.
func scores(seeds []string, edges []Edge) map[string]float64 {
	if len(seeds) == 0 {
		return map[string]float64{}
	}

	index := map[string]int{}
	var names []string
	node := func(name string) int {
		i, ok := index[name]
		if !ok {
			i = len(names)
			index[name] = i
			names = append(names, name)
		}
		return i
	}

	seedNodes := map[int]struct{}{}
	for _, seed := range seeds {
		seedNodes[node(seed)] = struct{}{}
	}

	// duplicate acknowledgements (one per schema) are one edge, and
	// acknowledging oneself passes nothing on
	type pair struct{ from, to int }
	seen := map[pair]struct{}{}
	var out [][]int
	for _, edge := range edges {
		p := pair{node(edge.From), node(edge.To)}
		if p.from == p.to {
			continue
		}
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		for len(out) <= p.from {
			out = append(out, nil)
		}
		out[p.from] = append(out[p.from], p.to)
	}
	for len(out) < len(names) {
		out = append(out, nil)
	}

	n := len(names)
	rank := make([]float64, n)
	for s := range seedNodes {
		rank[s] = 1 / float64(len(seedNodes))
	}
	next := make([]float64, n)

	// what a seed is handed each round, before anything flows back to it
	base := 0.0
	for range maxIterations {
		clear(next)
		// a node that acknowledges nobody has nowhere to pass its trust:
		// it goes back to the seeds
		dangling := 0.0
		for u, targets := range out {
			if len(targets) == 0 {
				dangling += rank[u]
				continue
			}
			share := rank[u] / float64(len(targets))
			for _, v := range targets {
				next[v] += share
			}
		}

		base = (1 - damping + damping*dangling) / float64(len(seedNodes))
		diff := 0.0
		for i := range next {
			value := damping * next[i]
			if _, seed := seedNodes[i]; seed {
				value += base
			}
			diff += math.Abs(value - rank[i])
			rank[i] = value
		}
		if diff < tolerance {
			break
		}
	}

	result := make(map[string]float64, n)
	for i, name := range names {
		if score := math.Min(1, rank[i]/base); score > 0 {
			result[name] = score
		}
	}
	return result
}
