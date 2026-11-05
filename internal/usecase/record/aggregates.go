package record

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
	"github.com/concrnt/concrnt/internal/utils"
)

// The count aggregates (C2). Each is a cache of what the matching
// repo.Compute* method returns; writes adjust an existing entry (Touch) or
// drop it (Invalidate), reads go through aggregate.Get.
const (
	// AggregateAssociationCounts: target = the associated record's cckv; one
	// entry holds every schema's variant counts (the per-schema counts are
	// their sums) and the set of authors. The name changed with the value's
	// shape: entries under the old name are left to expire.
	AggregateAssociationCounts = "association-counts-v2"
	// AggregateAckCounts: target = "from:<ccid>" (acks held for an acker) or
	// "to:<ccid>" (ackeds held for a target), every schema in one entry.
	AggregateAckCounts = "ack-counts"
)

// AggregateNames lists every aggregate definition.
func AggregateNames() []string {
	return []string{AggregateAssociationCounts, AggregateAckCounts}
}

// associationDelta is one association added (N = 1) or removed (N = -1).
type associationDelta struct {
	Schema  string
	Variant string
	Author  string
	N       int64
}

// maxTrackedAuthors bounds the author set kept in an entry; a target with
// more authors is listed from the primary data whenever an author filter
// is given.
const maxTrackedAuthors = 200

type variantCount struct {
	Variant string `json:"v"`
	Count   int64  `json:"n"`
}

// variantCounts are one schema's variants in display order.
type variantCounts []variantCount

func (vc variantCounts) ordered() *utils.OrderedKVMap[int64] {
	out := make(utils.OrderedKVMap[int64], len(vc))
	for i, c := range vc {
		out[c.Variant] = utils.OrderedKV[int64]{Value: c.Count, Order: int64(i)}
	}
	return &out
}

// associationCounts is the cached value of AggregateAssociationCounts.
type associationCounts struct {
	// Schemas maps a schema to its variants in order.
	Schemas map[string]variantCounts `json:"s"`
	// Authors is every author with an association on the target, when
	// AuthorsUntracked is false.
	Authors []string `json:"a,omitempty"`
	// AuthorsUntracked is set when the authors are too many to keep, or a
	// removal left the set uncertain; an author filter then reads the
	// primary data.
	AuthorsUntracked bool `json:"m,omitempty"`
}

func (ac associationCounts) bySchema() map[string]int64 {
	out := make(map[string]int64, len(ac.Schemas))
	for schema, variants := range ac.Schemas {
		for _, c := range variants {
			out[schema] += c.Count
		}
	}
	return out
}

// mayMatch reports whether a listing filtered by schema, variant and author
// ("" = no filter) can have rows. The cached "" variant is an association
// without a variant, so an empty variant filter never narrows.
func (ac associationCounts) mayMatch(schema, variant, author string) bool {
	if len(ac.Schemas) == 0 {
		return false
	}
	if author != "" && !ac.AuthorsUntracked && !slices.Contains(ac.Authors, author) {
		return false
	}
	if schema != "" {
		variants, ok := ac.Schemas[schema]
		if !ok {
			return false
		}
		return variant == "" || slices.ContainsFunc(variants, func(c variantCount) bool { return c.Variant == variant })
	}
	if variant == "" {
		return true
	}
	for _, variants := range ac.Schemas {
		if slices.ContainsFunc(variants, func(c variantCount) bool { return c.Variant == variant }) {
			return true
		}
	}
	return false
}

// countsOnly drops the variant order, which a cached entry and a fresh
// computation may legitimately disagree on (see applyAssociationDelta), and
// the author set, which a removal leaves untracked.
func (ac associationCounts) countsOnly() map[string]map[string]int64 {
	out := make(map[string]map[string]int64, len(ac.Schemas))
	for schema, variants := range ac.Schemas {
		m := make(map[string]int64, len(variants))
		for _, c := range variants {
			m[c.Variant] = c.Count
		}
		out[schema] = m
	}
	return out
}

// fromComputed orders each schema's variants by the createdAt of their
// earliest association (the repository's Order), ties by variant.
func fromComputed(counts map[string]utils.OrderedKVMap[int64], authors []string, authorsUntracked bool) associationCounts {
	out := associationCounts{Schemas: make(map[string]variantCounts, len(counts)), Authors: authors, AuthorsUntracked: authorsUntracked}
	for schema, variants := range counts {
		vc := make(variantCounts, 0, len(variants))
		for variant, c := range variants {
			vc = append(vc, variantCount{Variant: variant, Count: c.Value})
		}
		slices.SortFunc(vc, func(x, y variantCount) int {
			return cmp.Or(cmp.Compare(variants[x.Variant].Order, variants[y.Variant].Order), cmp.Compare(x.Variant, y.Variant))
		})
		out.Schemas[schema] = vc
	}
	return out
}

// applyAssociationDelta adjusts the counts by one association. A variant
// keeps its place while it has associations: a new one is appended, a
// removal never moves it, and a variant whose count drops to zero is gone
// (when it comes back it starts at the end). The cached order is therefore
// the order in which variants arrived here, while a recomputation orders by
// the earliest createdAt still present; the two may differ (a backdated
// association, the first association of a variant removed), and that
// difference is accepted. An added author joins the set until it is full;
// a removal leaves the set untracked, since the author may still have
// other associations.
func applyAssociationDelta(cur associationCounts, d associationDelta) associationCounts {
	if cur.Schemas == nil {
		cur.Schemas = map[string]variantCounts{}
	}
	variants := cur.Schemas[d.Schema]
	i := slices.IndexFunc(variants, func(c variantCount) bool { return c.Variant == d.Variant })
	switch {
	case i < 0 && d.N > 0:
		variants = append(variants, variantCount{Variant: d.Variant, Count: d.N})
	case i >= 0:
		variants[i].Count += d.N
		if variants[i].Count <= 0 {
			variants = slices.Delete(variants, i, i+1)
		}
	}
	if len(variants) == 0 {
		delete(cur.Schemas, d.Schema)
	} else {
		cur.Schemas[d.Schema] = variants
	}

	switch {
	case cur.AuthorsUntracked:
	case d.N < 0:
		cur.Authors, cur.AuthorsUntracked = nil, true
	case slices.Contains(cur.Authors, d.Author):
	case len(cur.Authors) >= maxTrackedAuthors:
		cur.Authors, cur.AuthorsUntracked = nil, true
	default:
		cur.Authors = append(cur.Authors, d.Author)
	}
	return cur
}

type aggregates struct {
	associations *aggregate.Definition[associationCounts, associationDelta]
	acks         *aggregate.Definition[map[string]int64, struct{}]
}

func newAggregates(repo Repository) aggregates {
	return aggregates{
		associations: &aggregate.Definition[associationCounts, associationDelta]{
			Name: AggregateAssociationCounts,
			Compute: func(ctx context.Context, target string) (associationCounts, error) {
				counts, err := repo.ComputeAssociationCounts(ctx, target)
				if err != nil {
					return associationCounts{}, err
				}
				// the author set is only listed when it can be complete:
				// the total bounds the distinct authors, so a target over
				// the limit is untracked without a second read, and an
				// empty target (the common case) needs none either
				var total int64
				for _, variants := range counts {
					for _, c := range variants {
						total += c.Value
					}
				}
				var authors []string
				untracked := total > maxTrackedAuthors
				if total > 0 && !untracked {
					authors, err = repo.ListAssociationAuthors(ctx, target, maxTrackedAuthors+1)
					if err != nil {
						return associationCounts{}, err
					}
					if len(authors) > maxTrackedAuthors {
						authors, untracked = nil, true
					}
				}
				return fromComputed(counts, authors, untracked), nil
			},
			Apply: applyAssociationDelta,
		},
		acks: &aggregate.Definition[map[string]int64, struct{}]{
			Name: AggregateAckCounts,
			Compute: func(ctx context.Context, target string) (map[string]int64, error) {
				side, ccid, ok := strings.Cut(target, ":")
				if !ok || (side != "from" && side != "to") {
					return nil, fmt.Errorf("malformed ack target %q", target)
				}
				if side == "from" {
					return repo.ComputeAcknowledgeCounts(ctx, ccid, "", "")
				}
				return repo.ComputeAcknowledgeCounts(ctx, "", ccid, "")
			},
			// an ack write's "applied" only says the state row moved, not
			// that validity flipped, so there is no delta form
		},
	}
}

func ackTarget(from, to string) string {
	if from != "" {
		return "from:" + from
	}
	return "to:" + to
}

// associationCounts reads a target's counts through the cache, or straight
// from the primary data when no store is configured.
func (uc *Usecase) associationCounts(ctx context.Context, targetURI string) (associationCounts, error) {
	if uc.agg == nil {
		return uc.aggs.associations.Compute(ctx, targetURI)
	}
	return aggregate.Get(ctx, uc.agg, uc.aggs.associations, targetURI)
}

// associationCountsTouched folds a committed association (n = 1) or the
// removal of one (n = -1) into the count cache. receivedAt is taken when the
// action runs, after the commit: a concurrent computation stamped earlier
// may have read a snapshot without the change and gets the delta too. The
// other choice (stamping inside the transaction) can leave a computation
// that missed the row uncorrected, and GetAssociatedRecords trusts an empty
// entry to skip the primary data, so an over-count is the error to prefer.
// A removal invalidates instead: it would leave the author set untracked
// and the entry worth less than a recomputation. A dump replay
// (LocalOnlyExecute) is a bulk path and invalidates too.
func (uc *Usecase) associationCountsTouched(associate, schema string, variant *string, author string, n int64, mode domain.CommitMode) PostProcessAction {
	v := ""
	if variant != nil {
		v = *variant
	}
	return func(ctx context.Context) error {
		if uc.agg == nil {
			return nil
		}
		if mode == domain.CommitModeLocalOnlyExecute || n < 0 {
			return uc.agg.Invalidate(ctx, AggregateAssociationCounts, associate)
		}
		return aggregate.Touch(ctx, uc.agg, uc.aggs.associations, associate, associationDelta{Schema: schema, Variant: v, Author: author, N: n}, time.Now())
	}
}

// associationCountsSeededEmpty stores the empty counts of a key that had no
// record until this commit, saving the first reader's computation. The
// ordering it assumes, record before associations, is the normal one; an
// association that reached the key while it was a parent placeholder would
// be missed until the entry expires.
func (uc *Usecase) associationCountsSeededEmpty(uri string) PostProcessAction {
	return func(ctx context.Context) error {
		if uc.agg == nil {
			return nil
		}
		return aggregate.Seed(ctx, uc.agg, uc.aggs.associations, uri, associationCounts{Schemas: map[string]variantCounts{}})
	}
}

// associationCountsOfTargetInvalidated drops the count cache of a record
// whose associations went away with it.
func (uc *Usecase) associationCountsOfTargetInvalidated(uri string) PostProcessAction {
	return func(ctx context.Context) error {
		if uc.agg == nil {
			return nil
		}
		return uc.agg.Invalidate(ctx, AggregateAssociationCounts, uri)
	}
}

// ackCountsInvalidated drops the ack count cache of the side a committed
// ack-kind document changed.
func (uc *Usecase) ackCountsInvalidated(kind, from, to string) PostProcessAction {
	target := ackTarget(from, "")
	if kind == "acked" || kind == "unacked" {
		target = ackTarget("", to)
	}
	return func(ctx context.Context) error {
		if uc.agg == nil {
			return nil
		}
		return uc.agg.Invalidate(ctx, AggregateAckCounts, target)
	}
}

// AggregateRef names one cache entry.
type AggregateRef struct {
	Name   string
	Target string
}

// AggregateTargets lists the cache entries a committed document contributes
// to, the way the commit path touches them (verification tooling).
func AggregateTargets(doc concrnt.Document[any]) []AggregateRef {
	switch doc.Kind {
	case "association":
		if doc.Associate == nil {
			return nil
		}
		return []AggregateRef{{AggregateAssociationCounts, *doc.Associate}}
	case "ack", "unack":
		return []AggregateRef{{AggregateAckCounts, ackTarget(doc.Author, "")}}
	case "acked", "unacked":
		if doc.Associate == nil {
			return nil
		}
		parsed, err := concrnt.ParseCCURI(*doc.Associate)
		if err != nil {
			return nil
		}
		return []AggregateRef{{AggregateAckCounts, ackTarget("", parsed.Owner)}}
	}
	return nil
}

// AggregateVerification compares a cache entry with a fresh computation.
type AggregateVerification struct {
	// Cold is true when the cache holds no entry (nothing to compare).
	Cold bool
	// Equal is true when the cached and computed values match.
	Equal            bool
	Cached, Computed string
}

// VerifyAggregate compares the cached entry of ref with the primary data
// without creating or refreshing the entry.
func (uc *Usecase) VerifyAggregate(ctx context.Context, ref AggregateRef) (AggregateVerification, error) {
	if uc.agg == nil {
		return AggregateVerification{}, errors.New("no aggregate store configured")
	}
	switch ref.Name {
	case AggregateAssociationCounts:
		// the variant order is allowed to differ (applyAssociationDelta)
		return verifyAggregate(ctx, uc.agg, uc.aggs.associations, ref.Target, func(c associationCounts) any { return c.countsOnly() })
	case AggregateAckCounts:
		return verifyAggregate(ctx, uc.agg, uc.aggs.acks, ref.Target, func(c map[string]int64) any { return c })
	default:
		return AggregateVerification{}, fmt.Errorf("unknown aggregate %q", ref.Name)
	}
}

// verifyAggregate compares compared(cached) with compared(computed);
// compared picks the part of the value the cache has to get right.
func verifyAggregate[V, D any](ctx context.Context, agg *aggregate.Usecase, def *aggregate.Definition[V, D], target string, compared func(V) any) (AggregateVerification, error) {
	cached, ok, err := aggregate.Cached(ctx, agg, def, target)
	if err != nil {
		return AggregateVerification{}, err
	}
	if !ok {
		return AggregateVerification{Cold: true}, nil
	}
	computed, err := def.Compute(ctx, target)
	if err != nil {
		return AggregateVerification{}, err
	}
	cachedJSON, err := json.Marshal(compared(cached))
	if err != nil {
		return AggregateVerification{}, err
	}
	computedJSON, err := json.Marshal(compared(computed))
	if err != nil {
		return AggregateVerification{}, err
	}
	return AggregateVerification{Equal: string(cachedJSON) == string(computedJSON), Cached: string(cachedJSON), Computed: string(computedJSON)}, nil
}

// InvalidateAggregate drops one cache entry.
func (uc *Usecase) InvalidateAggregate(ctx context.Context, ref AggregateRef) error {
	if uc.agg == nil {
		return nil
	}
	return uc.agg.Invalidate(ctx, ref.Name, ref.Target)
}
