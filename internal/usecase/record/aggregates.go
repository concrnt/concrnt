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
// repo.Compute* method returns, held as counters; writes adjust an existing
// entry by a delta (Touch) or drop it (Invalidate), reads go through the
// aggregate usecase's Get. A delta may be negative, and a blind decrement
// can land on an entry that never counted the item (computed after the
// removal), so a counter at or below zero is a normal state: the readers
// here leave it out of every count they report.
const (
	// AggregateAssociationCounts: target = the associated record's cckv; one
	// entry holds every schema's variant counts (the per-schema counts are
	// their sums) and the set of authors.
	AggregateAssociationCounts = "association-counts"
	// AggregateAckCounts: target = "from:<ccid>" (acks held for an acker) or
	// "to:<ccid>" (ackeds held for a target), every schema in one entry.
	AggregateAckCounts = "ack-counts"
)

// AggregateNames lists every aggregate definition.
func AggregateNames() []string {
	return []string{AggregateAssociationCounts, AggregateAckCounts}
}

// maxTrackedAuthors bounds the author set an entry is trusted for; a target
// with more authors is listed from the primary data whenever an author
// filter is given.
const maxTrackedAuthors = 1000

// An association-counts entry holds one counter per (schema, variant),
// named by associationCounter, whose minimum is the createdAt (unix
// nanoseconds) of the variant's earliest association, and the authors as
// its members. A counter name always contains the separator, so a name
// without one is free for the entry's own flags.
const (
	counterSeparator = "\x1f"
	// authorsUntrackedCounter is set by a computation that found more
	// associations than authors are tracked and so did not list them.
	authorsUntrackedCounter = "authors-untracked"
)

func associationCounter(schema, variant string) string {
	return schema + counterSeparator + variant
}

type variantCount struct {
	Variant string
	Count   int64
	// Earliest is the createdAt of the variant's earliest association in
	// unix nanoseconds; the variants are ordered by it.
	Earliest int64
}

// variantCounts are one schema's variants in display order.
type variantCounts []variantCount

func (vc variantCounts) ordered() *utils.OrderedKVMap[int64] {
	out := make(utils.OrderedKVMap[int64], len(vc))
	for _, c := range vc {
		if c.Count <= 0 {
			continue
		}
		out[c.Variant] = utils.OrderedKV[int64]{Value: c.Count, Order: int64(len(out))}
	}
	return &out
}

// associationCounts is the typed view of an AggregateAssociationCounts
// entry.
type associationCounts struct {
	// Schemas maps a schema to its variants in order. A variant whose
	// count is zero or less is kept (mayMatch must not turn a removal that
	// was counted twice into "no rows") but is not reported.
	Schemas map[string]variantCounts
	// Authors is every author with an association on the target, when
	// AuthorsUntracked is false. A removal leaves its author in.
	Authors []string
	// AuthorsUntracked is set when the authors are too many to keep; an
	// author filter then reads the primary data.
	AuthorsUntracked bool
}

func (ac associationCounts) bySchema() map[string]int64 {
	out := make(map[string]int64, len(ac.Schemas))
	for schema, variants := range ac.Schemas {
		for _, c := range variants {
			if c.Count > 0 {
				out[schema] += c.Count
			}
		}
	}
	return out
}

// mayMatch reports whether a listing filtered by schema, variant and author
// ("" = no filter) can have rows. The cached "" variant is an association
// without a variant, so an empty variant filter never narrows. It goes by
// the presence of a counter, never its value: a false "yes" costs one
// query, a false "no" would hide rows.
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
// computation may legitimately disagree on (a minimum outlives the
// association it came from when the entry survives the removal), and the
// author set, whose tracking depends on the entry's history.
func (ac associationCounts) countsOnly() map[string]map[string]int64 {
	out := make(map[string]map[string]int64, len(ac.Schemas))
	for schema, variants := range ac.Schemas {
		for _, c := range variants {
			if c.Count <= 0 {
				continue
			}
			if out[schema] == nil {
				out[schema] = map[string]int64{}
			}
			out[schema][c.Variant] = c.Count
		}
	}
	return out
}

// associationCountsOf reads an entry: each schema's variants ordered by
// their earliest association, ties by variant. The author set is trusted
// only while it is within the limit; deltas add authors blind, so an entry
// can grow past it.
func associationCountsOf(c aggregate.Counters) associationCounts {
	out := associationCounts{Schemas: map[string]variantCounts{}}
	for name, n := range c.Counts {
		schema, variant, ok := strings.Cut(name, counterSeparator)
		if !ok {
			continue
		}
		out.Schemas[schema] = append(out.Schemas[schema], variantCount{Variant: variant, Count: n, Earliest: c.Minima[name]})
	}
	for _, variants := range out.Schemas {
		slices.SortFunc(variants, func(x, y variantCount) int {
			return cmp.Or(cmp.Compare(x.Earliest, y.Earliest), cmp.Compare(x.Variant, y.Variant))
		})
	}
	if c.Counts[authorsUntrackedCounter] > 0 || len(c.Members) > maxTrackedAuthors {
		out.AuthorsUntracked = true
	} else {
		out.Authors = c.Members
	}
	return out
}

// associationCounters is the entry of a computation: each variant's count,
// its earliest createdAt as the minimum, and the authors, or untracked when
// they are too many to list.
func associationCounters(summary AssociationSummary) aggregate.Counters {
	out := aggregate.Counters{Counts: map[string]int64{}, Minima: map[string]int64{}}
	for _, v := range summary.Variants {
		name := associationCounter(v.Schema, v.Variant)
		out.Counts[name] = v.Count
		out.Minima[name] = v.Earliest.UnixNano()
	}
	if summary.AuthorsOverflow {
		out.Counts[authorsUntrackedCounter] = 1
	} else {
		out.Members = summary.Authors
	}
	return out
}

// associationDelta is one association added: its counter, its createdAt as
// a candidate for the variant's earliest, and its author.
func associationDelta(schema, variant, author string, createdAt time.Time) aggregate.Delta {
	name := associationCounter(schema, variant)
	return aggregate.Delta{
		Inc:        map[string]int64{name: 1},
		Min:        map[string]int64{name: createdAt.UnixNano()},
		AddMembers: []string{author},
	}
}

// associationRemovalDelta is one association removed. The variant's
// earliest and the author set stay as they are: neither can be taken back
// blind, and both only err towards listing (mayMatch) and a stale order.
func associationRemovalDelta(schema, variant string) aggregate.Delta {
	return aggregate.Delta{Inc: map[string]int64{associationCounter(schema, variant): -1}}
}

// ackCountsOf reads an AggregateAckCounts entry: the schemas with at least
// one valid state.
func ackCountsOf(c aggregate.Counters) map[string]int64 {
	out := make(map[string]int64, len(c.Counts))
	for schema, n := range c.Counts {
		if n > 0 {
			out[schema] = n
		}
	}
	return out
}

type aggregates struct {
	associations *aggregate.Definition
	acks         *aggregate.Definition
}

func newAggregates(repo Repository) aggregates {
	return aggregates{
		associations: &aggregate.Definition{
			Name: AggregateAssociationCounts,
			Compute: func(ctx context.Context, target string) (aggregate.Counters, error) {
				summary, err := repo.ComputeAssociationSummary(ctx, target, maxTrackedAuthors)
				if err != nil {
					return aggregate.Counters{}, err
				}
				return associationCounters(summary), nil
			},
		},
		// one counter per schema: the valid states of one side
		acks: &aggregate.Definition{
			Name: AggregateAckCounts,
			Compute: func(ctx context.Context, target string) (aggregate.Counters, error) {
				side, ccid, ok := strings.Cut(target, ":")
				if !ok || (side != "from" && side != "to") {
					return aggregate.Counters{}, fmt.Errorf("malformed ack target %q", target)
				}
				from, to := ccid, ""
				if side == "to" {
					from, to = "", ccid
				}
				counts, err := repo.ComputeAcknowledgeCounts(ctx, from, to, "")
				if err != nil {
					return aggregate.Counters{}, err
				}
				return aggregate.Counters{Counts: counts}, nil
			},
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
	var counters aggregate.Counters
	var err error
	if uc.agg == nil {
		counters, err = uc.aggs.associations.Compute(ctx, targetURI)
	} else {
		counters, err = uc.agg.Get(ctx, uc.aggs.associations, targetURI)
	}
	if err != nil {
		return associationCounts{}, err
	}
	return associationCountsOf(counters), nil
}

// associationCountsTouched folds a committed association into the count
// cache. receivedAt is taken when the action runs, after the commit: a
// concurrent computation stamped earlier may have read a snapshot without
// the association and gets the delta too. GetAssociatedRecords trusts an
// empty entry to skip the primary data, so an over-count is the error to
// prefer over a missed row. A dump replay (LocalOnlyExecute) is a bulk path
// and invalidates instead.
func (uc *Usecase) associationCountsTouched(associate, schema string, variant *string, author string, createdAt time.Time, mode domain.CommitMode) PostProcessAction {
	v := ""
	if variant != nil {
		v = *variant
	}
	return func(ctx context.Context) error {
		if uc.agg == nil {
			return nil
		}
		if mode == domain.CommitModeLocalOnlyExecute {
			return uc.agg.Invalidate(ctx, AggregateAssociationCounts, associate)
		}
		return uc.agg.Touch(ctx, uc.aggs.associations, associate, associationDelta(schema, v, author, createdAt), time.Now())
	}
}

// associationCountsSeededEmpty stores the empty counts of a key that had no
// record until this commit (RecordOutcome.FreshKey), saving the first
// reader's computation. The
// ordering it assumes, record before associations, is the normal one; an
// association that reached the key while it was a parent placeholder would
// be missed until the entry expires.
func (uc *Usecase) associationCountsSeededEmpty(uri string) PostProcessAction {
	return func(ctx context.Context) error {
		if uc.agg == nil {
			return nil
		}
		return uc.agg.Seed(ctx, uc.aggs.associations, uri, aggregate.Counters{})
	}
}

// associationCountsInvalidated drops the count cache of uri: the record
// went away with all its associations.
func (uc *Usecase) associationCountsInvalidated(uri string) PostProcessAction {
	return func(ctx context.Context) error {
		if uc.agg == nil {
			return nil
		}
		return uc.agg.Invalidate(ctx, AggregateAssociationCounts, uri)
	}
}

// associationCountsRemoved takes a deleted association out of the count
// cache of the record it was on. outcome is the delete's and index the
// association's removal in it: only a removal that found the association
// decrements, so a replayed delete does not count twice. A dump replay
// (LocalOnlyExecute) is a bulk path and invalidates instead.
func (uc *Usecase) associationCountsRemoved(associate, schema string, variant *string, outcome *DeleteOutcome, index int, mode domain.CommitMode) PostProcessAction {
	v := ""
	if variant != nil {
		v = *variant
	}
	return func(ctx context.Context) error {
		if uc.agg == nil {
			return nil
		}
		if mode == domain.CommitModeLocalOnlyExecute {
			return uc.agg.Invalidate(ctx, AggregateAssociationCounts, associate)
		}
		if index >= len(outcome.Removed) || !outcome.Removed[index] {
			return nil
		}
		return uc.agg.Touch(ctx, uc.aggs.associations, associate, associationRemovalDelta(schema, v), time.Now())
	}
}

// ackCountsTouched folds a committed ack-kind document into the ack count
// cache of the side it moved. outcome is read when the action runs: only a
// validity flip changes a count, a state that merely moved to a newer
// document changes none. A dump replay (LocalOnlyExecute) is a bulk path
// and invalidates instead.
func (uc *Usecase) ackCountsTouched(side AckSide, from, to, schema string, outcome *AckOutcome, mode domain.CommitMode) PostProcessAction {
	target := ackTarget(from, "")
	if side == TargetSide {
		target = ackTarget("", to)
	}
	return func(ctx context.Context) error {
		if uc.agg == nil {
			return nil
		}
		if mode == domain.CommitModeLocalOnlyExecute {
			return uc.agg.Invalidate(ctx, AggregateAckCounts, target)
		}
		if outcome.WasValid == outcome.IsValid {
			return nil
		}
		n := int64(1)
		if !outcome.IsValid {
			n = -1
		}
		return uc.agg.Touch(ctx, uc.aggs.acks, target, aggregate.Delta{Inc: map[string]int64{schema: n}}, time.Now())
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
		// the variant order is allowed to differ (countsOnly)
		return verifyAggregate(ctx, uc.agg, uc.aggs.associations, ref.Target, func(c aggregate.Counters) any { return associationCountsOf(c).countsOnly() })
	case AggregateAckCounts:
		return verifyAggregate(ctx, uc.agg, uc.aggs.acks, ref.Target, func(c aggregate.Counters) any { return ackCountsOf(c) })
	default:
		return AggregateVerification{}, fmt.Errorf("unknown aggregate %q", ref.Name)
	}
}

// verifyAggregate compares compared(cached) with compared(computed);
// compared picks the part of the entry the cache has to get right.
func verifyAggregate(ctx context.Context, agg *aggregate.Usecase, def *aggregate.Definition, target string, compared func(aggregate.Counters) any) (AggregateVerification, error) {
	cached, ok, err := agg.Cached(ctx, def, target)
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
