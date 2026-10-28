package record

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	// AggregateAssociationsBySchema: target = the associated record's cckv.
	AggregateAssociationsBySchema = "association-counts-by-schema"
	// AggregateAssociationsByVariant: target = cckv + " " + schema (neither
	// a URI nor a schema URL may contain a space).
	AggregateAssociationsByVariant = "association-counts-by-variant"
	// AggregateAckCounts: target = "from:<ccid>" (acks held for an acker) or
	// "to:<ccid>" (ackeds held for a target), every schema in one entry.
	AggregateAckCounts = "ack-counts"
)

// AggregateNames lists every aggregate definition.
func AggregateNames() []string {
	return []string{AggregateAssociationsBySchema, AggregateAssociationsByVariant, AggregateAckCounts}
}

type schemaDelta struct {
	Schema string
	N      int64
}

type variantDelta struct {
	Variant   string
	N         int64
	CreatedAt time.Time
}

// variantCount is one variant's count and the createdAt (unix nanoseconds)
// of its earliest association, which orders the variants.
type variantCount struct {
	Count int64 `json:"count"`
	First int64 `json:"first"`
}

type variantCounts map[string]variantCount

func (vc variantCounts) ordered() *utils.OrderedKVMap[int64] {
	out := make(utils.OrderedKVMap[int64], len(vc))
	for variant, c := range vc {
		out[variant] = utils.OrderedKV[int64]{Value: c.Count, Order: c.First}
	}
	return &out
}

type aggregates struct {
	bySchema  *aggregate.Definition[map[string]int64, schemaDelta]
	byVariant *aggregate.Definition[variantCounts, variantDelta]
	acks      *aggregate.Definition[map[string]int64, struct{}]
}

func newAggregates(repo Repository) aggregates {
	return aggregates{
		bySchema: &aggregate.Definition[map[string]int64, schemaDelta]{
			Name:    AggregateAssociationsBySchema,
			Compute: repo.ComputeAssociationCountsBySchema,
			Apply: func(cur map[string]int64, d schemaDelta) map[string]int64 {
				if cur == nil {
					cur = map[string]int64{}
				}
				cur[d.Schema] += d.N
				if cur[d.Schema] <= 0 {
					delete(cur, d.Schema)
				}
				return cur
			},
		},
		byVariant: &aggregate.Definition[variantCounts, variantDelta]{
			Name: AggregateAssociationsByVariant,
			Compute: func(ctx context.Context, target string) (variantCounts, error) {
				uri, schema, ok := strings.Cut(target, " ")
				if !ok {
					return nil, fmt.Errorf("malformed variant target %q", target)
				}
				counts, err := repo.ComputeAssociationCountsByVariant(ctx, uri, schema)
				if err != nil {
					return nil, err
				}
				out := make(variantCounts, len(*counts))
				for variant, c := range *counts {
					out[variant] = variantCount{Count: c.Value, First: c.Order}
				}
				return out, nil
			},
			Apply: func(cur variantCounts, d variantDelta) variantCounts {
				if cur == nil {
					cur = variantCounts{}
				}
				c, exists := cur[d.Variant]
				first := d.CreatedAt.UnixNano()
				if exists && c.First < first {
					first = c.First
				}
				c = variantCount{Count: c.Count + d.N, First: first}
				if c.Count <= 0 {
					delete(cur, d.Variant)
				} else {
					cur[d.Variant] = c
				}
				return cur
			},
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

func variantTarget(uri, schema string) string {
	return uri + " " + schema
}

func ackTarget(from, to string) string {
	if from != "" {
		return "from:" + from
	}
	return "to:" + to
}

// associationCountsTouched folds a committed association into both count
// caches. receivedAt is taken now, inside the commit, so a concurrent
// computation stamped earlier is known not to have seen the association. A
// dump replay (LocalOnlyExecute) is a bulk path and invalidates instead.
func (uc *Usecase) associationCountsTouched(associate, schema string, variant *string, createdAt time.Time, mode domain.CommitMode) PostProcessAction {
	receivedAt := time.Now()
	v := ""
	if variant != nil {
		v = *variant
	}
	return func(ctx context.Context) error {
		if uc.agg == nil {
			return nil
		}
		if mode == domain.CommitModeLocalOnlyExecute {
			return uc.invalidateAssociationCounts(ctx, associate, schema)
		}
		return errors.Join(
			aggregate.Touch(ctx, uc.agg, uc.aggs.bySchema, associate, schemaDelta{Schema: schema, N: 1}, receivedAt),
			aggregate.Touch(ctx, uc.agg, uc.aggs.byVariant, variantTarget(associate, schema), variantDelta{Variant: v, N: 1, CreatedAt: createdAt}, receivedAt),
		)
	}
}

// associationCountsInvalidated drops the count caches a deleted association
// contributed to (the variant order cannot be maintained by a delta).
func (uc *Usecase) associationCountsInvalidated(associate, schema string) PostProcessAction {
	return func(ctx context.Context) error {
		if uc.agg == nil {
			return nil
		}
		return uc.invalidateAssociationCounts(ctx, associate, schema)
	}
}

func (uc *Usecase) invalidateAssociationCounts(ctx context.Context, associate, schema string) error {
	return errors.Join(
		uc.agg.Invalidate(ctx, AggregateAssociationsBySchema, associate),
		uc.agg.Invalidate(ctx, AggregateAssociationsByVariant, variantTarget(associate, schema)),
	)
}

// associationCountsOfTargetInvalidated drops every count cache of a record
// whose associations went away with it (every schema's variant entry).
func (uc *Usecase) associationCountsOfTargetInvalidated(uri string) PostProcessAction {
	return func(ctx context.Context) error {
		if uc.agg == nil {
			return nil
		}
		return errors.Join(
			uc.agg.Invalidate(ctx, AggregateAssociationsBySchema, uri),
			uc.agg.InvalidateAll(ctx, aggregate.Key(AggregateAssociationsByVariant, variantTarget(uri, ""))),
		)
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
		return []AggregateRef{
			{AggregateAssociationsBySchema, *doc.Associate},
			{AggregateAssociationsByVariant, variantTarget(*doc.Associate, doc.Schema)},
		}
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
	case AggregateAssociationsBySchema:
		return verifyAggregate(ctx, uc.agg, uc.aggs.bySchema, ref.Target)
	case AggregateAssociationsByVariant:
		return verifyAggregate(ctx, uc.agg, uc.aggs.byVariant, ref.Target)
	case AggregateAckCounts:
		return verifyAggregate(ctx, uc.agg, uc.aggs.acks, ref.Target)
	default:
		return AggregateVerification{}, fmt.Errorf("unknown aggregate %q", ref.Name)
	}
}

func verifyAggregate[V, D any](ctx context.Context, agg *aggregate.Usecase, def *aggregate.Definition[V, D], target string) (AggregateVerification, error) {
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
	cachedJSON, err := json.Marshal(cached)
	if err != nil {
		return AggregateVerification{}, err
	}
	computedJSON, err := json.Marshal(computed)
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

// InvalidateAggregates drops every entry of definition name, or every entry
// when name is "".
func (uc *Usecase) InvalidateAggregates(ctx context.Context, name string) error {
	if uc.agg == nil {
		return nil
	}
	if name == "" {
		return uc.agg.InvalidateAll(ctx, "")
	}
	return uc.agg.InvalidateAll(ctx, aggregate.Key(name, ""))
}
