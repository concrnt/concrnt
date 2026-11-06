package firestore

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// queryShape describes the filter / order structure of one query the
// repositories issue. It is the Firestore counterpart of the index DDL of
// the SQL backends (CONTRACT.md C5): deploy/firestore/firestore.indexes.json
// is generated from the shapes and pinned by indexes_test.go, because the
// emulator does not enforce composite indexes, and VerifyIndexes probes them
// against a real database at start.
type queryShape struct {
	Name       string
	Collection string
	// Equalities are always-present equality filters; Optional ones may or
	// may not be present in a given call (each combination must be served).
	Equalities []string
	Optional   []string
	// ArrayContains is an array-contains filter field, if any.
	ArrayContains string
	// Range is the field carrying an inequality (it must also lead OrderBy).
	Range string
	// OrderBy is the ordering; firestore.DocumentID stands for __name__.
	OrderBy []orderField
	// BothDirections means the query runs with OrderBy in either direction.
	BothDirections bool
}

type orderField struct {
	Field string
	Desc  bool
}

var timelineOrder = []orderField{{"recordCreatedAt", true}, {"recordID", true}}

// shapes is the registry of every query the repositories issue, with the
// method it serves. Equality-only queries and single-field range queries
// are served by automatic indexes and are listed so the exemption check
// still covers their fields.
var shapes = []queryShape{
	// GetSignedDocument(s) / GetHierarchicalRecordPolicies by ccfs, the delete cascades
	{Name: "record_keys.byRecordID", Collection: ColRecordKeys, Equalities: []string{"recordID"}},
	// QueryByParent, the chunkline children scans
	{Name: "record_keys.byParent", Collection: ColRecordKeys, Equalities: []string{"parentURI"}, Optional: []string{"schema", "author"}, Range: "recordCreatedAt", OrderBy: timelineOrder, BothDirections: true},
	// QueryByParentOrderByKey (CIP-5 orderby=key, byte order of the uri)
	{Name: "record_keys.byParentOrderByKey", Collection: ColRecordKeys, Equalities: []string{"parentURI"}, Optional: []string{"schema", "author"}, Range: "uri", OrderBy: []orderField{{"uri", false}}, BothDirections: true},
	// QueryByPrefix
	{Name: "record_keys.byAncestor", Collection: ColRecordKeys, ArrayContains: "ancestors", Optional: []string{"schema", "author"}, Range: "recordCreatedAt", OrderBy: timelineOrder, BothDirections: true},
	// QueryRecordSubtree: the second ordering field drops placeholder keys
	{Name: "record_keys.subtree", Collection: ColRecordKeys, Range: "uri", OrderBy: []orderField{{"uri", false}, {"recordCreatedAt", false}}},
	// GetAssociatedRecords
	{Name: "associations.byTarget", Collection: ColAssociations, Equalities: []string{"targetURI"}, Optional: []string{"schema", "variant", "author"}, Range: "createdAt", OrderBy: []orderField{{"createdAt", true}, {firestore.DocumentID, true}}, BothDirections: true},
	// Compute* and the delete cascades
	{Name: "associations.countBySchema", Collection: ColAssociations, Equalities: []string{"targetURI"}, Optional: []string{"schema"}},
	{Name: "associations.byUnique", Collection: ColAssociations, Equalities: []string{"unique"}},
	// GetAcknowledgeRecords / ComputeAcknowledgeCounts
	{Name: "acks.byFrom", Collection: ColAcks, Equalities: []string{"from", "valid"}, Optional: []string{"schema"}, Range: "createdAt", OrderBy: []orderField{{"createdAt", true}, {"documentID", true}}, BothDirections: true},
	{Name: "acks.byDocumentID", Collection: ColAcks, Equalities: []string{"documentID"}},
	{Name: "ackeds.byTo", Collection: ColAckeds, Equalities: []string{"to", "valid"}, Optional: []string{"schema"}, Range: "createdAt", OrderBy: []orderField{{"createdAt", true}, {"documentID", true}}, BothDirections: true},
	{Name: "ackeds.byDocumentID", Collection: ColAckeds, Equalities: []string{"documentID"}},
	// GetAllCommitLogs
	{Name: "commits.byOwner", Collection: ColCommits, Equalities: []string{"owner"}, OrderBy: []orderField{{"cDate", false}}},
	// QueryCommitLogs (the replication cursor)
	{Name: "commits.query", Collection: ColCommits, Optional: []string{"owner"}, Range: "cDate", OrderBy: []orderField{{"cDate", false}, {firestore.DocumentID, false}}, BothDirections: true},
	// MarkCommitLogsGcCandidateByOwner
	{Name: "commits.unflaggedByOwner", Collection: ColCommits, Equalities: []string{"owner", "gcCandidate"}},
	// CountGcCandidates / ListGcCandidates
	{Name: "commits.gcCandidates", Collection: ColCommits, Equalities: []string{"gcCandidate"}, OrderBy: []orderField{{firestore.DocumentID, false}}},
	// ListCommitLogs
	{Name: "commits.page", Collection: ColCommits, Optional: []string{"owner"}, OrderBy: []orderField{{firestore.DocumentID, false}}},
	{Name: "entities.byAlias", Collection: ColEntities, Equalities: []string{"alias"}},
	{Name: "entities.byDocumentID", Collection: ColEntities, Equalities: []string{"documentID"}},
	{Name: "servers.byCSID", Collection: ColServers, Equalities: []string{"csID"}},
	// aggregate.Store.InvalidateDefinition
	{Name: "aggregate_entries.byName", Collection: ColAggregateEntries, Equalities: []string{"name"}},
}

// needsCompositeIndex reports whether a shape (with a given set of optional
// filters present) is beyond what automatic single-field indexes serve: an
// array-contains with an ordering, an equality with an ordering on another
// field, or an ordering over two fields.
func (s queryShape) needsCompositeIndex(optional []string) bool {
	if s.ArrayContains != "" && len(s.OrderBy) > 0 {
		return true
	}
	fields := 0
	for _, o := range s.OrderBy {
		if o.Field != firestore.DocumentID {
			fields++
		}
	}
	if fields == 0 {
		return false
	}
	if len(s.Equalities)+len(optional) == 0 {
		return fields > 1
	}
	return true
}

// indexFile mirrors the Firebase CLI's firestore.indexes.json.
type indexFile struct {
	Indexes        []compositeIndex `json:"indexes"`
	FieldOverrides []fieldOverride  `json:"fieldOverrides"`
}

type compositeIndex struct {
	CollectionGroup string       `json:"collectionGroup"`
	QueryScope      string       `json:"queryScope"`
	Fields          []indexField `json:"fields"`
}

type indexField struct {
	FieldPath   string `json:"fieldPath"`
	Order       string `json:"order,omitempty"`
	ArrayConfig string `json:"arrayConfig,omitempty"`
}

type fieldOverride struct {
	CollectionGroup string  `json:"collectionGroup"`
	FieldPath       string  `json:"fieldPath"`
	Indexes         []index `json:"indexes"`
}

type index struct {
	Order       string `json:"order,omitempty"`
	ArrayConfig string `json:"arrayConfig,omitempty"`
	QueryScope  string `json:"queryScope,omitempty"`
}

// exemptFields are large or never-queried fields whose automatic single-field
// indexes are disabled: they would only add index writes and storage.
var exemptFields = map[string][]string{
	ColCommits:          {"document", "proof", "ip"},
	ColRecords:          {"policies", "distributions", "redirect", "owner", "author", "schema"},
	ColRecordKeys:       {"policies", "distributions", "redirect", "owner", "cleanOnUpdate"},
	ColAssociations:     {"owner"},
	ColEntityMetas:      {"info", "inviter"},
	ColServers:          {"wellKnown", "tag", "layer"},
	ColSubscriptions:    {"subscription", "schemas", "prefixes"},
	ColAbuseReports:     {"body", "ip", "reporter", "targetURI"},
	ColAggregateEntries: {"value", "asOf", "deltas"},
}

// requiredIndexes derives the composite indexes the registered shapes need.
// A shape with optional equality filters gets one index per optional filter
// on top of the mandatory ones; Firestore merges those at query time for any
// combination. __name__ is implicit in every composite index and is omitted.
func requiredIndexes() []compositeIndex {
	seen := map[string]bool{}
	var out []compositeIndex
	add := func(collection string, fields []indexField) {
		raw, _ := json.Marshal(fields)
		key := collection + string(raw)
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, compositeIndex{CollectionGroup: collection, QueryScope: "COLLECTION", Fields: fields})
	}

	for _, s := range shapes {
		dirs := []bool{false}
		if s.BothDirections {
			dirs = []bool{false, true}
		}
		for _, desc := range dirs {
			orderFields := func() []indexField {
				var fs []indexField
				for _, o := range s.OrderBy {
					if o.Field == firestore.DocumentID {
						continue
					}
					wantDesc := o.Desc
					if s.BothDirections {
						wantDesc = desc
					}
					order := "ASCENDING"
					if wantDesc {
						order = "DESCENDING"
					}
					fs = append(fs, indexField{FieldPath: o.Field, Order: order})
				}
				return fs
			}
			prefix := func(extra string) []indexField {
				var fs []indexField
				if s.ArrayContains != "" {
					fs = append(fs, indexField{FieldPath: s.ArrayContains, ArrayConfig: "CONTAINS"})
				}
				for _, e := range s.Equalities {
					fs = append(fs, indexField{FieldPath: e, Order: "ASCENDING"})
				}
				if extra != "" {
					fs = append(fs, indexField{FieldPath: extra, Order: "ASCENDING"})
				}
				return fs
			}
			if s.needsCompositeIndex(nil) {
				add(s.Collection, append(prefix(""), orderFields()...))
			}
			for _, o := range s.Optional {
				if s.needsCompositeIndex([]string{o}) {
					add(s.Collection, append(prefix(o), orderFields()...))
				}
			}
		}
	}
	return out
}

// requiredIndexFile is the full indexes.json content derived from the shapes
// and exemptions.
func requiredIndexFile() indexFile {
	f := indexFile{Indexes: requiredIndexes(), FieldOverrides: []fieldOverride{}}
	collections := make([]string, 0, len(exemptFields))
	for c := range exemptFields {
		collections = append(collections, c)
	}
	sort.Strings(collections)
	for _, c := range collections {
		for _, field := range exemptFields[c] {
			f.FieldOverrides = append(f.FieldOverrides, fieldOverride{CollectionGroup: c, FieldPath: field, Indexes: []index{}})
		}
	}
	return f
}

// probe builds a representative query for the shape with every optional
// filter present, in the given direction.
func (s queryShape) probe(client *firestore.Client, desc bool) firestore.Query {
	q := client.Collection(s.Collection).Query
	for _, f := range s.Equalities {
		q = q.Where(f, "==", probeValue(f))
	}
	for _, f := range s.Optional {
		q = q.Where(f, "==", probeValue(f))
	}
	if s.ArrayContains != "" {
		q = q.Where(s.ArrayContains, "array-contains", "cckv://probe")
	}
	if s.Range != "" {
		q = q.Where(s.Range, "<", probeValue(s.Range))
	}
	for _, o := range s.OrderBy {
		// a both-directions shape flips every field together; a fixed shape
		// keeps its declared directions
		wantDesc := o.Desc
		if s.BothDirections {
			wantDesc = desc
		}
		dir := firestore.Asc
		if wantDesc {
			dir = firestore.Desc
		}
		q = q.OrderBy(o.Field, dir)
	}
	return q.Limit(1)
}

func probeValue(field string) any {
	switch field {
	case "valid", "gcCandidate":
		return false
	case "recordCreatedAt", "createdAt", "cDate":
		return time.Unix(0, 0)
	}
	return "probe"
}

// VerifyIndexes runs every registered query shape once (Limit 1, so at most
// one read each) and logs the composite indexes Firestore reports missing,
// with the console URL to create them. It is best-effort: any other error is
// logged and ignored.
func VerifyIndexes(ctx context.Context, client *firestore.Client) {
	missing := 0
	for _, s := range shapes {
		dirs := []bool{false}
		if s.BothDirections {
			dirs = []bool{false, true}
		}
		for _, desc := range dirs {
			_, err := s.probe(client, desc).Documents(ctx).GetAll()
			if err == nil {
				continue
			}
			if status.Code(err) == codes.FailedPrecondition {
				missing++
				slog.Error("firestore composite index missing", slog.String("shape", s.Name), slog.String("detail", err.Error()))
				continue
			}
			if errors.Is(err, context.Canceled) {
				return
			}
			slog.Warn("firestore index probe failed", slog.String("shape", s.Name), slog.String("error", err.Error()))
		}
	}
	if missing == 0 {
		slog.Info("firestore composite indexes verified", slog.Int("shapes", len(shapes)))
	}
}
