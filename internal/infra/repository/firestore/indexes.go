package firestore

import (
	"context"
	"errors"
	"log/slog"
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
	// ComputeAssociationSummary
	{Name: "associations.summary", Collection: ColAssociations, Equalities: []string{"targetURI"}},
	// Cleanup (the associations of ended incarnations)
	{Name: "associations.byGen", Collection: ColAssociations, Equalities: []string{"targetGen"}},
	// GetAcknowledgeRecords / ComputeAcknowledgeCounts
	{Name: "acks.byFrom", Collection: ColAcks, Equalities: []string{"from", "valid"}, Optional: []string{"schema"}, Range: "createdAt", OrderBy: []orderField{{"createdAt", true}, {"documentID", true}}, BothDirections: true},
	{Name: "ackeds.byTo", Collection: ColAckeds, Equalities: []string{"to", "valid"}, Optional: []string{"schema"}, Range: "createdAt", OrderBy: []orderField{{"createdAt", true}, {"documentID", true}}, BothDirections: true},
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
	// GetEntityByAlias
	{Name: "entities.byAlias", Collection: ColEntities, Equalities: []string{"alias"}},
	{Name: "servers.byCSID", Collection: ColServers, Equalities: []string{"csID"}},
	// aggregate.Store.InvalidateDefinition
	{Name: "aggregate_entries.byName", Collection: ColAggregateEntries, Equalities: []string{"name"}},
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
