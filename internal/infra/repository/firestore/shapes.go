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

// queryShape describes the filter/order structure of one repository query.
// deploy/firestore/firestore.indexes.json must cover every shape (checked
// offline by shapes_test.go, because the emulator does not enforce composite
// indexes), and VerifyIndexes probes them against a real database.
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

// shapes is the registry of every query the repositories issue. Equality-only
// queries and single-field range queries are served by automatic indexes and
// are listed so the exemption check still covers their fields.
var shapes = []queryShape{
	{Name: "record_keys.byRecordID", Collection: colRecordKeys, Equalities: []string{"recordID"}},
	{Name: "record_keys.byParent", Collection: colRecordKeys, Equalities: []string{"parentGen"}, Optional: []string{"schema", "author"}, Range: "recordCreatedAt", OrderBy: timelineOrder, BothDirections: true},
	{Name: "record_keys.byAncestor", Collection: colRecordKeys, ArrayContains: "ancestors", Optional: []string{"schema", "author"}, Range: "recordCreatedAt", OrderBy: timelineOrder, BothDirections: true},
	{Name: "record_keys.subtree", Collection: colRecordKeys, Range: "uri", OrderBy: []orderField{{"uri", false}}},
	{Name: "record_keys.chunkline", Collection: colRecordKeys, Equalities: []string{"parentGen"}, Range: "recordCreatedAt", OrderBy: timelineOrder, BothDirections: true},
	{Name: "associations.byTarget", Collection: colAssociations, Equalities: []string{"targetGen"}, Optional: []string{"schema", "variant", "author"}, Range: "createdAt", OrderBy: []orderField{{"createdAt", true}, {firestore.DocumentID, true}}, BothDirections: true},
	{Name: "associations.countBySchema", Collection: colAssociations, Equalities: []string{"targetGen"}, Optional: []string{"schema"}},
	{Name: "associations.byUnique", Collection: colAssociations, Equalities: []string{"unique"}},
	{Name: "acks.byFrom", Collection: colAcks, Equalities: []string{"from", "valid"}, Optional: []string{"schema"}, Range: "createdAt", OrderBy: []orderField{{"createdAt", true}, {"documentID", true}}, BothDirections: true},
	{Name: "acks.byDocumentID", Collection: colAcks, Equalities: []string{"documentID"}},
	{Name: "ackeds.byTo", Collection: colAckeds, Equalities: []string{"to", "valid"}, Optional: []string{"schema"}, Range: "createdAt", OrderBy: []orderField{{"createdAt", true}, {"documentID", true}}, BothDirections: true},
	{Name: "ackeds.byDocumentID", Collection: colAckeds, Equalities: []string{"documentID"}},
	{Name: "commits.byOwner", Collection: colCommits, Equalities: []string{"owner"}, OrderBy: []orderField{{"cDate", false}}},
	{Name: "commits.unflaggedByOwner", Collection: colCommits, Equalities: []string{"owner", "gcCandidate"}},
	{Name: "commits.gcCandidates", Collection: colCommits, Equalities: []string{"gcCandidate"}, OrderBy: []orderField{{firestore.DocumentID, false}}},
	{Name: "commits.page", Collection: colCommits, Optional: []string{"owner"}, OrderBy: []orderField{{firestore.DocumentID, false}}},
	{Name: "entities.byAlias", Collection: colEntities, Equalities: []string{"alias"}},
	{Name: "entities.byDocumentID", Collection: colEntities, Equalities: []string{"documentID"}},
	{Name: "servers.byCSID", Collection: colServers, Equalities: []string{"csID"}},
}

// needsCompositeIndex reports whether a shape (with a given set of optional
// filters present) is beyond what automatic single-field indexes serve.
func (s queryShape) needsCompositeIndex(optional []string) bool {
	if s.ArrayContains != "" && len(s.OrderBy) > 0 {
		return true
	}
	if len(s.OrderBy) == 0 {
		return false
	}
	eq := len(s.Equalities) + len(optional)
	if eq == 0 {
		return false
	}
	// an equality plus ordering on another field needs a composite; ordering
	// by __name__ alone is the automatic index of the equality field
	if len(s.OrderBy) == 1 && s.OrderBy[0].Field == firestore.DocumentID {
		return false
	}
	return true
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
