//go:build contractv1

package record

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"

	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
)

// inspector is the Firestore repotest.Inspector: white-box document reads
// the contract suites assert on, for tests only.
type inspector struct {
	client *firestore.Client
}

func newInspector(client *firestore.Client) repotest.Inspector {
	return &inspector{client: client}
}

// fetch reads a document; a missing document is (nil, false, nil).
func (i *inspector) fetch(ctx context.Context, collection, id string) (map[string]any, bool, error) {
	snap, err := i.client.Collection(collection).Doc(id).Get(ctx)
	if err != nil {
		if fs.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if !snap.Exists() {
		return nil, false, nil
	}
	return snap.Data(), true, nil
}

func (i *inspector) count(ctx context.Context, q firestore.Query) (int64, error) {
	res, err := q.NewAggregationQuery().WithCount("n").Get(ctx)
	if err != nil {
		return 0, err
	}
	return aggregationCount(res)
}

func (i *inspector) Commit(ctx context.Context, id string) (*repotest.CommitState, error) {
	m, found, err := i.fetch(ctx, fs.ColCommits, id)
	if err != nil || !found {
		return nil, err
	}
	c := commitFromMap(m)
	return &repotest.CommitState{
		IP:          c.IP,
		Document:    c.Document,
		Proof:       c.Proof,
		Owner:       c.Owner,
		GcCandidate: c.GcCandidate,
		CDate:       c.CDate,
	}, nil
}

func (i *inspector) SetCommitCDate(ctx context.Context, id string, cDate time.Time) error {
	_, err := i.client.Collection(fs.ColCommits).Doc(id).Update(ctx, []firestore.Update{{Path: "cDate", Value: cDate}})
	return err
}

func (i *inspector) SetRecordCreatedAt(ctx context.Context, documentID string, createdAt time.Time) error {
	_, err := i.client.Collection(fs.ColRecords).Doc(documentID).Update(ctx, []firestore.Update{{Path: "createdAt", Value: createdAt}})
	return err
}

func (i *inspector) Record(ctx context.Context, documentID string) (*repotest.RecordState, error) {
	m, found, err := i.fetch(ctx, fs.ColRecords, documentID)
	if err != nil || !found {
		return nil, err
	}
	rec := recordFromMap(m)
	distributions := rec.Distributions
	if distributions == nil {
		distributions = []string{}
	}
	return &repotest.RecordState{
		Owner:         rec.Owner,
		Schema:        rec.Schema,
		Redirect:      fs.StrPtr(rec.Redirect),
		Distributions: distributions,
		CreatedAt:     rec.CreatedAt,
	}, nil
}

func (i *inspector) RecordKey(ctx context.Context, uri string) (*repotest.KeyState, error) {
	m, found, err := i.fetch(ctx, fs.ColRecordKeys, keyID(uri))
	if err != nil || !found {
		return nil, err
	}
	key := recordKeyFromMap(m)
	return &repotest.KeyState{
		RecordID:        fs.StrPtr(key.RecordID),
		RecordCreatedAt: key.RecordCreatedAt,
		Redirect:        fs.StrPtr(key.Redirect),
	}, nil
}

func (i *inspector) Entity(ctx context.Context, ccid string) (*repotest.EntityState, error) {
	m, found, err := i.fetch(ctx, fs.ColEntities, ccid)
	if err != nil || !found {
		return nil, err
	}
	e := entityFromMap(m)
	return &repotest.EntityState{DocumentID: e.DocumentID, Domain: e.Domain, CreatedAt: e.CreatedAt}, nil
}

func (i *inspector) Association(ctx context.Context, documentID string) (*repotest.AssociationState, error) {
	m, found, err := i.fetch(ctx, fs.ColAssociations, documentID)
	if err != nil || !found {
		return nil, err
	}
	a := associationFromMap(m)
	return &repotest.AssociationState{Owner: a.Owner, Author: a.Author, Variant: fs.StrPtr(a.Variant), Unique: a.Unique}, nil
}

func (i *inspector) AssociationCountByUnique(ctx context.Context, unique string) (int64, error) {
	return i.count(ctx, i.client.Collection(fs.ColAssociations).Where("unique", "==", unique))
}

func (i *inspector) ack(ctx context.Context, collection, from, to, schema string) (*repotest.AckState, error) {
	m, found, err := i.fetch(ctx, collection, ackID(from, to, schema))
	if err != nil || !found {
		return nil, err
	}
	a := ackFromMap(m)
	return &repotest.AckState{DocumentID: a.DocumentID, Valid: a.Valid, CreatedAt: a.CreatedAt}, nil
}

func (i *inspector) Ack(ctx context.Context, from, to, schema string) (*repotest.AckState, error) {
	return i.ack(ctx, fs.ColAcks, from, to, schema)
}

func (i *inspector) Acked(ctx context.Context, from, to, schema string) (*repotest.AckState, error) {
	return i.ack(ctx, fs.ColAckeds, from, to, schema)
}

func (i *inspector) AckCount(ctx context.Context) (int64, error) {
	return i.count(ctx, i.client.Collection(fs.ColAcks).Query)
}

func (i *inspector) AbuseReportCount(ctx context.Context) (int64, error) {
	return i.count(ctx, i.client.Collection(fs.ColAbuseReports).Query)
}

func recordFromMap(m map[string]any) recordDoc {
	createdAt, _ := timeAt(m, "createdAt")
	return recordDoc{
		Owner:         str(m, "owner"),
		Author:        str(m, "author"),
		Schema:        str(m, "schema"),
		Redirect:      str(m, "redirect"),
		Policies:      str(m, "policies"),
		Distributions: strs(m, "distributions"),
		CreatedAt:     createdAt.UTC(),
	}
}
