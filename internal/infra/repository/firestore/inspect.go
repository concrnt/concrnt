package firestore

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/concrnt/concrnt/internal/infra/repository/inspect"
)

// inspector is the Firestore inspect.Inspector: white-box document reads the
// contract suites assert on, for tests only.
type inspector struct {
	client *firestore.Client
}

// NewInspector returns the inspect.Inspector over client.
func NewInspector(client *firestore.Client) inspect.Inspector {
	return &inspector{client: client}
}

// fetch decodes a document into dest; a missing document is false, nil.
func fetch(ctx context.Context, ref *firestore.DocumentRef, dest any) (bool, error) {
	snap, err := ref.Get(ctx)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if !snap.Exists() {
		return false, nil
	}
	return true, decode(snap, dest)
}

func (i *inspector) Commit(ctx context.Context, id string) (*inspect.CommitState, error) {
	var c commitDoc
	found, err := fetch(ctx, i.client.Collection(colCommits).Doc(id), &c)
	if err != nil || !found {
		return nil, err
	}
	return &inspect.CommitState{
		IP:          c.IP,
		Document:    c.Document,
		Proof:       c.Proof,
		Owner:       c.Owner,
		GcCandidate: c.GcCandidate,
		CDate:       c.CDate,
	}, nil
}

func (i *inspector) SetCommitCDate(ctx context.Context, id string, cDate time.Time) error {
	_, err := i.client.Collection(colCommits).Doc(id).Update(ctx, []firestore.Update{{Path: "cDate", Value: cDate}})
	return err
}

func (i *inspector) Record(ctx context.Context, documentID string) (*inspect.RecordState, error) {
	var rec recordDoc
	found, err := fetch(ctx, i.client.Collection(colRecords).Doc(documentID), &rec)
	if err != nil || !found {
		return nil, err
	}
	return &inspect.RecordState{
		Owner:         rec.Owner,
		Schema:        rec.Schema,
		Redirect:      strPtr(rec.Redirect),
		Distributions: rec.Distributions,
		CreatedAt:     rec.CreatedAt,
	}, nil
}

func (i *inspector) RecordKey(ctx context.Context, uri string) (*inspect.KeyState, error) {
	var key recordKeyDoc
	found, err := fetch(ctx, i.client.Collection(colRecordKeys).Doc(keyID(uri)), &key)
	if err != nil || !found {
		return nil, err
	}
	return &inspect.KeyState{
		RecordID:        strPtr(key.RecordID),
		RecordCreatedAt: key.RecordCreatedAt,
	}, nil
}

func (i *inspector) Entity(ctx context.Context, ccid string) (*inspect.EntityState, error) {
	var e entityDoc
	found, err := fetch(ctx, i.client.Collection(colEntities).Doc(ccid), &e)
	if err != nil || !found {
		return nil, err
	}
	return &inspect.EntityState{
		DocumentID: e.DocumentID,
		Domain:     e.Domain,
		CreatedAt:  e.CreatedAt,
	}, nil
}

func (i *inspector) Association(ctx context.Context, documentID string) (*inspect.AssociationState, error) {
	var a associationDoc
	found, err := fetch(ctx, i.client.Collection(colAssociations).Doc(documentID), &a)
	if err != nil || !found {
		return nil, err
	}
	return &inspect.AssociationState{
		Owner:   a.Owner,
		Author:  a.Author,
		Variant: strPtr(a.Variant),
		Unique:  a.Unique,
	}, nil
}

func (i *inspector) AssociationCountByUnique(ctx context.Context, unique string) (int64, error) {
	snaps, err := i.client.Collection(colAssociations).Where("unique", "==", unique).Select().Documents(ctx).GetAll()
	if err != nil {
		return 0, err
	}
	return int64(len(snaps)), nil
}

func (i *inspector) ack(ctx context.Context, collection, from, to, schema string) (*inspect.AckState, error) {
	var a ackDoc
	found, err := fetch(ctx, i.client.Collection(collection).Doc(ackID(from, to, schema)), &a)
	if err != nil || !found {
		return nil, err
	}
	return &inspect.AckState{
		DocumentID: a.DocumentID,
		Valid:      a.Valid,
		CreatedAt:  a.CreatedAt,
	}, nil
}

func (i *inspector) Ack(ctx context.Context, from, to, schema string) (*inspect.AckState, error) {
	return i.ack(ctx, colAcks, from, to, schema)
}

func (i *inspector) Acked(ctx context.Context, from, to, schema string) (*inspect.AckState, error) {
	return i.ack(ctx, colAckeds, from, to, schema)
}

func (i *inspector) AckCount(ctx context.Context) (int64, error) {
	snaps, err := i.client.Collection(colAcks).Select().Documents(ctx).GetAll()
	if err != nil {
		return 0, err
	}
	return int64(len(snaps)), nil
}
