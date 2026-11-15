package record

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/concrnt/concrnt"
	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// direction maps the repository's order string ("desc", anything else asc).
func direction(order string) firestore.Direction {
	if order == "desc" {
		return firestore.Desc
	}
	return firestore.Asc
}

// applyWindow adds the created-at window, the (timestamp, id) ordering the
// SQL backends use ("ORDER BY created_at DESC, id DESC") and the limit. The
// range field is the first orderBy, as Firestore requires.
func applyWindow(q firestore.Query, tsField string, idField string, since, until *time.Time, limit int, order string) firestore.Query {
	if since != nil {
		q = q.Where(tsField, ">=", *since)
	}
	if until != nil {
		q = q.Where(tsField, "<=", *until)
	}
	dir := direction(order)
	q = q.OrderBy(tsField, dir).OrderBy(idField, dir)
	if limit > 0 {
		q = q.Limit(limit)
	}
	return q
}

// getKey reads one key outside a transaction; nil when it does not exist.
func (r *Repository) getKey(ctx context.Context, uri string) (*recordKeyDoc, error) {
	snap, err := r.keyRef(uri).Get(ctx)
	if err != nil {
		if fs.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	key := recordKeyFromMap(snap.Data())
	return &key, nil
}

// decodeKeys decodes key snapshots. A query ordered by recordCreatedAt
// returns no placeholder (it lacks the field), so only a listing by key
// meets one.
func decodeKeys(snaps []*firestore.DocumentSnapshot) []recordKeyDoc {
	keys := make([]recordKeyDoc, 0, len(snaps))
	for _, snap := range snaps {
		keys = append(keys, recordKeyFromMap(snap.Data()))
	}
	return keys
}

// signedDocument builds the signed document of a key holding a record from
// the key alone.
func (d recordKeyDoc) signedDocument() (concrnt.SignedDocument, error) {
	return repoutil.RecordDocument(d.URI, d.Owner, d.RecordID, d.Document, d.Proof)
}

// keyRows builds the listing rows of keys in their order, carrying
// recordCreatedAt as the sort key. A placeholder key becomes a key-only row
// holding just its cckv (CIP-5 §3.2.1).
func keyRows(keys []recordKeyDoc) ([]ucrecord.QueryRow, error) {
	rows := make([]ucrecord.QueryRow, 0, len(keys))
	for _, k := range keys {
		if k.isPlaceholder() {
			uri := k.URI
			rows = append(rows, ucrecord.QueryRow{Row: concrnt.SignedDocument{CCKV: &uri}})
			continue
		}
		sd, err := k.signedDocument()
		if err != nil {
			return nil, err
		}
		rows = append(rows, ucrecord.QueryRow{Row: sd, CreatedAt: *k.RecordCreatedAt})
	}
	return rows, nil
}

// commitRow builds the listing row of a state document (association, ack)
// from the signed document it carries; owner is the namespace its ccfs URI
// is composed in.
func commitRow(owner, id, document, proof string, createdAt time.Time) (ucrecord.QueryRow, error) {
	sd, err := repoutil.CommitDocument(owner, id, document, proof)
	if err != nil {
		return ucrecord.QueryRow{}, err
	}
	return ucrecord.QueryRow{Row: sd, CreatedAt: createdAt}, nil
}
