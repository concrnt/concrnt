package record

import (
	"context"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/concrnt/concrnt"
	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// getAllBatch is the chunk size for BatchGetDocuments: commit documents are
// up to 32 KiB, and a response must stay under Firestore's 10 MiB limit.
const getAllBatch = 100

// inBatch is Firestore's cap on the disjunctions of one "in" filter.
const inBatch = 30

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

// getDoc reads one document outside a transaction; missing is (nil, false, nil).
func (r *Repository) getDoc(ctx context.Context, ref *firestore.DocumentRef) (map[string]any, bool, error) {
	snap, err := ref.Get(ctx)
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

// getAll reads documents by reference in batches outside a transaction;
// missing ones are absent from the result.
func (r *Repository) getAll(ctx context.Context, refs []*firestore.DocumentRef) (map[string]map[string]any, error) {
	out := make(map[string]map[string]any, len(refs))
	for start := 0; start < len(refs); start += getAllBatch {
		snaps, err := r.client.GetAll(ctx, refs[start:min(start+getAllBatch, len(refs))])
		if err != nil {
			return nil, err
		}
		for _, snap := range snaps {
			if snap.Exists() {
				out[snap.Ref.ID] = snap.Data()
			}
		}
	}
	return out, nil
}

// getCommits batch-fetches commits/{id}; commits that no longer exist are
// absent from the result (the SQL backends would have dropped their rows by
// cascade).
func (r *Repository) getCommits(ctx context.Context, ids []string) (map[string]commitDoc, error) {
	refs := make([]*firestore.DocumentRef, 0, len(ids))
	for _, id := range ids {
		refs = append(refs, r.col(fs.ColCommits).Doc(id))
	}
	docs, err := r.getAll(ctx, refs)
	if err != nil {
		return nil, err
	}
	out := make(map[string]commitDoc, len(docs))
	for id, m := range docs {
		out[id] = commitFromMap(m)
	}
	return out, nil
}

func (r *Repository) getKey(ctx context.Context, uri string) (*recordKeyDoc, error) {
	m, exists, err := r.getDoc(ctx, r.col(fs.ColRecordKeys).Doc(keyID(uri)))
	if err != nil || !exists {
		return nil, err
	}
	key := recordKeyFromMap(m)
	return &key, nil
}

// decodeKeys decodes key snapshots, leaving placeholders out when
// recordsOnly is set.
func decodeKeys(snaps []*firestore.DocumentSnapshot, recordsOnly bool) []recordKeyDoc {
	keys := make([]recordKeyDoc, 0, len(snaps))
	for _, snap := range snaps {
		k := recordKeyFromMap(snap.Data())
		if recordsOnly && k.isPlaceholder() {
			continue
		}
		keys = append(keys, k)
	}
	return keys
}

// keysToQueryRows resolves the commit of every listed key and builds the
// rows in the keys' order, carrying recordCreatedAt as the sort key. A
// placeholder key becomes a key-only row holding just its cckv (CIP-5
// §3.2.1); a key whose commit is gone is skipped.
func (r *Repository) keysToQueryRows(ctx context.Context, keys []recordKeyDoc) ([]ucrecord.QueryRow, error) {
	ids := make([]string, 0, len(keys))
	for _, k := range keys {
		if !k.isPlaceholder() {
			ids = append(ids, k.RecordID)
		}
	}
	commits, err := r.getCommits(ctx, ids)
	if err != nil {
		return nil, err
	}

	rows := make([]ucrecord.QueryRow, 0, len(keys))
	for _, k := range keys {
		uri := k.URI
		if k.isPlaceholder() {
			rows = append(rows, ucrecord.QueryRow{Row: concrnt.SignedDocument{CCKV: &uri}})
			continue
		}
		commit, ok := commits[k.RecordID]
		if !ok {
			continue
		}
		proof, err := commit.proof()
		if err != nil {
			return nil, err
		}
		ccfs := concrnt.ComposeCCFSURI(k.Owner, concrnt.CCFSTypeConcrnt, k.RecordID)
		rows = append(rows, ucrecord.QueryRow{
			Row:       concrnt.SignedDocument{CCKV: &uri, CCFS: &ccfs, Document: commit.Document, Proof: proof},
			CreatedAt: *k.RecordCreatedAt,
		})
	}
	return rows, nil
}
