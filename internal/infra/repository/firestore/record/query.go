//go:build contractv1

package record

import (
	"context"
	"strings"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// recordFilters adds the optional schema / author equalities; a placeholder
// key has neither field, so either filter drops it.
func recordFilters(q firestore.Query, schema, author string) firestore.Query {
	if schema != "" {
		q = q.Where("schema", "==", schema)
	}
	if author != "" {
		q = q.Where("author", "==", author)
	}
	return q
}

// parentURIOf normalises the parent of a listing: the owner root
// (cckv://<owner>, CIP-5 §3.1) is the parentURI top-level keys carry.
func parentURIOf(parent string) (string, error) {
	parsed, err := concrnt.ParseCCURI(parent)
	if err != nil {
		return "", err
	}
	if parsed.Key == "" {
		return "cckv://" + parsed.Owner, nil
	}
	return parent, nil
}

func (r *Repository) QueryByParent(
	ctx context.Context,
	parent, schema, author string,
	since, until *time.Time,
	limit int,
	order string,
) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryByParent")
	defer span.End()

	parentURI, err := parentURIOf(parent)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	q := recordFilters(r.col(fs.ColRecordKeys).Where("parentURI", "==", parentURI), schema, author)
	q = applyWindow(q, "recordCreatedAt", "recordID", since, until, limit, order)

	snaps, err := q.Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, fs.QueryErr(err)
	}
	return r.keysToQueryRows(ctx, decodeKeys(snaps, true))
}

// QueryByParentOrderByKey lists parent's direct children ordered by their
// cckv (CIP-5 orderby=key). Nothing orders by a record field, so placeholder
// keys come back as key-only rows; a schema or author filter compares record
// fields and so drops them. since / until are inclusive key bounds, the
// cursors of §3.3 for this ordering. Firestore orders strings by UTF-8
// bytes (CONTRACT.md "Key order").
func (r *Repository) QueryByParentOrderByKey(
	ctx context.Context,
	parent, schema, author string,
	since, until *string,
	limit int,
	order string,
) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryByParentOrderByKey")
	defer span.End()

	parentURI, err := parentURIOf(parent)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	q := recordFilters(r.col(fs.ColRecordKeys).Where("parentURI", "==", parentURI), schema, author)
	if since != nil {
		q = q.Where("uri", ">=", *since)
	}
	if until != nil {
		q = q.Where("uri", "<=", *until)
	}
	q = q.OrderBy("uri", direction(order))
	if limit > 0 {
		q = q.Limit(limit)
	}

	snaps, err := q.Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, fs.QueryErr(err)
	}
	return r.keysToQueryRows(ctx, decodeKeys(snaps, false))
}

// prefixOverfetch is the page size used while post-filtering a non-directory
// prefix (SQL "LIKE 'cckv://o/tl/e%'" also matches sibling "e2" keys).
const prefixOverfetch = 64

// QueryByPrefix serves "uri LIKE prefix%". The prefix is split at its last
// '/' into the directory that every match lives under, matched with
// array-contains on the key's ancestors, and an optional last-segment prefix
// that is checked client-side. A prefix without an owner and path (the
// all-owners LIKE) is rejected: it has no indexed form.
func (r *Repository) QueryByPrefix(
	ctx context.Context,
	prefix, schema, author string,
	since, until *time.Time,
	limit int,
	order string,
) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryByPrefix")
	defer span.End()

	scheme := strings.Index(prefix, "://")
	cut := strings.LastIndex(prefix, "/")
	if scheme < 0 || cut <= scheme+2 {
		err := domain.ValidationError{Field: "prefix", Message: "prefix must include the owner and a path (cckv://<owner>/...)"}
		span.RecordError(err)
		return nil, err
	}
	dir := prefix[:cut]
	exact := cut == len(prefix)-1

	q := recordFilters(r.col(fs.ColRecordKeys).Where("ancestors", "array-contains", dir), schema, author)
	q = applyWindow(q, "recordCreatedAt", "recordID", since, until, 0, order)

	page := limit
	if page <= 0 || !exact {
		page = max(limit*2, prefixOverfetch)
	}

	keys := []recordKeyDoc{}
	var last *firestore.DocumentSnapshot
	for {
		pq := q.Limit(page)
		if last != nil {
			pq = pq.StartAfter(last)
		}
		snaps, err := pq.Documents(ctx).GetAll()
		if err != nil {
			span.RecordError(err)
			return nil, fs.QueryErr(err)
		}
		for _, k := range decodeKeys(snaps, true) {
			if exact || strings.HasPrefix(k.URI, prefix) {
				keys = append(keys, k)
			}
		}
		if len(snaps) < page || (limit > 0 && len(keys) >= limit) {
			break
		}
		last = snaps[len(snaps)-1]
	}
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}

	return r.keysToQueryRows(ctx, keys)
}

// QueryRecordSubtree returns every live record whose key is base itself
// (includeSelf) or lies under base's path subtree, ordered by URI (byte
// order) so base leads; afterURI and limit page it. The subtree is the byte
// range [base+"/", base+"0") ('0' is the byte after '/'), so sibling keys
// ("item2" for "item") never match. The second ordering field drops
// placeholder keys, which lack it.
func (r *Repository) QueryRecordSubtree(ctx context.Context, base string, includeSelf bool, afterURI string, limit int) ([]concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryRecordSubtree")
	defer span.End()

	keys := []recordKeyDoc{}
	if includeSelf && afterURI < base {
		self, err := r.getKey(ctx, base)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		if self != nil && !self.isPlaceholder() {
			keys = append(keys, *self)
		}
	}

	remaining := 0
	if limit > 0 {
		remaining = limit - len(keys)
	}
	if limit <= 0 || remaining > 0 {
		q := r.col(fs.ColRecordKeys).Query
		if afterURI > base+"/" {
			q = q.Where("uri", ">", afterURI)
		} else {
			q = q.Where("uri", ">=", base+"/")
		}
		q = q.Where("uri", "<", base+"0").
			OrderBy("uri", firestore.Asc).
			OrderBy("recordCreatedAt", firestore.Asc)
		if remaining > 0 {
			q = q.Limit(remaining)
		}
		snaps, err := q.Documents(ctx).GetAll()
		if err != nil {
			span.RecordError(err)
			return nil, fs.QueryErr(err)
		}
		keys = append(keys, decodeKeys(snaps, true)...)
	}

	rows, err := r.keysToQueryRows(ctx, keys)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	sds := make([]concrnt.SignedDocument, 0, len(rows))
	for _, row := range rows {
		sds = append(sds, row.Row)
	}
	return sds, nil
}
