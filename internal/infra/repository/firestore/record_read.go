package firestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/internal/utils"
)

// getAllBatch is the chunk size for BatchGetDocuments: commit documents are
// up to 32 KiB, and a response must stay under Firestore's 10 MiB limit.
const getAllBatch = 100

// direction maps the repository's order string ("desc", anything else asc).
func direction(order string) firestore.Direction {
	if order == "desc" {
		return firestore.Desc
	}
	return firestore.Asc
}

// applyWindow adds the created-at window, the (timestamp, id) ordering the
// Postgres queries use ("ORDER BY created_at DESC, document_id DESC") and the
// limit. The range field is the first orderBy, as Firestore requires.
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
func (r *RecordRepository) getDoc(ctx context.Context, ref *firestore.DocumentRef) (*firestore.DocumentSnapshot, bool, error) {
	snap, err := ref.Get(ctx)
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return snap, snap.Exists(), nil
}

// getCommits batch-fetches commits/{id}; commits that no longer exist are
// absent from the result (Postgres would have dropped their rows by cascade).
func (r *RecordRepository) getCommits(ctx context.Context, ids []string) (map[string]commitDoc, error) {
	out := make(map[string]commitDoc, len(ids))
	for start := 0; start < len(ids); start += getAllBatch {
		end := min(start+getAllBatch, len(ids))
		refs := make([]*firestore.DocumentRef, 0, end-start)
		for _, id := range ids[start:end] {
			refs = append(refs, r.col(colCommits).Doc(id))
		}
		snaps, err := r.client.GetAll(ctx, refs)
		if err != nil {
			return nil, err
		}
		for _, snap := range snaps {
			if !snap.Exists() {
				continue
			}
			var c commitDoc
			if err := decode(snap, &c); err != nil {
				return nil, err
			}
			out[snap.Ref.ID] = c
		}
	}
	return out, nil
}

func parseProof(raw string) (concrnt.Proof, error) {
	var proof concrnt.Proof
	if err := json.Unmarshal([]byte(raw), &proof); err != nil {
		return proof, errors.Join(errors.New("failed to unmarshal proof"), err)
	}
	return proof, nil
}

func (r *RecordRepository) HasCommitLog(ctx context.Context, id string) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.HasCommitLog")
	defer span.End()

	// a projection to no fields avoids pulling the 32 KiB document body
	snaps, err := r.col(colCommits).Where(firestore.DocumentID, "==", r.col(colCommits).Doc(id)).Select().Limit(1).Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return false, queryErr(err)
	}
	return len(snaps) > 0, nil
}

// keyByRecordID finds the key currently pointing at a record version.
func (r *RecordRepository) keyByRecordID(ctx context.Context, recordID string) (*recordKeyDoc, error) {
	snaps, err := r.col(colRecordKeys).Where("recordID", "==", recordID).Limit(1).Documents(ctx).GetAll()
	if err != nil {
		return nil, queryErr(err)
	}
	if len(snaps) == 0 {
		return nil, nil
	}
	var key recordKeyDoc
	if err := decode(snaps[0], &key); err != nil {
		return nil, err
	}
	return &key, nil
}

func (r *RecordRepository) getKey(ctx context.Context, uri string) (*recordKeyDoc, error) {
	snap, exists, err := r.getDoc(ctx, r.col(colRecordKeys).Doc(keyID(uri)))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	var key recordKeyDoc
	if err := decode(snap, &key); err != nil {
		return nil, err
	}
	return &key, nil
}

func (r *RecordRepository) GetSignedDocument(ctx context.Context, uri string) (*concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetSignedDocument")
	defer span.End()

	parsed, err := concrnt.ParseCCURI(uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	switch parsed.Scheme {
	case "cckv":
		key, err := r.getKey(ctx, uri)
		if err != nil {
			span.RecordError(err)
			return nil, errors.Join(domain.NotFoundError{Resource: uri}, err)
		}
		if key == nil || key.RecordID == "" {
			err := fmt.Errorf("record key not found or record is nil for uri: %s", uri)
			span.RecordError(err)
			return nil, errors.Join(domain.NotFoundError{Resource: uri}, err)
		}

		snap, exists, err := r.getDoc(ctx, r.col(colCommits).Doc(key.RecordID))
		if err != nil || !exists {
			span.RecordError(errors.Join(errors.New("failed to query commit from cckv"), err))
			return nil, errors.Join(domain.NotFoundError{Resource: uri}, err)
		}
		var commit commitDoc
		if err := decode(snap, &commit); err != nil {
			span.RecordError(err)
			return nil, err
		}
		proof, err := parseProof(commit.Proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		ccfs := concrnt.ComposeCCFSURI(parsed.Owner, concrnt.CCFSTypeConcrnt, key.RecordID)
		return &concrnt.SignedDocument{
			CCKV:     &uri,
			CCFS:     &ccfs,
			Document: commit.Document,
			Proof:    proof,
		}, nil

	case "ccfs":
		if parsed.Type != concrnt.CCFSTypeConcrnt {
			return nil, domain.NotFoundError{Resource: uri}
		}
		snap, exists, err := r.getDoc(ctx, r.col(colCommits).Doc(parsed.CDID))
		if err != nil || !exists {
			span.RecordError(errors.Join(errors.New("failed to query commit from ccfs"), err))
			return nil, errors.Join(domain.NotFoundError{Resource: uri}, err)
		}
		var commit commitDoc
		if err := decode(snap, &commit); err != nil {
			span.RecordError(err)
			return nil, err
		}

		var cckv *string
		if key, err := r.keyByRecordID(ctx, parsed.CDID); err == nil && key != nil {
			cckv = &key.URI
		}

		proof, err := parseProof(commit.Proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		return &concrnt.SignedDocument{
			CCFS:     &uri,
			CCKV:     cckv,
			Document: commit.Document,
			Proof:    proof,
		}, nil
	default:
		err := fmt.Errorf("unsupported uri scheme: %s", parsed.Scheme)
		span.RecordError(err)
		return nil, err
	}
}

func (r *RecordRepository) GetTimelineRemoval(ctx context.Context, keyURI string) (string, string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetTimelineRemoval")
	defer span.End()

	key, err := r.getKey(ctx, keyURI)
	if err != nil {
		span.RecordError(err)
		return "", "", err
	}
	if key == nil {
		return "", "", domain.NotFoundError{Resource: keyURI}
	}

	// only keys with a parent and a record appear in chunkline bodies
	if key.ParentGen == "" || key.RecordCreatedAt == nil {
		return "", "", nil
	}
	parentURI, ok, err := parentURIOf(keyURI)
	if err != nil || !ok {
		return "", "", err
	}

	// derive the item ID exactly the way LoadLocalBody derives BodyItem.Href
	itemID := key.URI
	if key.Redirect != "" {
		itemID = key.Redirect
	}
	return parentURI, itemID, nil
}

func (r *RecordRepository) GetHierarchicalRecordPolicies(ctx context.Context, uri string) ([]concrnt.Policy, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetHierarchicalRecordPolicies")
	defer span.End()

	parsed, err := concrnt.ParseCCURI(uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	cckv := uri
	if parsed.Scheme == "ccfs" {
		if parsed.Type != concrnt.CCFSTypeConcrnt {
			return nil, domain.NotFoundError{Resource: uri}
		}
		key, err := r.keyByRecordID(ctx, parsed.CDID)
		if err != nil {
			span.RecordError(err)
			return nil, errors.Join(domain.NotFoundError{Resource: uri}, err)
		}
		if key == nil {
			return nil, domain.NotFoundError{Resource: uri}
		}
		cckv = key.URI
	}

	// the same root-first walk as the Postgres implementation
	hierarchy := []string{}
	currentURI := cckv
	for {
		hierarchy = append([]string{currentURI}, hierarchy...)
		parentURI, err := url.JoinPath(currentURI, "..")
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		if parentURI == currentURI {
			break
		}
		currentURI = parentURI
	}

	refs := make([]*firestore.DocumentRef, len(hierarchy))
	for i, u := range hierarchy {
		refs[i] = r.col(colRecordKeys).Doc(keyID(u))
	}
	snaps, err := r.client.GetAll(ctx, refs)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// CIP-12 §5.3: layers are emitted root-first, the target resource itself
	// last. A level with no policy of its own still matters when it
	// distributes, so it is emitted as an empty layer.
	policies := []concrnt.Policy{}
	for i, snap := range snaps {
		if !snap.Exists() {
			continue
		}
		var key recordKeyDoc
		if err := decode(snap, &key); err != nil {
			span.RecordError(err)
			return nil, err
		}
		if key.RecordID == "" || (key.Policies == "" && len(key.Distributions) == 0) {
			continue
		}

		var policyDoc concrnt.Policy
		if key.Policies != "" {
			if err := json.Unmarshal([]byte(key.Policies), &policyDoc); err != nil {
				span.RecordError(err)
				return nil, err
			}
		}
		policyDoc.Source = hierarchy[i]
		virtualParents := append([]string{}, key.Distributions...)
		policyDoc.VirtualParents = &virtualParents
		policies = append(policies, policyDoc)
	}

	return policies, nil
}

func (r *RecordRepository) GetDistributions(ctx context.Context, uri string) ([]string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetDistributions")
	defer span.End()

	parsed, err := concrnt.ParseCCURI(uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	switch parsed.Scheme {
	case "cckv":
		key, err := r.getKey(ctx, uri)
		if err != nil {
			span.RecordError(err)
			return nil, errors.Join(domain.NotFoundError{Resource: uri}, err)
		}
		if key == nil {
			return nil, domain.NotFoundError{Resource: uri}
		}
		return key.Distributions, nil
	case "ccfs":
		if parsed.Type != concrnt.CCFSTypeConcrnt {
			return nil, domain.NotFoundError{Resource: uri}
		}
		snap, exists, err := r.getDoc(ctx, r.col(colRecords).Doc(parsed.CDID))
		if err != nil || !exists {
			span.RecordError(err)
			return nil, errors.Join(domain.NotFoundError{Resource: uri}, err)
		}
		var rec recordDoc
		if err := decode(snap, &rec); err != nil {
			span.RecordError(err)
			return nil, err
		}
		return rec.Distributions, nil
	default:
		err := fmt.Errorf("unsupported uri scheme: %s", parsed.Scheme)
		span.RecordError(err)
		return nil, err
	}
}

// keysToQueryRows resolves the commit of every listed key and builds the rows
// in the keys' order, carrying recordCreatedAt as the sort key.
func (r *RecordRepository) keysToQueryRows(ctx context.Context, keys []recordKeyDoc) ([]record.QueryRow, error) {
	ids := make([]string, 0, len(keys))
	for _, k := range keys {
		ids = append(ids, k.RecordID)
	}
	commits, err := r.getCommits(ctx, ids)
	if err != nil {
		return nil, err
	}

	rows := make([]record.QueryRow, 0, len(keys))
	for _, k := range keys {
		commit, ok := commits[k.RecordID]
		if !ok {
			continue
		}
		proof, err := parseProof(commit.Proof)
		if err != nil {
			return nil, err
		}
		ccfs := concrnt.ComposeCCFSURI(k.Owner, concrnt.CCFSTypeConcrnt, k.RecordID)
		uri := k.URI
		rows = append(rows, record.QueryRow{
			Row: concrnt.SignedDocument{
				CCKV:     &uri,
				CCFS:     &ccfs,
				Document: commit.Document,
				Proof:    proof,
			},
			CreatedAt: *k.RecordCreatedAt,
		})
	}
	return rows, nil
}

func decodeKeys(snaps []*firestore.DocumentSnapshot) ([]recordKeyDoc, error) {
	keys := make([]recordKeyDoc, 0, len(snaps))
	for _, snap := range snaps {
		var k recordKeyDoc
		if err := decode(snap, &k); err != nil {
			return nil, err
		}
		if k.isPlaceholder() {
			continue
		}
		keys = append(keys, k)
	}
	return keys, nil
}

func (r *RecordRepository) QueryByParent(
	ctx context.Context,
	parent, schema, author string,
	since, until *time.Time,
	limit int,
	order string,
) ([]record.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryByParent")
	defer span.End()

	parentKey, err := r.getKey(ctx, parent)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if parentKey == nil {
		// "parent_id = (SELECT id ... WHERE uri = ?)" matches nothing
		return []record.QueryRow{}, nil
	}

	q := r.col(colRecordKeys).Where("parentGen", "==", parentKey.Gen)
	if schema != "" {
		q = q.Where("schema", "==", schema)
	}
	if author != "" {
		q = q.Where("author", "==", author)
	}
	q = applyWindow(q, "recordCreatedAt", "recordID", since, until, limit, order)

	snaps, err := q.Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, queryErr(err)
	}
	keys, err := decodeKeys(snaps)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return r.keysToQueryRows(ctx, keys)
}

// prefixOverfetch is the page size used while post-filtering a non-directory
// prefix (SQL "LIKE 'cckv://o/tl/e%'" also matches sibling "e2" keys).
const prefixOverfetch = 64

// QueryByPrefix serves "uri LIKE prefix%". The prefix is split at its last
// '/' into the directory that every match lives under — matched with
// array-contains on the key's ancestors — and an optional last-segment prefix
// that is checked client-side. A prefix without an owner and path (the
// all-owners LIKE) is rejected: it has no indexed form.
func (r *RecordRepository) QueryByPrefix(
	ctx context.Context,
	prefix, schema, author string,
	since, until *time.Time,
	limit int,
	order string,
) ([]record.QueryRow, error) {
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

	q := r.col(colRecordKeys).Where("ancestors", "array-contains", dir)
	if schema != "" {
		q = q.Where("schema", "==", schema)
	}
	if author != "" {
		q = q.Where("author", "==", author)
	}
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
			return nil, queryErr(err)
		}
		for _, snap := range snaps {
			var k recordKeyDoc
			if err := decode(snap, &k); err != nil {
				span.RecordError(err)
				return nil, err
			}
			if k.isPlaceholder() {
				continue
			}
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
// (includeSelf) or lies under base's path subtree, ordered by URI so base
// leads. The subtree is the byte range [base+"/", base+"0") — '0' is the
// byte after '/' — so sibling keys ("item2" for "item") never match.
func (r *RecordRepository) QueryRecordSubtree(ctx context.Context, base string, includeSelf bool) ([]concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryRecordSubtree")
	defer span.End()

	keys := []recordKeyDoc{}
	if includeSelf {
		self, err := r.getKey(ctx, base)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		if self != nil && !self.isPlaceholder() {
			keys = append(keys, *self)
		}
	}

	snaps, err := r.col(colRecordKeys).
		Where("uri", ">=", base+"/").
		Where("uri", "<", base+"0").
		OrderBy("uri", firestore.Asc).
		Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, queryErr(err)
	}
	subtree, err := decodeKeys(snaps)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	keys = append(keys, subtree...)

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

// targetGen resolves the association target key's identity; a missing key
// has no associations.
func (r *RecordRepository) targetGen(ctx context.Context, targetURI string) (string, bool, error) {
	key, err := r.getKey(ctx, targetURI)
	if err != nil || key == nil {
		return "", false, err
	}
	return key.Gen, true, nil
}

func (r *RecordRepository) GetAssociatedRecords(
	ctx context.Context,
	targetURI, schema, variant, author string,
	since, until *time.Time,
	limit int,
	order string,
) ([]record.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAssociatedRecords")
	defer span.End()

	gen, ok, err := r.targetGen(ctx, targetURI)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if !ok {
		return []record.QueryRow{}, nil
	}

	q := r.col(colAssociations).Where("targetGen", "==", gen)
	if schema != "" {
		q = q.Where("schema", "==", schema)
	}
	if variant != "" {
		q = q.Where("variant", "==", variant)
	}
	if author != "" {
		q = q.Where("author", "==", author)
	}
	// the association's document id is its Firestore document id
	q = applyWindow(q, "createdAt", firestore.DocumentID, since, until, limit, order)

	snaps, err := q.Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, queryErr(err)
	}

	assocs := make([]associationDoc, 0, len(snaps))
	ids := make([]string, 0, len(snaps))
	for _, snap := range snaps {
		var a associationDoc
		if err := decode(snap, &a); err != nil {
			span.RecordError(err)
			return nil, err
		}
		assocs = append(assocs, a)
		ids = append(ids, snap.Ref.ID)
	}
	commits, err := r.getCommits(ctx, ids)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	rows := make([]record.QueryRow, 0, len(assocs))
	for i, a := range assocs {
		commit, ok := commits[ids[i]]
		if !ok {
			continue
		}
		proof, err := parseProof(commit.Proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		ccfs := concrnt.ComposeCCFSURI(a.Owner, concrnt.CCFSTypeConcrnt, ids[i])
		rows = append(rows, record.QueryRow{
			Row: concrnt.SignedDocument{
				CCFS:     &ccfs,
				Document: commit.Document,
				Proof:    proof,
			},
			CreatedAt: a.CreatedAt,
		})
	}
	return rows, nil
}

// Firestore has count() but no GROUP BY, so the grouped counts scan the
// matching documents (projected to the grouping fields) and aggregate in
// memory. Cost is one document read per matching row.

func (r *RecordRepository) GetAssociatedRecordCountsBySchema(ctx context.Context, targetURI string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAssociatedRecordCountsBySchema")
	defer span.End()

	result := make(map[string]int64)
	gen, ok, err := r.targetGen(ctx, targetURI)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if !ok {
		return result, nil
	}

	it := r.col(colAssociations).Where("targetGen", "==", gen).Select("schema").Documents(ctx)
	defer it.Stop()
	for {
		snap, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			span.RecordError(err)
			return nil, queryErr(err)
		}
		schema, _ := snap.DataAt("schema")
		s, _ := schema.(string)
		result[s]++
	}
	return result, nil
}

func (r *RecordRepository) GetAssociatedRecordCountsByVariant(ctx context.Context, targetURI, schema string) (*utils.OrderedKVMap[int64], error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAssociatedRecordCountsByVariant")
	defer span.End()

	result := make(utils.OrderedKVMap[int64])
	gen, ok, err := r.targetGen(ctx, targetURI)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if !ok {
		return &result, nil
	}

	type agg struct {
		count int64
		min   time.Time
	}
	groups := map[string]*agg{}
	it := r.col(colAssociations).Where("targetGen", "==", gen).Where("schema", "==", schema).Select("variant", "createdAt").Documents(ctx)
	defer it.Stop()
	for {
		snap, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			span.RecordError(err)
			return nil, queryErr(err)
		}
		v, _ := snap.DataAt("variant")
		variant, _ := v.(string)
		c, _ := snap.DataAt("createdAt")
		createdAt, _ := c.(time.Time)
		g, ok := groups[variant]
		if !ok {
			g = &agg{min: createdAt}
			groups[variant] = g
		}
		g.count++
		if createdAt.Before(g.min) {
			g.min = createdAt
		}
	}

	for variant, g := range groups {
		result[variant] = utils.OrderedKV[int64]{
			Value: g.count,
			Order: g.min.UnixNano(),
		}
	}
	return &result, nil
}

// ackQuery is the shared filter of the ack/acked list and count queries.
func (r *RecordRepository) ackQuery(collection, sideField, side, schema string) firestore.Query {
	q := r.col(collection).Where(sideField, "==", side).Where("valid", "==", true)
	if schema != "" {
		q = q.Where("schema", "==", schema)
	}
	return q
}

func (r *RecordRepository) getAckRows(ctx context.Context, collection, sideField, side, schema string, since, until *time.Time, limit int, order string, ccfsOwner func(ackDoc) string) ([]record.QueryRow, error) {
	q := applyWindow(r.ackQuery(collection, sideField, side, schema), "createdAt", "documentID", since, until, limit, order)
	snaps, err := q.Documents(ctx).GetAll()
	if err != nil {
		return nil, queryErr(err)
	}

	acks := make([]ackDoc, 0, len(snaps))
	ids := make([]string, 0, len(snaps))
	for _, snap := range snaps {
		var a ackDoc
		if err := decode(snap, &a); err != nil {
			return nil, err
		}
		acks = append(acks, a)
		ids = append(ids, a.DocumentID)
	}
	commits, err := r.getCommits(ctx, ids)
	if err != nil {
		return nil, err
	}

	rows := make([]record.QueryRow, 0, len(acks))
	for _, a := range acks {
		commit, ok := commits[a.DocumentID]
		if !ok {
			continue
		}
		proof, err := parseProof(commit.Proof)
		if err != nil {
			return nil, err
		}
		ccfs := concrnt.ComposeCCFSURI(ccfsOwner(a), concrnt.CCFSTypeConcrnt, a.DocumentID)
		rows = append(rows, record.QueryRow{
			Row: concrnt.SignedDocument{
				CCFS:     &ccfs,
				Document: commit.Document,
				Proof:    proof,
			},
			CreatedAt: a.CreatedAt,
		})
	}
	return rows, nil
}

func (r *RecordRepository) GetAcknowledgeRecords(ctx context.Context, from, to, schema string, since, until *time.Time, limit int, order string) ([]record.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAcknowledgeRecords")
	defer span.End()

	var rows []record.QueryRow
	var err error
	switch {
	case from != "":
		rows, err = r.getAckRows(ctx, colAcks, "from", from, schema, since, until, limit, order, func(a ackDoc) string { return a.From })
	case to != "":
		// CIP-3 §3.4: an acked's ccfs identity is held by the associate
		// owner's server
		rows, err = r.getAckRows(ctx, colAckeds, "to", to, schema, since, until, limit, order, func(a ackDoc) string { return a.To })
	default:
		err = errors.New("either 'from' or 'to' must be specified")
	}
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return rows, nil
}

func (r *RecordRepository) GetAcknowledgeRecordCounts(ctx context.Context, from, to, schema string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAcknowledgeRecordCounts")
	defer span.End()

	var q firestore.Query
	switch {
	case from != "":
		q = r.ackQuery(colAcks, "from", from, schema)
	case to != "":
		q = r.ackQuery(colAckeds, "to", to, schema)
	default:
		err := errors.New("either 'from' or 'to' must be specified")
		span.RecordError(err)
		return nil, err
	}

	counts := make(map[string]int64)
	it := q.Select("schema").Documents(ctx)
	defer it.Stop()
	for {
		snap, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			span.RecordError(err)
			return nil, queryErr(err)
		}
		v, _ := snap.DataAt("schema")
		s, _ := v.(string)
		counts[s]++
	}
	return counts, nil
}

func (r *RecordRepository) GetAllCommitLogs(ctx context.Context, owner string) ([]concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAllCommitLogs")
	defer span.End()

	it := r.col(colCommits).Where("owner", "==", owner).OrderBy("cDate", firestore.Asc).Documents(ctx)
	defer it.Stop()

	sds := []concrnt.SignedDocument{}
	for {
		snap, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			span.RecordError(err)
			return nil, queryErr(err)
		}
		var c commitDoc
		if err := decode(snap, &c); err != nil {
			span.RecordError(err)
			return nil, err
		}
		proof, err := parseProof(c.Proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		sds = append(sds, concrnt.SignedDocument{Document: c.Document, Proof: proof})
	}
	return sds, nil
}
