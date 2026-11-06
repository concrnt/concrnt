package record

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func (r *Repository) CreateRecord(
	ctx context.Context,
	tx ucrecord.RepositoryTx,
	documentID string,
	key string,
	owner string,
	author string,
	schema string,
	onUpdate *string,
	policies *string,
	distributions []string,
	redirect *string,
	createdAt time.Time,
) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateRecord")
	defer span.End()

	t, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	// hierarchy[0] is the key itself, then its parent, grandparent, ... up to
	// (excluding) the owner root
	ancestors := ancestorsOf(key)
	hierarchy := []string{key}
	for i := len(ancestors) - 1; i >= 1; i-- {
		hierarchy = append(hierarchy, ancestors[i])
	}
	refs := make([]*firestore.DocumentRef, len(hierarchy))
	for i, uri := range hierarchy {
		refs[i] = r.col(fs.ColRecordKeys).Doc(keyID(uri))
	}
	// one round trip reads (and locks) the key and every ancestor
	// placeholder; the serializable validation at commit closes the
	// fresh-key race (the SQL backends' row lock + conditional upsert)
	states, err := t.readAll(refs)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	var old recordKeyDoc
	if states[0].exists {
		old = recordKeyFromMap(states[0].data)
	}

	// CIP-3 §3.4 accept-if-newer: only a strictly newer document takes the
	// key; an equal or older one is a no-op.
	if !old.isPlaceholder() && !createdAt.After(*old.RecordCreatedAt) {
		return false, nil
	}

	if distributions == nil {
		distributions = []string{}
	}
	t.set(r.col(fs.ColRecords).Doc(documentID), recordDoc{
		Owner:         owner,
		Author:        author,
		Schema:        schema,
		Redirect:      deref(redirect),
		Policies:      deref(policies),
		Distributions: distributions,
		CreatedAt:     createdAt,
	}.toMap())

	// create the missing ancestor placeholders top-down (existing ones are
	// only read, so a busy timeline's key is never rewritten per child)
	for i := len(hierarchy) - 1; i >= 1; i-- {
		if states[i].exists {
			continue
		}
		t.create(refs[i], recordKeyDoc{
			URI:       hierarchy[i],
			ParentURI: parentOf(hierarchy[i]),
			Ancestors: ancestorsOf(hierarchy[i]),
		}.toMap())
	}

	cleanOnUpdate := onUpdate == nil || *onUpdate != "retain"
	t.set(refs[0], recordKeyDoc{
		URI:             key,
		ParentURI:       parentOf(key),
		Ancestors:       ancestors,
		RecordID:        documentID,
		RecordCreatedAt: &createdAt,
		CleanOnUpdate:   cleanOnUpdate,
		Owner:           owner,
		Author:          author,
		Schema:          schema,
		Redirect:        deref(redirect),
		Policies:        deref(policies),
		Distributions:   distributions,
	}.toMap())

	// flag the superseded commit for GC and drop its record unless the old
	// version asked to be retained
	if old.CleanOnUpdate && old.RecordID != "" && old.RecordID != documentID {
		if err := t.updateIfExists(r.col(fs.ColCommits).Doc(old.RecordID), []firestore.Update{{Path: "gcCandidate", Value: true}}); err != nil {
			span.RecordError(err)
			return false, err
		}
		t.del(r.col(fs.ColRecords).Doc(old.RecordID))
	}

	return true, nil
}

func (r *Repository) GetHierarchicalRecordPolicies(ctx context.Context, uri string) ([]concrnt.Policy, error) {
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
			return nil, err
		}
		if key == nil {
			return nil, domain.NotFoundError{Resource: uri}
		}
		cckv = key.URI
	}

	stacks, err := r.policiesFor(ctx, []string{cckv})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return stacks[cckv], nil
}

// GetHierarchicalRecordPoliciesMulti is GetHierarchicalRecordPolicies for
// several cckv URIs in one batch read: the union of their ancestor chains is
// fetched at once and each stack assembled from it.
func (r *Repository) GetHierarchicalRecordPoliciesMulti(ctx context.Context, uris []string) (map[string][]concrnt.Policy, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetHierarchicalRecordPoliciesMulti")
	defer span.End()

	if err := repoutil.RequireCCKV(uris); err != nil {
		span.RecordError(err)
		return nil, err
	}
	stacks, err := r.policiesFor(ctx, uris)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return stacks, nil
}

// policiesFor builds the policy stack of every cckv URI with one batch
// read over the union of their ancestor chains. The chain is the key's
// own ancestors field plus itself, so it matches what was written.
func (r *Repository) policiesFor(ctx context.Context, cckvs []string) (map[string][]concrnt.Policy, error) {
	hierarchyOf := func(cckv string) ([]string, error) { return append(ancestorsOf(cckv), cckv), nil }
	return repoutil.PolicyStacks(cckvs, hierarchyOf, func(uris []string) (map[string]repoutil.PolicyLayer, error) {
		refs := make([]*firestore.DocumentRef, len(uris))
		for i, uri := range uris {
			refs[i] = r.col(fs.ColRecordKeys).Doc(keyID(uri))
		}
		docs, err := r.getAll(ctx, refs)
		if err != nil {
			return nil, err
		}
		layers := make(map[string]repoutil.PolicyLayer, len(docs))
		for _, m := range docs {
			key := recordKeyFromMap(m)
			if key.isPlaceholder() {
				continue
			}
			layer := repoutil.PolicyLayer{Distributions: key.Distributions}
			if key.Policies != "" {
				layer.Policy = &key.Policies
			}
			layers[key.URI] = layer
		}
		return layers, nil
	})
}

func (r *Repository) GetSignedDocument(ctx context.Context, uri string) (*concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetSignedDocument")
	defer span.End()

	parsed, err := concrnt.ParseCCURI(uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if parsed.Scheme != "cckv" && parsed.Scheme != "ccfs" {
		err := fmt.Errorf("unsupported uri scheme: %s", parsed.Scheme)
		span.RecordError(err)
		return nil, err
	}

	sds, err := r.GetSignedDocuments(ctx, []string{uri})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	sd, ok := sds[uri]
	if !ok {
		return nil, domain.NotFoundError{Resource: uri}
	}
	return sd, nil
}

// GetSignedDocuments fetches several documents in one batch read per step:
// cckv URIs through their keys, ccfs URIs through the commit log with the
// key currently pointing at each (if any). URIs that resolve to nothing are
// left out of the map.
func (r *Repository) GetSignedDocuments(ctx context.Context, uris []string) (map[string]*concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetSignedDocuments")
	defer span.End()

	cckvs, ids, idOf, err := repoutil.PartitionDocumentURIs(uris)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	keyRefs := make([]*firestore.DocumentRef, len(cckvs))
	keyURIs := make(map[string]string, len(cckvs)) // key doc id -> cckv uri
	for i, uri := range cckvs {
		keyRefs[i] = r.col(fs.ColRecordKeys).Doc(keyID(uri))
		keyURIs[keyRefs[i].ID] = uri
	}

	out := make(map[string]*concrnt.SignedDocument, len(uris))

	// cckv: key -> commit, the ccfs composed from the key's owner
	keyDocs, err := r.getAll(ctx, keyRefs)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	keys := make([]recordKeyDoc, 0, len(keyDocs))
	for id, m := range keyDocs {
		key := recordKeyFromMap(m)
		key.URI = keyURIs[id]
		if !key.isPlaceholder() {
			keys = append(keys, key)
		}
	}
	rows, err := r.keysToQueryRows(ctx, keys)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	for i := range rows {
		out[*rows[i].Row.CCKV] = &rows[i].Row
	}

	// ccfs: commit, plus the cckv of the key currently pointing at it
	if len(ids) > 0 {
		commits, err := r.getCommits(ctx, ids)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		cckvOf := map[string]string{}
		for start := 0; start < len(ids); start += inBatch {
			snaps, err := r.col(fs.ColRecordKeys).Where("recordID", "in", ids[start:min(start+inBatch, len(ids))]).Select("uri", "recordID").Documents(ctx).GetAll()
			if err != nil {
				span.RecordError(err)
				return nil, fs.QueryErr(err)
			}
			for _, snap := range snaps {
				cckvOf[str(snap.Data(), "recordID")] = str(snap.Data(), "uri")
			}
		}
		for uri, id := range idOf {
			commit, ok := commits[id]
			if !ok {
				continue
			}
			proof, err := commit.proof()
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
			ccfs := uri
			sd := &concrnt.SignedDocument{CCFS: &ccfs, Document: commit.Document, Proof: proof}
			if cckv, ok := cckvOf[id]; ok {
				sd.CCKV = &cckv
			}
			out[uri] = sd
		}
	}
	return out, nil
}

// DeleteRecordByKey deletes the record the key points at, the key itself
// and the associations targeting it, as the SQL backends' foreign keys
// cascade. Children keep a dangling parentURI and re-attach to a later
// record under the same key.
func (r *Repository) DeleteRecordByKey(ctx context.Context, tx ucrecord.RepositoryTx, targetURI string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteRecordByKey")
	defer span.End()

	t, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	ref := r.col(fs.ColRecordKeys).Doc(keyID(targetURI))
	m, exists, err := t.read(ref)
	if err != nil {
		span.RecordError(err)
		return err
	}
	if !exists {
		return nil
	}
	key := recordKeyFromMap(m)
	if key.RecordID == "" {
		return nil
	}

	t.del(r.col(fs.ColRecords).Doc(key.RecordID))
	if err := r.deleteKeyCascade(t, ref, targetURI); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

// deleteKeyCascade drops a key document and every association targeting it.
func (r *Repository) deleteKeyCascade(t *recordTx, keyRef *firestore.DocumentRef, uri string) error {
	t.del(keyRef)
	snaps, err := t.query(r.col(fs.ColAssociations).Where("targetURI", "==", uri).Select("unique"))
	if err != nil {
		return err
	}
	for _, ref := range associationRefs(r.client, snaps) {
		t.del(ref)
	}
	return nil
}

func (r *Repository) DeleteRecordByDocumentID(ctx context.Context, tx ucrecord.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteRecordByDocumentID")
	defer span.End()

	t, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	recordRef := r.col(fs.ColRecords).Doc(documentID)
	_, exists, err := t.read(recordRef)
	if err != nil {
		span.RecordError(err)
		return err
	}
	if !exists {
		return nil
	}

	// the key currently pointing at this version goes with it (a retained
	// older generation has no key and so only loses its record)
	keySnaps, err := t.query(r.col(fs.ColRecordKeys).Where("recordID", "==", documentID).Select("uri"))
	if err != nil {
		span.RecordError(err)
		return err
	}
	for _, s := range keySnaps {
		if err := r.deleteKeyCascade(t, s.Ref, str(s.Data(), "uri")); err != nil {
			span.RecordError(err)
			return err
		}
	}
	t.del(recordRef)
	return nil
}

func (r *Repository) GetTimelineRemoval(ctx context.Context, keyURI string) (string, string, error) {
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

	// only keys under a timeline key (not the owner root) and holding a
	// record appear in chunkline bodies
	if key.ParentURI == "" || isOwnerRoot(key.ParentURI) || key.RecordCreatedAt == nil {
		return "", "", nil
	}

	// derive the item ID exactly the way LoadLocalBody derives BodyItem.Href
	itemID := keyURI
	if key.Redirect != "" {
		itemID = key.Redirect
	}
	return key.ParentURI, itemID, nil
}
