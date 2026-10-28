package firestore

import (
	"context"
	"encoding/json"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// RecordRepository implements record.Repository and
// record.MaintenanceRepository on Firestore.
type RecordRepository struct {
	client *firestore.Client
}

func NewRecordRepository(client *firestore.Client) record.Repository {
	return &RecordRepository{client: client}
}

// NewRecordMaintenanceRepository exposes the offline-tooling slice (conctl
// gc/dump) over the same client.
func NewRecordMaintenanceRepository(client *firestore.Client) record.MaintenanceRepository {
	return &RecordRepository{client: client}
}

func (r *RecordRepository) col(name string) *firestore.CollectionRef {
	return r.client.Collection(name)
}

func (r *RecordRepository) CreateCommitLog(ctx context.Context, tx record.RepositoryTx, id string, ip string, document string, proof any, owner string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateCommitLog")
	defer span.End()

	t, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	proofBytes, err := json.Marshal(proof)
	if err != nil {
		span.RecordError(err)
		return err
	}

	// first write wins, like ON CONFLICT DO NOTHING on the primary key: a
	// re-delivery of the same id is ignored wholesale
	ref := r.col(colCommits).Doc(id)
	if t.pending(ref) != nil {
		return nil
	}
	_, exists, err := t.get(ref)
	if err != nil {
		span.RecordError(err)
		return err
	}
	if exists {
		return nil
	}
	t.set(ref, commitDoc{
		IP:       ip,
		Document: document,
		Proof:    string(proofBytes),
		Owner:    owner,
	}.toMap())
	return nil
}

func (r *RecordRepository) CreateEntity(ctx context.Context, tx record.RepositoryTx, ccid string, alias *string, domain string, documentID string, createdAt time.Time) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateEntity")
	defer span.End()

	t, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	ref := r.col(colEntities).Doc(ccid)
	snap, exists, err := t.get(ref)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	if !exists {
		t.set(ref, map[string]any{
			"alias":      deref(alias),
			"domain":     domain,
			"tag":        "",
			"documentID": documentID,
			"createdAt":  createdAt,
			"cDate":      firestore.ServerTimestamp,
			"mDate":      firestore.ServerTimestamp,
		})
		return true, nil
	}

	// CIP-3 §3.4 accept-if-newer on the document's createdAt: only a strictly
	// newer document replaces the stored one; the read above is validated at
	// commit, so concurrent writers converge on the same winner.
	var existing entityDoc
	if err := decode(snap, &existing); err != nil {
		span.RecordError(err)
		return false, err
	}
	if !createdAt.After(existing.CreatedAt) {
		return false, nil
	}

	t.update(ref, []firestore.Update{
		{Path: "alias", Value: deref(alias)},
		{Path: "domain", Value: domain},
		{Path: "documentID", Value: documentID},
		{Path: "createdAt", Value: createdAt},
		{Path: "mDate", Value: firestore.ServerTimestamp},
	})
	return true, nil
}

func (r *RecordRepository) CreateRecord(
	ctx context.Context,
	tx record.RepositoryTx,
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
	hierarchy := []string{key}
	for cur := key; ; {
		parent, ok, err := parentURIOf(cur)
		if err != nil {
			span.RecordError(err)
			return false, err
		}
		if !ok {
			break
		}
		hierarchy = append(hierarchy, parent)
		cur = parent
	}

	refs := make([]*firestore.DocumentRef, len(hierarchy))
	for i, uri := range hierarchy {
		refs[i] = r.col(colRecordKeys).Doc(keyID(uri))
	}
	// one round trip reads (and locks) the key and every ancestor placeholder;
	// the serializable validation at commit closes the fresh-key race the
	// same way Postgres' row lock + conditional upsert did
	snaps, err := t.getAll(refs)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	var old recordKeyDoc
	oldExists := snaps[0].Exists()
	if oldExists {
		if err := decode(snaps[0], &old); err != nil {
			span.RecordError(err)
			return false, err
		}
	}

	// CIP-3 §3.4 accept-if-newer: only a strictly newer document takes the
	// key; an equal or older one is a no-op.
	if !old.isPlaceholder() && !createdAt.After(*old.RecordCreatedAt) {
		return false, nil
	}

	if distributions == nil {
		distributions = []string{}
	}
	t.set(r.col(colRecords).Doc(documentID), recordDoc{
		Owner:         owner,
		Author:        author,
		Schema:        schema,
		Redirect:      deref(redirect),
		Policies:      deref(policies),
		Distributions: distributions,
		CreatedAt:     createdAt,
	}.toMap())

	// walk the ancestors top-down, creating missing placeholders and threading
	// each one's gen into its children (existing placeholders are only read,
	// so a busy timeline's key is never rewritten per child)
	parentGen := ""
	for i := len(hierarchy) - 1; i >= 1; i-- {
		var placeholder recordKeyDoc
		if snaps[i].Exists() {
			if err := decode(snaps[i], &placeholder); err != nil {
				span.RecordError(err)
				return false, err
			}
		} else {
			placeholder = recordKeyDoc{
				URI:       hierarchy[i],
				Gen:       newGen(r.client),
				ParentGen: parentGen,
				Ancestors: ancestorsOf(hierarchy[i]),
			}
			t.create(refs[i], placeholder.toMap())
		}
		parentGen = placeholder.Gen
	}

	gen := old.Gen
	if gen == "" {
		gen = newGen(r.client)
	}
	cleanOnUpdate := onUpdate == nil || *onUpdate != "retain"
	t.set(refs[0], recordKeyDoc{
		URI:             key,
		Gen:             gen,
		ParentGen:       parentGen,
		Ancestors:       ancestorsOf(key),
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
		if err := t.updateIfExists(r.col(colCommits).Doc(old.RecordID), []firestore.Update{{Path: "gcCandidate", Value: true}}); err != nil {
			span.RecordError(err)
			return false, err
		}
		t.del(r.col(colRecords).Doc(old.RecordID))
	}

	return true, nil
}

func (r *RecordRepository) CreateAssociation(ctx context.Context, tx record.RepositoryTx, documentID string, targetURI string, owner string, author string, schema string, variant *string, unique string, createdAt time.Time) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateAssociation")
	defer span.End()

	t, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	targetRef := r.col(colRecordKeys).Doc(keyID(targetURI))
	assocRef := r.col(colAssociations).Doc(documentID)
	uniqueRef := r.col(colAssocUniques).Doc(unique)
	snaps, err := t.getAll([]*firestore.DocumentRef{targetRef, assocRef, uniqueRef})
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	if !snaps[0].Exists() {
		err := domain.NotFoundError{Resource: "record key"}
		span.RecordError(err)
		return false, err
	}
	// duplicates on the document id (same document re-delivered) and on the
	// logical unique key (same association under a new document id) are a
	// silent success, like ON CONFLICT DO NOTHING
	if snaps[1].Exists() || snaps[2].Exists() {
		return false, nil
	}

	var target recordKeyDoc
	if err := decode(snaps[0], &target); err != nil {
		span.RecordError(err)
		return false, err
	}

	t.set(assocRef, associationDoc{
		TargetKeyID: targetRef.ID,
		TargetGen:   target.Gen,
		Owner:       owner,
		Author:      author,
		Schema:      schema,
		Variant:     deref(variant),
		Unique:      unique,
		CreatedAt:   createdAt,
	}.toMap())
	t.set(uniqueRef, map[string]any{"associationID": documentID})
	return true, nil
}

func (r *RecordRepository) processAck(ctx context.Context, tx record.RepositoryTx, collection string, documentID string, from string, to string, schema string, createdAt time.Time, valid bool) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.processAck")
	defer span.End()

	t, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	ref := r.col(collection).Doc(ackID(from, to, schema))
	snap, exists, err := t.get(ref)
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	if exists {
		// CIP-10 §4: only a document with a strictly newer createdAt may move
		// the (from, to, schema) state. The key is createdAt alone — never the
		// document id — so the acker's and the associate owner's servers,
		// which hold different documents with the same createdAt, reach the
		// same decision.
		var existing ackDoc
		if err := decode(snap, &existing); err != nil {
			span.RecordError(err)
			return false, err
		}
		if !createdAt.After(existing.CreatedAt) {
			return false, nil
		}
	}

	t.set(ref, ackDoc{
		From:       from,
		To:         to,
		Schema:     schema,
		DocumentID: documentID,
		Valid:      valid,
		CreatedAt:  createdAt,
	}.toMap())
	return true, nil
}

func (r *RecordRepository) Acknowledge(ctx context.Context, tx record.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAck(ctx, tx, colAcks, documentID, from, to, schema, createdAt, true)
}

func (r *RecordRepository) UnAcknowledge(ctx context.Context, tx record.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAck(ctx, tx, colAcks, documentID, from, to, schema, createdAt, false)
}

func (r *RecordRepository) Acknowledged(ctx context.Context, tx record.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAck(ctx, tx, colAckeds, documentID, from, to, schema, createdAt, true)
}

func (r *RecordRepository) UnAcknowledged(ctx context.Context, tx record.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAck(ctx, tx, colAckeds, documentID, from, to, schema, createdAt, false)
}

func (r *RecordRepository) MarkCommitLogGcCandidate(ctx context.Context, tx record.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.MarkCommitLogGcCandidate")
	defer span.End()

	t, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	// a missing commit is a no-op (UPDATE matching zero rows); a commit
	// created earlier in this transaction is patched in the buffer
	if err := t.updateIfExists(r.col(colCommits).Doc(documentID), []firestore.Update{{Path: "gcCandidate", Value: true}}); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

// DeleteRecordByKey drops the record the key points at and the key itself,
// mirroring the record_keys -> records ON DELETE CASCADE: children keep a
// dangling parentGen and associations a dangling targetGen, so a later
// record under the same key starts detached from them.
func (r *RecordRepository) DeleteRecordByKey(ctx context.Context, tx record.RepositoryTx, targetURI string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteRecordByKey")
	defer span.End()

	t, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	ref := r.col(colRecordKeys).Doc(keyID(targetURI))
	snap, exists, err := t.get(ref)
	if err != nil {
		span.RecordError(err)
		return err
	}
	if !exists {
		return nil
	}
	var key recordKeyDoc
	if err := decode(snap, &key); err != nil {
		span.RecordError(err)
		return err
	}
	if key.RecordID == "" {
		return nil
	}

	t.del(r.col(colRecords).Doc(key.RecordID))
	if err := r.deleteKeyCascade(ctx, t, ref, key.Gen); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

// deleteKeyCascade drops a key document and, as the associations.target_id
// FK cascade did, every association targeting that key instance.
func (r *RecordRepository) deleteKeyCascade(ctx context.Context, t *recordTx, keyRef *firestore.DocumentRef, gen string) error {
	t.del(keyRef)
	if gen == "" {
		return nil
	}
	snaps, err := t.documents(r.col(colAssociations).Where("targetGen", "==", gen).Select("unique")).GetAll()
	if err != nil {
		return queryErr(err)
	}
	for _, snap := range snaps {
		t.del(snap.Ref)
		u, _ := snap.DataAt("unique")
		if unique, ok := u.(string); ok && unique != "" {
			t.del(r.col(colAssocUniques).Doc(unique))
		}
	}
	return nil
}

func (r *RecordRepository) DeleteRecordByDocumentID(ctx context.Context, tx record.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteRecordByDocumentID")
	defer span.End()

	t, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	recordRef := r.col(colRecords).Doc(documentID)
	_, exists, err := t.get(recordRef)
	if err != nil {
		span.RecordError(err)
		return err
	}
	if !exists {
		return nil
	}

	// the key currently pointing at this version goes with it (FK cascade)
	keySnaps, err := t.documents(r.col(colRecordKeys).Where("recordID", "==", documentID).Select("gen")).GetAll()
	if err != nil {
		span.RecordError(err)
		return queryErr(err)
	}
	for _, s := range keySnaps {
		g, _ := s.DataAt("gen")
		gen, _ := g.(string)
		if err := r.deleteKeyCascade(ctx, t, s.Ref, gen); err != nil {
			span.RecordError(err)
			return err
		}
	}
	t.del(recordRef)
	return nil
}

func (r *RecordRepository) DeleteAssociation(ctx context.Context, tx record.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteAssociation")
	defer span.End()

	t, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	ref := r.col(colAssociations).Doc(documentID)
	snap, exists, err := t.get(ref)
	if err != nil {
		span.RecordError(err)
		return err
	}
	if !exists {
		return nil
	}
	var assoc associationDoc
	if err := decode(snap, &assoc); err != nil {
		span.RecordError(err)
		return err
	}

	t.del(ref)
	if assoc.Unique != "" {
		t.del(r.col(colAssocUniques).Doc(assoc.Unique))
	}
	return nil
}
