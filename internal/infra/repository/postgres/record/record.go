package record

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/lib/pq"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
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

	db, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	// Lock the RecordKey before writing anything. CIP-3 §3.4 accept-if-newer
	// on the document's createdAt: only a strictly newer document takes the
	// key, an equal or older one is a no-op. Deciding under the row lock
	// closes the read-then-write race between concurrent commits, same as
	// CreateEntity's conditional upsert.
	oldRecordKey, err := gorm.G[RecordKey](db, clause.Locking{Strength: "UPDATE"}).
		Where("uri = ?", key).
		Take(ctx)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		span.RecordError(err)
		return false, err
	}
	if oldRecordKey.RecordID != nil && oldRecordKey.RecordCreatedAt != nil && !createdAt.After(*oldRecordKey.RecordCreatedAt) {
		return false, nil
	}

	// ParentのRecordKeyを探す
	parentRK, err := getOrCreateParentRecordKey(ctx, db, key)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	var pid *int64
	if parentRK != nil {
		pid = &parentRK.ID
	}
	cleanOnUpdate := onUpdate == nil || *onUpdate != "retain"

	// The FOR UPDATE fast-path above cannot lock a RecordKey row that does not
	// exist yet, so two concurrent commits to a fresh key both pass it. The
	// conditional upsert closes that window the same way CreateEntity does:
	// only a strictly newer document may take the key. IS NULL keeps normal
	// writes to parent placeholder rows (record_id NULL) working.
	result := gorm.WithResult()
	err = gorm.G[any](db, result).Exec(ctx, `
INSERT INTO record_keys (uri, parent_id, record_id, owner, author, schema, redirect, policies, distributions, record_created_at, clean_on_update)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (uri) DO UPDATE
SET parent_id = excluded.parent_id,
    record_id = excluded.record_id,
    owner = excluded.owner,
    author = excluded.author,
    schema = excluded.schema,
    redirect = excluded.redirect,
    policies = excluded.policies,
    distributions = excluded.distributions,
    record_created_at = excluded.record_created_at,
    clean_on_update = excluded.clean_on_update
WHERE record_keys.record_id IS NULL
   OR record_keys.record_created_at IS NULL
   OR record_keys.record_created_at < excluded.record_created_at`,
		key, pid, documentID, owner, author, schema, redirect, policies, pq.StringArray(distributions), createdAt, cleanOnUpdate)
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	if result.RowsAffected == 0 {
		return false, nil
	}

	// 古いRecordKeyが指していたCommitのGCフラグを立てる。retainを要求した版は
	// commit logだけに残り、ccfsで読める
	if oldRecordKey.CleanOnUpdate && oldRecordKey.RecordID != nil && *oldRecordKey.RecordID != documentID {
		if _, err := gorm.G[CommitLog](db).
			Where("id = ?", *oldRecordKey.RecordID).
			Update(ctx, "gc_candidate", true); err != nil {
			span.RecordError(err)
			return false, err
		}
	}

	return true, nil

}

type policyTuple struct {
	Uri           string         `gorm:"column:uri"`
	Policy        *string        `gorm:"column:policy"`
	Distributions pq.StringArray `gorm:"column:distributions;type:text[]"`
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
	// for a ccfs URI, the layer the version itself carries: a version is
	// evaluated under its own policy, whether its key still points at it or
	// has moved on from it (a version kept by onUpdate retain)
	var leaf *policyTuple
	if parsed.Scheme == "ccfs" {
		if parsed.Type != concrnt.CCFSTypeConcrnt {
			return nil, domain.NotFoundError{Resource: uri}
		}
		commitLog, err := gorm.G[CommitLog](r.db).
			Select("document").
			Where("id = ? AND NOT gc_candidate", parsed.CDID).
			Take(ctx)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domain.NotFoundError{Resource: uri}
		}
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		var doc concrnt.Document[any]
		if err := json.Unmarshal([]byte(commitLog.Document), &doc); err != nil {
			span.RecordError(err)
			return nil, err
		}
		if doc.Kind != "record" || doc.Key == "" {
			return nil, domain.NotFoundError{Resource: uri}
		}
		cckv = doc.Key
		leaf = &policyTuple{Uri: doc.Key}
		if doc.Policy != nil {
			policyBytes, err := json.Marshal(doc.Policy)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
			policyStr := string(policyBytes)
			leaf.Policy = &policyStr
		}
		if doc.Distributes != nil {
			leaf.Distributions = *doc.Distributes
		}
	}

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

	var entries []policyTuple
	err = gorm.G[RecordKey](r.db).
		Select("uri, policies AS policy, distributions").
		Where("uri IN ?", hierarchy).
		Where("record_id IS NOT NULL").
		Scan(ctx, &entries)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// A level with no policy of its own still matters when it distributes: its
	// virtual parents (the destination timelines' policies) must reach the
	// stack, so emit it as an empty layer rather than dropping it.
	tupleMap := make(map[string]policyTuple)
	for _, res := range entries {
		if res.Policy != nil || len(res.Distributions) > 0 {
			tupleMap[res.Uri] = res
		}
	}
	if leaf != nil {
		delete(tupleMap, cckv)
		if leaf.Policy != nil || len(leaf.Distributions) > 0 {
			tupleMap[cckv] = *leaf
		}
	}

	// CIP-12 §5.3: layers are emitted root-first, the target resource itself
	// last (hierarchy is already built [root, ..., leaf]). With the evaluator's
	// strong=first-wins / weak=last-wins folding this makes strong conclusions
	// prefer the outer (global-side) layers and weak conclusions the resource
	// itself.
	policies := []concrnt.Policy{}
	for _, uri := range hierarchy {
		if t, ok := tupleMap[uri]; ok {
			var policyDoc concrnt.Policy
			if t.Policy != nil {
				err := json.Unmarshal([]byte(*t.Policy), &policyDoc)
				if err != nil {
					span.RecordError(err)
					return nil, err
				}
			}

			policyDoc.Source = uri
			// convert pq.StringArray to []string
			virtualParents := []string{}
			for _, dist := range t.Distributions {
				virtualParents = append(virtualParents, dist)
			}
			policyDoc.VirtualParents = &virtualParents

			policies = append(policies, policyDoc)
		}
	}

	return policies, nil
}

func (r *Repository) GetSignedDocument(ctx context.Context, uri string) (*concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetSignedDocument")
	defer span.End()

	parsed, err := concrnt.ParseCCURI(uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	switch parsed.Scheme {
	case "cckv":
		type recordRow struct {
			RecordID *string
			Document string
			Proof    string
		}
		recordKey, err := gorm.G[recordRow](r.db).Raw(`
SELECT k.record_id, c.document, c.proof
FROM record_keys k
LEFT JOIN commit_logs c ON c.id = k.record_id
WHERE k.uri = ?`, uri).Take(ctx)
		if err != nil {
			span.RecordError(errors.Join(errors.New("failed to query recordkey from cckv"), err))
			return nil, errors.Join(domain.NotFoundError{Resource: uri}, err)
		}

		if recordKey.RecordID == nil {
			err := fmt.Errorf("record key found but record is nil for uri: %s", uri)
			span.RecordError(err)
			return nil, errors.Join(domain.NotFoundError{Resource: uri}, err)
		}

		var proof concrnt.Proof
		err = json.Unmarshal([]byte(recordKey.Proof), &proof)
		if err != nil {
			span.RecordError(errors.Join(errors.New("failed to unmarshal proof from recordkey"), err))
			return nil, err
		}

		ccfs := concrnt.ComposeCCFSURI(parsed.Owner, concrnt.CCFSTypeConcrnt, *recordKey.RecordID)

		return &concrnt.SignedDocument{
			CCKV:     &uri,
			CCFS:     &ccfs,
			Document: recordKey.Document,
			Proof:    proof,
		}, nil

	case "ccfs":
		if parsed.Type != concrnt.CCFSTypeConcrnt {
			return nil, domain.NotFoundError{Resource: uri}
		}
		// A commit flagged for gc is a version that was deleted or replaced
		// without asking to be retained: it stays in the log only as a replay
		// tombstone and is not served.
		commitLog, err := gorm.G[CommitLog](r.db).
			Where("id = ? AND NOT gc_candidate", parsed.CDID).
			Take(ctx)
		if err != nil {
			span.RecordError(errors.Join(errors.New("failed to query commitlog from ccfs"), err))
			return nil, errors.Join(domain.NotFoundError{Resource: uri}, err)
		}

		var cckv *string = nil
		recordKey, err := gorm.G[RecordKey](r.db).
			Select("uri").
			Where("record_id = ?", commitLog.ID).
			Take(ctx)
		if err == nil {
			cckv = &recordKey.URI
		}

		var proof concrnt.Proof
		err = json.Unmarshal([]byte(commitLog.Proof), &proof)
		if err != nil {
			span.RecordError(errors.Join(errors.New("failed to unmarshal proof from commitlog"), err))
			return nil, err
		}

		return &concrnt.SignedDocument{
			CCFS:     &uri,
			CCKV:     cckv,
			Document: commitLog.Document,
			Proof:    proof,
		}, nil
	default:
		err := fmt.Errorf("unsupported uri scheme: %s", parsed.Scheme)
		span.RecordError(err)
		return nil, err
	}
}

func (r *Repository) DeleteRecordByKey(ctx context.Context, tx ucrecord.RepositoryTx, targetURI string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteRecordByKey")
	defer span.End()

	db, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	if err := removeRecordKey(ctx, db, targetURI); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

// DeleteRecordByDocumentID removes the record version documentID. When a key
// still holds it, the key's record goes as with DeleteRecordByKey; a version
// its key has moved on from (kept by onUpdate retain) is on no row, and the
// caller's gc flag on its commit is what stops it being served.
func (r *Repository) DeleteRecordByDocumentID(ctx context.Context, tx ucrecord.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteRecordByDocumentID")
	defer span.End()

	db, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	rk, err := gorm.G[RecordKey](db).Select("uri").Where("record_id = ?", documentID).Take(ctx)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		span.RecordError(err)
		return err
	}

	if err := removeRecordKey(ctx, db, rk.URI); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

// removeRecordKey takes the record off the key uri. A key without children
// is deleted (its associations cascade) and every ancestor placeholder left
// with nothing under it goes too; a key with children stays as a placeholder
// so that they remain listed under it, and its associations, which belonged
// to the removed record, are deleted explicitly.
//
// The key row is locked before the child check, and child writes take a key
// share lock on their parent (getOrCreateParentRecordKey), so a child cannot
// be linked to a row this removal is about to delete.
func removeRecordKey(ctx context.Context, db *gorm.DB, uri string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.removeRecordKey")
	defer span.End()

	rk, err := gorm.G[RecordKey](db, clause.Locking{Strength: "UPDATE"}).
		Where("uri = ?", uri).
		Take(ctx)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if rk.RecordID == nil {
		// already a placeholder: there is no record to remove
		return nil
	}

	hasChildren, err := recordKeyHasChildren(ctx, db, rk.ID)
	if err != nil {
		return err
	}
	if hasChildren {
		err := gorm.G[any](db).Exec(ctx, `
UPDATE record_keys
SET record_id = NULL,
    owner = NULL,
    author = NULL,
    schema = NULL,
    redirect = NULL,
    policies = NULL,
    distributions = NULL,
    record_created_at = NULL,
    clean_on_update = false
WHERE id = ?`, rk.ID)
		if err != nil {
			return err
		}
		_, err = gorm.G[Association](db).Where("target_id = ?", rk.ID).Delete(ctx)
		return err
	}

	if _, err := gorm.G[RecordKey](db).Where("id = ?", rk.ID).Delete(ctx); err != nil {
		return err
	}
	return pruneEmptyPlaceholders(ctx, db, rk.ParentID)
}

// pruneEmptyPlaceholders walks up from parentID and deletes placeholder keys
// that hold no record, no children and no associations, stopping at the
// first ancestor that still holds something.
func pruneEmptyPlaceholders(ctx context.Context, db *gorm.DB, parentID *int64) error {
	for parentID != nil {
		parent, err := gorm.G[RecordKey](db, clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", *parentID).
			Take(ctx)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if parent.RecordID != nil {
			return nil
		}

		hasChildren, err := recordKeyHasChildren(ctx, db, parent.ID)
		if err != nil {
			return err
		}
		if hasChildren {
			return nil
		}

		hasAssociations, err := gorm.G[bool](db).
			Raw("SELECT EXISTS (SELECT 1 FROM associations WHERE target_id = ?)", parent.ID).
			Take(ctx)
		if err != nil {
			return err
		}
		if hasAssociations {
			return nil
		}

		if _, err := gorm.G[RecordKey](db).Where("id = ?", parent.ID).Delete(ctx); err != nil {
			return err
		}
		parentID = parent.ParentID
	}
	return nil
}

func recordKeyHasChildren(ctx context.Context, db *gorm.DB, id int64) (bool, error) {
	return gorm.G[bool](db).
		Raw("SELECT EXISTS (SELECT 1 FROM record_keys WHERE parent_id = ?)", id).
		Take(ctx)
}

func (r *Repository) GetTimelineRemoval(ctx context.Context, keyURI string) (string, string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetTimelineRemoval")
	defer span.End()

	recordKey, err := gorm.G[RecordKey](r.db).
		Where("uri = ?", keyURI).
		Take(ctx)
	if err != nil {
		span.RecordError(err)
		return "", "", domain.NotFoundError{Resource: keyURI}
	}

	// only rows with a parent and a record_created_at appear in chunkline bodies
	if recordKey.ParentID == nil || recordKey.RecordCreatedAt == nil {
		return "", "", nil
	}

	parent, err := gorm.G[RecordKey](r.db).
		Select("uri").
		Where("id = ?", *recordKey.ParentID).
		Take(ctx)
	if err != nil {
		span.RecordError(err)
		return "", "", err
	}

	// derive the item ID exactly the way LoadLocalBody derives BodyItem.Href
	// (which is what BodyItem.ID() returns): the redirect target for
	// reference records, the record key URI otherwise
	itemID := recordKey.URI
	if recordKey.Redirect != nil {
		itemID = *recordKey.Redirect
	}

	return parent.URI, itemID, nil
}

func getOrCreateParentRecordKey(ctx context.Context, db *gorm.DB, uri string) (*RecordKey, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.getOrCreateParentRecordKey")
	defer span.End()

	parentURI, err := url.JoinPath(uri, "..")
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	parsed, err := url.Parse(parentURI)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	if parsed.Path == "" || parsed.Path == "/" {
		return nil, nil
	}

	// KEY SHARE: a concurrent removeRecordKey/pruneEmptyPlaceholders (FOR
	// UPDATE, then DELETE) cannot delete the parent while this child is being
	// linked to it, and once it has, this lookup finds no row and recreates
	// the parent. Overwrites of the parent's record are not blocked by it.
	parentRK, err := gorm.G[RecordKey](db, clause.Locking{Strength: "KEY SHARE"}).
		Where("uri = ?", parentURI).
		Take(ctx)
	if err == nil {
		return &parentRK, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		span.RecordError(err)
		return nil, err
	}

	grandParent, err := getOrCreateParentRecordKey(ctx, db, parentURI)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	var pid *int64
	if grandParent != nil {
		pid = &grandParent.ID
	}

	// DO NOTHING: another transaction may have created the parent first.
	// Either way the row exists now and is read back under the same lock.
	err = gorm.G[any](db).Exec(ctx, `
INSERT INTO record_keys (uri, parent_id)
VALUES (?, ?)
ON CONFLICT (uri) DO NOTHING`, parentURI, pid)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	created, err := gorm.G[RecordKey](db, clause.Locking{Strength: "KEY SHARE"}).
		Where("uri = ?", parentURI).
		Take(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	return &created, nil
}

func getRecordKeyByURI(ctx context.Context, db *gorm.DB, uri string) (*RecordKey, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.getRecordKeyByURI")
	defer span.End()

	recordKey, err := gorm.G[RecordKey](db).
		Where("uri = ?", uri).
		Take(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, domain.NotFoundError{Resource: "record key"}
	}

	return &recordKey, nil
}
