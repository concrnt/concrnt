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

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	// Lock the RecordKey before writing anything. CIP-3 §3.4 accept-if-newer
	// on the document's createdAt: only a strictly newer document takes the
	// key, an equal or older one is a no-op. Deciding under the row lock
	// closes the read-then-write race between concurrent commits, same as
	// CreateEntity's conditional upsert.
	var oldRecordKey RecordKey
	err = db.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("uri = ?", key).
		Take(&oldRecordKey).Error
	if err != nil && err != gorm.ErrRecordNotFound {
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

	// RecordKeyを作る
	rk := RecordKey{
		URI:             key,
		ParentID:        pid,
		RecordID:        &documentID,
		Owner:           owner,
		Author:          author,
		Schema:          schema,
		Redirect:        redirect,
		Policies:        policies,
		Distributions:   distributions,
		RecordCreatedAt: &createdAt,
		CleanOnUpdate:   cleanOnUpdate,
	}

	// The FOR UPDATE fast-path above cannot lock a RecordKey row that does not
	// exist yet, so two concurrent commits to a fresh key both pass it. The
	// conditional upsert closes that window the same way CreateEntity does:
	// only a strictly newer document may take the key. IS NULL keeps normal
	// writes to parent placeholder rows (RecordID nil) working.
	result := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "uri"}},
		DoUpdates: clause.Assignments(map[string]any{
			"record_id":         documentID,
			"parent_id":         pid,
			"owner":             owner,
			"author":            author,
			"schema":            schema,
			"redirect":          redirect,
			"policies":          policies,
			"distributions":     pq.StringArray(distributions),
			"record_created_at": createdAt,
			"clean_on_update":   cleanOnUpdate,
		}),
		Where: clause.Where{Exprs: []clause.Expression{gorm.Expr("record_keys.record_id IS NULL OR record_keys.record_created_at IS NULL OR record_keys.record_created_at < excluded.record_created_at")}},
	}).Create(&rk)
	if result.Error != nil {
		span.RecordError(result.Error)
		return false, result.Error
	}
	if result.RowsAffected == 0 {
		return false, nil
	}

	// 古いRecordKeyが指していたCommitのGCフラグを立てる。retainを要求した版は
	// commit logだけに残り、ccfsで読める
	if oldRecordKey.CleanOnUpdate && oldRecordKey.RecordID != nil && *oldRecordKey.RecordID != documentID {
		if err := db.Model(&CommitLog{}).
			Where("id = ?", oldRecordKey.RecordID).
			Update("gc_candidate", true).Error; err != nil {
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
		var commitLog CommitLog
		err = r.db.WithContext(ctx).
			Select("document").
			Where("id = ? AND NOT gc_candidate", parsed.CDID).
			Take(&commitLog).Error
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
	err = r.db.WithContext(ctx).
		Model(&RecordKey{}).
		Select("uri, policies AS policy, distributions").
		Where("uri IN ?", hierarchy).
		Where("record_id IS NOT NULL").
		Find(&entries).Error
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
		var recordKey RecordKey
		err = r.db.WithContext(ctx).
			Joins("Document").
			Where("record_keys.uri = ?", uri).
			Take(&recordKey).Error
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
		err = json.Unmarshal([]byte(recordKey.Document.Proof), &proof)
		if err != nil {
			span.RecordError(errors.Join(errors.New("failed to unmarshal proof from recordkey"), err))
			return nil, err
		}

		ccfs := concrnt.ComposeCCFSURI(parsed.Owner, concrnt.CCFSTypeConcrnt, *recordKey.RecordID)

		return &concrnt.SignedDocument{
			CCKV:     &uri,
			CCFS:     &ccfs,
			Document: recordKey.Document.Document,
			Proof:    proof,
		}, nil

	case "ccfs":
		if parsed.Type != concrnt.CCFSTypeConcrnt {
			return nil, domain.NotFoundError{Resource: uri}
		}
		// A commit flagged for gc is a version that was deleted or replaced
		// without asking to be retained: it stays in the log only as a replay
		// tombstone and is not served.
		var commitLog CommitLog
		err = r.db.WithContext(ctx).
			Where("id = ? AND NOT gc_candidate", parsed.CDID).
			Take(&commitLog).Error
		if err != nil {
			span.RecordError(errors.Join(errors.New("failed to query commitlog from ccfs"), err))
			return nil, errors.Join(domain.NotFoundError{Resource: uri}, err)
		}

		var cckv *string = nil
		var recordKey RecordKey
		err = r.db.WithContext(ctx).
			Select("uri").
			Where("record_id = ?", commitLog.ID).
			Take(&recordKey).Error
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

	db, err := getRecordTx(ctx, tx)
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

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	var rk RecordKey
	err = db.Select("uri").Where("record_id = ?", documentID).Take(&rk).Error
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

	var rk RecordKey
	err := db.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("uri = ?", uri).
		Take(&rk).Error
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

	hasChildren, err := recordKeyHasChildren(db, rk.ID)
	if err != nil {
		return err
	}
	if hasChildren {
		err := db.Model(&RecordKey{}).
			Where("id = ?", rk.ID).
			Updates(map[string]any{
				"record_id":         nil,
				"owner":             nil,
				"author":            nil,
				"schema":            nil,
				"redirect":          nil,
				"policies":          nil,
				"distributions":     nil,
				"record_created_at": nil,
				"clean_on_update":   false,
			}).Error
		if err != nil {
			return err
		}
		return db.Delete(&Association{}, "target_id = ?", rk.ID).Error
	}

	if err := db.Delete(&RecordKey{}, "id = ?", rk.ID).Error; err != nil {
		return err
	}
	return pruneEmptyPlaceholders(db, rk.ParentID)
}

// pruneEmptyPlaceholders walks up from parentID and deletes placeholder keys
// that hold no record, no children and no associations, stopping at the
// first ancestor that still holds something.
func pruneEmptyPlaceholders(db *gorm.DB, parentID *int64) error {
	for parentID != nil {
		var parent RecordKey
		err := db.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", *parentID).
			Take(&parent).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if parent.RecordID != nil {
			return nil
		}

		hasChildren, err := recordKeyHasChildren(db, parent.ID)
		if err != nil {
			return err
		}
		if hasChildren {
			return nil
		}

		var hasAssociations bool
		err = db.Raw("SELECT EXISTS (SELECT 1 FROM associations WHERE target_id = ?)", parent.ID).
			Scan(&hasAssociations).Error
		if err != nil {
			return err
		}
		if hasAssociations {
			return nil
		}

		if err := db.Delete(&RecordKey{}, "id = ?", parent.ID).Error; err != nil {
			return err
		}
		parentID = parent.ParentID
	}
	return nil
}

func recordKeyHasChildren(db *gorm.DB, id int64) (bool, error) {
	var exists bool
	err := db.Raw("SELECT EXISTS (SELECT 1 FROM record_keys WHERE parent_id = ?)", id).
		Scan(&exists).Error
	return exists, err
}

func (r *Repository) GetTimelineRemoval(ctx context.Context, keyURI string) (string, string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetTimelineRemoval")
	defer span.End()

	var recordKey RecordKey
	err := r.db.WithContext(ctx).
		Where("uri = ?", keyURI).
		Take(&recordKey).Error
	if err != nil {
		span.RecordError(err)
		return "", "", domain.NotFoundError{Resource: keyURI}
	}

	// only rows with a parent and a record_created_at appear in chunkline bodies
	if recordKey.ParentID == nil || recordKey.RecordCreatedAt == nil {
		return "", "", nil
	}

	var parent RecordKey
	err = r.db.WithContext(ctx).
		Select("uri").
		Where("id = ?", *recordKey.ParentID).
		Take(&parent).Error
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
	var parentRK RecordKey
	err = db.WithContext(ctx).
		Clauses(clause.Locking{Strength: "KEY SHARE"}).
		Where("uri = ?", parentURI).
		Take(&parentRK).Error
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

	newRecordKey := RecordKey{
		URI:      parentURI,
		RecordID: nil,
		ParentID: pid,
	}

	err = db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "uri"}},
		DoNothing: true,
	}).Create(&newRecordKey).Error
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// 別トランザクションが先に作成済み（競合）なら ID は 0 のまま → 再取得
	if newRecordKey.ID == 0 {
		var existing RecordKey
		err = db.WithContext(ctx).
			Clauses(clause.Locking{Strength: "KEY SHARE"}).
			Where("uri = ?", parentURI).
			Take(&existing).Error
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		return &existing, nil
	}

	return &newRecordKey, nil
}

func getRecordKeyByURI(ctx context.Context, db *gorm.DB, uri string) (*RecordKey, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.getRecordKeyByURI")
	defer span.End()

	var recordKey RecordKey
	err := db.WithContext(ctx).
		Where("uri = ?", uri).
		Take(&recordKey).Error
	if err != nil {
		span.RecordError(err)
		return nil, domain.NotFoundError{Resource: "record key"}
	}

	return &recordKey, nil
}
