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
	"github.com/concrnt/concrnt/internal/infra/database/models"
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
	var oldRecordKey models.RecordKey
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

	record := models.Record{
		DocumentID:    documentID,
		Owner:         owner,
		Author:        author,
		Redirect:      redirect,
		Schema:        schema,
		Policies:      policies,
		Distributions: distributions,
		CreatedAt:     createdAt,
	}

	if err := db.Clauses(clause.OnConflict{
		DoNothing: true,
	}).Create(&record).Error; err != nil {
		span.RecordError(err)
		return false, err
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
	rk := models.RecordKey{
		URI:             key,
		ParentID:        pid,
		RecordID:        &documentID,
		RecordCreatedAt: &createdAt,
		Redirect:        redirect,
		CleanOnUpdate:   cleanOnUpdate,
	}

	// The FOR UPDATE fast-path above cannot lock a RecordKey row that does not
	// exist yet, so two concurrent commits to a fresh key both pass it. The
	// conditional upsert closes that window the same way CreateEntity does:
	// only a strictly newer document may take the key. IS NULL keeps normal
	// writes to parent placeholder rows (RecordID nil) working.
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "uri"}},
		DoUpdates: clause.Assignments(map[string]any{"record_id": documentID, "parent_id": pid, "record_created_at": createdAt, "redirect": redirect, "clean_on_update": cleanOnUpdate}),
		Where:     clause.Where{Exprs: []clause.Expression{gorm.Expr("record_keys.record_id IS NULL OR record_keys.record_created_at IS NULL OR record_keys.record_created_at < excluded.record_created_at")}},
	}).Create(&rk)
	if result.Error != nil {
		span.RecordError(result.Error)
		return false, result.Error
	}
	if result.RowsAffected == 0 {
		return false, nil
	}

	// 古いRecordKeyが指していたCommitのGCフラグを立て、Recordは消す
	if oldRecordKey.CleanOnUpdate && oldRecordKey.RecordID != nil && *oldRecordKey.RecordID != documentID {
		if err := db.Model(&models.CommitLog{}).
			Where("id = ?", oldRecordKey.RecordID).
			Update("gc_candidate", true).Error; err != nil {
			span.RecordError(err)
			return false, err
		}
		if err := db.Delete(&models.Record{}, "document_id = ?", oldRecordKey.RecordID).Error; err != nil {
			span.RecordError(err)
			return false, err
		}
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
		var rk models.RecordKey
		err = r.db.WithContext(ctx).
			Where("record_id = ?", parsed.CDID).
			Take(&rk).Error
		if err != nil {
			span.RecordError(err)
			return nil, errors.Join(domain.NotFoundError{Resource: uri}, err)
		}
		cckv = rk.URI
	}

	stacks, err := r.policiesFor(ctx, []string{cckv})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return stacks[cckv], nil
}

// GetHierarchicalRecordPoliciesMulti is GetHierarchicalRecordPolicies for
// several cckv URIs in one statement: the union of their ancestor chains is
// fetched at once and each stack assembled from it.
func (r *Repository) GetHierarchicalRecordPoliciesMulti(ctx context.Context, uris []string) (map[string][]concrnt.Policy, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetHierarchicalRecordPoliciesMulti")
	defer span.End()

	for _, uri := range uris {
		parsed, err := concrnt.ParseCCURI(uri)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		if parsed.Scheme != "cckv" {
			err := fmt.Errorf("policy stacks are keyed by cckv, got %s", uri)
			span.RecordError(err)
			return nil, err
		}
	}
	stacks, err := r.policiesFor(ctx, uris)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return stacks, nil
}

// hierarchyOf lists the ancestor chain of a cckv URI, root first, the URI
// itself last.
func hierarchyOf(cckv string) ([]string, error) {
	hierarchy := []string{}
	currentURI := cckv
	for {
		hierarchy = append([]string{currentURI}, hierarchy...)
		parentURI, err := url.JoinPath(currentURI, "..")
		if err != nil {
			return nil, err
		}
		if parentURI == currentURI {
			break
		}
		currentURI = parentURI
	}
	return hierarchy, nil
}

// policiesFor builds the CIP-12 §5.3 policy stack (root first, the resource
// itself last) of every cckv URI with one query over the union of their
// ancestor chains.
func (r *Repository) policiesFor(ctx context.Context, cckvs []string) (map[string][]concrnt.Policy, error) {
	hierarchies := make(map[string][]string, len(cckvs))
	wanted := map[string]bool{}
	for _, cckv := range cckvs {
		if _, ok := hierarchies[cckv]; ok {
			continue
		}
		hierarchy, err := hierarchyOf(cckv)
		if err != nil {
			return nil, err
		}
		hierarchies[cckv] = hierarchy
		for _, uri := range hierarchy {
			wanted[uri] = true
		}
	}
	uris := make([]string, 0, len(wanted))
	for uri := range wanted {
		uris = append(uris, uri)
	}

	type tuple struct {
		Uri           string         `gorm:"column:uri"`
		Policy        *string        `gorm:"column:policy"`
		Distributions pq.StringArray `gorm:"column:distributions;type:text[]"`
	}
	var entries []tuple
	if len(uris) > 0 {
		err := r.db.WithContext(ctx).
			Model(&models.Record{}).
			Select("rk.uri AS uri, records.policies AS policy, records.distributions AS distributions").
			Joins("JOIN record_keys rk ON rk.record_id = records.document_id").
			Where("rk.uri IN ?", uris).
			Find(&entries).Error
		if err != nil {
			return nil, err
		}
	}

	// A level with no policy of its own still matters when it distributes: its
	// virtual parents (the destination timelines' policies) must reach the
	// stack, so emit it as an empty layer rather than dropping it.
	tupleMap := make(map[string]tuple)
	for _, res := range entries {
		if res.Policy != nil || len(res.Distributions) > 0 {
			tupleMap[res.Uri] = res
		}
	}

	// CIP-12 §5.3: layers are emitted root-first, the target resource itself
	// last. With the evaluator's strong=first-wins / weak=last-wins folding
	// this makes strong conclusions prefer the outer (global-side) layers and
	// weak conclusions the resource itself.
	stacks := make(map[string][]concrnt.Policy, len(hierarchies))
	for cckv, hierarchy := range hierarchies {
		policies := []concrnt.Policy{}
		for _, uri := range hierarchy {
			t, ok := tupleMap[uri]
			if !ok {
				continue
			}
			var policyDoc concrnt.Policy
			if t.Policy != nil {
				if err := json.Unmarshal([]byte(*t.Policy), &policyDoc); err != nil {
					return nil, err
				}
			}
			policyDoc.Source = uri
			virtualParents := append([]string{}, t.Distributions...)
			policyDoc.VirtualParents = &virtualParents
			policies = append(policies, policyDoc)
		}
		stacks[cckv] = policies
	}
	return stacks, nil
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
		rows, err := scanRecordRows(joinRecords(r.db.WithContext(ctx).Model(&models.RecordKey{}), false).Where("record_keys.uri = ?", uri))
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		if len(rows) == 0 {
			return nil, domain.NotFoundError{Resource: uri}
		}
		sd, err := rows[0].signedDocument()
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		return &sd, nil

	case "ccfs":
		if parsed.Type != concrnt.CCFSTypeConcrnt {
			return nil, domain.NotFoundError{Resource: uri}
		}
		sds, err := r.signedDocumentsByID(ctx, []string{parsed.CDID})
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		sd, ok := sds[parsed.CDID]
		if !ok {
			return nil, domain.NotFoundError{Resource: uri}
		}
		sd.CCFS = &uri
		return &sd, nil
	default:
		err := fmt.Errorf("unsupported uri scheme: %s", parsed.Scheme)
		span.RecordError(err)
		return nil, err
	}
}

// GetSignedDocuments fetches several documents in one statement per URI
// scheme; URIs that resolve to nothing are left out of the map.
func (r *Repository) GetSignedDocuments(ctx context.Context, uris []string) (map[string]*concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetSignedDocuments")
	defer span.End()

	keys := []string{}
	ids := []string{}
	idOf := map[string]string{} // ccfs uri -> commit id
	for _, uri := range uris {
		parsed, err := concrnt.ParseCCURI(uri)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		switch parsed.Scheme {
		case "cckv":
			keys = append(keys, uri)
		case "ccfs":
			if parsed.Type == concrnt.CCFSTypeConcrnt {
				ids = append(ids, parsed.CDID)
				idOf[uri] = parsed.CDID
			}
		default:
			err := fmt.Errorf("unsupported uri scheme: %s", parsed.Scheme)
			span.RecordError(err)
			return nil, err
		}
	}

	out := make(map[string]*concrnt.SignedDocument, len(uris))
	if len(keys) > 0 {
		rows, err := scanRecordRows(joinRecords(r.db.WithContext(ctx).Model(&models.RecordKey{}), false).Where("record_keys.uri IN ?", keys))
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		for _, row := range rows {
			sd, err := row.signedDocument()
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
			out[row.URI] = &sd
		}
	}
	if len(ids) > 0 {
		sds, err := r.signedDocumentsByID(ctx, ids)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		for uri, id := range idOf {
			if sd, ok := sds[id]; ok {
				uri := uri
				sd.CCFS = &uri
				out[uri] = &sd
			}
		}
	}
	return out, nil
}

// signedDocumentsByID reads commit logs by id, each with the key currently
// pointing at it (if any), in one statement.
func (r *Repository) signedDocumentsByID(ctx context.Context, ids []string) (map[string]concrnt.SignedDocument, error) {
	var rows []struct {
		ID       string
		Document string
		Proof    string
		URI      *string
	}
	err := r.db.WithContext(ctx).
		Model(&models.CommitLog{}).
		Select("commit_logs.id AS id, commit_logs.document AS document, commit_logs.proof AS proof, rk.uri AS uri").
		Joins("LEFT JOIN record_keys rk ON rk.record_id = commit_logs.id").
		Where("commit_logs.id IN ?", ids).
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make(map[string]concrnt.SignedDocument, len(rows))
	for _, row := range rows {
		var proof concrnt.Proof
		if err := json.Unmarshal([]byte(row.Proof), &proof); err != nil {
			return nil, errors.Join(errors.New("failed to unmarshal proof from commitlog"), err)
		}
		out[row.ID] = concrnt.SignedDocument{CCKV: row.URI, Document: row.Document, Proof: proof}
	}
	return out, nil
}

func (r *Repository) DeleteRecordByKey(ctx context.Context, tx ucrecord.RepositoryTx, targetURI string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteRecordByKey")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	q := db.Model(&models.RecordKey{}).Select("record_id").Where("uri = ?", targetURI)
	return db.Where("document_id = (?)", q).Delete(&models.Record{}).Error
}

func (r *Repository) DeleteRecordByDocumentID(ctx context.Context, tx ucrecord.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteRecordByDocumentID")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	return db.Delete(&models.Record{}, "document_id = ?", documentID).Error
}

func (r *Repository) GetTimelineRemoval(ctx context.Context, keyURI string) (string, string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetTimelineRemoval")
	defer span.End()

	var row struct {
		Redirect        *string
		RecordCreatedAt *time.Time
		ParentURI       *string
	}
	res := r.db.WithContext(ctx).
		Table("record_keys AS rk").
		Select("rk.redirect AS redirect, rk.record_created_at AS record_created_at, p.uri AS parent_uri").
		Joins("LEFT JOIN record_keys p ON p.id = rk.parent_id").
		Where("rk.uri = ?", keyURI).
		Scan(&row)
	if res.Error != nil {
		span.RecordError(res.Error)
		return "", "", res.Error
	}
	if res.RowsAffected == 0 {
		return "", "", domain.NotFoundError{Resource: keyURI}
	}

	// only rows with a parent and a record_created_at appear in chunkline bodies
	if row.ParentURI == nil || row.RecordCreatedAt == nil {
		return "", "", nil
	}

	// derive the item ID exactly the way LoadLocalBody derives BodyItem.Href
	// (which is what BodyItem.ID() returns): the redirect target for
	// reference records, the record key URI otherwise
	itemID := keyURI
	if row.Redirect != nil {
		itemID = *row.Redirect
	}

	return *row.ParentURI, itemID, nil
}

func getOrCreateParentRecordKey(ctx context.Context, db *gorm.DB, uri string) (*models.RecordKey, error) {
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

	parentRK, err := GetRecordKeyByURI(ctx, db, parentURI)
	if err != nil {
		if errors.Is(err, domain.NotFoundError{}) {

			parentID, err := getOrCreateParentRecordKey(ctx, db, parentURI)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}

			var pid *int64
			if parentID != nil {
				pid = &parentID.ID
			}

			newRecordKey := models.RecordKey{
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
				return GetRecordKeyByURI(ctx, db, parentURI)
			}

			return &newRecordKey, nil

		} else {
			span.RecordError(err)
			return nil, err
		}
	}

	return parentRK, nil
}

func GetRecordKeyByURI(ctx context.Context, db *gorm.DB, uri string) (*models.RecordKey, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetRecordKeyByURI")
	defer span.End()

	var recordKey models.RecordKey
	err := db.WithContext(ctx).
		Where("uri = ?", uri).
		Take(&recordKey).Error
	if err != nil {
		span.RecordError(err)
		return nil, domain.NotFoundError{Resource: "record key"}
	}

	return &recordKey, nil
}
