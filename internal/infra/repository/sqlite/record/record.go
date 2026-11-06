package record

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
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

	// CIP-3 §3.4 accept-if-newer on the document's createdAt: only a
	// strictly newer document takes the key, an equal or older one is a
	// no-op. The IMMEDIATE transaction holds the single writer lock, so this
	// read is already serialised against every other writer (Postgres locks
	// the row instead).
	var oldRecordKey sqlitedb.RecordKey
	err = db.Where("uri = ?", key).Take(&oldRecordKey).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		span.RecordError(err)
		return false, err
	}
	if oldRecordKey.RecordID != nil && oldRecordKey.RecordCreatedAt != nil && !createdAt.After(sqlitedb.Time(*oldRecordKey.RecordCreatedAt)) {
		return false, nil
	}

	record := sqlitedb.Record{
		DocumentID:    documentID,
		Owner:         owner,
		Author:        author,
		Redirect:      redirect,
		Schema:        schema,
		Policies:      policies,
		Distributions: distributions,
		CreatedAt:     sqlitedb.Micros(createdAt),
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
	rk := sqlitedb.RecordKey{
		URI:             key,
		ParentID:        pid,
		RecordID:        &documentID,
		RecordCreatedAt: sqlitedb.MicrosPtr(&createdAt),
		Redirect:        redirect,
		CleanOnUpdate:   cleanOnUpdate,
	}

	// The conditional upsert closes the window two concurrent commits to a
	// fresh key have (same as Postgres): only a strictly newer document may
	// take the key. IS NULL keeps normal writes to parent placeholder rows
	// (RecordID nil) working.
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "uri"}},
		DoUpdates: clause.Assignments(map[string]any{"record_id": documentID, "parent_id": pid, "record_created_at": sqlitedb.Micros(createdAt), "redirect": redirect, "clean_on_update": cleanOnUpdate}),
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
		if err := db.Model(&sqlitedb.CommitLog{}).
			Where("id = ?", oldRecordKey.RecordID).
			Update("gc_candidate", true).Error; err != nil {
			span.RecordError(err)
			return false, err
		}
		if err := db.Delete(&sqlitedb.Record{}, "document_id = ?", oldRecordKey.RecordID).Error; err != nil {
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
		var rk sqlitedb.RecordKey
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

// policiesFor builds the policy stack of every cckv URI with one query
// over the union of their ancestor chains.
func (r *Repository) policiesFor(ctx context.Context, cckvs []string) (map[string][]concrnt.Policy, error) {
	return repoutil.PolicyStacks(cckvs, repoutil.HierarchyOf, func(uris []string) (map[string]repoutil.PolicyLayer, error) {
		var entries []struct {
			Uri           string               `gorm:"column:uri"`
			Policy        *string              `gorm:"column:policy"`
			Distributions sqlitedb.JSONStrings `gorm:"column:distributions"`
		}
		err := r.db.WithContext(ctx).
			Model(&sqlitedb.Record{}).
			Select("rk.uri AS uri, records.policies AS policy, records.distributions AS distributions").
			Joins("JOIN record_keys rk ON rk.record_id = records.document_id").
			Where("rk.uri IN ?", uris).
			Find(&entries).Error
		if err != nil {
			return nil, err
		}
		layers := make(map[string]repoutil.PolicyLayer, len(entries))
		for _, e := range entries {
			layers[e.Uri] = repoutil.PolicyLayer{Policy: e.Policy, Distributions: e.Distributions}
		}
		return layers, nil
	})
}

func (r *Repository) GetSignedDocument(ctx context.Context, uri string) (*concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetSignedDocument")
	defer span.End()

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

// GetSignedDocuments fetches several documents in one statement per URI
// scheme; URIs that resolve to nothing are left out of the map.
func (r *Repository) GetSignedDocuments(ctx context.Context, uris []string) (map[string]*concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetSignedDocuments")
	defer span.End()

	keys, ids, idOf, err := repoutil.PartitionDocumentURIs(uris)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	out := make(map[string]*concrnt.SignedDocument, len(uris))
	if len(keys) > 0 {
		rows, err := scanRecordRows(joinRecords(r.db.WithContext(ctx).Model(&sqlitedb.RecordKey{}), false).Where("record_keys.uri IN ?", keys))
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
		Model(&sqlitedb.CommitLog{}).
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

	q := db.Model(&sqlitedb.RecordKey{}).Select("record_id").Where("uri = ?", targetURI)
	return db.Where("document_id = (?)", q).Delete(&sqlitedb.Record{}).Error
}

func (r *Repository) DeleteRecordByDocumentID(ctx context.Context, tx ucrecord.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteRecordByDocumentID")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	return db.Delete(&sqlitedb.Record{}, "document_id = ?", documentID).Error
}

func (r *Repository) GetTimelineRemoval(ctx context.Context, keyURI string) (string, string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetTimelineRemoval")
	defer span.End()

	var row struct {
		Redirect        *string
		RecordCreatedAt *int64
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

func getOrCreateParentRecordKey(ctx context.Context, db *gorm.DB, uri string) (*sqlitedb.RecordKey, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.getOrCreateParentRecordKey")
	defer span.End()

	parentURI, ok, err := repoutil.ParentURI(uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	parentRK, err := getRecordKeyByURI(ctx, db, parentURI)
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

			newRecordKey := sqlitedb.RecordKey{
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
				return getRecordKeyByURI(ctx, db, parentURI)
			}

			return &newRecordKey, nil

		} else {
			span.RecordError(err)
			return nil, err
		}
	}

	return parentRK, nil
}

func getRecordKeyByURI(ctx context.Context, db *gorm.DB, uri string) (*sqlitedb.RecordKey, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.getRecordKeyByURI")
	defer span.End()

	var recordKey sqlitedb.RecordKey
	err := db.WithContext(ctx).
		Where("uri = ?", uri).
		Take(&recordKey).Error
	if err != nil {
		span.RecordError(err)
		return nil, domain.NotFoundError{Resource: "record key"}
	}

	return &recordKey, nil
}
