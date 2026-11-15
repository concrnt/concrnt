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

func (r *Repository) ApplyRecord(ctx context.Context, commit ucrecord.RecordCommit) (ucrecord.RecordOutcome, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ApplyRecord")
	defer span.End()

	var outcome ucrecord.RecordOutcome
	status, err := r.apply(ctx, func(tx *gorm.DB) (ucrecord.ApplyStatus, error) {
		inserted, err := insertCommit(tx, commit.Commit)
		if err != nil {
			return ucrecord.Applied, err
		}
		if !inserted {
			return ucrecord.Duplicate, nil
		}
		outcome, err = createRecord(ctx, tx, commit)
		return outcome.Status, err
	})
	if err != nil {
		span.RecordError(err)
		return ucrecord.RecordOutcome{}, err
	}
	outcome.Status = status
	return outcome, nil
}

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
	db, err := getRecordTx(ctx, tx)
	if err != nil {
		return false, err
	}
	outcome, err := createRecord(ctx, db, ucrecord.RecordCommit{
		Commit:        ucrecord.Commit{ID: documentID, Owner: owner},
		Key:           key,
		Author:        author,
		Schema:        schema,
		OnUpdate:      onUpdate,
		Policies:      policies,
		Distributions: distributions,
		Redirect:      redirect,
		CreatedAt:     createdAt,
	})
	return outcome.Status == ucrecord.Applied, err
}

func createRecord(ctx context.Context, db *gorm.DB, commit ucrecord.RecordCommit) (ucrecord.RecordOutcome, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateRecord")
	defer span.End()

	db = db.WithContext(ctx)

	// Lock the RecordKey before writing anything. CIP-3 §3.4 accept-if-newer
	// on the document's createdAt: only a strictly newer document takes the
	// key, an equal or older one is a no-op. Deciding under the row lock
	// closes the read-then-write race between concurrent commits, same as
	// CreateEntity's conditional upsert.
	var oldRecordKey models.RecordKey
	err := db.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("uri = ?", commit.Key).
		Take(&oldRecordKey).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		span.RecordError(err)
		return ucrecord.RecordOutcome{}, err
	}
	if oldRecordKey.RecordID != nil && oldRecordKey.RecordCreatedAt != nil && !commit.CreatedAt.After(*oldRecordKey.RecordCreatedAt) {
		return ucrecord.RecordOutcome{Status: ucrecord.Stale}, nil
	}

	record := models.Record{
		DocumentID:    commit.ID,
		Owner:         commit.Owner,
		Author:        commit.Author,
		Redirect:      commit.Redirect,
		Schema:        commit.Schema,
		Policies:      commit.Policies,
		Distributions: commit.Distributions,
		CreatedAt:     commit.CreatedAt,
	}

	if err := db.Clauses(clause.OnConflict{
		DoNothing: true,
	}).Create(&record).Error; err != nil {
		span.RecordError(err)
		return ucrecord.RecordOutcome{}, err
	}

	// ParentのRecordKeyを探す
	parentRK, err := getOrCreateParentRecordKey(ctx, db, commit.Key)
	if err != nil {
		span.RecordError(err)
		return ucrecord.RecordOutcome{}, err
	}

	var pid *int64
	if parentRK != nil {
		pid = &parentRK.ID
	}
	cleanOnUpdate := commit.OnUpdate == nil || *commit.OnUpdate != "retain"

	// RecordKeyを作る
	rk := models.RecordKey{
		URI:             commit.Key,
		ParentID:        pid,
		RecordID:        &commit.ID,
		RecordCreatedAt: &commit.CreatedAt,
		Redirect:        commit.Redirect,
		CleanOnUpdate:   cleanOnUpdate,
	}

	// The FOR UPDATE fast-path above cannot lock a RecordKey row that does not
	// exist yet, so two concurrent commits to a fresh key both pass it. The
	// conditional upsert closes that window the same way CreateEntity does:
	// only a strictly newer document may take the key. IS NULL keeps normal
	// writes to parent placeholder rows (RecordID nil) working.
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "uri"}},
		DoUpdates: clause.Assignments(map[string]any{"record_id": commit.ID, "parent_id": pid, "record_created_at": commit.CreatedAt, "redirect": commit.Redirect, "clean_on_update": cleanOnUpdate}),
		Where:     clause.Where{Exprs: []clause.Expression{gorm.Expr("record_keys.record_id IS NULL OR record_keys.record_created_at IS NULL OR record_keys.record_created_at < excluded.record_created_at")}},
	}).Create(&rk)
	if result.Error != nil {
		span.RecordError(result.Error)
		return ucrecord.RecordOutcome{}, result.Error
	}
	if result.RowsAffected == 0 {
		return ucrecord.RecordOutcome{Status: ucrecord.Stale}, nil
	}

	// 古いRecordKeyが指していたCommitのGCフラグを立て、Recordは消す
	if oldRecordKey.CleanOnUpdate && oldRecordKey.RecordID != nil && *oldRecordKey.RecordID != commit.ID {
		if err := db.Model(&models.CommitLog{}).
			Where("id = ?", oldRecordKey.RecordID).
			Update("gc_candidate", true).Error; err != nil {
			span.RecordError(err)
			return ucrecord.RecordOutcome{}, err
		}
		if err := db.Delete(&models.Record{}, "document_id = ?", oldRecordKey.RecordID).Error; err != nil {
			span.RecordError(err)
			return ucrecord.RecordOutcome{}, err
		}
	}

	outcome := ucrecord.RecordOutcome{Status: ucrecord.Applied, FreshKey: oldRecordKey.RecordID == nil}
	if oldRecordKey.RecordID != nil {
		outcome.PreviousID = *oldRecordKey.RecordID
	}
	return outcome, nil
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
			Joins("JOIN records r ON r.document_id = record_keys.record_id").
			Where("r.document_id = ?", parsed.CDID).
			Take(&rk).Error
		if err != nil {
			span.RecordError(err)
			return nil, errors.Join(domain.NotFoundError{Resource: uri}, err)
		}
		cckv = rk.URI
	}

	stacks, err := r.GetHierarchicalRecordPoliciesMulti(ctx, []string{cckv})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	return stacks[cckv], nil
}

func (r *Repository) GetHierarchicalRecordPoliciesMulti(ctx context.Context, uris []string) (map[string][]concrnt.Policy, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetHierarchicalRecordPoliciesMulti")
	defer span.End()

	hierarchies := make(map[string][]string, len(uris))
	ancestors := []string{}
	for _, cckv := range uris {
		parsed, err := concrnt.ParseCCURI(cckv)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		if parsed.Scheme != "cckv" {
			err := fmt.Errorf("policy stacks are keyed by cckv, got %s", cckv)
			span.RecordError(err)
			return nil, err
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
		hierarchies[cckv] = hierarchy
		ancestors = append(ancestors, hierarchy...)
	}

	stacks := make(map[string][]concrnt.Policy, len(uris))
	if len(ancestors) == 0 {
		return stacks, nil
	}

	type tuple struct {
		Uri           string         `gorm:"column:uri"`
		Policy        *string        `gorm:"column:policy"`
		Distributions pq.StringArray `gorm:"column:distributions;type:text[]"`
	}

	var entries []tuple
	err := r.db.WithContext(ctx).
		Model(&models.Record{}).
		Select("rk.uri AS uri, records.policies AS policy, records.distributions AS distributions").
		Joins("JOIN record_keys rk ON rk.record_id = records.document_id").
		Where("rk.uri IN ?", ancestors).
		Find(&entries).Error
	if err != nil {
		span.RecordError(err)
		return nil, err
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
	// last (hierarchy is already built [root, ..., leaf]). With the evaluator's
	// strong=first-wins / weak=last-wins folding this makes strong conclusions
	// prefer the outer (global-side) layers and weak conclusions the resource
	// itself.
	for cckv, hierarchy := range hierarchies {
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
		var recordKey models.RecordKey
		err = r.db.WithContext(ctx).
			Joins("Record").
			Joins("Record.Document").
			Where("uri = ?", uri).
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
		err = json.Unmarshal([]byte(recordKey.Record.Document.Proof), &proof)
		if err != nil {
			span.RecordError(errors.Join(errors.New("failed to unmarshal proof from recordkey"), err))
			return nil, err
		}

		ccfs := concrnt.ComposeCCFSURI(parsed.Owner, concrnt.CCFSTypeConcrnt, *recordKey.RecordID)

		return &concrnt.SignedDocument{
			CCKV:     &uri,
			CCFS:     &ccfs,
			Document: recordKey.Record.Document.Document,
			Proof:    proof,
		}, nil

	case "ccfs":
		if parsed.Type != concrnt.CCFSTypeConcrnt {
			return nil, domain.NotFoundError{Resource: uri}
		}
		var commitLog models.CommitLog
		err = r.db.WithContext(ctx).
			Where("id = ?", parsed.CDID).
			Take(&commitLog).Error
		if err != nil {
			span.RecordError(errors.Join(errors.New("failed to query commitlog from ccfs"), err))
			return nil, errors.Join(domain.NotFoundError{Resource: uri}, err)
		}

		var cckv *string = nil
		var recordKey models.RecordKey
		err = r.db.WithContext(ctx).
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

func (r *Repository) GetSignedDocuments(ctx context.Context, uris []string) (map[string]*concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetSignedDocuments")
	defer span.End()

	cckvs := []string{}
	ids := []string{}
	idOf := map[string]string{}
	for _, uri := range uris {
		parsed, err := concrnt.ParseCCURI(uri)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		switch parsed.Scheme {
		case "cckv":
			cckvs = append(cckvs, uri)
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

	if len(cckvs) > 0 {
		var rks []models.RecordKey
		err := r.db.WithContext(ctx).
			Model(&models.RecordKey{}).
			Joins("JOIN records r ON r.document_id = record_keys.record_id").
			Joins("JOIN commit_logs c ON c.id = record_keys.record_id").
			Where("record_keys.uri IN ?", cckvs).
			Select(recordKeySelect).
			Find(&rks).Error
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		rows, err := recordKeysToQueryRows(rks, span)
		if err != nil {
			return nil, err
		}
		for i := range rows {
			out[*rows[i].Row.CCKV] = &rows[i].Row
		}
	}

	if len(ids) > 0 {
		var commits []struct {
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
			Scan(&commits).Error
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		byID := make(map[string]concrnt.SignedDocument, len(commits))
		for _, commit := range commits {
			var proof concrnt.Proof
			if err := json.Unmarshal([]byte(commit.Proof), &proof); err != nil {
				span.RecordError(errors.Join(errors.New("failed to unmarshal proof from commitlog"), err))
				return nil, err
			}
			byID[commit.ID] = concrnt.SignedDocument{CCKV: commit.URI, Document: commit.Document, Proof: proof}
		}
		for uri, id := range idOf {
			if sd, ok := byID[id]; ok {
				sd.CCFS = &uri
				out[uri] = &sd
			}
		}
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

	var recordKey struct {
		models.RecordKey
		ParentURI *string
	}
	err := r.db.WithContext(ctx).
		Model(&models.RecordKey{}).
		Select("record_keys.*, parent.uri AS parent_uri").
		Joins("LEFT JOIN record_keys parent ON parent.id = record_keys.parent_id").
		Where("record_keys.uri = ?", keyURI).
		Take(&recordKey).Error
	if err != nil {
		span.RecordError(err)
		return "", "", domain.NotFoundError{Resource: keyURI}
	}

	// only rows with a parent and a record_created_at appear in chunkline bodies
	if recordKey.ParentURI == nil || recordKey.RecordCreatedAt == nil {
		return "", "", nil
	}

	// derive the item ID exactly the way LoadLocalBody derives BodyItem.Href
	// (which is what BodyItem.ID() returns): the redirect target for
	// reference records, the record key URI otherwise
	itemID := recordKey.URI
	if recordKey.Redirect != nil {
		itemID = *recordKey.Redirect
	}

	return *recordKey.ParentURI, itemID, nil
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

func getRecordKeyByURI(ctx context.Context, db *gorm.DB, uri string) (*models.RecordKey, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.getRecordKeyByURI")
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
