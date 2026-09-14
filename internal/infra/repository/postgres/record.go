package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/lib/pq"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/internal/utils"
)

type RecordRepository struct {
	db *gorm.DB
}

type recordTx struct {
	tx *gorm.DB
}

func (*recordTx) IsRepositoryTx() {}

func NewRecordRepository(db *gorm.DB) record.Repository {
	return &RecordRepository{db: db}
}

// RunInTx runs fn inside one database transaction: fn's error rolls back and
// is returned unchanged, a nil return commits.
func (r *RecordRepository) RunInTx(ctx context.Context, fn func(tx record.RepositoryTx) error) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return fn(&recordTx{tx: tx})
	})
}

func getRecordTx(ctx context.Context, tx record.RepositoryTx) (*gorm.DB, error) {
	recordTx, ok := tx.(*recordTx)
	if !ok || recordTx == nil || recordTx.tx == nil {
		return nil, errors.New("invalid record repository transaction")
	}
	return recordTx.tx.WithContext(ctx), nil
}

func (r *RecordRepository) CreateEntity(
	ctx context.Context,
	tx record.RepositoryTx,
	ccid string,
	alias *string,
	domain string,
	documentID string,
	createdAt time.Time,
) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateEntity")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	modelEntity := models.Entity{
		ID:         ccid,
		Alias:      alias,
		Domain:     domain,
		DocumentID: documentID,
		CreatedAt:  createdAt,
	}

	// CIP-3 §3.4 accept-if-newer on the document's createdAt: only a strictly
	// newer document replaces the stored one; an equal or older one is a
	// no-op (RowsAffected 0). The conditional upsert makes newer-wins atomic
	// at the row lock, so concurrent writers converge on the same winner.
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"alias", "domain", "document_id", "created_at"}),
		Where:     clause.Where{Exprs: []clause.Expression{gorm.Expr("entities.created_at < excluded.created_at")}},
	}).Create(&modelEntity)
	if result.Error != nil {
		return false, result.Error
	}

	return result.RowsAffected > 0, nil
}

func (r *RecordRepository) HasCommitLog(ctx context.Context, id string) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.HasCommitLog")
	defer span.End()

	var commitLog models.CommitLog
	err := r.db.WithContext(ctx).
		Select("id").
		Where("id = ?", id).
		Take(&commitLog).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	return true, nil
}

func (r *RecordRepository) CreateCommitLog(ctx context.Context, tx record.RepositoryTx, id string, ip string, document string, proof any, owner string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateCommitLog")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	proofBytes, err := json.Marshal(proof)
	if err != nil {
		span.RecordError(err)
		return err
	}

	commitLog := models.CommitLog{
		ID:       id,
		IP:       ip,
		Document: document,
		Owner:    owner,
		Proof:    string(proofBytes),
	}

	if err := db.Clauses(clause.OnConflict{
		DoNothing: true,
	}).Create(&commitLog).Error; err != nil {
		span.RecordError(err)
		return err
	}

	return nil
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
		CleanOnUpdate:   cleanOnUpdate,
	}

	// The FOR UPDATE fast-path above cannot lock a RecordKey row that does not
	// exist yet, so two concurrent commits to a fresh key both pass it. The
	// conditional upsert closes that window the same way CreateEntity does:
	// only a strictly newer document may take the key. IS NULL keeps normal
	// writes to parent placeholder rows (RecordID nil) working.
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "uri"}},
		DoUpdates: clause.Assignments(map[string]any{"record_id": documentID, "parent_id": pid, "record_created_at": createdAt, "clean_on_update": cleanOnUpdate}),
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

func (r *RecordRepository) CreateAssociation(ctx context.Context, tx record.RepositoryTx, documentID string, targetURI string, owner string, author string, schema string, variant *string, unique string, createdAt time.Time) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateAssociation")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	targetRK, err := GetRecordKeyByURI(ctx, db, targetURI)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	association := models.Association{
		TargetID:   targetRK.ID,
		DocumentID: documentID,
		Unique:     unique,

		Owner:     owner,
		Author:    author,
		Variant:   variant,
		Schema:    schema,
		CreatedAt: createdAt,
	}

	// A targetless ON CONFLICT DO NOTHING suppresses duplicates on both the
	// document_id primary key (same document re-delivered) and
	// uni_associations_unique (same logical association under a new document
	// id), making duplicate deliveries a silent success like record/ack.
	result := db.Clauses(clause.OnConflict{
		DoNothing: true,
	}).Create(&association)
	if result.Error != nil {
		span.RecordError(result.Error)
		return false, result.Error
	}

	return result.RowsAffected > 0, nil
}

func (r *RecordRepository) processAck(ctx context.Context, tx record.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time, valid bool) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.processAck")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		return false, err
	}

	ack := models.Ack{
		From:       from,
		To:         to,
		Schema:     schema,
		DocumentID: documentID,
		Valid:      valid,
		CreatedAt:  createdAt,
	}

	// CIP-10 §4: only a document with a strictly newer createdAt may move
	// the (from, to, schema) state; an equal or older one is a no-op
	// (RowsAffected 0). The key is createdAt alone — never the document id —
	// so the acker's and the associate owner's servers, which hold different
	// documents with the same createdAt, reach the same decision.
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "from"}, {Name: "to"}, {Name: "schema"}},
		DoUpdates: clause.Assignments(map[string]any{"valid": valid, "document_id": documentID, "created_at": createdAt}),
		Where:     clause.Where{Exprs: []clause.Expression{gorm.Expr("acks.created_at < excluded.created_at")}},
	}).Create(&ack)
	if result.Error != nil {
		span.RecordError(result.Error)
		return false, result.Error
	}

	return result.RowsAffected > 0, nil

}

func (r *RecordRepository) Acknowledge(ctx context.Context, tx record.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAck(ctx, tx, documentID, from, to, schema, createdAt, true)

}

func (r *RecordRepository) UnAcknowledge(ctx context.Context, tx record.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAck(ctx, tx, documentID, from, to, schema, createdAt, false)
}

func (r *RecordRepository) processAcked(ctx context.Context, tx record.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time, valid bool) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.processAcked")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		return false, err
	}

	ack := models.Acked{
		From:       from,
		To:         to,
		Schema:     schema,
		DocumentID: documentID,
		Valid:      valid,
		CreatedAt:  createdAt,
	}

	// CIP-10 §4: only a document with a strictly newer createdAt may move
	// the (from, to, schema) state; an equal or older one is a no-op
	// (RowsAffected 0). The key is createdAt alone — never the document id —
	// so the acker's and the associate owner's servers, which hold different
	// documents with the same createdAt, reach the same decision.
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "from"}, {Name: "to"}, {Name: "schema"}},
		DoUpdates: clause.Assignments(map[string]any{"valid": valid, "document_id": documentID, "created_at": createdAt}),
		Where:     clause.Where{Exprs: []clause.Expression{gorm.Expr("ackeds.created_at < excluded.created_at")}},
	}).Create(&ack)
	if result.Error != nil {
		span.RecordError(result.Error)
		return false, result.Error
	}

	return result.RowsAffected > 0, nil

}

func (r *RecordRepository) Acknowledged(ctx context.Context, tx record.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAcked(ctx, tx, documentID, from, to, schema, createdAt, true)

}

func (r *RecordRepository) UnAcknowledged(ctx context.Context, tx record.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAcked(ctx, tx, documentID, from, to, schema, createdAt, false)
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

	type tuple struct {
		Uri           string         `gorm:"column:uri"`
		Policy        *string        `gorm:"column:policy"`
		Distributions pq.StringArray `gorm:"column:distributions;type:text[]"`
	}

	var entries []tuple
	err = r.db.WithContext(ctx).
		Model(&models.Record{}).
		Select("rk.uri AS uri, records.policies AS policy, records.distributions AS distributions").
		Joins("JOIN record_keys rk ON rk.record_id = records.document_id").
		Where("rk.uri IN ?", hierarchy).
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
		var recordKey models.RecordKey
		err = r.db.WithContext(ctx).
			Preload("Record").
			Preload("Record.Document").
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

func (r *RecordRepository) MarkCommitLogGcCandidate(ctx context.Context, tx record.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.MarkCommitLogGcCandidate")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	if err := db.Model(&models.CommitLog{}).
		Where("id = ?", documentID).
		Update("gc_candidate", true).Error; err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}

func (r *RecordRepository) DeleteRecordByKey(ctx context.Context, tx record.RepositoryTx, targetURI string) error {
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

func (r *RecordRepository) DeleteRecordByDocumentID(ctx context.Context, tx record.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteRecordByDocumentID")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	return db.Delete(&models.Record{}, "document_id = ?", documentID).Error
}

func (r *RecordRepository) GetTimelineRemoval(ctx context.Context, keyURI string) (string, string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetTimelineRemoval")
	defer span.End()

	var recordKey models.RecordKey
	err := r.db.WithContext(ctx).
		Preload("Record").
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

	var parent models.RecordKey
	err = r.db.WithContext(ctx).
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
	if recordKey.Record.Redirect != nil {
		itemID = *recordKey.Record.Redirect
	}

	return parent.URI, itemID, nil
}

func (r *RecordRepository) DeleteAssociation(ctx context.Context, tx record.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteAssociation")
	defer span.End()

	db, err := getRecordTx(ctx, tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	return db.Delete(&models.Association{}, "document_id = ?", documentID).Error
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
		var recordKey models.RecordKey
		err = r.db.WithContext(ctx).Preload("Record").
			Preload("Record.Document").
			Where("uri = ?", uri).
			Take(&recordKey).Error
		if err != nil {
			span.RecordError(err)
			return nil, errors.Join(domain.NotFoundError{Resource: uri}, err)
		}
		return recordKey.Record.Distributions, nil
	case "ccfs":
		if parsed.Type != concrnt.CCFSTypeConcrnt {
			return nil, domain.NotFoundError{Resource: uri}
		}
		var record models.Record
		err = r.db.WithContext(ctx).
			Where("document_id = ?", parsed.CDID).
			Take(&record).Error
		if err != nil {
			span.RecordError(err)
			return nil, errors.Join(domain.NotFoundError{Resource: uri}, err)
		}
		return record.Distributions, nil
	default:
		err := fmt.Errorf("unsupported uri scheme: %s", parsed.Scheme)
		span.RecordError(err)
		return nil, err
	}
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

	var associations []models.Association

	query := r.db.WithContext(ctx).
		Model(&models.Association{}).
		Preload("Document").
		Joins("JOIN record_keys rk ON rk.id = associations.target_id").
		Where("rk.uri = ?", targetURI)

	if schema != "" {
		query = query.Where("associations.schema = ?", schema)
	}
	if variant != "" {
		query = query.Where("associations.variant = ?", variant)
	}
	if author != "" {
		query = query.Where("associations.author = ?", author)
	}
	if since != nil {
		query = query.Where("associations.created_at >= ?", *since)
	}
	if until != nil {
		query = query.Where("associations.created_at <= ?", *until)
	}

	if order == "desc" {
		query = query.Order("associations.created_at DESC, associations.document_id DESC")
	} else {
		query = query.Order("associations.created_at ASC, associations.document_id ASC")
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	if err := query.Find(&associations).Error; err != nil {
		return nil, err
	}

	rows := make([]record.QueryRow, len(associations))
	for i, assoc := range associations {
		var proof concrnt.Proof
		err := json.Unmarshal([]byte(assoc.Document.Proof), &proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		ccfs := concrnt.ComposeCCFSURI(assoc.Owner, concrnt.CCFSTypeConcrnt, assoc.DocumentID)

		rows[i] = record.QueryRow{
			Row: concrnt.SignedDocument{
				CCFS:     &ccfs,
				Document: assoc.Document.Document,
				Proof:    proof,
			},
			CreatedAt: assoc.CreatedAt,
		}
	}

	return rows, nil
}

func (r *RecordRepository) GetAssociatedRecordCountsBySchema(ctx context.Context, targetURI string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAssociatedRecordCountsBySchema")
	defer span.End()

	var counts []struct {
		Schema string
		Count  int64
	}

	err := r.db.WithContext(ctx).
		Model(&models.Association{}).
		Select("schema, COUNT(*) AS count").
		Joins("JOIN record_keys rk ON rk.id = associations.target_id").
		Where("rk.uri = ?", targetURI).
		Group("schema").
		Scan(&counts).Error

	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	result := make(map[string]int64)
	for _, c := range counts {
		result[c.Schema] = c.Count
	}

	return result, nil
}

func (r *RecordRepository) GetAssociatedRecordCountsByVariant(ctx context.Context, targetURI, schema string) (*utils.OrderedKVMap[int64], error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAssociatedRecordCountsByVariant")
	defer span.End()

	var counts []struct {
		Variant      string
		Count        int64
		MinCreatedAt time.Time
	}

	err := r.db.WithContext(ctx).
		Model(&models.Association{}).
		Select("variant, COUNT(*) AS count, MIN(created_at) AS min_created_at").
		Joins("JOIN record_keys rk ON rk.id = associations.target_id").
		Where("rk.uri = ?", targetURI).
		Where("associations.schema = ?", schema).
		Group("variant").
		Order("min_created_at ASC").
		Scan(&counts).Error
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	result := make(utils.OrderedKVMap[int64])
	for _, c := range counts {
		result[c.Variant] = utils.OrderedKV[int64]{
			Value: c.Count,
			Order: c.MinCreatedAt.UnixNano(),
		}
	}

	return &result, nil
}

func (r *RecordRepository) QueryByPrefix(
	ctx context.Context,
	prefix, schema, author string,
	since, until *time.Time,
	limit int,
	order string,
) ([]record.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.Query")
	defer span.End()

	var rks []models.RecordKey

	query := r.db.WithContext(ctx).
		Model(&models.RecordKey{}).
		Joins("JOIN records r ON r.document_id = record_keys.record_id").
		Where("uri LIKE ?", prefix+"%")

	if schema != "" {
		query = query.Where("r.schema = ?", schema)
	}
	if author != "" {
		query = query.Where("r.author = ?", author)
	}
	if since != nil {
		query = query.Where("r.created_at >= ?", *since)
	}
	if until != nil {
		query = query.Where("r.created_at <= ?", *until)
	}

	if order == "desc" {
		query = query.Order("r.created_at DESC, r.document_id DESC")
	} else {
		query = query.Order("r.created_at ASC, r.document_id ASC")
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	if err := query.Preload("Record.Document").Find(&rks).Error; err != nil {
		span.RecordError(err)
		return nil, err
	}

	return recordKeysToQueryRows(rks, span)
}

// recordKeysToQueryRows converts preloaded record keys into query rows
// carrying the created_at sort key the query ordered by.
func recordKeysToQueryRows(rks []models.RecordKey, span trace.Span) ([]record.QueryRow, error) {
	rows := make([]record.QueryRow, 0, len(rks))
	for _, rk := range rks {
		var proof concrnt.Proof
		err := json.Unmarshal([]byte(rk.Record.Document.Proof), &proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		ccfs := concrnt.ComposeCCFSURI(rk.Record.Owner, concrnt.CCFSTypeConcrnt, rk.Record.DocumentID)

		rows = append(rows, record.QueryRow{
			Row: concrnt.SignedDocument{
				CCKV:     &rk.URI,
				CCFS:     &ccfs,
				Document: rk.Record.Document.Document,
				Proof:    proof,
			},
			CreatedAt: rk.Record.CreatedAt,
		})
	}

	return rows, nil
}

// likeEscaper escapes LIKE metacharacters so a key containing '%' or '_'
// can't over-match when embedded in a pattern.
var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

// QueryRecordSubtree returns every live record whose key is base itself
// (includeSelf) or lies under base's path subtree (base + "/..."), ordered by
// URI so base leads. Unlike QueryByPrefix's raw prefix match this never picks
// up sibling keys ("item2" for base "item"), and metacharacters in base are
// escaped — the result is exactly the set a range delete may remove.
func (r *RecordRepository) QueryRecordSubtree(ctx context.Context, base string, includeSelf bool) ([]concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryRecordSubtree")
	defer span.End()

	query := r.db.WithContext(ctx).
		Model(&models.RecordKey{}).
		Joins("JOIN records r ON r.document_id = record_keys.record_id")

	subtreePattern := likeEscaper.Replace(base) + `/%`
	if includeSelf {
		query = query.Where(`uri = ? OR uri LIKE ? ESCAPE '\'`, base, subtreePattern)
	} else {
		query = query.Where(`uri LIKE ? ESCAPE '\'`, subtreePattern)
	}

	var rks []models.RecordKey
	if err := query.Order("uri ASC").Preload("Record.Document").Find(&rks).Error; err != nil {
		span.RecordError(err)
		return nil, err
	}

	sds := make([]concrnt.SignedDocument, 0, len(rks))
	for _, rk := range rks {
		var proof concrnt.Proof
		err := json.Unmarshal([]byte(rk.Record.Document.Proof), &proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		ccfs := concrnt.ComposeCCFSURI(rk.Record.Owner, concrnt.CCFSTypeConcrnt, rk.Record.DocumentID)

		sds = append(sds, concrnt.SignedDocument{
			CCKV:     &rk.URI,
			CCFS:     &ccfs,
			Document: rk.Record.Document.Document,
			Proof:    proof,
		})
	}

	return sds, nil
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

	var rks []models.RecordKey

	query := r.db.WithContext(ctx).
		Model(&models.RecordKey{}).
		Joins("JOIN records r ON r.document_id = record_keys.record_id").
		Where("parent_id = (SELECT id FROM record_keys WHERE uri = ?)", parent)

	if schema != "" {
		query = query.Where("r.schema = ?", schema)
	}
	if author != "" {
		query = query.Where("r.author = ?", author)
	}
	if since != nil {
		query = query.Where("r.created_at >= ?", *since)
	}
	if until != nil {
		query = query.Where("r.created_at <= ?", *until)
	}

	if order == "desc" {
		query = query.Order("r.created_at DESC, r.document_id DESC")
	} else {
		query = query.Order("r.created_at ASC, r.document_id ASC")
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	if err := query.Preload("Record.Document").Find(&rks).Error; err != nil {
		span.RecordError(err)
		return nil, err
	}

	return recordKeysToQueryRows(rks, span)
}

func (r *RecordRepository) getAckRecords(ctx context.Context, from, schema string, since, until *time.Time, limit int, order string) ([]record.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.getAckRecords")
	defer span.End()

	var acks []models.Ack
	query := r.db.WithContext(ctx).
		Model(&models.Ack{}).
		Preload("Document").
		Where("acks.from = ?", from).
		Where("acks.valid = ?", true)

	if schema != "" {
		query = query.Where("acks.schema = ?", schema)
	}

	if since != nil {
		query = query.Where("acks.created_at >= ?", *since)
	}
	if until != nil {
		query = query.Where("acks.created_at <= ?", *until)
	}

	if order == "desc" {
		query = query.Order("acks.created_at DESC, acks.document_id DESC")
	} else {
		query = query.Order("acks.created_at ASC, acks.document_id ASC")
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	err := query.Find(&acks).Error
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	rows := make([]record.QueryRow, len(acks))
	for i, ack := range acks {
		var proof concrnt.Proof
		err := json.Unmarshal([]byte(ack.Document.Proof), &proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		ccfs := concrnt.ComposeCCFSURI(ack.From, concrnt.CCFSTypeConcrnt, ack.DocumentID)

		rows[i] = record.QueryRow{
			Row: concrnt.SignedDocument{
				CCFS:     &ccfs,
				Document: ack.Document.Document,
				Proof:    proof,
			},
			CreatedAt: ack.CreatedAt,
		}
	}

	return rows, nil
}

func (r *RecordRepository) getAckedRecords(ctx context.Context, to, schema string, since, until *time.Time, limit int, order string) ([]record.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.getAckedRecords")
	defer span.End()

	var acked []models.Acked
	query := r.db.WithContext(ctx).
		Model(&models.Acked{}).
		Preload("Document").
		Where("ackeds.to = ?", to).
		Where("ackeds.valid = ?", true)

	if schema != "" {
		query = query.Where("ackeds.schema = ?", schema)
	}

	if since != nil {
		query = query.Where("ackeds.created_at >= ?", *since)
	}
	if until != nil {
		query = query.Where("ackeds.created_at <= ?", *until)
	}

	if order == "desc" {
		query = query.Order("ackeds.created_at DESC, ackeds.document_id DESC")
	} else {
		query = query.Order("ackeds.created_at ASC, ackeds.document_id ASC")
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	err := query.Find(&acked).Error
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	rows := make([]record.QueryRow, len(acked))
	for i, ack := range acked {
		var proof concrnt.Proof
		err := json.Unmarshal([]byte(ack.Document.Proof), &proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		// CIP-3 §3.4: an acked's ccfs identity is held by the associate
		// owner's server
		ccfs := concrnt.ComposeCCFSURI(ack.To, concrnt.CCFSTypeConcrnt, ack.DocumentID)

		rows[i] = record.QueryRow{
			Row: concrnt.SignedDocument{
				CCFS:     &ccfs,
				Document: ack.Document.Document,
				Proof:    proof,
			},
			CreatedAt: ack.CreatedAt,
		}
	}

	return rows, nil
}

func (r *RecordRepository) GetAcknowledgeRecords(ctx context.Context, from, to, schema string, since, until *time.Time, limit int, order string) ([]record.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAcknowledgeRecords")
	defer span.End()

	if from != "" {
		return r.getAckRecords(ctx, from, schema, since, until, limit, order)
	} else if to != "" {
		return r.getAckedRecords(ctx, to, schema, since, until, limit, order)
	} else {
		err := errors.New("either 'from' or 'to' must be specified")
		span.RecordError(err)
		return nil, err
	}
}

func (r *RecordRepository) getAckRecordCounts(ctx context.Context, from, schema string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.getAckRecordCounts")
	defer span.End()

	type result struct {
		Schema string
		Count  int64
	}

	var results []result

	query := r.db.WithContext(ctx).
		Model(&models.Ack{}).
		Select("schema, COUNT(*) AS count").
		Where("valid = ?", true).
		Where("acks.from = ?", from)

	if schema != "" {
		query = query.Where("acks.schema = ?", schema)
	}

	err := query.Group("schema").Scan(&results).Error
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	counts := make(map[string]int64)
	for _, r := range results {
		counts[r.Schema] = r.Count
	}

	return counts, nil
}

func (r *RecordRepository) getAckedRecordCounts(ctx context.Context, to, schema string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.getAckedRecordCounts")
	defer span.End()

	type result struct {
		Schema string
		Count  int64
	}

	var results []result

	query := r.db.WithContext(ctx).
		Model(&models.Acked{}).
		Select("schema, COUNT(*) AS count").
		Where("valid = ?", true).
		Where("ackeds.to = ?", to)

	if schema != "" {
		query = query.Where("ackeds.schema = ?", schema)
	}

	err := query.Group("schema").Scan(&results).Error
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	counts := make(map[string]int64)
	for _, r := range results {
		counts[r.Schema] = r.Count
	}

	return counts, nil
}

func (r *RecordRepository) GetAcknowledgeRecordCounts(ctx context.Context, from, to, schema string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAcknowledgeRecordCounts")
	defer span.End()

	if from != "" {
		return r.getAckRecordCounts(ctx, from, schema)
	} else if to != "" {
		return r.getAckedRecordCounts(ctx, to, schema)
	} else {
		err := errors.New("either 'from' or 'to' must be specified")
		span.RecordError(err)
		return nil, err
	}
}

func (r *RecordRepository) GetAllCommitLogs(ctx context.Context, owner string) ([]concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAllCommitLogs")
	defer span.End()

	var commitLogs []models.CommitLog
	err := r.db.WithContext(ctx).
		Where("owner = ?", owner).
		Order("c_date ASC").
		Find(&commitLogs).Error
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	sds := make([]concrnt.SignedDocument, len(commitLogs))
	for i, cl := range commitLogs {
		var proof concrnt.Proof
		err := json.Unmarshal([]byte(cl.Proof), &proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		sds[i] = concrnt.SignedDocument{
			Document: cl.Document,
			Proof:    proof,
		}
	}

	return sds, nil
}
