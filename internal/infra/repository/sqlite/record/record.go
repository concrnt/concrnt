package record

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func (r *Repository) ApplyRecord(ctx context.Context, commit ucrecord.RecordCommit) (ucrecord.RecordOutcome, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ApplyRecord")
	defer span.End()

	var outcome ucrecord.RecordOutcome
	status, err := r.apply(ctx, func(tx *gorm.DB) (ucrecord.ApplyStatus, error) {
		outcome = ucrecord.RecordOutcome{}

		inserted, err := insertCommit(tx, commit.Commit)
		if err != nil {
			return ucrecord.Applied, err
		}
		if !inserted {
			return ucrecord.Duplicate, nil
		}

		// CIP-3 §3.4 accept-if-newer on the document's createdAt: only a
		// strictly newer document takes the key, an equal or older one is
		// Stale. The IMMEDIATE transaction holds the single writer lock, so
		// this read is already serialised against every other writer
		// (Postgres locks the row instead).
		var oldRecordKey sqlitedb.RecordKey
		err = tx.Where("uri = ?", commit.Key).Take(&oldRecordKey).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			return ucrecord.Applied, err
		}
		createdAt := sqlitedb.Micros(commit.CreatedAt)
		if oldRecordKey.RecordID != nil && oldRecordKey.RecordCreatedAt != nil && createdAt <= *oldRecordKey.RecordCreatedAt {
			return ucrecord.Stale, nil
		}

		record := sqlitedb.Record{
			DocumentID:    commit.ID,
			Owner:         commit.Owner,
			Author:        commit.Author,
			Redirect:      commit.Redirect,
			Schema:        commit.Schema,
			Policies:      commit.Policies,
			Distributions: commit.Distributions,
			CreatedAt:     createdAt,
		}
		if err := tx.Create(&record).Error; err != nil {
			return ucrecord.Applied, err
		}

		pid, err := parentKeyID(tx, commit.Key)
		if err != nil {
			return ucrecord.Applied, err
		}
		cleanOnUpdate := commit.OnUpdate == nil || *commit.OnUpdate != "retain"

		// creates the key, or takes over the placeholder / the older version
		rk := sqlitedb.RecordKey{
			URI:             commit.Key,
			ParentID:        pid,
			RecordID:        &commit.ID,
			RecordCreatedAt: &createdAt,
			Redirect:        commit.Redirect,
			CleanOnUpdate:   cleanOnUpdate,
		}
		err = tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "uri"}},
			DoUpdates: clause.Assignments(map[string]any{"record_id": commit.ID, "parent_id": pid, "record_created_at": createdAt, "redirect": commit.Redirect, "clean_on_update": cleanOnUpdate}),
		}).Create(&rk).Error
		if err != nil {
			return ucrecord.Applied, err
		}

		outcome.FreshKey = oldRecordKey.RecordID == nil
		if oldRecordKey.RecordID != nil {
			outcome.PreviousID = *oldRecordKey.RecordID
		}

		// 古いRecordKeyが指していたCommitのGCフラグを立て、Recordは消す
		if oldRecordKey.CleanOnUpdate && oldRecordKey.RecordID != nil {
			if err := markGcCandidates(tx, []string{*oldRecordKey.RecordID}); err != nil {
				return ucrecord.Applied, err
			}
			if err := tx.Delete(&sqlitedb.Record{}, "document_id = ?", oldRecordKey.RecordID).Error; err != nil {
				return ucrecord.Applied, err
			}
		}

		return ucrecord.Applied, nil
	})
	if err != nil {
		span.RecordError(err)
		return ucrecord.RecordOutcome{}, err
	}
	outcome.Status = status
	return outcome, nil
}

// ApplyDelete removes rows in the order given. Deleting a record takes its
// key row along (ON DELETE CASCADE on record_keys.record_id), and the key
// row its associations (associations.target_id): that is the end of the
// key's incarnation, complete inside this transaction.
func (r *Repository) ApplyDelete(ctx context.Context, commit ucrecord.DeleteCommit) (ucrecord.DeleteOutcome, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ApplyDelete")
	defer span.End()

	var removed []bool
	status, err := r.apply(ctx, func(tx *gorm.DB) (ucrecord.ApplyStatus, error) {
		removed = make([]bool, len(commit.Removals))

		if commit.Commit != nil {
			inserted, err := insertCommit(tx, *commit.Commit)
			if err != nil {
				return ucrecord.Applied, err
			}
			if !inserted {
				return ucrecord.Duplicate, nil
			}
		}

		for i, removal := range commit.Removals {
			var result *gorm.DB
			switch removal.Kind {
			case ucrecord.RemoveKey:
				q := tx.Model(&sqlitedb.RecordKey{}).Select("record_id").Where("uri = ?", removal.URI)
				result = tx.Where("document_id = (?)", q).Delete(&sqlitedb.Record{})
			case ucrecord.RemoveVersion:
				result = tx.Delete(&sqlitedb.Record{}, "document_id = ?", removal.DocumentID)
			case ucrecord.RemoveAssociation:
				result = tx.Delete(&sqlitedb.Association{}, "document_id = ?", removal.DocumentID)
			default:
				return ucrecord.Applied, fmt.Errorf("unknown removal kind %d", removal.Kind)
			}
			if result.Error != nil {
				return ucrecord.Applied, result.Error
			}
			removed[i] = result.RowsAffected > 0
		}

		return ucrecord.Applied, markGcCandidates(tx, commit.GcCommitIDs)
	})
	if err != nil {
		span.RecordError(err)
		return ucrecord.DeleteOutcome{}, err
	}
	if status != ucrecord.Applied {
		return ucrecord.DeleteOutcome{Status: status}, nil
	}
	return ucrecord.DeleteOutcome{Status: status, Removed: removed}, nil
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

	return *row.ParentURI, repoutil.ItemID(keyURI, row.Redirect), nil
}

// parentKeyID is the row id of the key one segment above uri, nil for a
// top-level key. A parent that does not exist yet is created as a
// placeholder, and so is every ancestor above it.
func parentKeyID(tx *gorm.DB, uri string) (*int64, error) {
	parentURI, ok, err := repoutil.ParentURI(uri)
	if err != nil || !ok {
		return nil, err
	}

	var id int64
	found := tx.Model(&sqlitedb.RecordKey{}).Select("id").Where("uri = ?", parentURI).Scan(&id)
	if found.Error != nil {
		return nil, found.Error
	}
	if found.RowsAffected > 0 {
		return &id, nil
	}

	grandparent, err := parentKeyID(tx, parentURI)
	if err != nil {
		return nil, err
	}
	placeholder := sqlitedb.RecordKey{URI: parentURI, ParentID: grandparent}
	if err := tx.Create(&placeholder).Error; err != nil {
		return nil, err
	}
	return &placeholder.ID, nil
}
