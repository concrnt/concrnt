package record

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
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

		// Lock the RecordKey before writing anything. CIP-3 §3.4 accept-if-newer
		// on the document's createdAt: only a strictly newer document takes the
		// key, an equal or older one is Stale. Deciding under the row lock
		// closes the read-then-write race between concurrent commits, same as
		// ApplyEntity's conditional upsert.
		var oldRecordKey postgres.RecordKey
		err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("uri = ?", commit.Key).
			Take(&oldRecordKey).Error
		keyExisted := err == nil
		if err != nil && err != gorm.ErrRecordNotFound {
			return ucrecord.Applied, err
		}
		if oldRecordKey.RecordID != nil && oldRecordKey.RecordCreatedAt != nil && !commit.CreatedAt.After(*oldRecordKey.RecordCreatedAt) {
			return ucrecord.Stale, nil
		}

		record := postgres.Record{
			DocumentID:    commit.ID,
			Owner:         commit.Owner,
			Author:        commit.Author,
			Redirect:      commit.Redirect,
			Schema:        commit.Schema,
			Policies:      commit.Policies,
			Distributions: commit.Distributions,
			CreatedAt:     commit.CreatedAt,
		}
		if err := tx.Create(&record).Error; err != nil {
			return ucrecord.Applied, err
		}

		pid, err := parentKeyID(tx, commit.Key)
		if err != nil {
			return ucrecord.Applied, err
		}
		cleanOnUpdate := commit.OnUpdate == nil || *commit.OnUpdate != "retain"

		// The FOR UPDATE above cannot lock a RecordKey row that does not
		// exist yet, so two concurrent commits to a fresh key both pass it.
		// The conditional upsert closes that window: only a strictly newer
		// document may take the key. IS NULL keeps normal writes to parent
		// placeholder rows (RecordID nil) working. xmax tells an insert
		// from an update of a row that appeared after the read.
		var upserted []struct{ Inserted bool }
		err = tx.Raw(`INSERT INTO record_keys (uri, parent_id, record_id, record_created_at, redirect, clean_on_update)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT (uri) DO UPDATE SET record_id = excluded.record_id, parent_id = excluded.parent_id,
				record_created_at = excluded.record_created_at, redirect = excluded.redirect, clean_on_update = excluded.clean_on_update
			WHERE record_keys.record_id IS NULL OR record_keys.record_created_at IS NULL OR record_keys.record_created_at < excluded.record_created_at
			RETURNING (xmax = 0) AS inserted`,
			commit.Key, pid, commit.ID, commit.CreatedAt, commit.Redirect, cleanOnUpdate).
			Scan(&upserted).Error
		if err != nil {
			return ucrecord.Applied, err
		}
		if len(upserted) == 0 {
			return ucrecord.Stale, nil
		}
		if !keyExisted && !upserted[0].Inserted {
			// a concurrent commit created the key between the read and the
			// upsert: what this write replaced is unknown, so start over
			// with the row there to lock
			return ucrecord.Applied, errRetryTx
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
			if err := tx.Delete(&postgres.Record{}, "document_id = ?", oldRecordKey.RecordID).Error; err != nil {
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
				q := tx.Model(&postgres.RecordKey{}).Select("record_id").Where("uri = ?", removal.URI)
				result = tx.Where("document_id = (?)", q).Delete(&postgres.Record{})
			case ucrecord.RemoveVersion:
				result = tx.Delete(&postgres.Record{}, "document_id = ?", removal.DocumentID)
			case ucrecord.RemoveAssociation:
				result = tx.Delete(&postgres.Association{}, "document_id = ?", removal.DocumentID)
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
		var rk postgres.RecordKey
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
			Uri           string         `gorm:"column:uri"`
			Policy        *string        `gorm:"column:policy"`
			Distributions pq.StringArray `gorm:"column:distributions;type:text[]"`
		}
		err := r.db.WithContext(ctx).
			Model(&postgres.Record{}).
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
		rows, err := scanRecordRows(joinRecords(r.db.WithContext(ctx).Model(&postgres.RecordKey{}), false).Where("record_keys.uri IN ?", keys))
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
		Model(&postgres.CommitLog{}).
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
	found := tx.Model(&postgres.RecordKey{}).Select("id").Where("uri = ?", parentURI).Scan(&id)
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

	// one statement for the placeholder: the insert's id, or the id of the
	// row a concurrent transaction created first (DO NOTHING returns none).
	// A DO UPDATE upsert would lock the parent row for the whole
	// transaction and serialize every commit into the same timeline.
	created := tx.Raw(`WITH ins AS (
		INSERT INTO record_keys (uri, parent_id, clean_on_update) VALUES (?, ?, false)
		ON CONFLICT (uri) DO NOTHING RETURNING id
	) SELECT id FROM ins UNION ALL SELECT id FROM record_keys WHERE uri = ? LIMIT 1`, parentURI, grandparent, parentURI).
		Scan(&id)
	if created.Error != nil {
		return nil, created.Error
	}
	if created.RowsAffected == 0 {
		// the row seen by neither branch: inserted and deleted in between
		return nil, errRetryTx
	}
	return &id, nil
}
