package postgres

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"

	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/infra/repository/inspect"
)

// inspector is the Postgres inspect.Inspector: white-box reads of the tables
// the contract suites assert on, for tests only.
type inspector struct {
	db *gorm.DB
}

// NewInspector returns the inspect.Inspector over db.
func NewInspector(db *gorm.DB) inspect.Inspector {
	return &inspector{db: db}
}

// take runs query into dest, mapping a missing row to false, nil.
func take(query *gorm.DB, dest any) (bool, error) {
	err := query.Take(dest).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (i *inspector) Commit(ctx context.Context, id string) (*inspect.CommitState, error) {
	var commit models.CommitLog
	found, err := take(i.db.WithContext(ctx).Where("id = ?", id), &commit)
	if err != nil || !found {
		return nil, err
	}
	return &inspect.CommitState{
		IP:          commit.IP,
		Document:    commit.Document,
		Proof:       commit.Proof,
		Owner:       commit.Owner,
		GcCandidate: commit.GcCandidate,
		CDate:       commit.CDate.UTC(),
	}, nil
}

func (i *inspector) SetCommitCDate(ctx context.Context, id string, cDate time.Time) error {
	return i.db.WithContext(ctx).Model(&models.CommitLog{}).Where("id = ?", id).Update("c_date", cDate).Error
}

func (i *inspector) Record(ctx context.Context, documentID string) (*inspect.RecordState, error) {
	var rec models.Record
	found, err := take(i.db.WithContext(ctx).Where("document_id = ?", documentID), &rec)
	if err != nil || !found {
		return nil, err
	}
	return &inspect.RecordState{
		Owner:         rec.Owner,
		Schema:        rec.Schema,
		Redirect:      rec.Redirect,
		Distributions: []string(rec.Distributions),
		CreatedAt:     rec.CreatedAt,
	}, nil
}

func (i *inspector) RecordKey(ctx context.Context, uri string) (*inspect.KeyState, error) {
	var key models.RecordKey
	found, err := take(i.db.WithContext(ctx).Where("uri = ?", uri), &key)
	if err != nil || !found {
		return nil, err
	}
	return &inspect.KeyState{
		RecordID:        key.RecordID,
		RecordCreatedAt: key.RecordCreatedAt,
	}, nil
}

func (i *inspector) Entity(ctx context.Context, ccid string) (*inspect.EntityState, error) {
	var entity models.Entity
	found, err := take(i.db.WithContext(ctx).Where("id = ?", ccid), &entity)
	if err != nil || !found {
		return nil, err
	}
	return &inspect.EntityState{
		DocumentID: entity.DocumentID,
		Domain:     entity.Domain,
		CreatedAt:  entity.CreatedAt,
	}, nil
}

func (i *inspector) Association(ctx context.Context, documentID string) (*inspect.AssociationState, error) {
	var assoc models.Association
	found, err := take(i.db.WithContext(ctx).Where("document_id = ?", documentID), &assoc)
	if err != nil || !found {
		return nil, err
	}
	return &inspect.AssociationState{
		Owner:   assoc.Owner,
		Author:  assoc.Author,
		Variant: assoc.Variant,
		Unique:  assoc.Unique,
	}, nil
}

func (i *inspector) AssociationCountByUnique(ctx context.Context, unique string) (int64, error) {
	var count int64
	err := i.db.WithContext(ctx).Model(&models.Association{}).Where(`"unique" = ?`, unique).Count(&count).Error
	return count, err
}

func (i *inspector) Ack(ctx context.Context, from, to, schema string) (*inspect.AckState, error) {
	var ack models.Ack
	found, err := take(i.db.WithContext(ctx).Where(`"from" = ? AND "to" = ? AND schema = ?`, from, to, schema), &ack)
	if err != nil || !found {
		return nil, err
	}
	return &inspect.AckState{
		DocumentID: ack.DocumentID,
		Valid:      ack.Valid,
		CreatedAt:  ack.CreatedAt,
	}, nil
}

func (i *inspector) Acked(ctx context.Context, from, to, schema string) (*inspect.AckState, error) {
	var acked models.Acked
	found, err := take(i.db.WithContext(ctx).Where(`"from" = ? AND "to" = ? AND schema = ?`, from, to, schema), &acked)
	if err != nil || !found {
		return nil, err
	}
	return &inspect.AckState{
		DocumentID: acked.DocumentID,
		Valid:      acked.Valid,
		CreatedAt:  acked.CreatedAt,
	}, nil
}

func (i *inspector) AckCount(ctx context.Context) (int64, error) {
	var count int64
	err := i.db.WithContext(ctx).Model(&models.Ack{}).Count(&count).Error
	return count, err
}
