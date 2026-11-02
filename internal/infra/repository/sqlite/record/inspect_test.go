package record

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"

	"github.com/concrnt/concrnt/internal/infra/repository/repotest"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
)

// inspector is the SQLite repotest.Inspector: white-box reads of the tables
// the contract suites assert on, for tests only.
type inspector struct {
	db *gorm.DB
}

// NewInspector returns the repotest.Inspector over db.
func newInspector(db *gorm.DB) repotest.Inspector {
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

func (i *inspector) Commit(ctx context.Context, id string) (*repotest.CommitState, error) {
	var commit sqlitedb.CommitLog
	found, err := take(i.db.WithContext(ctx).Where("id = ?", id), &commit)
	if err != nil || !found {
		return nil, err
	}
	return &repotest.CommitState{
		IP:          commit.IP,
		Document:    commit.Document,
		Proof:       commit.Proof,
		Owner:       commit.Owner,
		GcCandidate: commit.GcCandidate,
		CDate:       sqlitedb.Time(commit.CDate),
	}, nil
}

func (i *inspector) SetCommitCDate(ctx context.Context, id string, cDate time.Time) error {
	return i.db.WithContext(ctx).Model(&sqlitedb.CommitLog{}).Where("id = ?", id).Update("c_date", sqlitedb.Micros(cDate)).Error
}

func (i *inspector) Record(ctx context.Context, documentID string) (*repotest.RecordState, error) {
	var rec sqlitedb.Record
	found, err := take(i.db.WithContext(ctx).Where("document_id = ?", documentID), &rec)
	if err != nil || !found {
		return nil, err
	}
	return &repotest.RecordState{
		Owner:         rec.Owner,
		Schema:        rec.Schema,
		Redirect:      rec.Redirect,
		Distributions: []string(rec.Distributions),
		CreatedAt:     sqlitedb.Time(rec.CreatedAt),
	}, nil
}

func (i *inspector) RecordKey(ctx context.Context, uri string) (*repotest.KeyState, error) {
	var key sqlitedb.RecordKey
	found, err := take(i.db.WithContext(ctx).Where("uri = ?", uri), &key)
	if err != nil || !found {
		return nil, err
	}
	return &repotest.KeyState{
		RecordID:        key.RecordID,
		RecordCreatedAt: sqlitedb.TimePtr(key.RecordCreatedAt),
		Redirect:        key.Redirect,
	}, nil
}

func (i *inspector) Entity(ctx context.Context, ccid string) (*repotest.EntityState, error) {
	var entity sqlitedb.Entity
	found, err := take(i.db.WithContext(ctx).Where("id = ?", ccid), &entity)
	if err != nil || !found {
		return nil, err
	}
	return &repotest.EntityState{
		DocumentID: entity.DocumentID,
		Domain:     entity.Domain,
		CreatedAt:  sqlitedb.Time(entity.CreatedAt),
	}, nil
}

func (i *inspector) Association(ctx context.Context, documentID string) (*repotest.AssociationState, error) {
	var assoc sqlitedb.Association
	found, err := take(i.db.WithContext(ctx).Where("document_id = ?", documentID), &assoc)
	if err != nil || !found {
		return nil, err
	}
	return &repotest.AssociationState{
		Owner:   assoc.Owner,
		Author:  assoc.Author,
		Variant: assoc.Variant,
		Unique:  assoc.Unique,
	}, nil
}

func (i *inspector) AssociationCountByUnique(ctx context.Context, unique string) (int64, error) {
	var count int64
	err := i.db.WithContext(ctx).Model(&sqlitedb.Association{}).Where(`"unique" = ?`, unique).Count(&count).Error
	return count, err
}

func (i *inspector) Ack(ctx context.Context, from, to, schema string) (*repotest.AckState, error) {
	var ack sqlitedb.Ack
	found, err := take(i.db.WithContext(ctx).Where(`"from" = ? AND "to" = ? AND schema = ?`, from, to, schema), &ack)
	if err != nil || !found {
		return nil, err
	}
	return &repotest.AckState{
		DocumentID: ack.DocumentID,
		Valid:      ack.Valid,
		CreatedAt:  sqlitedb.Time(ack.CreatedAt),
	}, nil
}

func (i *inspector) Acked(ctx context.Context, from, to, schema string) (*repotest.AckState, error) {
	var acked sqlitedb.Acked
	found, err := take(i.db.WithContext(ctx).Where(`"from" = ? AND "to" = ? AND schema = ?`, from, to, schema), &acked)
	if err != nil || !found {
		return nil, err
	}
	return &repotest.AckState{
		DocumentID: acked.DocumentID,
		Valid:      acked.Valid,
		CreatedAt:  sqlitedb.Time(acked.CreatedAt),
	}, nil
}

func (i *inspector) AckCount(ctx context.Context) (int64, error) {
	var count int64
	err := i.db.WithContext(ctx).Model(&sqlitedb.Ack{}).Count(&count).Error
	return count, err
}

func (i *inspector) AbuseReportCount(ctx context.Context) (int64, error) {
	var count int64
	err := i.db.WithContext(ctx).Model(&sqlitedb.AbuseReport{}).Count(&count).Error
	return count, err
}
