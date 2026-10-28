package record

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func (r *Repository) CreateEntity(ctx context.Context, tx ucrecord.RepositoryTx, ccid string, alias *string, domainName string, documentID string, createdAt time.Time) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateEntity")
	defer span.End()

	db, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	// CIP-3 §3.4 accept-if-newer on the document's createdAt: only a
	// strictly newer document replaces the stored one
	now := nowMicros()
	res, err := db.ExecContext(ctx, `INSERT INTO entities (id, alias, domain, tag, document_id, created_at, c_date, m_date) VALUES (?, ?, ?, '', ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET alias = excluded.alias, domain = excluded.domain, document_id = excluded.document_id, created_at = excluded.created_at, m_date = excluded.m_date
		WHERE entities.created_at < excluded.created_at`,
		ccid, sqlitedb.NullString(alias), domainName, documentID, sqlitedb.Micros(createdAt), now, now)
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	affected, err := res.RowsAffected()
	return affected > 0, err
}

func (r *Repository) MarkCommitLogsGcCandidateByOwner(ctx context.Context, owner string) error {
	ctx, span := tracer.Start(ctx, "Repository.MarkCommitLogsGcCandidateByOwner")
	defer span.End()

	_, err := r.db.ExecContext(ctx, "UPDATE commit_logs SET gc_candidate = 1 WHERE owner = ? AND gc_candidate = 0", owner)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (r *Repository) getEntity(ctx context.Context, where string, arg string) (*domain.Entity, error) {
	var entity domain.Entity
	var alias sql.NullString
	var document, proofJSON string
	err := r.db.QueryRowContext(ctx, "SELECT e.id, e.alias, e.domain, e.tag, c.document, c.proof FROM entities e JOIN commit_logs c ON c.id = e.document_id WHERE "+where, arg).
		Scan(&entity.ID, &alias, &entity.Domain, &entity.TagString, &document, &proofJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.NotFoundError{Resource: "entity"}
	}
	if err != nil {
		return nil, err
	}
	var proof concrnt.Proof
	if err := json.Unmarshal([]byte(proofJSON), &proof); err != nil {
		return nil, err
	}
	entity.Alias = sqlitedb.StringPtr(alias)
	entity.SignedDocument = &concrnt.SignedDocument{Document: document, Proof: proof}
	return &entity, nil
}

func (r *Repository) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetEntityByCCID")
	defer span.End()

	entity, err := r.getEntity(ctx, "e.id = ?", ccid)
	if err != nil {
		span.RecordError(err)
	}
	return entity, err
}

func (r *Repository) GetEntityByAlias(ctx context.Context, alias string) (*domain.Entity, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetEntityByAlias")
	defer span.End()

	entity, err := r.getEntity(ctx, "e.alias = ?", alias)
	if err != nil {
		span.RecordError(err)
	}
	return entity, err
}
