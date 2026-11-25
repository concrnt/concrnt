package record

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func (r *Repository) CreateEntity(
	ctx context.Context,
	tx ucrecord.RepositoryTx,
	ccid string,
	alias *string,
	domain string,
	documentID string,
	createdAt time.Time,
) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateEntity")
	defer span.End()

	db, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	// CIP-3 §3.4 accept-if-newer on the document's createdAt: only a strictly
	// newer document replaces the stored one; an equal or older one is a
	// no-op (RowsAffected 0). The conditional upsert makes newer-wins atomic
	// at the row lock, so concurrent writers converge on the same winner.
	result := gorm.WithResult()
	err = gorm.G[any](db, result).Exec(ctx, `
INSERT INTO entities (id, alias, domain, tag, document_id, created_at, m_date)
VALUES (?, ?, ?, '', ?, ?, now())
ON CONFLICT (id) DO UPDATE
SET alias = excluded.alias,
    domain = excluded.domain,
    document_id = excluded.document_id,
    created_at = excluded.created_at
WHERE entities.created_at < excluded.created_at`,
		ccid, alias, domain, documentID, createdAt)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	return result.RowsAffected > 0, nil
}

// MarkCommitLogsGcCandidateByOwner flags every commit log owned by the given
// ccid as a GC candidate. Every commit has exactly one owner (an ack between
// two local users is two commits: the ack owned by the acker and the acked
// owned by the target), so nothing another user holds is touched. Idempotent.
func (r *Repository) MarkCommitLogsGcCandidateByOwner(ctx context.Context, owner string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.MarkCommitLogsGcCandidateByOwner")
	defer span.End()

	_, err := gorm.G[CommitLog](r.db).
		Where("owner = ?", owner).
		Where("NOT gc_candidate").
		Update(ctx, "gc_candidate", true)
	if err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}

// entityRow is an entity joined with its signed document.
type entityRow struct {
	ID       string
	Alias    *string
	Domain   string
	Tag      string
	Document string
	Proof    string
}

func (r *Repository) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetEntityByCCID")
	defer span.End()

	entity, err := gorm.G[entityRow](r.db).Raw(`
SELECT e.id, e.alias, e.domain, e.tag, c.document, c.proof
FROM entities e
JOIN commit_logs c ON c.id = e.document_id
WHERE e.id = ?`, ccid).Take(ctx)
	if err != nil {
		span.RecordError(err)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domain.NotFoundError{Resource: "entity"}
		}
		return nil, err
	}

	var proof concrnt.Proof
	err = json.Unmarshal([]byte(entity.Proof), &proof)
	if err != nil {
		return nil, err
	}

	signedDocument := concrnt.SignedDocument{
		Document: entity.Document,
		Proof:    proof,
	}

	return &domain.Entity{
		ID:             entity.ID,
		Domain:         entity.Domain,
		Alias:          entity.Alias,
		TagString:      entity.Tag,
		SignedDocument: &signedDocument,
	}, nil
}

func (r *Repository) GetEntityByAlias(ctx context.Context, alias string) (*domain.Entity, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetEntityByAlias")
	defer span.End()

	entity, err := gorm.G[entityRow](r.db).Raw(`
SELECT e.id, e.alias, e.domain, e.tag, c.document, c.proof
FROM entities e
JOIN commit_logs c ON c.id = e.document_id
WHERE e.alias = ?`, alias).Take(ctx)
	if err != nil {
		span.RecordError(err)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, domain.NotFoundError{Resource: "entity"}
		}
		return nil, err
	}

	var proof concrnt.Proof
	err = json.Unmarshal([]byte(entity.Proof), &proof)
	if err != nil {
		return nil, err
	}

	signedDocument := concrnt.SignedDocument{
		Document: entity.Document,
		Proof:    proof,
	}

	return &domain.Entity{
		ID:             entity.ID,
		Domain:         entity.Domain,
		Alias:          entity.Alias,
		TagString:      entity.Tag,
		SignedDocument: &signedDocument,
	}, nil
}
