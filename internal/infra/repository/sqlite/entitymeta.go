package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase"
)

// EntityMetaRepository keeps the local registration state (entity_metas).
type EntityMetaRepository struct {
	db *DB
}

func NewEntityMetaRepository(db *DB) usecase.EntityMetaRepository {
	return &EntityMetaRepository{db: db}
}

func (r *EntityMetaRepository) SaveMeta(ctx context.Context, meta domain.EntityMeta) error {
	ctx, span := tracer.Start(ctx, "Repository.Register")
	defer span.End()

	now := nowMicros()
	_, err := r.db.ExecContext(ctx, `INSERT INTO entity_metas (id, inviter, info, c_date, m_date) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET inviter = excluded.inviter, info = excluded.info, m_date = excluded.m_date`,
		meta.ID, NullString(meta.Inviter), meta.Info, now, now)
	return err
}

func (r *EntityMetaRepository) GetMeta(ctx context.Context, ccid string) (*domain.EntityMeta, error) {
	ctx, span := tracer.Start(ctx, "Repository.GetMeta")
	defer span.End()

	var meta domain.EntityMeta
	var inviter sql.NullString
	err := r.db.QueryRowContext(ctx, "SELECT id, inviter, info FROM entity_metas WHERE id = ?", ccid).Scan(&meta.ID, &inviter, &meta.Info)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.NotFoundError{Resource: ccid}
	}
	if err != nil {
		return nil, err
	}
	meta.Inviter = StringPtr(inviter)
	return &meta, nil
}

func (r *EntityMetaRepository) UpdateMetaInfo(ctx context.Context, ccid string, info string) error {
	ctx, span := tracer.Start(ctx, "Repository.UpdateMetaInfo")
	defer span.End()

	res, err := r.db.ExecContext(ctx, "UPDATE entity_metas SET info = ?, m_date = ? WHERE id = ?", info, nowMicros(), ccid)
	if err != nil {
		span.RecordError(err)
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return domain.NotFoundError{Resource: "entity meta"}
	}
	return nil
}

func (r *EntityMetaRepository) DeleteMeta(ctx context.Context, ccid string) error {
	ctx, span := tracer.Start(ctx, "Repository.DeleteMeta")
	defer span.End()

	_, err := r.db.ExecContext(ctx, "DELETE FROM entity_metas WHERE id = ?", ccid)
	return err
}

func (r *EntityMetaRepository) ListMetas(ctx context.Context, owner string) ([]domain.EntityMeta, error) {
	ctx, span := tracer.Start(ctx, "Repository.ListMetas")
	defer span.End()

	query := "SELECT id, inviter, info FROM entity_metas"
	args := []any{}
	if owner != "" {
		query += " WHERE id = ?"
		args = append(args, owner)
	}
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	defer rows.Close()

	result := []domain.EntityMeta{}
	for rows.Next() {
		var meta domain.EntityMeta
		var inviter sql.NullString
		if err := rows.Scan(&meta.ID, &inviter, &meta.Info); err != nil {
			return nil, err
		}
		meta.Inviter = StringPtr(inviter)
		result = append(result, meta)
	}
	return result, rows.Err()
}
