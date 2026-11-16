package spanner

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase"
)

// EntityMetaRepository keeps the local registration state (entity_meta), a
// table keyed by ccid with no ties to the record store.
type EntityMetaRepository struct {
	client *spanner.Client
}

func NewEntityMetaRepository(client *spanner.Client) usecase.EntityMetaRepository {
	return &EntityMetaRepository{client: client}
}

type entityMetaRow struct {
	ID      string  `spanner:"ccid"`
	Inviter *string `spanner:"inviter"`
	Info    string  `spanner:"info"`
}

func (m entityMetaRow) domain() domain.EntityMeta {
	return domain.EntityMeta{ID: m.ID, Inviter: m.Inviter, Info: m.Info}
}

func (r *EntityMetaRepository) SaveMeta(ctx context.Context, meta domain.EntityMeta) error {
	ctx, span := tracer.Start(ctx, "Repository.Register")
	defer span.End()

	info := meta.Info
	if info == "" {
		info = "null"
	}
	_, err := r.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, found, err := QueryOne[entityMetaRow](ctx, tx, spanner.Statement{
			SQL:    "SELECT ccid, inviter, info FROM entity_meta WHERE ccid = @ccid",
			Params: map[string]any{"ccid": meta.ID},
		})
		if err != nil {
			return err
		}
		params := map[string]any{"ccid": meta.ID, "inviter": meta.Inviter, "info": info}
		if found {
			_, err = tx.Update(ctx, spanner.Statement{
				SQL:    "UPDATE entity_meta SET inviter = @inviter, info = @info, m_date = CURRENT_TIMESTAMP() WHERE ccid = @ccid",
				Params: params,
			})
			return err
		}
		_, err = tx.Update(ctx, spanner.Statement{
			SQL:    "INSERT INTO entity_meta (ccid, inviter, info, c_date, m_date) VALUES (@ccid, @inviter, @info, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())",
			Params: params,
		})
		return err
	})
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (r *EntityMetaRepository) GetMeta(ctx context.Context, ccid string) (*domain.EntityMeta, error) {
	ctx, span := tracer.Start(ctx, "Repository.GetMeta")
	defer span.End()

	meta, found, err := QueryOne[entityMetaRow](ctx, r.client.Single(), spanner.Statement{
		SQL:    "SELECT ccid, inviter, info FROM entity_meta WHERE ccid = @ccid",
		Params: map[string]any{"ccid": ccid},
	})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if !found {
		return nil, domain.NotFoundError{Resource: ccid}
	}
	out := meta.domain()
	return &out, nil
}

// UpdateMetaInfo replaces only the info column, preserving inviter (unlike
// SaveMeta, whose upsert overwrites both).
func (r *EntityMetaRepository) UpdateMetaInfo(ctx context.Context, ccid string, info string) error {
	ctx, span := tracer.Start(ctx, "Repository.UpdateMetaInfo")
	defer span.End()

	var affected int64
	_, err := r.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var err error
		affected, err = tx.Update(ctx, spanner.Statement{
			SQL:    "UPDATE entity_meta SET info = @info, m_date = CURRENT_TIMESTAMP() WHERE ccid = @ccid",
			Params: map[string]any{"ccid": ccid, "info": info},
		})
		return err
	})
	if err != nil {
		span.RecordError(err)
		return err
	}
	if affected == 0 {
		return domain.NotFoundError{Resource: "entity meta"}
	}
	return nil
}

func (r *EntityMetaRepository) DeleteMeta(ctx context.Context, ccid string) error {
	ctx, span := tracer.Start(ctx, "Repository.DeleteMeta")
	defer span.End()

	_, err := r.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, spanner.Statement{
			SQL:    "DELETE FROM entity_meta WHERE ccid = @ccid",
			Params: map[string]any{"ccid": ccid},
		})
		return err
	})
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (r *EntityMetaRepository) ListMetas(ctx context.Context, owner string) ([]domain.EntityMeta, error) {
	ctx, span := tracer.Start(ctx, "Repository.ListMetas")
	defer span.End()

	stmt := spanner.Statement{SQL: "SELECT ccid, inviter, info FROM entity_meta", Params: map[string]any{}}
	if owner != "" {
		stmt.SQL += " WHERE ccid = @ccid"
		stmt.Params["ccid"] = owner
	}
	metas, err := QueryAll[entityMetaRow](ctx, r.client.Single(), stmt)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	result := make([]domain.EntityMeta, 0, len(metas))
	for _, m := range metas {
		result = append(result, m.domain())
	}
	return result, nil
}

// now is the application-side timestamp of a row written outside a commit
// timestamp column.
func now() time.Time {
	return time.Now().UTC()
}
