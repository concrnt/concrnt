package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/server"
)

type ServerRepository struct {
	config *domain.Config
	db     *DB
	client *client.Client
}

func NewServerRepository(config *domain.Config, db *DB, cl *client.Client) server.Repository {
	return &ServerRepository{config: config, db: db, client: cl}
}

func toDomainServer(tag, wellKnown string) (*domain.Server, error) {
	var wkc concrnt.WellKnownConcrnt
	if err := json.Unmarshal([]byte(wellKnown), &wkc); err != nil {
		return nil, err
	}
	return &domain.Server{TagString: tag, WellKnown: wkc}, nil
}

func (r *ServerRepository) Resolve(ctx context.Context, identifier string, hint *string) (*domain.Server, error) {
	ctx, span := tracer.Start(ctx, "ServerRepository.Resolve")
	defer span.End()

	if concrnt.IsCSID(identifier) {
		return r.getAndCache(ctx, "cs_id", identifier, hint, true)
	}
	return r.getAndCache(ctx, "id", identifier, nil, false)
}

// getAndCache answers from the cached row under column = identifier, or
// fetches the well-known through the client and upserts it by domain. A
// csid lookup with no cached row and no hint is a miss.
func (r *ServerRepository) getAndCache(ctx context.Context, column, identifier string, hint *string, needsHint bool) (*domain.Server, error) {
	var tag, wellKnown string
	err := r.db.QueryRowContext(ctx, "SELECT tag, well_known FROM servers WHERE "+column+" = ?", identifier).Scan(&tag, &wellKnown)
	if err == nil && wellKnown != "" {
		return toDomainServer(tag, wellKnown)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if needsHint && hint == nil {
		return nil, domain.ErrNotFound
	}

	wkc, err := r.client.GetServer(ctx, identifier, hint)
	if err != nil {
		return nil, err
	}
	serialized, err := json.Marshal(wkc)
	if err != nil {
		return nil, err
	}
	now := nowMicros()
	_, err = r.db.ExecContext(ctx, `INSERT INTO servers (id, cs_id, layer, tag, well_known, c_date, m_date) VALUES (?, ?, ?, '', ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET cs_id = excluded.cs_id, layer = excluded.layer, tag = excluded.tag, well_known = excluded.well_known, m_date = excluded.m_date`,
		wkc.Domain, wkc.CSID, wkc.Layer, string(serialized), now, now)
	if err != nil {
		return nil, err
	}
	return toDomainServer("", string(serialized))
}

func (r *ServerRepository) List(ctx context.Context) ([]*concrnt.WellKnownConcrnt, error) {
	ctx, span := tracer.Start(ctx, "ServerRepository.List")
	defer span.End()

	rows, err := r.db.QueryContext(ctx, "SELECT id, well_known FROM servers")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := []*concrnt.WellKnownConcrnt{}
	for rows.Next() {
		var id, wellKnown string
		if err := rows.Scan(&id, &wellKnown); err != nil {
			return nil, err
		}
		if wellKnown == "" {
			span.RecordError(fmt.Errorf("server with empty well-known: %s", id))
			continue
		}
		var wkc concrnt.WellKnownConcrnt
		if err := json.Unmarshal([]byte(wellKnown), &wkc); err != nil {
			span.RecordError(err)
			continue
		}
		result = append(result, &wkc)
	}
	return result, rows.Err()
}
