package server

import (
	"context"
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/otel"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/server"
)

var tracer = otel.Tracer("repository")

type Repository struct {
	config *domain.Config
	db     *gorm.DB
	client *client.Client
}

var _ server.Repository = (*Repository)(nil)

func New(config *domain.Config, db *gorm.DB, cl *client.Client) *Repository {
	return &Repository{
		config: config,
		db:     db,
		client: cl,
	}
}

func (r *Repository) Migrate() error {
	return r.db.AutoMigrate(&Server{})
}

func modelToDomainServer(m *Server) (*domain.Server, error) {
	var wkc concrnt.WellKnownConcrnt
	if err := json.Unmarshal([]byte(m.WellKnown), &wkc); err != nil {
		return nil, err
	}
	return &domain.Server{
		TagString: m.Tag,
		WellKnown: wkc,
	}, nil
}

func (r *Repository) Resolve(ctx context.Context, identifier string, hint *string) (*domain.Server, error) {
	ctx, span := tracer.Start(ctx, "Repository.Server.Resolve")
	defer span.End()

	if concrnt.IsCSID(identifier) {
		return r.GetAndCacheByCSID(ctx, identifier, hint)
	} else {
		return r.GetAndCacheByFQDN(ctx, identifier)
	}
}

func (r *Repository) GetAndCacheByCSID(ctx context.Context, csid string, hint *string) (*domain.Server, error) {
	ctx, span := tracer.Start(ctx, "Repository.Server.GetAndCacheByCSID")
	defer span.End()

	server, err := gorm.G[Server](r.db).
		Where("cs_id = ?", csid).
		Take(ctx)
	if err == nil && server.WellKnown != "" {
		return modelToDomainServer(&server)
	}

	if hint == nil {
		return nil, domain.ErrNotFound
	}

	wkc, err := r.client.GetServer(ctx, csid, hint)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	serialized, err := json.Marshal(wkc)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	err = gorm.G[any](r.db).Exec(ctx, `
INSERT INTO servers (id, cs_id, layer, tag, well_known, m_date)
VALUES (?, ?, ?, '', ?, now())
ON CONFLICT (id) DO UPDATE
SET cs_id = excluded.cs_id,
    layer = excluded.layer,
    tag = excluded.tag,
    well_known = excluded.well_known`,
		wkc.Domain, wkc.CSID, wkc.Layer, string(serialized))
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	return &domain.Server{WellKnown: wkc}, nil
}

func (r *Repository) GetAndCacheByFQDN(ctx context.Context, fqdn string) (*domain.Server, error) {
	ctx, span := tracer.Start(ctx, "Repository.Server.GetAndCacheByFQDN")
	defer span.End()

	server, err := gorm.G[Server](r.db).
		Where("id = ?", fqdn).
		Take(ctx)
	if err == nil && server.WellKnown != "" {
		return modelToDomainServer(&server)
	}

	wkc, err := r.client.GetServer(ctx, fqdn, nil)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	serialized, err := json.Marshal(wkc)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	err = gorm.G[any](r.db).Exec(ctx, `
INSERT INTO servers (id, cs_id, layer, tag, well_known, m_date)
VALUES (?, ?, ?, '', ?, now())
ON CONFLICT (id) DO UPDATE
SET cs_id = excluded.cs_id,
    layer = excluded.layer,
    tag = excluded.tag,
    well_known = excluded.well_known`,
		wkc.Domain, wkc.CSID, wkc.Layer, string(serialized))
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	return &domain.Server{WellKnown: wkc}, nil
}

func (r *Repository) List(ctx context.Context) ([]*concrnt.WellKnownConcrnt, error) {
	ctx, span := tracer.Start(ctx, "Repository.Server.List")
	defer span.End()

	servers, err := gorm.G[Server](r.db).Find(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]*concrnt.WellKnownConcrnt, 0, len(servers))
	for _, server := range servers {
		if server.WellKnown == "" {
			span.RecordError(fmt.Errorf("server with empty well-known: %s", server.ID))
			continue
		}
		var wkc concrnt.WellKnownConcrnt
		err := json.Unmarshal([]byte(server.WellKnown), &wkc)
		if err != nil {
			span.RecordError(err)
			continue
		}
		result = append(result, &wkc)
	}

	return result, nil
}
