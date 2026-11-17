package server

import (
	"context"
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/otel"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

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

	var server Server
	err := r.db.WithContext(ctx).
		Where("cs_id = ?", csid).
		Take(&server).Error
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

	newServer := Server{
		ID:        wkc.Domain,
		CSID:      wkc.CSID,
		Layer:     wkc.Layer,
		Tag:       "",
		WellKnown: string(serialized),
	}

	err = r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"cs_id", "layer", "tag", "well_known"}),
	}).Create(&newServer).Error
	if err != nil {
		return nil, err
	}

	return modelToDomainServer(&newServer)
}

func (r *Repository) GetAndCacheByFQDN(ctx context.Context, fqdn string) (*domain.Server, error) {
	ctx, span := tracer.Start(ctx, "Repository.Server.GetAndCacheByFQDN")
	defer span.End()

	var server Server
	err := r.db.WithContext(ctx).
		Where("id = ?", fqdn).
		Take(&server).Error
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

	newServer := Server{
		ID:        wkc.Domain,
		CSID:      wkc.CSID,
		Layer:     wkc.Layer,
		Tag:       "",
		WellKnown: string(serialized),
	}

	err = r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"cs_id", "layer", "tag", "well_known"}),
	}).Create(&newServer).Error
	if err != nil {
		return nil, err
	}

	return modelToDomainServer(&newServer)
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
