package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	"github.com/concrnt/concrnt/internal/usecase/server"
)

type ServerRepository struct {
	db     *gorm.DB
	client *client.Client
}

func NewServerRepository(db *gorm.DB, cl *client.Client) server.Repository {
	return &ServerRepository{
		db:     db,
		client: cl,
	}
}

func (r *ServerRepository) Resolve(ctx context.Context, identifier string, hint *string) (*domain.Server, error) {
	ctx, span := tracer.Start(ctx, "ServerRepository.Resolve")
	defer span.End()

	byCSID := concrnt.IsCSID(identifier)
	column := "id"
	if byCSID {
		column = "cs_id"
	} else {
		hint = nil
	}
	var server Server
	err := r.db.WithContext(ctx).Where(column+" = ?", identifier).Take(&server).Error
	if err == nil && server.WellKnown != "" {
		return repoutil.ServerFromWellKnown(server.Tag, server.WellKnown)
	}
	if byCSID && hint == nil {
		return nil, domain.ErrNotFound
	}

	wkc, err := r.client.GetServer(ctx, identifier, hint)
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
	return repoutil.ServerFromWellKnown(newServer.Tag, newServer.WellKnown)
}

func (r *ServerRepository) List(ctx context.Context) ([]*concrnt.WellKnownConcrnt, error) {
	ctx, span := tracer.Start(ctx, "ServerRepository.List")
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
