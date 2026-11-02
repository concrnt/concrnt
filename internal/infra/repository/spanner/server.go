package spanner

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/spanner"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/server"
)

type ServerRepository struct {
	config *domain.Config
	client *spanner.Client
	cl     *client.Client
}

func NewServerRepository(config *domain.Config, client *spanner.Client, cl *client.Client) server.Repository {
	return &ServerRepository{config: config, client: client, cl: cl}
}

type serverRow struct {
	ID        string `spanner:"fqdn"`
	CSID      string `spanner:"cs_id"`
	Tag       string `spanner:"tag"`
	Layer     string `spanner:"layer"`
	WellKnown string `spanner:"well_known"`
}

const serverSelect = "SELECT fqdn, cs_id, tag, layer, well_known FROM servers"

func modelToDomainServer(m *serverRow) (*domain.Server, error) {
	var wkc concrnt.WellKnownConcrnt
	if err := json.Unmarshal([]byte(m.WellKnown), &wkc); err != nil {
		return nil, err
	}
	return &domain.Server{
		TagString: m.Tag,
		WellKnown: wkc,
	}, nil
}

func (r *ServerRepository) Resolve(ctx context.Context, identifier string, hint *string) (*domain.Server, error) {
	ctx, span := tracer.Start(ctx, "ServerRepository.Resolve")
	defer span.End()

	if concrnt.IsCSID(identifier) {
		return r.GetAndCacheByCSID(ctx, identifier, hint)
	}
	return r.GetAndCacheByFQDN(ctx, identifier)
}

// cache upserts the well-known document by fqdn.
func (r *ServerRepository) cache(ctx context.Context, wkc concrnt.WellKnownConcrnt) (*domain.Server, error) {
	serialized, err := json.Marshal(wkc)
	if err != nil {
		return nil, err
	}
	newServer := serverRow{ID: wkc.Domain, CSID: wkc.CSID, Layer: wkc.Layer, Tag: "", WellKnown: string(serialized)}

	_, err = r.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, found, err := queryOne[serverRow](ctx, tx, spanner.Statement{
			SQL:    serverSelect + " WHERE fqdn = @fqdn",
			Params: map[string]any{"fqdn": newServer.ID},
		})
		if err != nil {
			return err
		}
		params := map[string]any{"fqdn": newServer.ID, "cs_id": newServer.CSID, "layer": newServer.Layer, "tag": newServer.Tag, "well_known": newServer.WellKnown}
		if found {
			_, err = tx.Update(ctx, spanner.Statement{
				SQL:    "UPDATE servers SET cs_id = @cs_id, layer = @layer, tag = @tag, well_known = @well_known, m_date = CURRENT_TIMESTAMP() WHERE fqdn = @fqdn",
				Params: params,
			})
			return err
		}
		_, err = tx.Update(ctx, spanner.Statement{
			SQL: `INSERT INTO servers (fqdn, cs_id, tag, layer, well_known, c_date, m_date)
				VALUES (@fqdn, @cs_id, @tag, @layer, @well_known, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())`,
			Params: params,
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	return modelToDomainServer(&newServer)
}

func (r *ServerRepository) GetAndCacheByCSID(ctx context.Context, csid string, hint *string) (*domain.Server, error) {
	ctx, span := tracer.Start(ctx, "ServerRepository.GetAndCacheByCSID")
	defer span.End()

	srv, found, err := queryOne[serverRow](ctx, r.client.Single(), spanner.Statement{
		SQL:    serverSelect + " WHERE cs_id = @cs_id",
		Params: map[string]any{"cs_id": csid},
	})
	if err == nil && found && srv.WellKnown != "" {
		return modelToDomainServer(&srv)
	}

	if hint == nil {
		return nil, domain.ErrNotFound
	}

	wkc, err := r.cl.GetServer(ctx, csid, hint)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return r.cache(ctx, wkc)
}

func (r *ServerRepository) GetAndCacheByFQDN(ctx context.Context, fqdn string) (*domain.Server, error) {
	ctx, span := tracer.Start(ctx, "ServerRepository.GetAndCacheByFQDN")
	defer span.End()

	srv, found, err := queryOne[serverRow](ctx, r.client.Single(), spanner.Statement{
		SQL:    serverSelect + " WHERE fqdn = @fqdn",
		Params: map[string]any{"fqdn": fqdn},
	})
	if err == nil && found && srv.WellKnown != "" {
		return modelToDomainServer(&srv)
	}

	wkc, err := r.cl.GetServer(ctx, fqdn, nil)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return r.cache(ctx, wkc)
}

func (r *ServerRepository) List(ctx context.Context) ([]*concrnt.WellKnownConcrnt, error) {
	ctx, span := tracer.Start(ctx, "ServerRepository.List")
	defer span.End()

	servers, err := queryAll[serverRow](ctx, r.client.Single(), spanner.Statement{SQL: serverSelect})
	if err != nil {
		return nil, err
	}

	result := make([]*concrnt.WellKnownConcrnt, 0, len(servers))
	for _, srv := range servers {
		if srv.WellKnown == "" {
			span.RecordError(fmt.Errorf("server with empty well-known: %s", srv.ID))
			continue
		}
		var wkc concrnt.WellKnownConcrnt
		if err := json.Unmarshal([]byte(srv.WellKnown), &wkc); err != nil {
			span.RecordError(err)
			continue
		}
		result = append(result, &wkc)
	}
	return result, nil
}
