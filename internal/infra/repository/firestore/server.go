package firestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/server"
)

// ServerRepository is the write-through cache of remote servers' well-known
// documents.
type ServerRepository struct {
	config *domain.Config
	client *firestore.Client
	cl     *client.Client
}

func NewServerRepository(config *domain.Config, fs *firestore.Client, cl *client.Client) server.Repository {
	return &ServerRepository{config: config, client: fs, cl: cl}
}

func serverDocToDomain(m *serverDoc) (*domain.Server, error) {
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

// cache stores a freshly fetched well-known document under its FQDN.
func (r *ServerRepository) cache(ctx context.Context, wkc *concrnt.WellKnownConcrnt) (*domain.Server, error) {
	serialized, err := json.Marshal(wkc)
	if err != nil {
		return nil, err
	}
	doc := serverDoc{
		CSID:      wkc.CSID,
		Layer:     wkc.Layer,
		Tag:       "",
		WellKnown: string(serialized),
	}
	err = upsert(ctx, r.client.Collection(colServers).Doc(wkc.Domain), map[string]any{
		"csID":      doc.CSID,
		"layer":     doc.Layer,
		"tag":       doc.Tag,
		"wellKnown": doc.WellKnown,
		"mDate":     firestore.ServerTimestamp,
	})
	if err != nil {
		return nil, err
	}
	return serverDocToDomain(&doc)
}

func (r *ServerRepository) GetAndCacheByCSID(ctx context.Context, csid string, hint *string) (*domain.Server, error) {
	ctx, span := tracer.Start(ctx, "ServerRepository.GetAndCacheByCSID")
	defer span.End()

	snaps, err := r.client.Collection(colServers).Where("csID", "==", csid).Limit(1).Documents(ctx).GetAll()
	if err == nil && len(snaps) == 1 {
		var s serverDoc
		if err := decode(snaps[0], &s); err == nil && s.WellKnown != "" {
			return serverDocToDomain(&s)
		}
	}

	if hint == nil {
		return nil, domain.ErrNotFound
	}

	wkc, err := r.cl.GetServer(ctx, csid, hint)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return r.cache(ctx, &wkc)
}

func (r *ServerRepository) GetAndCacheByFQDN(ctx context.Context, fqdn string) (*domain.Server, error) {
	ctx, span := tracer.Start(ctx, "ServerRepository.GetAndCacheByFQDN")
	defer span.End()

	snap, err := r.client.Collection(colServers).Doc(fqdn).Get(ctx)
	if err == nil && snap.Exists() {
		var s serverDoc
		if err := decode(snap, &s); err == nil && s.WellKnown != "" {
			return serverDocToDomain(&s)
		}
	}

	wkc, err := r.cl.GetServer(ctx, fqdn, nil)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return r.cache(ctx, &wkc)
}

func (r *ServerRepository) List(ctx context.Context) ([]*concrnt.WellKnownConcrnt, error) {
	ctx, span := tracer.Start(ctx, "ServerRepository.List")
	defer span.End()

	it := r.client.Collection(colServers).Documents(ctx)
	defer it.Stop()

	result := []*concrnt.WellKnownConcrnt{}
	for {
		snap, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		var s serverDoc
		if err := decode(snap, &s); err != nil {
			span.RecordError(err)
			continue
		}
		if s.WellKnown == "" {
			span.RecordError(fmt.Errorf("server with empty well-known: %s", snap.Ref.ID))
			continue
		}
		var wkc concrnt.WellKnownConcrnt
		if err := json.Unmarshal([]byte(s.WellKnown), &wkc); err != nil {
			span.RecordError(err)
			continue
		}
		result = append(result, &wkc)
	}
	return result, nil
}
