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
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	"github.com/concrnt/concrnt/internal/usecase/server"
)

// ServerRepository is the write-through cache of remote servers' well-known
// documents.
type ServerRepository struct {
	client *firestore.Client
	cl     *client.Client
}

func NewServerRepository(fs *firestore.Client, cl *client.Client) server.Repository {
	return &ServerRepository{client: fs, cl: cl}
}

func (r *ServerRepository) Resolve(ctx context.Context, identifier string, hint *string) (*domain.Server, error) {
	ctx, span := tracer.Start(ctx, "ServerRepository.Resolve")
	defer span.End()

	byCSID := concrnt.IsCSID(identifier)
	var snap *firestore.DocumentSnapshot
	if byCSID {
		snaps, err := r.client.Collection(ColServers).Where("csID", "==", identifier).Limit(1).Documents(ctx).GetAll()
		if err == nil && len(snaps) == 1 {
			snap = snaps[0]
		}
	} else {
		hint = nil
		if s, err := r.client.Collection(ColServers).Doc(identifier).Get(ctx); err == nil && s.Exists() {
			snap = s
		}
	}
	if snap != nil {
		var s serverDoc
		if err := decode(snap, &s); err == nil && s.WellKnown != "" {
			return repoutil.ServerFromWellKnown(s.Tag, s.WellKnown)
		}
	}
	if byCSID && hint == nil {
		return nil, domain.ErrNotFound
	}

	wkc, err := r.cl.GetServer(ctx, identifier, hint)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return r.cache(ctx, &wkc)
}

// cache stores a freshly fetched well-known document under its FQDN.
func (r *ServerRepository) cache(ctx context.Context, wkc *concrnt.WellKnownConcrnt) (*domain.Server, error) {
	serialized, err := json.Marshal(wkc)
	if err != nil {
		return nil, err
	}
	doc := serverDoc{CSID: wkc.CSID, Layer: wkc.Layer, Tag: "", WellKnown: string(serialized)}
	err = upsert(ctx, r.client.Collection(ColServers).Doc(wkc.Domain), map[string]any{
		"csID":      doc.CSID,
		"layer":     doc.Layer,
		"tag":       doc.Tag,
		"wellKnown": doc.WellKnown,
		"mDate":     firestore.ServerTimestamp,
	})
	if err != nil {
		return nil, err
	}
	return repoutil.ServerFromWellKnown(doc.Tag, doc.WellKnown)
}

func (r *ServerRepository) List(ctx context.Context) ([]*concrnt.WellKnownConcrnt, error) {
	ctx, span := tracer.Start(ctx, "ServerRepository.List")
	defer span.End()

	it := r.client.Collection(ColServers).Documents(ctx)
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
