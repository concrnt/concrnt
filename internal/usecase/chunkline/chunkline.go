package chunkline

import (
	"context"
	"fmt"
	"time"

	"github.com/concrnt/concrnt/chunkline"
	"github.com/concrnt/concrnt/internal/usecase"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

type Usecase struct {
	repo     record.Repository
	gateway  Gateway
	kvs      usecase.KVS
	resolver URIResolver
}

// Gateway encapsulates external timeline resolution.
type Gateway interface {
	QueryDescending(ctx context.Context, uris []string, until time.Time, limit int) ([]chunkline.BodyItemWithSource, error)
}

// URIResolver rewrites a timeline URI to the form it is stored under
// (cckv://@<alias FQDN>/<key> to the CCID form, CIP-0 §7.2). The record
// usecase implements it.
type URIResolver interface {
	CanonicalizeURI(ctx context.Context, uri string) (string, error)
}

func New(repo record.Repository, gateway Gateway, kvs usecase.KVS, resolver URIResolver) *Usecase {
	return &Usecase{
		repo:     repo,
		gateway:  gateway,
		kvs:      kvs,
		resolver: resolver,
	}
}

func (uc *Usecase) GetChunklineManifest(ctx context.Context, uri string) (*chunkline.Manifest, error) {
	if uc.resolver != nil {
		canonical, err := uc.resolver.CanonicalizeURI(ctx, uri)
		if err != nil {
			return nil, err
		}
		uri = canonical
	}
	return uc.repo.GetChunklineManifest(ctx, uri)
}

func (uc *Usecase) LookupLocalItrs(ctx context.Context, uris []string, chunkID int64) (map[string]int64, error) {
	return uc.repo.LookupLocalItrs(ctx, uris, chunkID)
}

func (uc *Usecase) LoadLocalBody(ctx context.Context, uri string, chunkID int64) ([]chunkline.BodyItem, error) {
	return uc.repo.LoadLocalBody(ctx, uri, chunkID)
}

// GetLocalRemovedItems serves the /chunkline/removed endpoint: the item IDs
// recently removed from a locally-hosted timeline, advertised so readers can
// drop them from already-cached chunks.
func (uc *Usecase) GetLocalRemovedItems(ctx context.Context, uri string) ([]string, error) {
	if uc.kvs == nil {
		return []string{}, nil
	}
	items, err := uc.kvs.SetMembers(ctx, usecase.RemovedItemsKey(uri))
	if err != nil {
		return nil, err
	}
	if items == nil {
		items = []string{}
	}
	return items, nil
}

func (uc *Usecase) GetRecent(ctx context.Context, uris []string, until time.Time, limit int) ([]chunkline.BodyItemWithSource, error) {

	if uc.gateway == nil {
		return nil, fmt.Errorf("chunkline gateway not configured")
	}

	items, err := uc.gateway.QueryDescending(ctx, uris, until, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query descending: %v", err)
	}

	return items, nil
}
