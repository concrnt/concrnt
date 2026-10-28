package firestore

import (
	"context"
	"net/url"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"golang.org/x/sync/errgroup"

	"github.com/concrnt/concrnt/chunkline"
	"github.com/concrnt/concrnt/internal/domain"
	chunklineuc "github.com/concrnt/concrnt/internal/usecase/chunkline"
)

const (
	defaultChunkSize = 32
	// lookupConcurrency bounds the parallel top-1 queries of LookupLocalItrs.
	lookupConcurrency = 16
)

// ChunklineRepository serves timelines from record_keys alone: a timeline is
// a key, its items are the keys whose parentGen is the timeline's gen, and
// the redirect needed for the body href is denormalized onto the item key.
type ChunklineRepository struct {
	client *firestore.Client
	keys   *RecordRepository
}

func NewChunklineRepository(client *firestore.Client) chunklineuc.Repository {
	return &ChunklineRepository{client: client, keys: &RecordRepository{client: client}}
}

// children is the item query of a timeline, newest first when desc.
func (r *ChunklineRepository) children(gen string, dir firestore.Direction) firestore.Query {
	return r.client.Collection(colRecordKeys).
		Where("parentGen", "==", gen).
		OrderBy("recordCreatedAt", dir).
		OrderBy("recordID", dir)
}

func (r *ChunklineRepository) GetChunklineManifest(ctx context.Context, uri string) (*chunkline.Manifest, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.GetChunklineManifest")
	defer span.End()

	key, err := r.keys.getKey(ctx, uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if key == nil {
		return nil, domain.NotFoundError{Resource: "record key"}
	}

	var firstChunk *int64
	snaps, err := r.children(key.Gen, firestore.Asc).Limit(1).Select("recordCreatedAt").Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, queryErr(err)
	}
	if len(snaps) == 1 {
		if ts, ok := timestampAt(snaps[0], "recordCreatedAt"); ok {
			chunk := ts.Unix() / 600
			firstChunk = &chunk
		}
	}

	safeURI := url.QueryEscape(uri)

	return &chunkline.Manifest{
		Version:    "1.0",
		ChunkSize:  600,
		FirstChunk: firstChunk,
		Descending: &chunkline.Endpoint{
			Iterator: "/api/v2/chunkline/itr/{chunk}?uri=" + safeURI,
			Body:     "/api/v2/chunkline/body/{chunk}?uri=" + safeURI,
		},
		Removed: "/api/v2/chunkline/removed?uri=" + safeURI,
	}, nil
}

// LookupLocalItrs finds, per timeline, the chunk holding the newest item at
// or before the requested chunk: one top-1 query per timeline, run in
// parallel. Timelines without a key or without such an item are absent.
func (r *ChunklineRepository) LookupLocalItrs(ctx context.Context, uris []string, chunkID int64) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.LookupLocalItrs")
	defer span.End()

	cutoff := time.Unix((chunkID+1)*600, 0) // descending order

	var mu sync.Mutex
	lookup := make(map[string]int64)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(lookupConcurrency)
	for _, uri := range uris {
		g.Go(func() error {
			key, err := r.keys.getKey(gctx, uri)
			if err != nil {
				return err
			}
			if key == nil {
				return nil
			}
			snaps, err := r.children(key.Gen, firestore.Desc).
				Where("recordCreatedAt", "<", cutoff).
				Limit(1).Select("recordCreatedAt").
				Documents(gctx).GetAll()
			if err != nil {
				return queryErr(err)
			}
			if len(snaps) != 1 {
				return nil
			}
			ts, ok := timestampAt(snaps[0], "recordCreatedAt")
			if !ok {
				return nil
			}
			mu.Lock()
			lookup[uri] = ts.Unix() / 600
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		span.RecordError(err)
		return nil, err
	}
	return lookup, nil
}

func (r *ChunklineRepository) LoadLocalBody(ctx context.Context, uri string, chunkID int64) ([]chunkline.BodyItem, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.LoadLocalBody")
	defer span.End()

	chunkDate := time.Unix((chunkID+1)*600, 0)
	prevChunkDate := time.Unix((chunkID-1)*600, 0)

	key, err := r.keys.getKey(ctx, uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if key == nil {
		return nil, domain.NotFoundError{Resource: "record key"}
	}

	base := r.children(key.Gen, firestore.Desc).Where("recordCreatedAt", "<", chunkDate)
	snaps, err := base.Limit(defaultChunkSize).Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, queryErr(err)
	}
	members, err := decodeKeys(snaps)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// Judge window coverage by recordCreatedAt — the field the queries
	// filter and order by. If the newest 32 items do not reach back past the
	// previous chunk, load the whole window instead.
	if len(members) == 0 || !members[len(members)-1].RecordCreatedAt.Before(prevChunkDate) {
		snaps, err = base.Where("recordCreatedAt", ">=", prevChunkDate).Documents(ctx).GetAll()
		if err != nil {
			span.RecordError(err)
			return nil, queryErr(err)
		}
		members, err = decodeKeys(snaps)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
	}

	bodyItems := make([]chunkline.BodyItem, 0, len(members))
	for _, member := range members {
		href := member.URI
		if member.Redirect != "" {
			href = member.Redirect
		}
		bodyItems = append(bodyItems, chunkline.BodyItem{
			Timestamp:   *member.RecordCreatedAt,
			Href:        href,
			ContentType: "application/concrnt.document+json",
		})
	}
	return bodyItems, nil
}

// timestampAt reads a timestamp field from a (possibly projected) snapshot.
func timestampAt(snap *firestore.DocumentSnapshot, field string) (time.Time, bool) {
	v, err := snap.DataAt(field)
	if err != nil {
		return time.Time{}, false
	}
	ts, ok := v.(time.Time)
	return ts, ok
}
