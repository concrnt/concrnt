package record

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"golang.org/x/sync/errgroup"

	"github.com/concrnt/concrnt/chunkline"
	"github.com/concrnt/concrnt/internal/domain"
	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
)

const (
	defaultChunkSize = 32
	// lookupConcurrency bounds the parallel top-1 queries of LookupLocalItrs.
	lookupConcurrency = 16
)

// children is the item query of a timeline: the keys whose parentURI is
// the timeline, newest first when desc. Placeholders have no
// recordCreatedAt and so never appear.
func (r *Repository) children(parentURI string, dir firestore.Direction) firestore.Query {
	return r.col(fs.ColRecordKeys).
		Where("parentURI", "==", parentURI).
		OrderBy("recordCreatedAt", dir).
		OrderBy("recordID", dir)
}

func (r *Repository) GetChunklineManifest(ctx context.Context, uri string) (*chunkline.Manifest, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.GetChunklineManifest")
	defer span.End()

	key, err := r.getKey(ctx, uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if key == nil {
		err := domain.NotFoundError{Resource: "record key"}
		span.RecordError(err)
		return nil, err
	}

	var firstChunk *int64
	snaps, err := r.children(uri, firestore.Asc).Limit(1).Select("recordCreatedAt").Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, fs.QueryErr(err)
	}
	if len(snaps) == 1 {
		if ts, ok := timeAt(snaps[0].Data(), "recordCreatedAt"); ok {
			chunk := ts.Unix() / repoutil.ChunkSize
			firstChunk = &chunk
		}
	}

	return repoutil.ChunklineManifest(uri, firstChunk), nil
}

// LookupLocalItrs finds, per timeline, the chunk holding the newest item at
// or before the requested chunk: one batch read of the timeline keys, then
// one top-1 query per existing timeline, run in parallel. Timelines without
// a key or without such an item are absent.
func (r *Repository) LookupLocalItrs(ctx context.Context, uris []string, chunkID int64) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.LookupLocalItrs")
	defer span.End()

	cutoff := time.Unix((chunkID+1)*repoutil.ChunkSize, 0) // descending order

	refs := make([]*firestore.DocumentRef, 0, len(uris))
	for _, uri := range uris {
		refs = append(refs, r.col(fs.ColRecordKeys).Doc(keyID(uri)))
	}
	keys, err := r.getAll(ctx, refs)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	var mu sync.Mutex
	lookup := make(map[string]int64)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(lookupConcurrency)
	for _, uri := range uris {
		if _, ok := keys[keyID(uri)]; !ok {
			continue
		}
		g.Go(func() error {
			snaps, err := r.children(uri, firestore.Desc).
				Where("recordCreatedAt", "<", cutoff).
				Limit(1).Select("recordCreatedAt").
				Documents(gctx).GetAll()
			if err != nil {
				return fs.QueryErr(err)
			}
			if len(snaps) != 1 {
				return nil
			}
			ts, ok := timeAt(snaps[0].Data(), "recordCreatedAt")
			if !ok {
				return nil
			}
			mu.Lock()
			lookup[uri] = ts.Unix() / repoutil.ChunkSize
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

func (r *Repository) LoadLocalBody(ctx context.Context, uri string, chunkID int64) ([]chunkline.BodyItem, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.LoadLocalBody")
	defer span.End()

	chunkDate := time.Unix((chunkID+1)*repoutil.ChunkSize, 0)
	prevChunkDate := time.Unix((chunkID-1)*repoutil.ChunkSize, 0)

	key, err := r.getKey(ctx, uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if key == nil {
		err := domain.NotFoundError{Resource: "record key"}
		span.RecordError(err)
		return nil, err
	}

	base := r.children(uri, firestore.Desc).Where("recordCreatedAt", "<", chunkDate)
	snaps, err := base.Limit(defaultChunkSize).Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, fs.QueryErr(err)
	}
	members := decodeKeys(snaps, true)

	// Judge window coverage by recordCreatedAt, the field the queries filter
	// and order by. If the newest 32 items do not reach back past the
	// previous chunk, load the whole window instead.
	if len(members) == 0 || !members[len(members)-1].RecordCreatedAt.Before(prevChunkDate) {
		snaps, err = base.Where("recordCreatedAt", ">=", prevChunkDate).Documents(ctx).GetAll()
		if err != nil {
			span.RecordError(err)
			return nil, fs.QueryErr(err)
		}
		members = decodeKeys(snaps, true)
	}

	// every field a body item needs is on the key (redirect and
	// recordCreatedAt are denormalised there), so no record is read
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
