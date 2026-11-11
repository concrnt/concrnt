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

// lookupConcurrency bounds the parallel top-1 queries of LookupLocalItrs.
const lookupConcurrency = 16

// children is the item query of a timeline: the keys whose parentURI is
// the timeline, newest first when desc. Placeholders have no
// recordCreatedAt and so never appear. A chunkline view needs none of the
// embedded document, so the query is projected to the item fields (and
// recordID, which a cursor on this ordering takes from the snapshot).
func (r *Repository) children(parentURI string, dir firestore.Direction) firestore.Query {
	return r.col(fs.ColRecordKeys).
		Where("parentURI", "==", parentURI).
		OrderBy("recordCreatedAt", dir).
		OrderBy("recordID", dir).
		Select("uri", "redirect", "recordCreatedAt", "recordID")
}

// GetChunklineManifest asks for the timeline's earliest item and, in
// parallel, for the timeline's key: an empty timeline and a missing one
// only differ in the latter.
func (r *Repository) GetChunklineManifest(ctx context.Context, uri string) (*chunkline.Manifest, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.GetChunklineManifest")
	defer span.End()

	var snaps []*firestore.DocumentSnapshot
	key, err := r.withKey(ctx, uri, func(ctx context.Context) error {
		var err error
		snaps, err = r.children(uri, firestore.Asc).Limit(1).Documents(ctx).GetAll()
		return fs.QueryErr(err)
	})
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
	if len(snaps) == 1 {
		if ts, ok := timeAt(snaps[0].Data(), "recordCreatedAt"); ok {
			chunk := ts.Unix() / repoutil.ChunkSize
			firstChunk = &chunk
		}
	}

	return repoutil.ChunklineManifest(uri, firstChunk), nil
}

// LookupLocalItrs finds, per timeline, the chunk holding the newest item at
// or before the requested chunk: one top-1 query per timeline, run in
// parallel with each other and with one batch read of the timeline keys.
// Timelines without a key or without such an item are absent.
func (r *Repository) LookupLocalItrs(ctx context.Context, uris []string, chunkID int64) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.LookupLocalItrs")
	defer span.End()

	cutoff := time.Unix((chunkID+1)*repoutil.ChunkSize, 0) // descending order

	keys := newReadSet()
	for _, uri := range uris {
		keys.add(r.keyRef(uri))
	}

	var mu sync.Mutex
	found := make(map[string]int64)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(lookupConcurrency)
	g.Go(func() error { return keys.load(gctx, r.client) })
	for _, uri := range uris {
		g.Go(func() error {
			snaps, err := r.children(uri, firestore.Desc).
				Where("recordCreatedAt", "<", cutoff).
				Limit(1).
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
			found[uri] = ts.Unix() / repoutil.ChunkSize
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		span.RecordError(err)
		return nil, err
	}

	lookup := make(map[string]int64, len(found))
	for uri, chunk := range found {
		if _, exists := keys.data(r.keyRef(uri)); exists {
			lookup[uri] = chunk
		}
	}
	return lookup, nil
}

// LoadLocalBody reads the newest page of items before the chunk's end, in
// parallel with the timeline's key (an empty timeline and a missing one
// only differ there). That page is the body unless the window (the chunk
// and the one before it) holds more than a page: then the rest of the
// window is read from where the page stopped, so no item is read twice.
// The SQL backends unite both in one statement; here a second query would
// have to re-read the page, and a window of more than a page is the rare
// case that pays the second round trip instead.
func (r *Repository) LoadLocalBody(ctx context.Context, uri string, chunkID int64) ([]chunkline.BodyItem, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.LoadLocalBody")
	defer span.End()

	chunkDate := time.Unix((chunkID+1)*repoutil.ChunkSize, 0)
	prevChunkDate := time.Unix((chunkID-1)*repoutil.ChunkSize, 0)

	base := r.children(uri, firestore.Desc).Where("recordCreatedAt", "<", chunkDate)
	var snaps []*firestore.DocumentSnapshot
	key, err := r.withKey(ctx, uri, func(ctx context.Context) error {
		var err error
		snaps, err = base.Limit(repoutil.BodyPageSize).Documents(ctx).GetAll()
		return fs.QueryErr(err)
	})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if key == nil {
		err := domain.NotFoundError{Resource: "record key"}
		span.RecordError(err)
		return nil, err
	}

	// a full page whose oldest item is still inside the window: the window
	// goes on past it
	if len(snaps) == repoutil.BodyPageSize {
		last := snaps[len(snaps)-1]
		if ts, ok := timeAt(last.Data(), "recordCreatedAt"); ok && !ts.Before(prevChunkDate) {
			rest, err := base.Where("recordCreatedAt", ">=", prevChunkDate).StartAfter(last).Documents(ctx).GetAll()
			if err != nil {
				span.RecordError(err)
				return nil, fs.QueryErr(err)
			}
			snaps = append(snaps, rest...)
		}
	}

	// every field a body item needs is on the key (redirect and
	// recordCreatedAt are denormalised there)
	bodyItems := make([]chunkline.BodyItem, 0, len(snaps))
	for _, snap := range snaps {
		m := snap.Data()
		ts, ok := timeAt(m, "recordCreatedAt")
		if !ok {
			continue
		}
		redirect := str(m, "redirect")
		bodyItems = append(bodyItems, repoutil.BodyItem(str(m, "uri"), &redirect, ts.UTC()))
	}
	return bodyItems, nil
}
