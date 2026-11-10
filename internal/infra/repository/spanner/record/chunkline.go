//go:build contractv1

package record

import (
	"context"
	"time"

	spannerdb "github.com/concrnt/concrnt/internal/infra/repository/spanner"

	"cloud.google.com/go/spanner"

	"github.com/concrnt/concrnt/chunkline"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
)

const (
	defaultChunkSize = 32
)

func (r *Repository) GetChunklineManifest(ctx context.Context, uri string) (*chunkline.Manifest, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.GetChunklineManifest")
	defer span.End()

	recordKey, err := readRecordKey(ctx, r.client.Single(), uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if recordKey == nil {
		err := domain.NotFoundError{Resource: "record key"}
		span.RecordError(err)
		return nil, err
	}

	var firstChunk *int64
	first, found, err := spannerdb.QueryOne[struct {
		RecordCreatedAt time.Time `spanner:"record_created_at"`
	}](ctx, r.client.Single(), spanner.Statement{
		SQL: `SELECT record_created_at FROM record_keys
			WHERE parent_uri = @parent AND record_created_at IS NOT NULL ORDER BY record_created_at ASC LIMIT 1`,
		Params: map[string]any{"parent": uri},
	})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if found {
		chunk := first.RecordCreatedAt.Unix() / repoutil.ChunkSize
		firstChunk = &chunk
	}

	return repoutil.ChunklineManifest(uri, firstChunk), nil
}

func (r *Repository) LookupLocalItrs(ctx context.Context, uris []string, chunkID int64) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.LookupLocalItrs")
	defer span.End()

	cutoff := time.Unix((chunkID+1)*repoutil.ChunkSize, 0) // descending order

	rows, err := spannerdb.QueryAll[struct {
		URI          string     `spanner:"uri"`
		MaxCreatedAt *time.Time `spanner:"max_created_at"`
	}](ctx, r.client.Single(), spanner.Statement{
		SQL: `SELECT parent.uri AS uri,
			(SELECT child.record_created_at FROM record_keys child
			   WHERE child.parent_uri = parent.uri AND child.record_created_at < @cutoff
			   ORDER BY child.record_created_at DESC LIMIT 1) AS max_created_at
			FROM record_keys parent WHERE parent.uri IN UNNEST(@uris)`,
		Params: map[string]any{"uris": uris, "cutoff": cutoff},
	})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	lookup := make(map[string]int64)
	for _, row := range rows {
		if row.MaxCreatedAt == nil {
			continue
		}
		lookup[row.URI] = row.MaxCreatedAt.Unix() / repoutil.ChunkSize
	}
	return lookup, nil
}

func (r *Repository) LoadLocalBody(ctx context.Context, uri string, chunkID int64) ([]chunkline.BodyItem, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.LoadLocalBody")
	defer span.End()

	chunkDate := time.Unix((chunkID+1)*repoutil.ChunkSize, 0)
	prevChunkDate := time.Unix((chunkID-1)*repoutil.ChunkSize, 0)

	parentRecordKey, err := readRecordKey(ctx, r.client.Single(), uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if parentRecordKey == nil {
		err := domain.NotFoundError{Resource: "record key"}
		span.RecordError(err)
		return nil, err
	}

	const memberSelect = "SELECT uri, parent_uri, record_id, record_created_at, redirect, clean_on_update FROM record_keys WHERE parent_uri = @parent AND record_created_at < @chunk_date "
	params := map[string]any{"parent": uri, "chunk_date": chunkDate, "limit": int64(defaultChunkSize)}
	members, err := spannerdb.QueryAll[recordKeyRow](ctx, r.client.Single(), spanner.Statement{
		SQL:    memberSelect + "ORDER BY record_created_at DESC LIMIT @limit",
		Params: params,
	})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// Judge window coverage by record_created_at — the same column the queries
	// filter and order by — not the joined record's createdAt, which can
	// diverge (e.g. backdated records) and would skip the window scan.
	if len(members) == 0 || members[len(members)-1].RecordCreatedAt == nil || !members[len(members)-1].RecordCreatedAt.Before(prevChunkDate) {
		params["prev_chunk_date"] = prevChunkDate
		members, err = spannerdb.QueryAll[recordKeyRow](ctx, r.client.Single(), spanner.Statement{
			SQL:    memberSelect + "AND record_created_at >= @prev_chunk_date ORDER BY record_created_at DESC",
			Params: params,
		})
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
	}

	// every field a body item needs is on the key row (redirect and
	// record_created_at are denormalised there), so no record is read
	bodyItems := make([]chunkline.BodyItem, 0, len(members))
	for _, member := range members {
		href := member.URI
		if member.Redirect != nil {
			href = *member.Redirect
		}
		bodyItems = append(bodyItems, chunkline.BodyItem{
			Timestamp:   member.RecordCreatedAt.UTC(),
			Href:        href,
			ContentType: "application/concrnt.document+json",
		})
	}

	return bodyItems, nil
}
