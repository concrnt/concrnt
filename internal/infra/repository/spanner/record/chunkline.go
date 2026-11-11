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

func (r *Repository) GetChunklineManifest(ctx context.Context, uri string) (*chunkline.Manifest, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.GetChunklineManifest")
	defer span.End()

	// the timeline's key and its earliest member in one statement
	row, found, err := spannerdb.QueryOne[struct {
		First *time.Time `spanner:"first"`
	}](ctx, r.client.Single(), spanner.Statement{
		SQL: `SELECT (
				SELECT child.record_created_at FROM record_keys child
				WHERE child.parent_uri = parent.uri AND child.record_created_at IS NOT NULL
				ORDER BY child.record_created_at ASC LIMIT 1
			) AS first
			FROM record_keys parent WHERE parent.uri = @uri`,
		Params: map[string]any{"uri": uri},
	})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if !found {
		err := domain.NotFoundError{Resource: "record key"}
		span.RecordError(err)
		return nil, err
	}

	var firstChunk *int64
	if row.First != nil {
		chunk := row.First.Unix() / repoutil.ChunkSize
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

// LoadLocalBody reads the body in one statement: the page of the newest
// members before the chunk's end, united with every member of the window
// (the chunk and the one before it), so a window holding more than a page
// comes back whole and a sparse one is filled up from older members. The
// outer row of the timeline's key tells an empty timeline from a missing
// one. Every field a body item needs is on the key row (redirect and
// record_created_at are denormalised there), so no record is read.
func (r *Repository) LoadLocalBody(ctx context.Context, uri string, chunkID int64) ([]chunkline.BodyItem, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.LoadLocalBody")
	defer span.End()

	chunkDate := time.Unix((chunkID+1)*repoutil.ChunkSize, 0)
	prevChunkDate := time.Unix((chunkID-1)*repoutil.ChunkSize, 0)

	rows, err := spannerdb.QueryAll[struct {
		URI             *string    `spanner:"uri"`
		Redirect        *string    `spanner:"redirect"`
		RecordCreatedAt *time.Time `spanner:"record_created_at"`
	}](ctx, r.client.Single(), spanner.Statement{
		SQL: `SELECT member.uri AS uri, member.redirect AS redirect, member.record_created_at AS record_created_at
			FROM record_keys parent
			LEFT JOIN (
				SELECT page.parent_uri, page.uri, page.redirect, page.record_created_at FROM (
					SELECT parent_uri, uri, redirect, record_created_at FROM record_keys
					WHERE parent_uri = @uri AND record_created_at < @end
					ORDER BY record_created_at DESC LIMIT @page
				) page
				UNION DISTINCT
				SELECT parent_uri, uri, redirect, record_created_at FROM record_keys
				WHERE parent_uri = @uri AND record_created_at < @end AND record_created_at >= @start
			) member ON member.parent_uri = parent.uri
			WHERE parent.uri = @uri
			ORDER BY member.record_created_at DESC`,
		Params: map[string]any{"uri": uri, "end": chunkDate, "start": prevChunkDate, "page": int64(repoutil.BodyPageSize)},
	})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if len(rows) == 0 {
		err := domain.NotFoundError{Resource: "record key"}
		span.RecordError(err)
		return nil, err
	}

	bodyItems := make([]chunkline.BodyItem, 0, len(rows))
	for _, row := range rows {
		if row.URI == nil || row.RecordCreatedAt == nil {
			// the timeline's own row, without a member joined to it
			continue
		}
		bodyItems = append(bodyItems, repoutil.BodyItem(*row.URI, row.Redirect, row.RecordCreatedAt.UTC()))
	}

	return bodyItems, nil
}
