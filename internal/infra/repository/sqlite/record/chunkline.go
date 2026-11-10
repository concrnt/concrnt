package record

import (
	"context"
	"database/sql"
	"time"

	"github.com/concrnt/concrnt/chunkline"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
)

const (
	defaultChunkSize = 32
)

func (r *Repository) GetChunklineManifest(ctx context.Context, uri string) (*chunkline.Manifest, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.GetChunklineManifest")
	defer span.End()

	// the timeline's key and its earliest member in one statement
	var rows []struct {
		First *int64
	}
	err := r.db.WithContext(ctx).Raw(`SELECT (
			SELECT child.record_created_at FROM record_keys child
			WHERE child.parent_id = parent.id AND child.record_created_at IS NOT NULL
			ORDER BY child.record_created_at ASC LIMIT 1
		) AS first
		FROM record_keys parent WHERE parent.uri = ?`, uri).
		Scan(&rows).Error
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if len(rows) == 0 {
		err := domain.NotFoundError{Resource: "record key"}
		span.RecordError(err)
		return nil, err
	}

	var firstChunk *int64
	if rows[0].First != nil {
		chunk := sqlitedb.Time(*rows[0].First).Unix() / repoutil.ChunkSize
		firstChunk = &chunk
	}

	return repoutil.ChunklineManifest(uri, firstChunk), nil
}

func (r *Repository) LookupLocalItrs(ctx context.Context, uris []string, chunkID int64) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.LookupLocalItrs")
	defer span.End()

	type TimelineRow struct {
		URI          string `gorm:"column:uri"`
		MaxCreatedAt *int64 `gorm:"column:max_created_at"`
	}

	var res []TimelineRow

	cutoff := time.Unix((chunkID+1)*repoutil.ChunkSize, 0) // descending order

	latestSubQuery := r.db.
		Table("record_keys AS child").
		Select("child.record_created_at").
		Where("child.parent_id = parent.id").
		Where("child.record_created_at < ?", sqlitedb.Micros(cutoff)).
		Order("child.record_created_at DESC").
		Limit(1)

	err := r.db.WithContext(ctx).
		Table("record_keys AS parent").
		Select("parent.uri AS uri, (?) AS max_created_at", latestSubQuery).
		Where("parent.uri IN ?", uris).
		Scan(&res).Error

	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	lookup := make(map[string]int64)
	for _, row := range res {
		if row.MaxCreatedAt == nil {
			continue
		}
		lookup[row.URI] = sqlitedb.Time(*row.MaxCreatedAt).Unix() / repoutil.ChunkSize
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

	var rows []struct {
		URI             *string
		Redirect        *string
		RecordCreatedAt *int64
	}
	err := r.db.WithContext(ctx).Raw(`SELECT member.uri AS uri, member.redirect AS redirect, member.record_created_at AS record_created_at
		FROM record_keys parent
		LEFT JOIN (
			SELECT page.* FROM (
				SELECT parent_id, uri, redirect, record_created_at FROM record_keys
				WHERE parent_id = (SELECT id FROM record_keys WHERE uri = @uri) AND record_created_at < @end
				ORDER BY record_created_at DESC LIMIT @page
			) page
			UNION
			SELECT parent_id, uri, redirect, record_created_at FROM record_keys
			WHERE parent_id = (SELECT id FROM record_keys WHERE uri = @uri) AND record_created_at < @end AND record_created_at >= @start
		) member ON member.parent_id = parent.id
		WHERE parent.uri = @uri
		ORDER BY member.record_created_at DESC`,
		sql.Named("uri", uri), sql.Named("end", sqlitedb.Micros(chunkDate)), sql.Named("start", sqlitedb.Micros(prevChunkDate)), sql.Named("page", defaultChunkSize)).
		Scan(&rows).Error
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
		href := *row.URI
		if row.Redirect != nil {
			href = *row.Redirect
		}
		bodyItems = append(bodyItems, chunkline.BodyItem{
			Timestamp:   sqlitedb.Time(*row.RecordCreatedAt),
			Href:        href,
			ContentType: "application/concrnt.document+json",
		})
	}

	return bodyItems, nil
}
