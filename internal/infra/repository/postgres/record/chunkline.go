package record

import (
	"context"
	"database/sql"
	"net/url"
	"time"

	"github.com/concrnt/concrnt/chunkline"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/database/models"
)

const (
	defaultChunkSize = 32
)

func (r *Repository) GetChunklineManifest(ctx context.Context, uri string) (*chunkline.Manifest, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.GetChunklineManifest")
	defer span.End()

	var rows []struct {
		First *time.Time
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
		chunk := rows[0].First.Unix() / 600
		firstChunk = &chunk
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
		// Metadata: recordKey.Record.Value,
	}, nil
}

func (r *Repository) LookupLocalItrs(ctx context.Context, uris []string, chunkID int64) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.LookupLocalItrs")
	defer span.End()

	type TimelineRow struct {
		URI          string     `gorm:"column:uri"`
		MaxCreatedAt *time.Time `gorm:"column:max_created_at"`
	}

	var res []TimelineRow

	cutoff := time.Unix((chunkID+1)*600, 0) // descending order

	latestSubQuery := r.db.
		Table("record_keys AS child").
		Select("child.record_created_at").
		Where("child.parent_id = parent.id").
		Where("child.record_created_at < ?", cutoff).
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
		lookup[row.URI] = row.MaxCreatedAt.Unix() / 600
	}
	return lookup, nil
}

func (r *Repository) LoadLocalBody(ctx context.Context, uri string, chunkID int64) ([]chunkline.BodyItem, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.LoadLocalBody")
	defer span.End()

	chunkDate := time.Unix((chunkID+1)*600, 0)
	prevChunkDate := time.Unix((chunkID-1)*600, 0)

	// Not the newest page first and the whole window only when the page does
	// not cover it: that is a second round trip. Not an inner join on the
	// timeline's key either: an empty timeline would look like a missing one.
	var members []models.RecordKey
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
		sql.Named("uri", uri), sql.Named("end", chunkDate), sql.Named("start", prevChunkDate), sql.Named("page", defaultChunkSize)).
		Scan(&members).Error
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if len(members) == 0 {
		err := domain.NotFoundError{Resource: "record key"}
		span.RecordError(err)
		return nil, err
	}

	bodyItems := make([]chunkline.BodyItem, 0, len(members))

	for _, member := range members {
		if member.RecordCreatedAt == nil {
			// the timeline's own row, without a member joined to it
			continue
		}

		href := member.URI
		contentType := "application/concrnt.document+json"
		if member.Redirect != nil {
			href = *member.Redirect
		}

		item := chunkline.BodyItem{
			Timestamp:   *member.RecordCreatedAt,
			Href:        href,
			ContentType: contentType,
		}
		bodyItems = append(bodyItems, item)
	}

	return bodyItems, nil
}
