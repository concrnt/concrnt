package record

import (
	"context"
	"errors"
	"net/url"
	"time"

	"gorm.io/gorm"

	"github.com/concrnt/concrnt/chunkline"
)

const (
	defaultChunkSize = 32
)

type ChunklineRepository struct {
	db *gorm.DB
}

func NewChunklineRepository(db *gorm.DB) *ChunklineRepository {
	return &ChunklineRepository{db: db}
}

func (r *ChunklineRepository) GetChunklineManifest(ctx context.Context, uri string) (*chunkline.Manifest, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.GetChunklineManifest")
	defer span.End()

	recordKey, err := getRecordKeyByURI(ctx, r.db, uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	var firstChunk *int64
	firstCollectionMember, err := gorm.G[RecordKey](r.db).
		Where("record_keys.parent_id = ?", recordKey.ID).
		Where("record_keys.record_created_at IS NOT NULL").
		Order("record_keys.record_created_at ASC").
		Take(ctx)
	if err == nil {
		chunk := firstCollectionMember.RecordCreatedAt.Unix() / 600
		firstChunk = &chunk
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		span.RecordError(err)
		return nil, err
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

func (r *ChunklineRepository) LookupLocalItrs(ctx context.Context, uris []string, chunkID int64) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.LookupLocalItrs")
	defer span.End()

	type TimelineRow struct {
		URI          string     `gorm:"column:uri"`
		MaxCreatedAt *time.Time `gorm:"column:max_created_at"`
	}

	cutoff := time.Unix((chunkID+1)*600, 0) // descending order

	res, err := gorm.G[TimelineRow](r.db).Raw(`
SELECT parent.uri AS uri, (
  SELECT child.record_created_at FROM record_keys child
  WHERE child.parent_id = parent.id AND child.record_created_at < ?
  ORDER BY child.record_created_at DESC
  LIMIT 1
) AS max_created_at
FROM record_keys parent
WHERE parent.uri IN ?`, cutoff, uris).Find(ctx)

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

func (r *ChunklineRepository) LoadLocalBody(ctx context.Context, uri string, chunkID int64) ([]chunkline.BodyItem, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.LoadLocalBody")
	defer span.End()

	chunkDate := time.Unix((chunkID+1)*600, 0)
	prevChunkDate := time.Unix((chunkID-1)*600, 0)

	parentRecordKey, err := getRecordKeyByURI(ctx, r.db, uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	members, err := gorm.G[RecordKey](r.db).
		Where("parent_id = ?", parentRecordKey.ID).
		Where("record_created_at < ?", chunkDate).
		Order("record_created_at DESC").
		Limit(defaultChunkSize).
		Find(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// Judge window coverage by record_created_at, the column the queries
	// filter and order by.
	if len(members) == 0 || members[len(members)-1].RecordCreatedAt == nil || !members[len(members)-1].RecordCreatedAt.Before(prevChunkDate) {
		members, err = gorm.G[RecordKey](r.db).
			Where("parent_id = ?", parentRecordKey.ID).
			Where("record_created_at < ?", chunkDate).
			Where("record_created_at >= ?", prevChunkDate).
			Order("record_created_at DESC").
			Find(ctx)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
	}

	bodyItems := make([]chunkline.BodyItem, 0, len(members))

	for _, member := range members {

		href := member.URI
		contentType := "application/concrnt.document+json"
		if member.Redirect != nil {
			href = *member.Redirect
		}

		var timestamp time.Time
		if member.RecordCreatedAt != nil {
			timestamp = *member.RecordCreatedAt
		}

		item := chunkline.BodyItem{
			Timestamp:   timestamp,
			Href:        href,
			ContentType: contentType,
		}
		bodyItems = append(bodyItems, item)
	}

	return bodyItems, nil
}
