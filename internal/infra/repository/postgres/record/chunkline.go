package record

import (
	"context"
	"errors"
	"net/url"
	"time"

	"gorm.io/gorm"

	"github.com/concrnt/concrnt/chunkline"
	"github.com/concrnt/concrnt/internal/infra/database/models"
)

const (
	defaultChunkSize = 32
)

func (r *Repository) GetChunklineManifest(ctx context.Context, uri string) (*chunkline.Manifest, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.GetChunklineManifest")
	defer span.End()

	recordKey, err := GetRecordKeyByURI(ctx, r.db, uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	var firstChunk *int64
	firstCollectionMember := models.RecordKey{}
	err = r.db.WithContext(ctx).
		Model(&models.RecordKey{}).
		Where("record_keys.parent_id = ?", recordKey.ID).
		Where("record_keys.record_created_at IS NOT NULL").
		Order("record_keys.record_created_at ASC").
		Limit(1).
		Take(&firstCollectionMember).Error
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

	parentRecordKey, err := GetRecordKeyByURI(ctx, r.db, uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	var members []models.RecordKey
	err = r.db.WithContext(ctx).
		Where("parent_id = ?", parentRecordKey.ID).
		Where("record_created_at < ?", chunkDate).
		Order("record_created_at DESC").
		Limit(defaultChunkSize).
		Preload("Record").
		Preload("Record.Document").
		Find(&members).Error
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// Judge window coverage by record_created_at — the same column the queries
	// filter and order by — not the joined Record.CreatedAt, which can diverge
	// (e.g. backdated records) and would skip the window scan.
	if len(members) == 0 || members[len(members)-1].RecordCreatedAt == nil || !members[len(members)-1].RecordCreatedAt.Before(prevChunkDate) {
		err = r.db.WithContext(ctx).
			Where("parent_id = ?", parentRecordKey.ID).
			Where("record_created_at < ?", chunkDate).
			Where("record_created_at >= ?", prevChunkDate).
			Order("record_created_at DESC").
			Preload("Record").
			Preload("Record.Document").
			Find(&members).Error
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
	}

	bodyItems := make([]chunkline.BodyItem, 0, len(members))

	for _, member := range members {

		href := member.URI
		contentType := "application/concrnt.document+json"
		if member.Record.Redirect != nil {
			href = *member.Record.Redirect
		}

		timestamp := member.Record.CreatedAt
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
