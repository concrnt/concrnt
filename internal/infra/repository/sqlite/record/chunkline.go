package record

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"time"

	"github.com/concrnt/concrnt/chunkline"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
)

const defaultChunkSize = 32

func (r *Repository) GetChunklineManifest(ctx context.Context, uri string) (*chunkline.Manifest, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.GetChunklineManifest")
	defer span.End()

	parent, err := getRecordKey(ctx, r.db, uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	var firstChunk *int64
	var first int64
	err = r.db.QueryRowContext(ctx, "SELECT record_created_at FROM record_keys WHERE parent_id = ? AND record_created_at IS NOT NULL ORDER BY record_created_at ASC LIMIT 1", parent.ID).Scan(&first)
	if err == nil {
		chunk := sqlitedb.Time(first).Unix() / 600
		firstChunk = &chunk
	} else if !errors.Is(err, sql.ErrNoRows) {
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
	}, nil
}

func (r *Repository) LookupLocalItrs(ctx context.Context, uris []string, chunkID int64) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.LookupLocalItrs")
	defer span.End()

	lookup := map[string]int64{}
	if len(uris) == 0 {
		return lookup, nil
	}
	cutoff := time.Unix((chunkID+1)*600, 0) // descending order

	args := append([]any{sqlitedb.Micros(cutoff)}, sqlitedb.Args(uris)...)
	rows, err := r.db.QueryContext(ctx, `SELECT parent.uri,
			(SELECT child.record_created_at FROM record_keys child WHERE child.parent_id = parent.id AND child.record_created_at < ? ORDER BY child.record_created_at DESC LIMIT 1)
		FROM record_keys parent WHERE parent.uri IN (`+sqlitedb.Placeholders(len(uris))+`)`, args...)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var uri string
		var max sql.NullInt64
		if err := rows.Scan(&uri, &max); err != nil {
			return nil, err
		}
		if max.Valid {
			lookup[uri] = sqlitedb.Time(max.Int64).Unix() / 600
		}
	}
	return lookup, rows.Err()
}

type member struct {
	URI             string
	Redirect        sql.NullString
	RecordCreatedAt int64
}

func (r *Repository) loadMembers(ctx context.Context, query string, args ...any) ([]member, error) {
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	members := []member{}
	for rows.Next() {
		var m member
		if err := rows.Scan(&m.URI, &m.Redirect, &m.RecordCreatedAt); err != nil {
			return nil, err
		}
		members = append(members, m)
	}
	return members, rows.Err()
}

func (r *Repository) LoadLocalBody(ctx context.Context, uri string, chunkID int64) ([]chunkline.BodyItem, error) {
	ctx, span := tracer.Start(ctx, "Repository.Chunkline.LoadLocalBody")
	defer span.End()

	chunkDate := sqlitedb.Micros(time.Unix((chunkID+1)*600, 0))
	prevChunkDate := sqlitedb.Micros(time.Unix((chunkID-1)*600, 0))

	parent, err := getRecordKey(ctx, r.db, uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	const memberSelect = "SELECT uri, redirect, record_created_at FROM record_keys WHERE parent_id = ? AND record_created_at < ?"
	members, err := r.loadMembers(ctx, memberSelect+" ORDER BY record_created_at DESC LIMIT ?", parent.ID, chunkDate, defaultChunkSize)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// the window is not covered by the newest 32: read the whole window
	if len(members) == 0 || members[len(members)-1].RecordCreatedAt >= prevChunkDate {
		members, err = r.loadMembers(ctx, memberSelect+" AND record_created_at >= ? ORDER BY record_created_at DESC", parent.ID, chunkDate, prevChunkDate)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
	}

	bodyItems := make([]chunkline.BodyItem, 0, len(members))
	for _, m := range members {
		href := m.URI
		if m.Redirect.Valid {
			href = m.Redirect.String
		}
		bodyItems = append(bodyItems, chunkline.BodyItem{
			Timestamp:   sqlitedb.Time(m.RecordCreatedAt),
			Href:        href,
			ContentType: "application/concrnt.document+json",
		})
	}
	return bodyItems, nil
}
