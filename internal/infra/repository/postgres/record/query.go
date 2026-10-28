package record

import (
	"context"
	"strings"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func (r *Repository) QueryByPrefix(
	ctx context.Context,
	prefix, schema, author string,
	since, until *time.Time,
	limit int,
	order string,
) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.Query")
	defer span.End()

	// the prefix is a literal (CONTRACT.md): its LIKE metacharacters are
	// escaped so "it_em" cannot match "items"
	query := joinRecords(r.db.WithContext(ctx).Model(&models.RecordKey{}), false).
		Where(`uri LIKE ? ESCAPE '\'`, likeEscaper.Replace(prefix)+"%")

	if schema != "" {
		query = query.Where("r.schema = ?", schema)
	}
	if author != "" {
		query = query.Where("r.author = ?", author)
	}
	if since != nil {
		query = query.Where("r.created_at >= ?", *since)
	}
	if until != nil {
		query = query.Where("r.created_at <= ?", *until)
	}

	if order == "desc" {
		query = query.Order("r.created_at DESC, r.document_id DESC")
	} else {
		query = query.Order("r.created_at ASC, r.document_id ASC")
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	rows, err := scanRecordRows(query)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return queryRows(rows)
}

// likeEscaper escapes LIKE metacharacters so a key containing '%' or '_'
// can't over-match when embedded in a pattern.
var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

// QueryRecordSubtree returns every live record whose key is base itself
// (includeSelf) or lies under base's path subtree (base + "/..."), ordered by
// URI (byte order) so base leads. Unlike QueryByPrefix's raw prefix match this never picks
// up sibling keys ("item2" for base "item"), and metacharacters in base are
// escaped — the result is exactly the set a range delete may remove.
func (r *Repository) QueryRecordSubtree(ctx context.Context, base string, includeSelf bool) ([]concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryRecordSubtree")
	defer span.End()

	query := joinRecords(r.db.WithContext(ctx).Model(&models.RecordKey{}), false)

	subtreePattern := likeEscaper.Replace(base) + `/%`
	if includeSelf {
		query = query.Where(`uri = ? OR uri LIKE ? ESCAPE '\'`, base, subtreePattern)
	} else {
		query = query.Where(`uri LIKE ? ESCAPE '\'`, subtreePattern)
	}

	rows, err := scanRecordRows(query.Order(`uri COLLATE "C" ASC`))
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	sds := make([]concrnt.SignedDocument, 0, len(rows))
	for _, row := range rows {
		sd, err := row.signedDocument()
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		sds = append(sds, sd)
	}

	return sds, nil
}

func (r *Repository) QueryByParent(
	ctx context.Context,
	parent, schema, author string,
	since, until *time.Time,
	limit int,
	order string,
) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryByParent")
	defer span.End()

	query := joinRecords(r.db.WithContext(ctx).Model(&models.RecordKey{}), false)

	// The owner root (cckv://<owner>) has no record_keys row and its top-level
	// keys carry parent_id NULL (getOrCreateParentRecordKey), so the root is
	// matched by URI prefix instead (CIP-5 §3.1).
	parsedParent, err := concrnt.ParseCCURI(parent)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if parsedParent.Key == "" {
		query = query.Where(`parent_id IS NULL AND uri LIKE ? ESCAPE '\'`, likeEscaper.Replace("cckv://"+parsedParent.Owner)+"/%")
	} else {
		query = query.Where("parent_id = (SELECT id FROM record_keys WHERE uri = ?)", parent)
	}

	if schema != "" {
		query = query.Where("r.schema = ?", schema)
	}
	if author != "" {
		query = query.Where("r.author = ?", author)
	}
	if since != nil {
		query = query.Where("r.created_at >= ?", *since)
	}
	if until != nil {
		query = query.Where("r.created_at <= ?", *until)
	}

	if order == "desc" {
		query = query.Order("r.created_at DESC, r.document_id DESC")
	} else {
		query = query.Order("r.created_at ASC, r.document_id ASC")
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	rows, err := scanRecordRows(query)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return queryRows(rows)
}

// QueryByParentOrderByKey lists parent's direct children ordered by their
// cckv (CIP-5 orderby=key). The LEFT JOIN keeps placeholder keys (record_id
// NULL) so intermediate keys come back as key-only rows; a schema or author
// filter compares record columns and so drops them. since/until are
// inclusive key bounds, the cursors of §3.3 for this ordering.
func (r *Repository) QueryByParentOrderByKey(
	ctx context.Context,
	parent, schema, author string,
	since, until *string,
	limit int,
	order string,
) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryByParentOrderByKey")
	defer span.End()

	query := joinRecords(r.db.WithContext(ctx).Model(&models.RecordKey{}), true)

	parsedParent, err := concrnt.ParseCCURI(parent)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if parsedParent.Key == "" {
		query = query.Where(`parent_id IS NULL AND uri LIKE ? ESCAPE '\'`, likeEscaper.Replace("cckv://"+parsedParent.Owner)+"/%")
	} else {
		query = query.Where("parent_id = (SELECT id FROM record_keys WHERE uri = ?)", parent)
	}

	if schema != "" {
		query = query.Where("r.schema = ?", schema)
	}
	if author != "" {
		query = query.Where("r.author = ?", author)
	}
	// byte order (CONTRACT.md "Key order"): the collation is forced on the
	// bounds and the ordering alike so the (parent_id, uri COLLATE "C") index
	// serves both
	if since != nil {
		query = query.Where(`record_keys.uri COLLATE "C" >= ?`, *since)
	}
	if until != nil {
		query = query.Where(`record_keys.uri COLLATE "C" <= ?`, *until)
	}

	if order == "desc" {
		query = query.Order(`record_keys.uri COLLATE "C" DESC`)
	} else {
		query = query.Order(`record_keys.uri COLLATE "C" ASC`)
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	rows, err := scanRecordRows(query)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return queryRows(rows)
}
