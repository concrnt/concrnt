package record

import (
	"context"
	"strings"
	"time"

	"github.com/concrnt/concrnt"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
	"gorm.io/gorm"
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
	query := joinRecords(r.db.WithContext(ctx).Model(&sqlitedb.RecordKey{}), false).
		Where(`uri LIKE ? ESCAPE '\'`, likeEscaper.Replace(prefix)+"%")

	query = orderLimit(recordFilters(query, schema, author, since, until), order, limit)

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
func (r *Repository) QueryRecordSubtree(ctx context.Context, base string, includeSelf bool, afterURI string, limit int) ([]concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryRecordSubtree")
	defer span.End()

	query := joinRecords(r.db.WithContext(ctx).Model(&sqlitedb.RecordKey{}), false)

	subtreePattern := likeEscaper.Replace(base) + `/%`
	if includeSelf {
		query = query.Where(`uri = ? OR uri LIKE ? ESCAPE '\'`, base, subtreePattern)
	} else {
		query = query.Where(`uri LIKE ? ESCAPE '\'`, subtreePattern)
	}

	if afterURI != "" {
		query = query.Where("uri > ?", afterURI)
	}
	query = query.Order("uri ASC")
	if limit > 0 {
		query = query.Limit(limit)
	}

	rows, err := scanRecordRows(query)
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

	query := joinRecords(r.db.WithContext(ctx).Model(&sqlitedb.RecordKey{}), false)

	query, err := parentPredicate(query, parent)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	query = orderLimit(recordFilters(query, schema, author, since, until), order, limit)

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

	query := joinRecords(r.db.WithContext(ctx).Model(&sqlitedb.RecordKey{}), true)

	query, err := parentPredicate(query, parent)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	query = recordFilters(query, schema, author, nil, nil)
	// byte order (CONTRACT.md "Key order") is SQLite's default BINARY
	// collation, so the (parent_id, uri) index serves bounds and ordering
	if since != nil {
		query = query.Where("record_keys.uri >= ?", *since)
	}
	if until != nil {
		query = query.Where("record_keys.uri <= ?", *until)
	}

	if order == "desc" {
		query = query.Order("record_keys.uri DESC")
	} else {
		query = query.Order("record_keys.uri ASC")
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

// recordFilters adds the schema, author and createdAt window of a listing
// over joined records (r).
func recordFilters(query *gorm.DB, schema, author string, since, until *time.Time) *gorm.DB {
	if schema != "" {
		query = query.Where("r.schema = ?", schema)
	}
	if author != "" {
		query = query.Where("r.author = ?", author)
	}
	if since != nil {
		query = query.Where("r.created_at >= ?", sqlitedb.Micros(*since))
	}
	if until != nil {
		query = query.Where("r.created_at <= ?", sqlitedb.Micros(*until))
	}
	return query
}

// orderLimit orders a listing by (created_at, id) and bounds the page.
func orderLimit(query *gorm.DB, order string, limit int) *gorm.DB {
	if order == "desc" {
		query = query.Order("r.created_at DESC, r.document_id DESC")
	} else {
		query = query.Order("r.created_at ASC, r.document_id ASC")
	}
	if limit > 0 {
		query = query.Limit(limit)
	}
	return query
}

// parentPredicate restricts a listing to parent's direct children. The
// owner root (cckv://<owner>) has no record_keys row and its top-level keys
// carry parent_id NULL (getOrCreateParentRecordKey), so the root is matched
// by URI prefix instead (CIP-5 §3.1).
func parentPredicate(query *gorm.DB, parent string) (*gorm.DB, error) {
	parsedParent, err := concrnt.ParseCCURI(parent)
	if err != nil {
		return nil, err
	}
	if parsedParent.Key == "" {
		return query.Where(`parent_id IS NULL AND uri LIKE ? ESCAPE '\'`, likeEscaper.Replace("cckv://"+parsedParent.Owner)+"/%"), nil
	}
	return query.Where("parent_id = (SELECT id FROM record_keys WHERE uri = ?)", parent), nil
}
