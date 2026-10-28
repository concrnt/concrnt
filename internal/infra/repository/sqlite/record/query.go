package record

import (
	"context"
	"strings"
	"time"

	"github.com/concrnt/concrnt"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// likeEscaper escapes LIKE metacharacters so a key containing '%' or '_'
// can't over-match when embedded in a pattern.
var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

// recordFilters appends the schema / author / createdAt filters, ordering
// and limit shared by the createdAt-ordered listings.
func recordFilters(query string, args []any, schema, author string, since, until *time.Time, limit int, order string) (string, []any) {
	if schema != "" {
		query += " AND r.schema = ?"
		args = append(args, schema)
	}
	if author != "" {
		query += " AND r.author = ?"
		args = append(args, author)
	}
	if since != nil {
		query += " AND r.created_at >= ?"
		args = append(args, sqlitedb.Micros(*since))
	}
	if until != nil {
		query += " AND r.created_at <= ?"
		args = append(args, sqlitedb.Micros(*until))
	}
	if order == "desc" {
		query += " ORDER BY r.created_at DESC, r.document_id DESC"
	} else {
		query += " ORDER BY r.created_at ASC, r.document_id ASC"
	}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	return query, args
}

func (r *Repository) QueryByPrefix(ctx context.Context, prefix, schema, author string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.Query")
	defer span.End()

	query, args := recordFilters(joinRecords(false)+`WHERE record_keys.uri LIKE ? ESCAPE '\'`, []any{likeEscaper.Replace(prefix) + "%"}, schema, author, since, until, limit, order)
	rows, err := scanRecordRows(ctx, r.db, query, args...)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return queryRows(rows)
}

// QueryRecordSubtree returns every live record whose key is base itself
// (includeSelf) or lies under base's path subtree, in byte order.
func (r *Repository) QueryRecordSubtree(ctx context.Context, base string, includeSelf bool) ([]concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryRecordSubtree")
	defer span.End()

	subtreePattern := likeEscaper.Replace(base) + `/%`
	query := joinRecords(false)
	args := []any{}
	if includeSelf {
		query += `WHERE record_keys.uri = ? OR record_keys.uri LIKE ? ESCAPE '\'`
		args = append(args, base, subtreePattern)
	} else {
		query += `WHERE record_keys.uri LIKE ? ESCAPE '\'`
		args = append(args, subtreePattern)
	}
	rows, err := scanRecordRows(ctx, r.db, query+" ORDER BY record_keys.uri ASC", args...)
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

// parentFilter matches parent's direct children. The owner root
// (cckv://<owner>) has no record_keys row and its top-level keys carry
// parent_id NULL, so the root is matched by URI prefix instead (CIP-5 §3.1).
func parentFilter(parent string) (string, []any, error) {
	parsedParent, err := concrnt.ParseCCURI(parent)
	if err != nil {
		return "", nil, err
	}
	if parsedParent.Key == "" {
		return `WHERE record_keys.parent_id IS NULL AND record_keys.uri LIKE ? ESCAPE '\'`, []any{likeEscaper.Replace("cckv://"+parsedParent.Owner) + "/%"}, nil
	}
	return "WHERE record_keys.parent_id = (SELECT id FROM record_keys WHERE uri = ?)", []any{parent}, nil
}

func (r *Repository) QueryByParent(ctx context.Context, parent, schema, author string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryByParent")
	defer span.End()

	where, args, err := parentFilter(parent)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	query, args := recordFilters(joinRecords(false)+where, args, schema, author, since, until, limit, order)
	rows, err := scanRecordRows(ctx, r.db, query, args...)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return queryRows(rows)
}

// QueryByParentOrderByKey lists parent's direct children in byte order of
// their key (CIP-5 orderby=key); the LEFT JOIN keeps placeholder keys as
// key-only rows unless a record filter drops them.
func (r *Repository) QueryByParentOrderByKey(ctx context.Context, parent, schema, author string, since, until *string, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryByParentOrderByKey")
	defer span.End()

	where, args, err := parentFilter(parent)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	query := joinRecords(true) + where
	if schema != "" {
		query += " AND r.schema = ?"
		args = append(args, schema)
	}
	if author != "" {
		query += " AND r.author = ?"
		args = append(args, author)
	}
	if since != nil {
		query += " AND record_keys.uri >= ?"
		args = append(args, *since)
	}
	if until != nil {
		query += " AND record_keys.uri <= ?"
		args = append(args, *until)
	}
	if order == "desc" {
		query += " ORDER BY record_keys.uri DESC"
	} else {
		query += " ORDER BY record_keys.uri ASC"
	}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := scanRecordRows(ctx, r.db, query, args...)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return queryRows(rows)
}
