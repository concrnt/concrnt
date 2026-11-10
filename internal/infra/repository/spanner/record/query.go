//go:build contractv1

package record

import (
	"context"
	"strings"
	"time"

	"github.com/concrnt/concrnt"
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

	// the prefix is a literal (CONTRACT.md); STARTS_WITH has no
	// metacharacters, and the scan is a range on the primary key
	params := map[string]any{"prefix": prefix}
	where := []string{"STARTS_WITH(rk.uri, @prefix)"}
	where = recordFilters(where, params, schema, author, since, until)
	sql := joinRecords(false) + "WHERE " + strings.Join(where, " AND ") + orderLimit("r.created_at", "r.id", order, limit, params)

	rows, err := scanRecordRows(ctx, r.client.Single(), sql, params)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return queryRows(rows)
}

func recordFilters(where []string, params map[string]any, schema, author string, since, until *time.Time) []string {
	if schema != "" {
		where = append(where, "r.schema = @schema")
		params["schema"] = schema
	}
	if author != "" {
		where = append(where, "r.author = @author")
		params["author"] = author
	}
	return timeRange(where, params, "r.created_at", since, until)
}

// QueryRecordSubtree returns every live record whose key is base itself
// (includeSelf) or lies under base's path subtree (base + "/..."), ordered by
// URI (byte order) so base leads. Unlike QueryByPrefix's raw prefix match this
// never picks up sibling keys ("item2" for base "item").
func (r *Repository) QueryRecordSubtree(ctx context.Context, base string, includeSelf bool, afterURI string, limit int) ([]concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryRecordSubtree")
	defer span.End()

	params := map[string]any{"base": base, "subtree": base + "/"}
	where := "STARTS_WITH(rk.uri, @subtree)"
	if includeSelf {
		where = "(rk.uri = @base OR " + where + ")"
	}
	if afterURI != "" {
		where += " AND rk.uri > @after"
		params["after"] = afterURI
	}
	sql := joinRecords(false) + "WHERE " + where + " ORDER BY rk.uri ASC"
	if limit > 0 {
		sql += " LIMIT @limit"
		params["limit"] = int64(limit)
	}
	rows, err := scanRecordRows(ctx, r.client.Single(), sql, params)
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

// parentPredicate selects parent's direct children. The owner root
// (cckv://<owner>) has no record_keys row and its top-level keys carry
// parent_uri NULL (ensureParentRecordKeys), so the root is matched by URI
// prefix instead (CIP-5 §3.1).
func parentPredicate(parent string, params map[string]any) (string, error) {
	parsedParent, err := concrnt.ParseCCURI(parent)
	if err != nil {
		return "", err
	}
	if parsedParent.Key == "" {
		params["root"] = "cckv://" + parsedParent.Owner + "/"
		return "rk.parent_uri IS NULL AND STARTS_WITH(rk.uri, @root)", nil
	}
	params["parent"] = parent
	return "rk.parent_uri = @parent", nil
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

	params := map[string]any{}
	predicate, err := parentPredicate(parent, params)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	where := recordFilters([]string{predicate}, params, schema, author, since, until)
	sql := joinRecords(false) + "WHERE " + strings.Join(where, " AND ") + orderLimit("r.created_at", "r.id", order, limit, params)

	rows, err := scanRecordRows(ctx, r.client.Single(), sql, params)
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
// inclusive key bounds, the cursors of §3.3 for this ordering. Spanner
// orders STRING by code point, which is UTF-8 byte order (CONTRACT.md "Key
// order").
func (r *Repository) QueryByParentOrderByKey(
	ctx context.Context,
	parent, schema, author string,
	since, until *string,
	limit int,
	order string,
) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryByParentOrderByKey")
	defer span.End()

	params := map[string]any{}
	predicate, err := parentPredicate(parent, params)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	where := recordFilters([]string{predicate}, params, schema, author, nil, nil)
	if since != nil {
		where = append(where, "rk.uri >= @since")
		params["since"] = *since
	}
	if until != nil {
		where = append(where, "rk.uri <= @until")
		params["until"] = *until
	}
	dir := "ASC"
	if order == "desc" {
		dir = "DESC"
	}
	sql := joinRecords(true) + "WHERE " + strings.Join(where, " AND ") + " ORDER BY rk.uri " + dir
	if limit > 0 {
		sql += " LIMIT @limit"
		params["limit"] = int64(limit)
	}

	rows, err := scanRecordRows(ctx, r.client.Single(), sql, params)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return queryRows(rows)
}
