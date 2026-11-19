package record

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

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

	query := gorm.G[RecordKey](r.db).
		Joins(clause.LeftJoin.Association("Document"), nil).
		Where("record_keys.record_id IS NOT NULL").
		Where("record_keys.uri LIKE ?", prefix+"%")

	if schema != "" {
		query = query.Where("record_keys.schema = ?", schema)
	}
	if author != "" {
		query = query.Where("record_keys.author = ?", author)
	}
	if since != nil {
		query = query.Where("record_keys.record_created_at >= ?", *since)
	}
	if until != nil {
		query = query.Where("record_keys.record_created_at <= ?", *until)
	}

	if order == "desc" {
		query = query.Order("record_keys.record_created_at DESC, record_keys.record_id DESC")
	} else {
		query = query.Order("record_keys.record_created_at ASC, record_keys.record_id ASC")
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	rks, err := query.Find(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	return recordKeysToQueryRows(rks, span)
}

// likeEscaper escapes LIKE metacharacters so a key containing '%' or '_'
// can't over-match when embedded in a pattern.
var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

// QueryRecordSubtree returns every live record whose key is base itself
// (includeSelf) or lies under base's path subtree (base + "/..."), ordered by
// URI so base leads. Unlike QueryByPrefix's raw prefix match this never picks
// up sibling keys ("item2" for base "item"), and metacharacters in base are
// escaped — the result is exactly the set a range delete may remove.
func (r *Repository) QueryRecordSubtree(ctx context.Context, base string, includeSelf bool) ([]concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryRecordSubtree")
	defer span.End()

	query := gorm.G[RecordKey](r.db).
		Joins(clause.LeftJoin.Association("Document"), nil).
		Where("record_keys.record_id IS NOT NULL")

	subtreePattern := likeEscaper.Replace(base) + `/%`
	if includeSelf {
		query = query.Where(`record_keys.uri = ? OR record_keys.uri LIKE ? ESCAPE '\'`, base, subtreePattern)
	} else {
		query = query.Where(`record_keys.uri LIKE ? ESCAPE '\'`, subtreePattern)
	}

	rks, err := query.Order("record_keys.uri ASC").Find(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	sds := make([]concrnt.SignedDocument, 0, len(rks))
	for _, rk := range rks {
		var proof concrnt.Proof
		err := json.Unmarshal([]byte(rk.Document.Proof), &proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		ccfs := concrnt.ComposeCCFSURI(rk.Owner, concrnt.CCFSTypeConcrnt, *rk.RecordID)

		sds = append(sds, concrnt.SignedDocument{
			CCKV:     &rk.URI,
			CCFS:     &ccfs,
			Document: rk.Document.Document,
			Proof:    proof,
		})
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

	// record_created_at IS NOT NULL keeps placeholders out and matches the
	// predicate of the partial idx_record_keys_parent_created_at_record_id,
	// which serves the ordering, so the limit applies before the commit log
	// join.
	query := gorm.G[RecordKey](r.db).
		Joins(clause.LeftJoin.Association("Document"), nil).
		Where("record_keys.record_id IS NOT NULL AND record_keys.record_created_at IS NOT NULL")

	// The owner root (cckv://<owner>) has no record_keys row and its top-level
	// keys carry parent_id NULL (getOrCreateParentRecordKey), so the root is
	// matched by URI prefix instead (CIP-5 §3.1).
	parsedParent, err := concrnt.ParseCCURI(parent)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if parsedParent.Key == "" {
		query = query.Where(`record_keys.parent_id IS NULL AND record_keys.uri LIKE ? ESCAPE '\'`, likeEscaper.Replace("cckv://"+parsedParent.Owner)+"/%")
	} else {
		query = query.Where("record_keys.parent_id = (SELECT id FROM record_keys WHERE uri = ?)", parent)
	}

	if schema != "" {
		query = query.Where("record_keys.schema = ?", schema)
	}
	if author != "" {
		query = query.Where("record_keys.author = ?", author)
	}
	if since != nil {
		query = query.Where("record_keys.record_created_at >= ?", *since)
	}
	if until != nil {
		query = query.Where("record_keys.record_created_at <= ?", *until)
	}

	if order == "desc" {
		query = query.Order("record_keys.record_created_at DESC, record_keys.record_id DESC")
	} else {
		query = query.Order("record_keys.record_created_at ASC, record_keys.record_id ASC")
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	rks, err := query.Find(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	return recordKeysToQueryRows(rks, span)
}

// QueryByParentOrderByKey lists parent's direct children ordered by their
// cckv (CIP-5 orderby=key). Placeholder keys (record_id NULL; the commit log
// join is a LEFT JOIN) come back as key-only rows; a schema or author filter
// compares record columns and so drops them. since/until are
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

	query := gorm.G[RecordKey](r.db).
		Joins(clause.LeftJoin.Association("Document"), nil)

	parsedParent, err := concrnt.ParseCCURI(parent)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if parsedParent.Key == "" {
		query = query.Where(`record_keys.parent_id IS NULL AND record_keys.uri LIKE ? ESCAPE '\'`, likeEscaper.Replace("cckv://"+parsedParent.Owner)+"/%")
	} else {
		query = query.Where("record_keys.parent_id = (SELECT id FROM record_keys WHERE uri = ?)", parent)
	}

	if schema != "" {
		query = query.Where("record_keys.schema = ?", schema)
	}
	if author != "" {
		query = query.Where("record_keys.author = ?", author)
	}
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

	rks, err := query.Find(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	return recordKeysToQueryRows(rks, span)
}
