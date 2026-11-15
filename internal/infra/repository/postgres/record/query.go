package record

import (
	"context"
	"encoding/json"
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

	var rks []models.RecordKey

	query := r.db.WithContext(ctx).
		Model(&models.RecordKey{}).
		Joins("JOIN records r ON r.document_id = record_keys.record_id").
		Joins("JOIN commit_logs c ON c.id = record_keys.record_id").
		Where(`record_keys.uri COLLATE "C" LIKE ? ESCAPE '\'`, likeEscaper.Replace(prefix)+"%")

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

	if err := query.Select(recordKeySelect).Find(&rks).Error; err != nil {
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

	query := r.db.WithContext(ctx).
		Model(&models.RecordKey{}).
		Joins("JOIN records r ON r.document_id = record_keys.record_id").
		Joins("JOIN commit_logs c ON c.id = record_keys.record_id")

	subtreePattern := likeEscaper.Replace(base) + `/%`
	if includeSelf {
		// not the disjunction alone: it hides the LIKE prefix from the planner,
		// which then walks the whole index. base and base/... both sort in
		// [base, base+"0") ('/' < '0')
		query = query.Where(`record_keys.uri COLLATE "C" >= ? AND record_keys.uri COLLATE "C" < ? AND (uri = ? OR record_keys.uri COLLATE "C" LIKE ? ESCAPE '\')`,
			base, base+"0", base, subtreePattern)
	} else {
		query = query.Where(`record_keys.uri COLLATE "C" LIKE ? ESCAPE '\'`, subtreePattern)
	}

	var rks []models.RecordKey
	if err := query.Order(`uri COLLATE "C" ASC`).Select(recordKeySelect).Find(&rks).Error; err != nil {
		span.RecordError(err)
		return nil, err
	}

	sds := make([]concrnt.SignedDocument, 0, len(rks))
	for _, rk := range rks {
		var proof concrnt.Proof
		err := json.Unmarshal([]byte(rk.Record.Document.Proof), &proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		ccfs := concrnt.ComposeCCFSURI(rk.Record.Owner, concrnt.CCFSTypeConcrnt, rk.Record.DocumentID)

		sds = append(sds, concrnt.SignedDocument{
			CCKV:     &rk.URI,
			CCFS:     &ccfs,
			Document: rk.Record.Document.Document,
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

	var rks []models.RecordKey

	query := r.db.WithContext(ctx).
		Model(&models.RecordKey{}).
		Joins("JOIN records r ON r.document_id = record_keys.record_id").
		Joins("JOIN commit_logs c ON c.id = record_keys.record_id")

	// The owner root (cckv://<owner>) has no record_keys row and its top-level
	// keys carry parent_id NULL (getOrCreateParentRecordKey), so the root is
	// matched by URI prefix instead (CIP-5 §3.1).
	parsedParent, err := concrnt.ParseCCURI(parent)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if parsedParent.Key == "" {
		query = query.Where(`parent_id IS NULL AND record_keys.uri COLLATE "C" LIKE ? ESCAPE '\'`, likeEscaper.Replace("cckv://"+parsedParent.Owner)+"/%")
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

	if err := query.Select(recordKeySelect).Find(&rks).Error; err != nil {
		span.RecordError(err)
		return nil, err
	}

	return recordKeysToQueryRows(rks, span)
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

	var rks []models.RecordKey

	query := r.db.WithContext(ctx).
		Model(&models.RecordKey{}).
		Joins("LEFT JOIN records r ON r.document_id = record_keys.record_id").
		Joins("LEFT JOIN commit_logs c ON c.id = record_keys.record_id")

	parsedParent, err := concrnt.ParseCCURI(parent)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if parsedParent.Key == "" {
		query = query.Where(`parent_id IS NULL AND record_keys.uri COLLATE "C" LIKE ? ESCAPE '\'`, likeEscaper.Replace("cckv://"+parsedParent.Owner)+"/%")
	} else {
		query = query.Where("parent_id = (SELECT id FROM record_keys WHERE uri = ?)", parent)
	}

	if schema != "" {
		query = query.Where("r.schema = ?", schema)
	}
	if author != "" {
		query = query.Where("r.author = ?", author)
	}
	// not the database's default collation: it orders keys differently
	// from server to server (CIP-5 orderby=key must not)
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

	if err := query.Select(recordKeySelect).Find(&rks).Error; err != nil {
		span.RecordError(err)
		return nil, err
	}

	return recordKeysToQueryRows(rks, span)
}
