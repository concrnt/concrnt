package record

import (
	"context"
	"strings"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/concrnt/concrnt/internal/domain"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/internal/utils"
)

func (r *Repository) CreateAssociation(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, targetURI string, owner string, author string, schema string, variant *string, unique string, createdAt time.Time) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateAssociation")
	defer span.End()

	stx, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	targetRK, err := readRecordKey(ctx, stx, targetURI)
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	if targetRK == nil {
		err := domain.NotFoundError{Resource: "record key"}
		span.RecordError(err)
		return false, err
	}

	// Duplicates are a silent success like record/ack: the same document
	// re-delivered is caught by INSERT OR IGNORE on the id, the same logical
	// association under a new document id by the unique_key lookup (a unique
	// index violation would otherwise fail the statement).
	_, found, err := queryOne[struct {
		ID string `spanner:"id"`
	}](ctx, stx, spanner.Statement{
		SQL:    "SELECT id FROM associations WHERE unique_key = @unique",
		Params: map[string]any{"unique": unique},
	})
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	if found {
		return false, nil
	}

	affected, err := stx.Update(ctx, spanner.Statement{
		SQL: `INSERT OR IGNORE INTO associations (id, target_uri, owner, author, schema, variant, unique_key, created_at, c_date)
			VALUES (@id, @target_uri, @owner, @author, @schema, @variant, @unique, @created_at, CURRENT_TIMESTAMP())`,
		Params: map[string]any{
			"id": documentID, "target_uri": targetURI, "owner": owner, "author": author, "schema": schema,
			"variant": variant, "unique": unique, "created_at": createdAt,
		},
	})
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	return affected > 0, nil
}

func (r *Repository) DeleteAssociation(ctx context.Context, tx ucrecord.RepositoryTx, documentID string) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteAssociation")
	defer span.End()

	stx, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	affected, err := stx.Update(ctx, spanner.Statement{
		SQL:    "DELETE FROM associations WHERE id = @id",
		Params: map[string]any{"id": documentID},
	})
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	return affected > 0, nil
}

func (r *Repository) GetAssociatedRecords(
	ctx context.Context,
	targetURI, schema, variant, author string,
	since, until *time.Time,
	limit int,
	order string,
) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAssociatedRecords")
	defer span.End()

	params := map[string]any{"target": targetURI}
	where := []string{"a.target_uri = @target"}
	if schema != "" {
		where = append(where, "a.schema = @schema")
		params["schema"] = schema
	}
	if variant != "" {
		where = append(where, "a.variant = @variant")
		params["variant"] = variant
	}
	if author != "" {
		where = append(where, "a.author = @author")
		params["author"] = author
	}
	where = timeRange(where, params, "a.created_at", since, until)

	sql := `SELECT a.id AS document_id, a.owner AS owner, a.created_at AS created_at, c.document AS document, c.proof AS proof
		FROM associations a JOIN commit_logs c ON c.id = a.id WHERE ` + strings.Join(where, " AND ") +
		orderLimit("a.created_at", "a.id", order, limit, params)
	rows, err := queryAll[commitRow](ctx, r.client.Single(), spanner.Statement{SQL: sql, Params: params})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return commitRows(rows)
}

func (r *Repository) ComputeAssociationCounts(ctx context.Context, targetURI string) (map[string]utils.OrderedKVMap[int64], error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ComputeAssociationCounts")
	defer span.End()

	// the earliest association of each variant decides the variant order
	counts, err := queryAll[struct {
		Schema       string    `spanner:"schema"`
		Variant      *string   `spanner:"variant"`
		Count        int64     `spanner:"count"`
		MinCreatedAt time.Time `spanner:"min_created_at"`
	}](ctx, r.client.Single(), spanner.Statement{
		SQL: `SELECT schema, variant, COUNT(*) AS count, MIN(created_at) AS min_created_at FROM associations
			WHERE target_uri = @target GROUP BY schema, variant`,
		Params: map[string]any{"target": targetURI},
	})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	result := make(map[string]utils.OrderedKVMap[int64])
	for _, c := range counts {
		variants, ok := result[c.Schema]
		if !ok {
			variants = make(utils.OrderedKVMap[int64])
			result[c.Schema] = variants
		}
		variant := ""
		if c.Variant != nil {
			variant = *c.Variant
		}
		variants[variant] = utils.OrderedKV[int64]{
			Value: c.Count,
			Order: c.MinCreatedAt.UnixNano(),
		}
	}
	return result, nil
}
