package record

import (
	"context"
	"strings"
	"time"

	spannerdb "github.com/concrnt/concrnt/internal/infra/repository/spanner"

	"cloud.google.com/go/spanner"

	"github.com/concrnt/concrnt/internal/domain"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// currentIncarnation joins associations (a) to the key row (rk) their
// target has now. An association whose target_gen is not that row's gen
// belongs to an incarnation that has ended: no read may return it, and it
// stays only until Cleanup has removed it.
const currentIncarnation = "record_keys rk JOIN associations a ON a.target_uri = rk.uri AND a.target_gen = rk.gen "

// ApplyAssociation binds the association to the incarnation of the target
// key it finds: it stores the key row's gen. The read locks the key row
// and the uniqueness key within that incarnation, so neither a concurrent
// removal of the key nor a concurrent association with the same Unique can
// commit beside this one.
func (r *Repository) ApplyAssociation(ctx context.Context, commit ucrecord.AssociationCommit) (ucrecord.ApplyStatus, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ApplyAssociation")
	defer span.End()

	status, err := r.apply(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) (ucrecord.ApplyStatus, error) {
		read, err := readSet[struct {
			Duplicate bool    `spanner:"duplicate"`
			Gen       *string `spanner:"gen"`
			Taken     bool    `spanner:"taken"`
		}](ctx, tx, spanner.Statement{
			SQL: "SELECT " + duplicateCommit + `,
				(SELECT gen FROM record_keys WHERE uri = @target) AS gen,
				EXISTS(SELECT 1 FROM ` + currentIncarnation + `WHERE rk.uri = @target AND a.unique_key = @unique) AS taken`,
			Params: map[string]any{"id": commit.ID, "target": commit.Target, "unique": commit.Unique},
		})
		if err != nil {
			return ucrecord.Applied, err
		}
		if read.Duplicate {
			return ucrecord.Duplicate, nil
		}
		if read.Gen == nil {
			return ucrecord.Applied, domain.NotFoundError{Resource: "record key"}
		}
		if read.Taken {
			return ucrecord.Stale, nil
		}

		insert, err := insertCommit(commit.Commit, false)
		if err != nil {
			return ucrecord.Applied, err
		}
		return ucrecord.Applied, tx.BufferWrite([]*spanner.Mutation{insert, spanner.Insert("associations",
			[]string{"id", "target_uri", "target_gen", "owner", "author", "schema", "variant", "unique_key", "created_at", "c_date"},
			[]any{commit.ID, commit.Target, *read.Gen, commit.Owner, commit.Author, commit.Schema, commit.Variant, commit.Unique, commit.CreatedAt, time.Now().UTC()})})
	})
	if err != nil {
		span.RecordError(err)
	}
	return status, err
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
	where := []string{"rk.uri = @target"}
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
		FROM ` + currentIncarnation + `JOIN commit_logs c ON c.id = a.id WHERE ` + strings.Join(where, " AND ") +
		orderLimit("a.created_at", "a.id", order, limit, params)
	rows, err := spannerdb.QueryAll[commitRow](ctx, r.client.Single(), spanner.Statement{SQL: sql, Params: params})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return commitRows(rows)
}

// ComputeAssociationSummary reads a target's associations in one statement
// (one snapshot): the counts per (schema, variant), and the distinct
// authors, which stop one past maxAuthors (that one row is what tells an
// overflow). Both halves are served by the (target_uri, target_gen, schema,
// variant, author) index.
func (r *Repository) ComputeAssociationSummary(ctx context.Context, targetURI string, maxAuthors int) (ucrecord.AssociationSummary, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ComputeAssociationSummary")
	defer span.End()

	rows, err := spannerdb.QueryAll[struct {
		Kind     int64      `spanner:"kind"`
		Schema   *string    `spanner:"schema"`
		Variant  *string    `spanner:"variant"`
		Author   *string    `spanner:"author"`
		Count    int64      `spanner:"count"`
		Earliest *time.Time `spanner:"earliest"`
	}](ctx, r.client.Single(), spanner.Statement{
		SQL: `SELECT 0 AS kind, a.schema AS schema, a.variant AS variant, CAST(NULL AS STRING) AS author, COUNT(*) AS count, MIN(a.created_at) AS earliest
				FROM ` + currentIncarnation + `WHERE rk.uri = @target GROUP BY a.schema, a.variant
			UNION ALL
			SELECT 1, NULL, NULL, d.author, 0, NULL
				FROM (SELECT DISTINCT a.author FROM ` + currentIncarnation + `WHERE rk.uri = @target LIMIT @authors) d`,
		Params: map[string]any{"target": targetURI, "authors": int64(maxAuthors) + 1},
	})
	if err != nil {
		span.RecordError(err)
		return ucrecord.AssociationSummary{}, err
	}

	var summary ucrecord.AssociationSummary
	for _, row := range rows {
		if row.Kind == 1 {
			summary.Authors = append(summary.Authors, deref(row.Author))
			continue
		}
		variant := ucrecord.AssociationVariant{Schema: deref(row.Schema), Variant: deref(row.Variant), Count: row.Count}
		if row.Earliest != nil {
			variant.Earliest = row.Earliest.UTC()
		}
		summary.Variants = append(summary.Variants, variant)
	}
	if len(summary.Authors) > maxAuthors {
		summary.Authors, summary.AuthorsOverflow = nil, true
	}
	return summary, nil
}
