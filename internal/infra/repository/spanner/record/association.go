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

// The association indexes, each starting with the incarnation
// (target_uri, target_gen). Every statement names the one it reads: which
// range a statement scans decides what it costs on a target with many
// associations and, inside a transaction, what it locks, so it is not left
// to the optimizer's statistics.
const (
	// (.., created_at DESC, id DESC): the unfiltered listing.
	indexByCreatedAt = "idx_associations_target_gen_created_at_desc"
	// (.., schema, variant, created_at DESC, id DESC): the listings
	// filtered by schema, and the counts per (schema, variant).
	indexBySchemaVariant = "idx_associations_target_gen_schema_variant_created_at_desc"
	// (.., author, created_at DESC, id DESC): the listings filtered by
	// author, and the distinct authors.
	indexByAuthor = "idx_associations_target_gen_author_created_at_desc"
	// UNIQUE (.., unique_key): the uniqueness check.
	indexByUnique = "uni_associations_target_gen_unique"
)

// currentIncarnation joins associations (a), read through index, to the
// key row (rk) their target has now: a seek of the key row by uri, then of
// the index by that row's (uri, gen). An association whose target_gen is
// not that row's gen belongs to an incarnation that has ended: no read may
// return it, and it stays only until Cleanup has removed it.
func currentIncarnation(index string) string {
	return "record_keys rk JOIN associations@{FORCE_INDEX=" + index + "} a ON a.target_uri = rk.uri AND a.target_gen = rk.gen "
}

// listingIndex is the index a listing with these filters reads. An author
// has few associations on one target, so that filter goes first; schema
// and variant then narrow what the author's range returns.
func listingIndex(schema, author string) string {
	switch {
	case author != "":
		return indexByAuthor
	case schema != "":
		return indexBySchemaVariant
	default:
		return indexByCreatedAt
	}
}

// ApplyAssociation binds the association to the incarnation of the target
// key it finds: it stores the key row's gen. The read locks the key row
// and the uniqueness key within that incarnation, so neither a concurrent
// removal of the key nor a concurrent association with the same Unique can
// commit beside this one. It locks no more than that one entry of the
// unique index: associations with other Unique values on the same target
// commit side by side.
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
				EXISTS(SELECT 1 FROM ` + currentIncarnation(indexByUnique) + `WHERE rk.uri = @target AND a.unique_key = @unique) AS taken`,
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

	rows, err := spannerdb.QueryAll[commitRow](ctx, r.client.Single(), associatedRecordsStatement(targetURI, schema, variant, author, since, until, limit, order))
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return commitRows(rows)
}

// associatedRecordsStatement is the listing of GetAssociatedRecords. In
// the default order (desc) it reads the page it returns, not the target's
// associations: the index ends with (created_at DESC, id DESC) after the
// columns the filters fix. Ascending, or filtered by schema without a
// variant, it reads the whole range the filters leave and sorts it.
func associatedRecordsStatement(targetURI, schema, variant, author string, since, until *time.Time, limit int, order string) spanner.Statement {
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
		FROM ` + currentIncarnation(listingIndex(schema, author)) + `JOIN commit_logs c ON c.id = a.id WHERE ` + strings.Join(where, " AND ") +
		orderLimit("a.created_at", "a.id", order, limit, params)
	return spanner.Statement{SQL: sql, Params: params}
}

// ComputeAssociationSummary reads a target's associations in one statement
// (one snapshot): the counts per (schema, variant), and the distinct
// authors, which stop one past maxAuthors (that one row is what tells an
// overflow). Each half reads the index that has its grouping columns as
// the key after the incarnation and nothing else.
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
				FROM ` + currentIncarnation(indexBySchemaVariant) + `WHERE rk.uri = @target GROUP BY a.schema, a.variant
			UNION ALL
			SELECT 1, NULL, NULL, d.author, 0, NULL
				FROM (SELECT DISTINCT a.author FROM ` + currentIncarnation(indexByAuthor) + `WHERE rk.uri = @target LIMIT @authors) d`,
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
