package record

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func (r *Repository) ApplyAssociation(ctx context.Context, commit ucrecord.AssociationCommit) (ucrecord.ApplyStatus, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ApplyAssociation")
	defer span.End()

	status, err := r.apply(ctx, func(tx *gorm.DB) (ucrecord.ApplyStatus, error) {
		inserted, err := insertCommit(tx, commit.Commit)
		if err != nil {
			return ucrecord.Applied, err
		}
		if !inserted {
			return ucrecord.Duplicate, nil
		}

		// the association references the key row itself, which is what
		// binds it to the key's incarnation: the row goes when the key is
		// removed (taking the association along) and a key recreated later
		// is another row
		var targetRK postgres.RecordKey
		err = tx.Where("uri = ?", commit.Target).Take(&targetRK).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ucrecord.Applied, domain.NotFoundError{Resource: commit.Target}
		}
		if err != nil {
			return ucrecord.Applied, err
		}

		association := postgres.Association{
			TargetID:   targetRK.ID,
			DocumentID: commit.ID,
			Unique:     commit.Unique,

			Owner:     commit.Owner,
			Author:    commit.Author,
			Variant:   commit.Variant,
			Schema:    commit.Schema,
			CreatedAt: commit.CreatedAt,
		}

		// ON CONFLICT DO NOTHING on uni_associations_unique: the same
		// logical association under another document id is Stale
		result := tx.Clauses(clause.OnConflict{
			DoNothing: true,
		}).Create(&association)
		if result.Error != nil {
			return ucrecord.Applied, result.Error
		}
		if result.RowsAffected == 0 {
			return ucrecord.Stale, nil
		}
		return ucrecord.Applied, nil
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

	query := r.db.WithContext(ctx).
		Model(&postgres.Association{}).
		Joins("JOIN record_keys rk ON rk.id = associations.target_id").
		Joins("JOIN commit_logs c ON c.id = associations.document_id").
		Where("rk.uri = ?", targetURI)

	if schema != "" {
		query = query.Where("associations.schema = ?", schema)
	}
	if variant != "" {
		query = query.Where("associations.variant = ?", variant)
	}
	if author != "" {
		query = query.Where("associations.author = ?", author)
	}
	if since != nil {
		query = query.Where("associations.created_at >= ?", *since)
	}
	if until != nil {
		query = query.Where("associations.created_at <= ?", *until)
	}

	if order == "desc" {
		query = query.Order("associations.created_at DESC, associations.document_id DESC")
	} else {
		query = query.Order("associations.created_at ASC, associations.document_id ASC")
	}

	if limit > 0 {
		query = query.Limit(limit)
	}

	var associations []commitRow
	if err := query.Select("associations.document_id AS document_id, associations.owner AS owner, associations.created_at AS created_at, c.document AS document, c.proof AS proof").Scan(&associations).Error; err != nil {
		return nil, err
	}

	return commitRows(associations)
}

// commitRow is a state row (association, ack, acked) joined with its commit
// log; Owner is the namespace its ccfs URI is composed in.
type commitRow struct {
	DocumentID string
	Owner      string
	CreatedAt  time.Time
	Document   string
	Proof      string
}

func commitRows(rows []commitRow) ([]ucrecord.QueryRow, error) {
	out := make([]ucrecord.QueryRow, len(rows))
	for i, row := range rows {
		sd, err := repoutil.CommitDocument(row.Owner, row.DocumentID, row.Document, row.Proof)
		if err != nil {
			return nil, err
		}
		out[i] = ucrecord.QueryRow{
			Row:       sd,
			CreatedAt: row.CreatedAt,
		}
	}
	return out, nil
}

// ComputeAssociationSummary reads a target's associations once: the CTE is
// scanned for the counts per (schema, variant) and again, from the same
// materialised rows, for the distinct authors, which stop one past
// maxAuthors (that one row is what tells an overflow).
func (r *Repository) ComputeAssociationSummary(ctx context.Context, targetURI string, maxAuthors int) (ucrecord.AssociationSummary, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ComputeAssociationSummary")
	defer span.End()

	var rows []struct {
		Kind     int
		Schema   *string
		Variant  *string
		Author   *string
		Count    int64
		Earliest *time.Time
	}
	err := r.db.WithContext(ctx).Raw(`WITH a AS MATERIALIZED (
			SELECT associations.schema, associations.variant, associations.author, associations.created_at
			FROM associations JOIN record_keys rk ON rk.id = associations.target_id
			WHERE rk.uri = ?
		)
		SELECT 0 AS kind, schema, variant, NULL AS author, COUNT(*) AS count, MIN(created_at) AS earliest
			FROM a GROUP BY schema, variant
		UNION ALL
		SELECT 1, NULL, NULL, author, 0, NULL
			FROM (SELECT DISTINCT author FROM a LIMIT ?) d`, targetURI, maxAuthors+1).
		Scan(&rows).Error
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
			variant.Earliest = *row.Earliest
		}
		summary.Variants = append(summary.Variants, variant)
	}
	if len(summary.Authors) > maxAuthors {
		summary.Authors, summary.AuthorsOverflow = nil, true
	}
	return summary, nil
}
