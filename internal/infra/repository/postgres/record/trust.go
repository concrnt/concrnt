package record

import (
	"context"

	"github.com/lib/pq"

	uctrust "github.com/concrnt/concrnt/internal/usecase/trust"
)

var _ uctrust.Repository = (*Repository)(nil)

// GetTrustSeeds lists the entities marked as trust seeds.
func (r *Repository) GetTrustSeeds(ctx context.Context) ([]string, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetTrustSeeds")
	defer span.End()

	var seeds []string
	err := r.db.WithContext(ctx).Model(&Entity{}).Where("is_trust_seed").Pluck("id", &seeds).Error
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return seeds, nil
}

// SetTrustSeed marks or unmarks an entity as a trust seed, and reports
// whether this server holds that entity.
func (r *Repository) SetTrustSeed(ctx context.Context, ccid string, seed bool) (bool, error) {
	result := r.db.WithContext(ctx).Model(&Entity{}).Where("id = ?", ccid).Update("is_trust_seed", seed)
	return result.RowsAffected > 0, result.Error
}

// GetLocalAckEdges lists the valid acknowledgements made by entities of
// localDomain, each with the domain its target lives on. schemas, when not
// empty, restricts them to those schemas. Acknowledgements whose target
// entity this server does not hold are left out.
func (r *Repository) GetLocalAckEdges(ctx context.Context, localDomain string, schemas []string) ([]uctrust.AckEdge, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetLocalAckEdges")
	defer span.End()

	query := r.db.WithContext(ctx).
		Table("acks").
		Select(`acks."from" AS "from", acks."to" AS "to", target.domain AS to_domain`).
		Joins(`JOIN entities AS source ON source.id = acks."from"`).
		Joins(`JOIN entities AS target ON target.id = acks."to"`).
		Where("acks.valid = ?", true).
		Where("source.domain = ?", localDomain)
	if len(schemas) > 0 {
		query = query.Where("acks.schema IN ?", schemas)
	}

	var edges []uctrust.AckEdge
	if err := query.Scan(&edges).Error; err != nil {
		span.RecordError(err)
		return nil, err
	}
	return edges, nil
}

// SaveEntityTrust stores a trust computation's result: every entity in scores
// gets its score, and every other entity 0. Only rows whose score changed are
// written.
func (r *Repository) SaveEntityTrust(ctx context.Context, scores map[string]float64) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.SaveEntityTrust")
	defer span.End()

	// nobody holds trust (no seeds): clearing needs no join, and with
	// nothing to clear it reads only the partial index
	if len(scores) == 0 {
		err := r.db.WithContext(ctx).Exec("UPDATE entities SET trust = 0 WHERE trust > 0").Error
		if err != nil {
			span.RecordError(err)
		}
		return err
	}

	ids := make(pq.StringArray, 0, len(scores))
	values := make(pq.Float64Array, 0, len(scores))
	for id, score := range scores {
		ids = append(ids, id)
		values = append(values, score)
	}

	err := r.db.WithContext(ctx).Exec(`
		UPDATE entities
		SET trust = COALESCE(scored.trust, 0)
		FROM entities AS current
		LEFT JOIN unnest(?::text[], ?::double precision[]) AS scored(id, trust) ON scored.id = current.id
		WHERE entities.id = current.id
		  AND entities.trust <> COALESCE(scored.trust, 0)
	`, ids, values).Error
	if err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

// GetDomainTrust adds up the stored trust of each domain's entities. Domains
// without trust are left out.
func (r *Repository) GetDomainTrust(ctx context.Context) (map[string]float64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetDomainTrust")
	defer span.End()

	var rows []struct {
		Domain string
		Trust  float64
	}
	err := r.db.WithContext(ctx).
		Model(&Entity{}).
		Select("domain, SUM(trust) AS trust").
		Where("trust > 0").
		Group("domain").
		Scan(&rows).Error
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	domains := make(map[string]float64, len(rows))
	for _, row := range rows {
		domains[row.Domain] = row.Trust
	}
	return domains, nil
}
