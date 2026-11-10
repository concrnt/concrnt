//go:build contractv1

package record

import (
	"context"
	"encoding/json"
	"time"

	spannerdb "github.com/concrnt/concrnt/internal/infra/repository/spanner"

	"cloud.google.com/go/spanner"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func (r *Repository) CreateEntity(
	ctx context.Context,
	tx ucrecord.RepositoryTx,
	ccid string,
	alias *string,
	domain string,
	documentID string,
	createdAt time.Time,
) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateEntity")
	defer span.End()

	stx, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return false, err
	}

	// CIP-3 §3.4 accept-if-newer on the document's createdAt: only a strictly
	// newer document replaces the stored one; an equal or older one is a
	// no-op. The read locks the row, so concurrent writers serialise (one
	// aborts and re-runs) and converge on the same winner.
	old, found, err := spannerdb.QueryOne[struct {
		CreatedAt time.Time `spanner:"created_at"`
	}](ctx, stx, spanner.Statement{
		SQL:    "SELECT created_at FROM entities WHERE ccid = @ccid",
		Params: map[string]any{"ccid": ccid},
	})
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	params := map[string]any{"ccid": ccid, "alias": alias, "domain": domain, "document_id": documentID, "created_at": createdAt}
	var affected int64
	if found {
		if !createdAt.After(old.CreatedAt) {
			return false, nil
		}
		affected, err = stx.Update(ctx, spanner.Statement{
			SQL: `UPDATE entities SET alias = @alias, domain = @domain, document_id = @document_id, created_at = @created_at, m_date = CURRENT_TIMESTAMP()
				WHERE ccid = @ccid`,
			Params: params,
		})
	} else {
		affected, err = stx.Update(ctx, spanner.Statement{
			SQL: `INSERT OR IGNORE INTO entities (ccid, alias, domain, tag, document_id, created_at, c_date, m_date)
				VALUES (@ccid, @alias, @domain, '', @document_id, @created_at, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())`,
			Params: params,
		})
	}
	if err != nil {
		span.RecordError(err)
		return false, err
	}
	return affected > 0, nil
}

// MarkCommitLogsGcCandidateByOwner flags every commit log owned by the given
// ccid as a GC candidate. Every commit has exactly one owner (an ack between
// two local users is two commits: the ack owned by the acker and the acked
// owned by the target), so nothing another user holds is touched. Idempotent.
func (r *Repository) MarkCommitLogsGcCandidateByOwner(ctx context.Context, owner string) error {
	ctx, span := tracer.Start(ctx, "Repository.MarkCommitLogsGcCandidateByOwner")
	defer span.End()

	_, err := r.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, spanner.Statement{
			SQL:    "UPDATE commit_logs SET gc_candidate = true WHERE owner = @owner AND NOT gc_candidate",
			Params: map[string]any{"owner": owner},
		})
		return err
	})
	if err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

type entityRow struct {
	ID       string  `spanner:"ccid"`
	Alias    *string `spanner:"alias"`
	Domain   string  `spanner:"domain"`
	Tag      string  `spanner:"tag"`
	Document string  `spanner:"document"`
	Proof    string  `spanner:"proof"`
}

const entitySelect = `SELECT e.ccid AS ccid, e.alias AS alias, e.domain AS domain, e.tag AS tag, c.document AS document, c.proof AS proof
	FROM entities e JOIN commit_logs c ON c.id = e.document_id `

func (r *Repository) getEntity(ctx context.Context, where string, params map[string]any) (*domain.Entity, error) {
	entity, found, err := spannerdb.QueryOne[entityRow](ctx, r.client.Single(), spanner.Statement{SQL: entitySelect + where, Params: params})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, domain.NotFoundError{Resource: "entity"}
	}

	var proof concrnt.Proof
	if err := json.Unmarshal([]byte(entity.Proof), &proof); err != nil {
		return nil, err
	}
	return &domain.Entity{
		ID:             entity.ID,
		Domain:         entity.Domain,
		Alias:          entity.Alias,
		TagString:      entity.Tag,
		SignedDocument: &concrnt.SignedDocument{Document: entity.Document, Proof: proof},
	}, nil
}

func (r *Repository) GetEntityByCCID(ctx context.Context, ccid string) (*domain.Entity, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetEntityByCCID")
	defer span.End()

	entity, err := r.getEntity(ctx, "WHERE e.ccid = @ccid", map[string]any{"ccid": ccid})
	if err != nil {
		span.RecordError(err)
	}
	return entity, err
}

func (r *Repository) GetEntityByAlias(ctx context.Context, alias string) (*domain.Entity, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetEntityByAlias")
	defer span.End()

	entity, err := r.getEntity(ctx, "WHERE e.alias = @alias", map[string]any{"alias": alias})
	if err != nil {
		span.RecordError(err)
	}
	return entity, err
}
