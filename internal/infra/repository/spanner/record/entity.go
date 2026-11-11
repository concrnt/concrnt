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

// ApplyEntity is CIP-3 §3.4 accept-if-newer on the document's createdAt:
// only a strictly newer document replaces the stored one, an equal or
// older one is Stale. The read locks the entity row (or its absence), so
// concurrent writers serialise and converge on the same winner.
func (r *Repository) ApplyEntity(ctx context.Context, commit ucrecord.EntityCommit) (ucrecord.ApplyStatus, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ApplyEntity")
	defer span.End()

	status, err := r.apply(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) (ucrecord.ApplyStatus, error) {
		read, err := readSet[struct {
			Duplicate bool       `spanner:"duplicate"`
			CreatedAt *time.Time `spanner:"created_at"`
		}](ctx, tx, spanner.Statement{
			SQL:    "SELECT " + duplicateCommit + ", (SELECT created_at FROM entities WHERE ccid = @ccid) AS created_at",
			Params: map[string]any{"id": commit.ID, "ccid": commit.CCID},
		})
		if err != nil {
			return ucrecord.Applied, err
		}
		if read.Duplicate {
			return ucrecord.Duplicate, nil
		}
		if read.CreatedAt != nil && !commit.CreatedAt.After(*read.CreatedAt) {
			return ucrecord.Stale, nil
		}

		insert, err := insertCommit(commit.Commit, false)
		if err != nil {
			return ucrecord.Applied, err
		}
		now := time.Now().UTC()
		entity := spanner.Insert("entities",
			[]string{"ccid", "alias", "domain", "tag", "document_id", "created_at", "c_date", "m_date"},
			[]any{commit.CCID, commit.Alias, commit.Domain, "", commit.ID, commit.CreatedAt, now, now})
		if read.CreatedAt != nil {
			// tag and c_date belong to the entity, not to the document that
			// currently registers it
			entity = spanner.Update("entities",
				[]string{"ccid", "alias", "domain", "document_id", "created_at", "m_date"},
				[]any{commit.CCID, commit.Alias, commit.Domain, commit.ID, commit.CreatedAt, now})
		}
		return ucrecord.Applied, tx.BufferWrite([]*spanner.Mutation{insert, entity})
	})
	if err != nil {
		span.RecordError(err)
	}
	return status, err
}

// MarkCommitLogsGcCandidateByOwner flags every commit log owned by the given
// ccid as a GC candidate. Every commit has exactly one owner (an ack between
// two local users is two commits: the ack owned by the acker and the acked
// owned by the target), so nothing another user holds is touched. The
// statement runs as partitioned DML: an owner's log has no upper bound and
// one transaction over it would meet the commit's mutation limit, while the
// partitions commit separately. That is not atomic, which the contract does
// not ask for, and the statement is idempotent as partitioned DML must be.
func (r *Repository) MarkCommitLogsGcCandidateByOwner(ctx context.Context, owner string) error {
	ctx, span := tracer.Start(ctx, "Repository.MarkCommitLogsGcCandidateByOwner")
	defer span.End()

	_, err := r.client.PartitionedUpdate(ctx, spanner.Statement{
		SQL:    "UPDATE commit_logs SET gc_candidate = true WHERE owner = @owner AND NOT gc_candidate",
		Params: map[string]any{"owner": owner},
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
