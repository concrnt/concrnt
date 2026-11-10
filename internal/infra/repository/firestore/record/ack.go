package record

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"

	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// ackSide names the two sides of an ack relationship: the acker's documents
// (acks, listed and owned by from) and the target's (ackeds, by to).
type ackSide struct {
	collection string
	field      string // the field the listing filters on and the ccfs namespace is composed from
}

var (
	ackerSide  = ackSide{collection: fs.ColAcks, field: "from"}
	targetSide = ackSide{collection: fs.ColAckeds, field: "to"}
)

func sideOf(side ucrecord.AckSide) ackSide {
	if side == ucrecord.TargetSide {
		return targetSide
	}
	return ackerSide
}

// ApplyAck is the CIP-10 §4 state move: only a document with a strictly
// newer createdAt may move the (from, to, schema) state; an equal or older
// one is Stale. The key is createdAt alone, never the document id, so the
// acker's and the associate owner's servers, which hold different documents
// with the same createdAt, reach the same decision. The state document is
// keyed by the triple, so the read set is the commit and that one document,
// and a move is one write.
func (r *Repository) ApplyAck(ctx context.Context, commit ucrecord.AckCommit) (ucrecord.AckOutcome, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ApplyAck")
	defer span.End()

	side := sideOf(commit.Side)
	stateID := ackID(commit.From, commit.To, commit.Schema)
	entry, err := newCommit(commit.Commit, side.collection, stateID)
	if err != nil {
		span.RecordError(err)
		return ucrecord.AckOutcome{}, err
	}
	commitRef := r.col(fs.ColCommits).Doc(commit.ID)
	stateRef := r.col(side.collection).Doc(stateID)

	var outcome ucrecord.AckOutcome
	status, err := r.apply(ctx, func(tx *firestore.Transaction) (ucrecord.ApplyStatus, error) {
		outcome = ucrecord.AckOutcome{}

		reads := newReadSet(commitRef, stateRef)
		if err := reads.loadTx(tx); err != nil {
			return ucrecord.Applied, err
		}
		if _, exists := reads.data(commitRef); exists {
			return ucrecord.Duplicate, nil
		}
		if m, exists := reads.data(stateRef); exists {
			state := ackFromMap(m)
			outcome.WasValid = state.Valid
			if !commit.CreatedAt.After(state.CreatedAt) {
				outcome.IsValid = state.Valid
				return ucrecord.Stale, nil
			}
		}

		if err := tx.Create(commitRef, entry.toMap()); err != nil {
			return ucrecord.Applied, err
		}
		outcome.IsValid = commit.Valid
		return ucrecord.Applied, tx.Set(stateRef, ackDoc{
			From:       commit.From,
			To:         commit.To,
			Schema:     commit.Schema,
			DocumentID: commit.ID,
			Valid:      commit.Valid,
			CreatedAt:  commit.CreatedAt,
			Document:   entry.Document,
			Proof:      entry.Proof,
		}.toMap())
	})
	if err != nil {
		span.RecordError(err)
		return ucrecord.AckOutcome{}, err
	}
	if status == ucrecord.Duplicate {
		return ucrecord.AckOutcome{Status: status}, nil
	}
	outcome.Status = status
	return outcome, nil
}

// ackQuery is the shared filter of the list and count queries of one side.
func (r *Repository) ackQuery(side ackSide, ccid, schema string) firestore.Query {
	q := r.col(side.collection).Where(side.field, "==", ccid).Where("valid", "==", true)
	if schema != "" {
		q = q.Where("schema", "==", schema)
	}
	return q
}

// sideQuery picks the side a from / to pair asks for: exactly one is set.
func (r *Repository) sideQuery(from, to, schema string) (ackSide, firestore.Query, error) {
	switch {
	case from != "":
		return ackerSide, r.ackQuery(ackerSide, from, schema), nil
	case to != "":
		return targetSide, r.ackQuery(targetSide, to, schema), nil
	default:
		return ackSide{}, firestore.Query{}, errors.New("either 'from' or 'to' must be specified")
	}
}

// GetAcknowledgeRecords lists the valid states of one side, each carrying
// the document it is anchored to. CIP-3 §3.4: the ccfs identity of an ack
// is held by the acker's server, of an acked by the associate owner's.
func (r *Repository) GetAcknowledgeRecords(ctx context.Context, from, to, schema string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAcknowledgeRecords")
	defer span.End()

	side, q, err := r.sideQuery(from, to, schema)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	snaps, err := applyWindow(q, "createdAt", "documentID", since, until, limit, order).Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, fs.QueryErr(err)
	}

	rows := make([]ucrecord.QueryRow, 0, len(snaps))
	for _, snap := range snaps {
		a := ackFromMap(snap.Data())
		owner := a.From
		if side == targetSide {
			owner = a.To
		}
		row, err := commitRow(owner, a.DocumentID, a.Document, a.Proof, a.CreatedAt)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// ComputeAcknowledgeCounts scans the valid states of one side projected to
// the schema and groups them here (Firestore has no GROUP BY); only the
// aggregate layer calls it (C2).
func (r *Repository) ComputeAcknowledgeCounts(ctx context.Context, from, to, schema string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ComputeAcknowledgeCounts")
	defer span.End()

	_, q, err := r.sideQuery(from, to, schema)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	counts := make(map[string]int64)
	it := q.Select("schema").Documents(ctx)
	defer it.Stop()
	for {
		snap, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			span.RecordError(err)
			return nil, fs.QueryErr(err)
		}
		counts[str(snap.Data(), "schema")]++
	}
	return counts, nil
}
