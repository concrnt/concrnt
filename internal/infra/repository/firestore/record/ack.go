//go:build contractv1

package record

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"

	"github.com/concrnt/concrnt"
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

// upsertAck is the CIP-10 §4 state move: only a document with a strictly
// newer createdAt may move the (from, to, schema) state; an equal or older
// one is a no-op (false). The key is createdAt alone, never the document id,
// so the acker's and the associate owner's servers, which hold different
// documents with the same createdAt, reach the same decision. The state doc
// is keyed by the triple, so a move is one write.
func (r *Repository) upsertAck(ctx context.Context, tx ucrecord.RepositoryTx, side ackSide, documentID, from, to, schema string, createdAt time.Time, valid bool) (bool, error) {
	t, err := getRecordTx(tx)
	if err != nil {
		return false, err
	}

	ref := r.col(side.collection).Doc(ackID(from, to, schema))
	m, exists, err := t.read(ref)
	if err != nil {
		return false, err
	}
	if exists && !createdAt.After(ackFromMap(m).CreatedAt) {
		return false, nil
	}

	t.set(ref, ackDoc{
		From:       from,
		To:         to,
		Schema:     schema,
		DocumentID: documentID,
		Valid:      valid,
		CreatedAt:  createdAt,
	}.toMap())
	return true, nil
}

func (r *Repository) processAck(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time, valid bool) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.processAck")
	defer span.End()

	moved, err := r.upsertAck(ctx, tx, ackerSide, documentID, from, to, schema, createdAt, valid)
	if err != nil {
		span.RecordError(err)
	}
	return moved, err
}

func (r *Repository) Acknowledge(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAck(ctx, tx, documentID, from, to, schema, createdAt, true)
}

func (r *Repository) UnAcknowledge(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAck(ctx, tx, documentID, from, to, schema, createdAt, false)
}

func (r *Repository) processAcked(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time, valid bool) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.processAcked")
	defer span.End()

	moved, err := r.upsertAck(ctx, tx, targetSide, documentID, from, to, schema, createdAt, valid)
	if err != nil {
		span.RecordError(err)
	}
	return moved, err
}

func (r *Repository) Acknowledged(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAcked(ctx, tx, documentID, from, to, schema, createdAt, true)
}

func (r *Repository) UnAcknowledged(ctx context.Context, tx ucrecord.RepositoryTx, documentID string, from string, to string, schema string, createdAt time.Time) (bool, error) {
	return r.processAcked(ctx, tx, documentID, from, to, schema, createdAt, false)
}

// ackQuery is the shared filter of the list and count queries of one side.
func (r *Repository) ackQuery(side ackSide, ccid, schema string) firestore.Query {
	q := r.col(side.collection).Where(side.field, "==", ccid).Where("valid", "==", true)
	if schema != "" {
		q = q.Where("schema", "==", schema)
	}
	return q
}

// ackRecords lists the valid states of one side; CIP-3 §3.4: the ccfs
// identity of an ack is held by the acker's server, of an acked by the
// associate owner's.
func (r *Repository) ackRecords(ctx context.Context, side ackSide, ccid, schema string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	q := applyWindow(r.ackQuery(side, ccid, schema), "createdAt", "documentID", since, until, limit, order)
	snaps, err := q.Documents(ctx).GetAll()
	if err != nil {
		return nil, fs.QueryErr(err)
	}

	acks := make([]ackDoc, 0, len(snaps))
	ids := make([]string, 0, len(snaps))
	for _, snap := range snaps {
		a := ackFromMap(snap.Data())
		acks = append(acks, a)
		ids = append(ids, a.DocumentID)
	}
	commits, err := r.getCommits(ctx, ids)
	if err != nil {
		return nil, err
	}

	rows := make([]ucrecord.QueryRow, 0, len(acks))
	for _, a := range acks {
		commit, ok := commits[a.DocumentID]
		if !ok {
			continue
		}
		proof, err := commit.proof()
		if err != nil {
			return nil, err
		}
		owner := a.From
		if side == targetSide {
			owner = a.To
		}
		ccfs := concrnt.ComposeCCFSURI(owner, concrnt.CCFSTypeConcrnt, a.DocumentID)
		rows = append(rows, ucrecord.QueryRow{
			Row:       concrnt.SignedDocument{CCFS: &ccfs, Document: commit.Document, Proof: proof},
			CreatedAt: a.CreatedAt,
		})
	}
	return rows, nil
}

func (r *Repository) GetAcknowledgeRecords(ctx context.Context, from, to, schema string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAcknowledgeRecords")
	defer span.End()

	var rows []ucrecord.QueryRow
	var err error
	switch {
	case from != "":
		rows, err = r.ackRecords(ctx, ackerSide, from, schema, since, until, limit, order)
	case to != "":
		rows, err = r.ackRecords(ctx, targetSide, to, schema, since, until, limit, order)
	default:
		err = errors.New("either 'from' or 'to' must be specified")
	}
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return rows, nil
}

func (r *Repository) ComputeAcknowledgeCounts(ctx context.Context, from, to, schema string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ComputeAcknowledgeCounts")
	defer span.End()

	var q firestore.Query
	switch {
	case from != "":
		q = r.ackQuery(ackerSide, from, schema)
	case to != "":
		q = r.ackQuery(targetSide, to, schema)
	default:
		err := errors.New("either 'from' or 'to' must be specified")
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
