package firestore

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/concrnt/concrnt/internal/usecase/record"
)

// txMaxAttempts bounds the closure re-runs on contention (Firestore aborts the
// loser of two overlapping transactions; the SDK retries with backoff).
const txMaxAttempts = 8

type opKind int

const (
	opSet opKind = iota
	opCreate
	opUpdate
	opDelete
)

type pendingWrite struct {
	ref     *firestore.DocumentRef
	kind    opKind
	data    map[string]any
	updates []firestore.Update
}

// recordTx is the RepositoryTx handle. Reads go straight to the Firestore
// transaction (they take locks and are validated at commit); writes are
// buffered and flushed after the usecase closure returns, because the SDK
// rejects any read that follows a write. Writes to the same document are
// coalesced so a document is written at most once per commit.
type recordTx struct {
	c     *firestore.Client
	ftx   *firestore.Transaction
	ops   map[string]*pendingWrite
	order []string
}

func (*recordTx) IsRepositoryTx() {}

func newRecordTx(c *firestore.Client, ftx *firestore.Transaction) *recordTx {
	return &recordTx{c: c, ftx: ftx, ops: map[string]*pendingWrite{}}
}

func getRecordTx(tx record.RepositoryTx) (*recordTx, error) {
	t, ok := tx.(*recordTx)
	if !ok || t == nil || t.ftx == nil {
		return nil, errInvalidTx
	}
	return t, nil
}

// get reads one document; a missing document is (nil, false, nil).
func (t *recordTx) get(ref *firestore.DocumentRef) (*firestore.DocumentSnapshot, bool, error) {
	snap, err := t.ftx.Get(ref)
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return snap, snap.Exists(), nil
}

// getAll reads several documents in one round trip; missing ones come back
// with Exists() == false.
func (t *recordTx) getAll(refs []*firestore.DocumentRef) ([]*firestore.DocumentSnapshot, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	return t.ftx.GetAll(refs)
}

func (t *recordTx) documents(q firestore.Query) *firestore.DocumentIterator {
	return t.ftx.Documents(q)
}

func (t *recordTx) pending(ref *firestore.DocumentRef) *pendingWrite {
	return t.ops[ref.Path]
}

func (t *recordTx) put(p *pendingWrite) {
	if _, exists := t.ops[p.ref.Path]; !exists {
		t.order = append(t.order, p.ref.Path)
	}
	t.ops[p.ref.Path] = p
}

// set replaces the document (and any pending write to it).
func (t *recordTx) set(ref *firestore.DocumentRef, data map[string]any) {
	t.put(&pendingWrite{ref: ref, kind: opSet, data: data})
}

// create writes a document that must not exist yet. Callers read first, so
// the precondition only fires on a genuine concurrent creation, which the
// serializable validation would have caught anyway.
func (t *recordTx) create(ref *firestore.DocumentRef, data map[string]any) {
	if p := t.pending(ref); p != nil && p.kind == opDelete {
		t.put(&pendingWrite{ref: ref, kind: opSet, data: data})
		return
	}
	t.put(&pendingWrite{ref: ref, kind: opCreate, data: data})
}

// update patches top-level fields, folding into a pending Set/Create of the
// same document when there is one.
func (t *recordTx) update(ref *firestore.DocumentRef, updates []firestore.Update) {
	if p := t.pending(ref); p != nil {
		switch p.kind {
		case opSet, opCreate:
			for _, u := range updates {
				if u.Value == firestore.Delete {
					delete(p.data, u.Path)
				} else {
					p.data[u.Path] = u.Value
				}
			}
			return
		case opUpdate:
			p.updates = append(p.updates, updates...)
			return
		case opDelete:
			// updating a deleted document is a no-op, like UPDATE ... WHERE
			// matching zero rows
			return
		}
	}
	t.put(&pendingWrite{ref: ref, kind: opUpdate, updates: updates})
}

// updateIfExists is UPDATE ... WHERE id = ? semantics: a missing document is
// silently skipped (a bare Update would fail the whole transaction).
func (t *recordTx) updateIfExists(ref *firestore.DocumentRef, updates []firestore.Update) error {
	if p := t.pending(ref); p != nil {
		t.update(ref, updates)
		return nil
	}
	_, exists, err := t.get(ref)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	t.update(ref, updates)
	return nil
}

func (t *recordTx) del(ref *firestore.DocumentRef) {
	t.put(&pendingWrite{ref: ref, kind: opDelete})
}

// flush hands the buffered writes to the Firestore transaction in order.
func (t *recordTx) flush() error {
	for _, path := range t.order {
		p := t.ops[path]
		var err error
		switch p.kind {
		case opSet:
			err = t.ftx.Set(p.ref, p.data)
		case opCreate:
			err = t.ftx.Create(p.ref, p.data)
		case opUpdate:
			err = t.ftx.Update(p.ref, p.updates)
		case opDelete:
			err = t.ftx.Delete(p.ref)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// RunInTx runs fn inside a Firestore transaction. fn is re-run when the
// transaction aborts on contention; a non-nil error from fn rolls back and is
// returned unchanged.
func (r *RecordRepository) RunInTx(ctx context.Context, fn func(tx record.RepositoryTx) error) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.RunInTx")
	defer span.End()

	start := time.Now()
	attempts := 0
	err := r.client.RunTransaction(ctx, func(_ context.Context, ftx *firestore.Transaction) error {
		attempts++
		tx := newRecordTx(r.client, ftx)
		if err := fn(tx); err != nil {
			return err
		}
		return tx.flush()
	}, firestore.MaxAttempts(txMaxAttempts))

	observeTx(start, attempts, err)
	if err != nil && status.Code(err) == codes.Aborted {
		err = errors.Join(errTxContention, err)
	}
	return err
}

// errTxContention marks a transaction that still aborted after every retry.
var errTxContention = errors.New("firestore transaction aborted after retries")
