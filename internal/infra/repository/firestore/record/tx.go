//go:build contractv1

package record

import (
	"maps"

	"cloud.google.com/go/firestore"

	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
)

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

// recordTx is the RepositoryTx handle. Reads go to the Firestore transaction
// (they take locks and are validated at commit) with the buffered writes
// laid over them, so a method sees what the earlier methods of the same
// closure wrote; the writes themselves are flushed after the closure
// returns, because the SDK rejects any read that follows a write. Writes to
// the same document are coalesced so a document is written at most once
// per commit.
type recordTx struct {
	ftx   *firestore.Transaction
	ops   map[string]*pendingWrite
	order []string
}

func (*recordTx) IsRepositoryTx() {}

// docState is one document as the transaction sees it: the committed data
// with the pending writes applied.
type docState struct {
	ref    *firestore.DocumentRef
	data   map[string]any
	exists bool
}

func (t *recordTx) pending(ref *firestore.DocumentRef) *pendingWrite {
	return t.ops[ref.Path]
}

// overlay applies a pending write to the committed state of its document.
func overlay(p *pendingWrite, committed map[string]any, exists bool) (map[string]any, bool) {
	if p == nil {
		return committed, exists
	}
	switch p.kind {
	case opDelete:
		return nil, false
	case opSet, opCreate:
		return maps.Clone(p.data), true
	case opUpdate:
		if !exists {
			return nil, false
		}
		data := maps.Clone(committed)
		for _, u := range p.updates {
			if u.Value == firestore.Delete {
				delete(data, u.Path)
			} else {
				data[u.Path] = u.Value
			}
		}
		return data, true
	}
	return committed, exists
}

// read returns one document; a missing document is (nil, false, nil).
func (t *recordTx) read(ref *firestore.DocumentRef) (map[string]any, bool, error) {
	states, err := t.readAll([]*firestore.DocumentRef{ref})
	if err != nil {
		return nil, false, err
	}
	return states[0].data, states[0].exists, nil
}

// readAll reads several documents in one round trip (those with a pending
// set, create or delete are answered from the buffer), in the order given.
func (t *recordTx) readAll(refs []*firestore.DocumentRef) ([]docState, error) {
	states := make([]docState, len(refs))
	var fetch []*firestore.DocumentRef
	var fetchAt []int
	for i, ref := range refs {
		states[i].ref = ref
		if p := t.pending(ref); p != nil && p.kind != opUpdate {
			states[i].data, states[i].exists = overlay(p, nil, false)
			continue
		}
		fetch = append(fetch, ref)
		fetchAt = append(fetchAt, i)
	}
	if len(fetch) > 0 {
		snaps, err := t.ftx.GetAll(fetch)
		if err != nil {
			return nil, err
		}
		for j, snap := range snaps {
			i := fetchAt[j]
			var committed map[string]any
			if snap.Exists() {
				committed = snap.Data()
			}
			states[i].data, states[i].exists = overlay(t.pending(refs[i]), committed, snap.Exists())
		}
	}
	return states, nil
}

// query runs q in the transaction and drops the documents a pending delete
// removed. Documents the closure created or changed are not reflected (the
// committed state is what the query sees); no method needs that.
func (t *recordTx) query(q firestore.Query) ([]*firestore.DocumentSnapshot, error) {
	snaps, err := t.ftx.Documents(q).GetAll()
	if err != nil {
		return nil, fs.QueryErr(err)
	}
	out := snaps[:0]
	for _, snap := range snaps {
		if p := t.pending(snap.Ref); p != nil && p.kind == opDelete {
			continue
		}
		out = append(out, snap)
	}
	return out, nil
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

// update patches top-level fields, folding into a pending Set / Create of
// the same document when there is one.
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
	_, exists, err := t.read(ref)
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
