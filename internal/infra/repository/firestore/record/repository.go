// Package record is the Firestore implementation of record.Repository
// (CONTRACT.md; the data model is described in the parent package).
//
// Writes: every Apply* is one transaction that reads once, a single batch
// get of documents whose ids follow from the arguments, and then writes. No
// query runs inside a transaction, so a commit locks only the handful of
// documents it names. The client re-runs the closure when the transaction
// aborts on contention.
//
// Reads are plain strong reads outside any transaction, and whatever one
// returns is on the documents its query or get lands on.
//
// Dependents (P4): an association carries the gen of the key incarnation
// it was created on and is live only while the key document carries that
// gen. Removing a key deletes its document alone, which ends the
// incarnation for every read at once; the associations and uniqueness
// markers left behind are removed by Cleanup, a page at a time.
package record

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"cloud.google.com/go/firestore"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

type Repository struct {
	client *firestore.Client
}

func New(client *firestore.Client) ucrecord.Repository {
	return &Repository{client: client}
}

var tracer = otel.Tracer("repository")

func (r *Repository) col(name string) *firestore.CollectionRef {
	return r.client.Collection(name)
}

// txMaxAttempts bounds the closure re-runs on contention (Firestore aborts
// the loser of two overlapping transactions; the SDK retries with backoff).
const txMaxAttempts = 8

// errTxContention marks a transaction that still aborted after every retry.
var errTxContention = errors.New("firestore transaction aborted after retries")

// apply runs fn as one transaction and returns its status. fn is re-run
// from scratch when the transaction aborts on contention, so it must reset
// whatever it reports through captured variables at its start. A status
// other than Applied must come with no write buffered: the transaction then
// commits nothing, which is what Stale and Duplicate mean.
func (r *Repository) apply(ctx context.Context, fn func(tx *firestore.Transaction) (ucrecord.ApplyStatus, error)) (ucrecord.ApplyStatus, error) {
	start := time.Now()
	attempts := 0
	result := ucrecord.Applied
	err := r.client.RunTransaction(ctx, func(_ context.Context, tx *firestore.Transaction) error {
		attempts++
		var err error
		result, err = fn(tx)
		return err
	}, firestore.MaxAttempts(txMaxAttempts))

	fs.ObserveTx(start, attempts, err)
	if err != nil && status.Code(err) == codes.Aborted {
		err = errors.Join(errTxContention, err)
	}
	return result, err
}

// readSet is the documents one operation reads, each once, in one batch
// get: inside a transaction that is its whole read phase.
type readSet struct {
	refs  []*firestore.DocumentRef
	index map[string]int
	snaps []*firestore.DocumentSnapshot
}

func newReadSet(refs ...*firestore.DocumentRef) *readSet {
	s := &readSet{index: map[string]int{}}
	for _, ref := range refs {
		s.add(ref)
	}
	return s
}

func (s *readSet) add(ref *firestore.DocumentRef) {
	if _, ok := s.index[ref.Path]; ok {
		return
	}
	s.index[ref.Path] = len(s.refs)
	s.refs = append(s.refs, ref)
}

// loadTx reads the set inside tx (and locks it until the commit).
func (s *readSet) loadTx(tx *firestore.Transaction) error {
	if len(s.refs) == 0 {
		return nil
	}
	snaps, err := tx.GetAll(s.refs)
	s.snaps = snaps
	return err
}

// load reads the set outside a transaction.
func (s *readSet) load(ctx context.Context, client *firestore.Client) error {
	if len(s.refs) == 0 {
		return nil
	}
	snaps, err := client.GetAll(ctx, s.refs)
	s.snaps = snaps
	return err
}

// snap returns the snapshot of ref, nil when the document does not exist
// (or was not part of the set).
func (s *readSet) snap(ref *firestore.DocumentRef) *firestore.DocumentSnapshot {
	i, ok := s.index[ref.Path]
	if !ok || i >= len(s.snaps) || s.snaps[i] == nil || !s.snaps[i].Exists() {
		return nil
	}
	return s.snaps[i]
}

// data returns the fields of ref; false when the document does not exist.
func (s *readSet) data(ref *firestore.DocumentRef) (map[string]any, bool) {
	snap := s.snap(ref)
	if snap == nil {
		return nil, false
	}
	return snap.Data(), true
}

// newCommit is the commit log entry of commit, anchored to what it stores.
func newCommit(commit ucrecord.Commit, anchor, anchorID string) (commitDoc, error) {
	proof, err := json.Marshal(commit.Proof)
	if err != nil {
		return commitDoc{}, err
	}
	return commitDoc{
		IP:       commit.IP,
		Document: commit.Document,
		Proof:    string(proof),
		Owner:    commit.Owner,
		Anchor:   anchor,
		AnchorID: anchorID,
	}, nil
}

var flagGc = []firestore.Update{{Path: "gcCandidate", Value: true}}
