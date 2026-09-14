// Package repotest is the backend-agnostic contract test suite for the
// repository interfaces. A backend (Postgres, Firestore, ...) wires its
// repositories plus an Inspector into a Backend and runs the Run*Suite
// functions from its own test package; every suite exercises the store only
// through the usecase repository interfaces and reads white-box state through
// the Inspector, so the assertions are identical across backends.
package repotest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/usecase/chunkline"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/internal/usecase/residence"
)

// Backend is one backend under test.
type Backend struct {
	Record    record.Repository
	Residence residence.Repository
	Chunkline chunkline.Repository
	Inspect   Inspector
}

// Inspector exposes the white-box state the suites assert on; each backend
// implements it over its own store (Postgres: GORM queries). Every lookup
// returns nil, nil when the state is absent.
type Inspector interface {
	// Commit returns the commit log stored under id.
	Commit(ctx context.Context, id string) (*CommitState, error)
	// Record returns the record payload stored for documentID (nil once the
	// record has been forgotten or deleted while its commit log stays).
	Record(ctx context.Context, documentID string) (*RecordState, error)
	// RecordKey returns the key state for uri, placeholder parents included
	// (a placeholder has RecordID == nil).
	RecordKey(ctx context.Context, uri string) (*KeyState, error)
	// Entity returns the entity registered under ccid.
	Entity(ctx context.Context, ccid string) (*EntityState, error)
	// Association returns the association stored for documentID.
	Association(ctx context.Context, documentID string) (*AssociationState, error)
	// AssociationCountByUnique counts the associations sharing unique.
	AssociationCountByUnique(ctx context.Context, unique string) (int64, error)
	// Ack returns the acker-side state of the (from, to, schema) relationship.
	Ack(ctx context.Context, from, to, schema string) (*AckState, error)
	// Acked returns the target-side state of the (from, to, schema) relationship.
	Acked(ctx context.Context, from, to, schema string) (*AckState, error)
	// AckCount counts every acker-side ack state held, valid or not.
	AckCount(ctx context.Context) (int64, error)
}

// CommitState is one commit log entry.
type CommitState struct {
	IP       string
	Document string
	// Proof is the JSON encoding of the stored proof.
	Proof       string
	Owner       string
	GcCandidate bool
}

// RecordState is one record payload.
type RecordState struct {
	Owner         string
	Schema        string
	Redirect      *string
	Distributions []string
	CreatedAt     time.Time
}

// KeyState is one record key (URI) entry.
type KeyState struct {
	// RecordID is the document id the key currently points at; nil for a
	// parent placeholder that no record has been written to.
	RecordID *string
	// RecordCreatedAt is the createdAt of that document; nil for a placeholder.
	RecordCreatedAt *time.Time
}

// EntityState is one registered entity.
type EntityState struct {
	DocumentID string
	Domain     string
	CreatedAt  time.Time
}

// AssociationState is one association.
type AssociationState struct {
	Owner   string
	Author  string
	Variant *string
	Unique  string
}

// AckState is the ack (or acked) state of one (from, to, schema) triple.
type AckState struct {
	DocumentID string
	Valid      bool
	CreatedAt  time.Time
}

// ErrTestRollback aborts a RunInTx closure so nothing it wrote persists.
var ErrTestRollback = errors.New("test rollback")

// foreignTx is a RepositoryTx of a type no backend recognises; every write
// method must reject it.
type foreignTx struct{}

func (foreignTx) IsRepositoryTx() {}

// WithCommit runs fn inside a commit transaction that already holds the
// commit log for sd, recorded under owner (the single entity every commit is
// attributed to).
func WithCommit(t *testing.T, ctx context.Context, repo record.Repository, id string, ip string, sd concrnt.SignedDocument, owner string, fn func(tx record.RepositoryTx) error) {
	t.Helper()

	require.NoError(t, repo.RunInTx(ctx, func(tx record.RepositoryTx) error {
		if err := repo.CreateCommitLog(ctx, tx, id, ip, sd.Document, sd.Proof, owner); err != nil {
			return err
		}
		return fn(tx)
	}))
}

// InTx runs fn in a transaction and commits it; fn reports failures through t.
func InTx(t *testing.T, ctx context.Context, repo record.Repository, fn func(tx record.RepositoryTx)) {
	t.Helper()
	require.NoError(t, repo.RunInTx(ctx, func(tx record.RepositoryTx) error {
		fn(tx)
		return nil
	}))
}

// InTxRollback runs fn in a transaction that is always rolled back, so only
// the values fn observed matter.
func InTxRollback(t *testing.T, ctx context.Context, repo record.Repository, fn func(tx record.RepositoryTx)) {
	t.Helper()
	err := repo.RunInTx(ctx, func(tx record.RepositoryTx) error {
		fn(tx)
		return ErrTestRollback
	})
	require.ErrorIs(t, err, ErrTestRollback)
}

// Ptr returns a pointer to v.
func Ptr[T any](v T) *T {
	return &v
}

// SignedDocument marshals doc into a SignedDocument carrying a "none" proof.
func SignedDocument(t *testing.T, doc any) concrnt.SignedDocument {
	t.Helper()

	docBytes, err := json.Marshal(doc)
	require.NoError(t, err)

	return concrnt.SignedDocument{
		Document: string(docBytes),
		Proof: concrnt.Proof{
			Type: concrnt.ProofTypeNone,
		},
	}
}

// CreateChunklineRecord commits a plain record under key, owned by con1owner,
// with the given createdAt — the shape the chunkline suites feed on.
func CreateChunklineRecord(t *testing.T, ctx context.Context, repo record.Repository, id string, key string, createdAt time.Time) {
	t.Helper()

	sd := SignedDocument(t, concrnt.Document[map[string]string]{
		Kind:      "record",
		Key:       key,
		Value:     map[string]string{"body": id},
		Author:    "con1author",
		Schema:    "https://schema.example/post.json",
		CreatedAt: createdAt,
	})

	WithCommit(t, ctx, repo, id, "127.0.0.1", sd, "con1owner", func(tx record.RepositoryTx) error {
		_, err := repo.CreateRecord(ctx, tx, id, key, "con1owner", "con1owner", "https://schema.example/post.json", nil, nil, []string{}, nil, createdAt)
		return err
	})
}

// mustCommit fetches the commit log id and fails the test when it is absent.
func mustCommit(t *testing.T, ctx context.Context, in Inspector, id string) CommitState {
	t.Helper()
	commit, err := in.Commit(ctx, id)
	require.NoError(t, err)
	require.NotNil(t, commit, "commit log %s must exist", id)
	return *commit
}

// commitExists reports whether the commit log id is present.
func commitExists(t *testing.T, ctx context.Context, in Inspector, id string) bool {
	t.Helper()
	commit, err := in.Commit(ctx, id)
	require.NoError(t, err)
	return commit != nil
}

// mustRecord fetches the record payload for documentID and fails the test
// when it is absent.
func mustRecord(t *testing.T, ctx context.Context, in Inspector, documentID string) RecordState {
	t.Helper()
	rec, err := in.Record(ctx, documentID)
	require.NoError(t, err)
	require.NotNil(t, rec, "record %s must exist", documentID)
	return *rec
}

// recordExists reports whether the record payload for documentID is present.
func recordExists(t *testing.T, ctx context.Context, in Inspector, documentID string) bool {
	t.Helper()
	rec, err := in.Record(ctx, documentID)
	require.NoError(t, err)
	return rec != nil
}

// mustRecordKey fetches the key state for uri and fails the test when the
// key is absent altogether.
func mustRecordKey(t *testing.T, ctx context.Context, in Inspector, uri string) KeyState {
	t.Helper()
	key, err := in.RecordKey(ctx, uri)
	require.NoError(t, err)
	require.NotNil(t, key, "record key %s must exist", uri)
	return *key
}

// keyPointsAtRecord reports whether uri currently resolves to a record (a
// missing key and a placeholder both count as not pointing anywhere).
func keyPointsAtRecord(t *testing.T, ctx context.Context, in Inspector, uri string) bool {
	t.Helper()
	key, err := in.RecordKey(ctx, uri)
	require.NoError(t, err)
	return key != nil && key.RecordID != nil
}

// mustEntity fetches the entity ccid and fails the test when it is absent.
func mustEntity(t *testing.T, ctx context.Context, in Inspector, ccid string) EntityState {
	t.Helper()
	entity, err := in.Entity(ctx, ccid)
	require.NoError(t, err)
	require.NotNil(t, entity, "entity %s must exist", ccid)
	return *entity
}

// mustAssociation fetches the association documentID and fails the test when
// it is absent.
func mustAssociation(t *testing.T, ctx context.Context, in Inspector, documentID string) AssociationState {
	t.Helper()
	assoc, err := in.Association(ctx, documentID)
	require.NoError(t, err)
	require.NotNil(t, assoc, "association %s must exist", documentID)
	return *assoc
}

// associationExists reports whether the association documentID is present.
func associationExists(t *testing.T, ctx context.Context, in Inspector, documentID string) bool {
	t.Helper()
	assoc, err := in.Association(ctx, documentID)
	require.NoError(t, err)
	return assoc != nil
}

// mustAck fetches the acker-side state of (from, to, schema) and fails the
// test when it is absent.
func mustAck(t *testing.T, ctx context.Context, in Inspector, from, to, schema string) AckState {
	t.Helper()
	ack, err := in.Ack(ctx, from, to, schema)
	require.NoError(t, err)
	require.NotNil(t, ack, "ack state %s -> %s (%s) must exist", from, to, schema)
	return *ack
}

// mustAcked fetches the target-side state of (from, to, schema) and fails
// the test when it is absent.
func mustAcked(t *testing.T, ctx context.Context, in Inspector, from, to, schema string) AckState {
	t.Helper()
	acked, err := in.Acked(ctx, from, to, schema)
	require.NoError(t, err)
	require.NotNil(t, acked, "acked state %s -> %s (%s) must exist", from, to, schema)
	return *acked
}

// requireCommitOwner asserts the single owner a commit log is recorded under.
func requireCommitOwner(t *testing.T, ctx context.Context, in Inspector, commitID string, owner string) {
	t.Helper()
	commit := mustCommit(t, ctx, in, commitID)
	require.Equal(t, owner, commit.Owner, "owner of commit %s", commitID)
}
