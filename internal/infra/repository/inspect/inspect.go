// Package inspect declares the white-box view of a repository backend that
// the repotest contract suites assert on. It lives apart from repotest so a
// backend's production package can implement it without linking test
// libraries.
package inspect

import (
	"context"
	"time"
)

// Inspector exposes the stored state behind the repository interfaces; each
// backend implements it over its own store. Every lookup returns nil, nil
// when the state is absent.
type Inspector interface {
	// Commit returns the commit log stored under id.
	Commit(ctx context.Context, id string) (*CommitState, error)
	// SetCommitCDate overwrites the receive time of commit id, so suites can
	// pin an ordering that otherwise depends on the store's clock.
	SetCommitCDate(ctx context.Context, id string, cDate time.Time) error
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
	CDate       time.Time
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
