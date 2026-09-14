package record

import (
	"context"

	"github.com/concrnt/concrnt"
)

// CommitLogEntry is one commit log row as offline tooling sees it.
type CommitLogEntry struct {
	ID       string
	Document string
	Proof    concrnt.Proof
	Owner    string
}

// CommitLogPage selects a page of commit logs ordered by id ascending. Commit
// log ids are time-prefixed CDIDs, so id bounds are time bounds.
type CommitLogPage struct {
	// FromID is an inclusive lower bound; "" means none. Ignored when AfterID is set.
	FromID string
	// AfterID is an exclusive lower bound (the last id of the previous page).
	AfterID string
	// UntilID is an inclusive upper bound; "" means none.
	UntilID string
	// Owner restricts to commits owned by this ccid; "" means all.
	Owner string
	Limit int
}

// MaintenanceRepository is the offline-tooling slice of the commit log store
// used by conctl (gc-commitlog, dump-commitlog). It is separate from
// Repository so the request path and its test stubs are unaffected.
type MaintenanceRepository interface {
	// CountGcCandidates counts gc-flagged commit logs whose id sorts below
	// cutoffID (i.e. whose document is older than the cutoff).
	CountGcCandidates(ctx context.Context, cutoffID string) (int64, error)
	// ListGcCandidateIDs returns up to limit ids of gc-flagged commit logs
	// below cutoffID, in id order.
	ListGcCandidateIDs(ctx context.Context, cutoffID string, limit int) ([]string, error)
	// DeleteCommitLogs removes the given commit logs together with every row
	// that hangs off them (record, record key, ack, acked, association,
	// entity). Missing ids are ignored; the count of commit logs actually
	// removed is returned.
	DeleteCommitLogs(ctx context.Context, ids []string) (int, error)
	// ListCommitLogs returns one page of commit logs ordered by id ascending.
	ListCommitLogs(ctx context.Context, page CommitLogPage) ([]CommitLogEntry, error)
}
