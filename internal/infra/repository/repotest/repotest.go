// Package repotest is the backend-agnostic contract test suite for the
// repository interfaces. A backend (Postgres, Firestore, ...) wires its
// repositories into a Backend and runs Run from its own test package. The
// suites are black-box: they write and read only through the contract
// methods, so they say nothing about tables, collections or columns. The
// Inspector is the two things no contract method can do.
package repotest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase"
	"github.com/concrnt/concrnt/internal/usecase/abuse"
	"github.com/concrnt/concrnt/internal/usecase/notification"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/internal/usecase/server"
)

// Backend is one backend under test. Server, Notification and Abuse are
// only needed by RunWorldSuite; Server must be built over the client
// PeerClient returns.
type Backend struct {
	Record       record.Repository
	EntityMeta   usecase.EntityMetaRepository
	Server       server.Repository
	Notification notification.Repository
	Abuse        abuse.Repository
	Inspect      Inspector
}

// Run runs every contract suite against the backend open returns; open
// must return a fresh, isolated backend per call and is called once per
// test.
func Run(t *testing.T, open func(t *testing.T) Backend) {
	t.Run("Record", func(t *testing.T) { RunRecordSuite(t, open) })
	t.Run("Association", func(t *testing.T) { RunAssociationSuite(t, open) })
	t.Run("Ack", func(t *testing.T) { RunAckSuite(t, open) })
	t.Run("Delete", func(t *testing.T) { RunDeleteSuite(t, open) })
	t.Run("Chunkline", func(t *testing.T) { RunChunklineSuite(t, open) })
	t.Run("Residence", func(t *testing.T) { RunResidenceSuite(t, open) })
	t.Run("Replication", func(t *testing.T) { RunReplicationSuite(t, open) })
	t.Run("Maintenance", func(t *testing.T) { RunMaintenanceSuite(t, open) })
	t.Run("World", func(t *testing.T) { RunWorldSuite(t, open) })
}

// Inspector is what the suites need and the contract cannot give them;
// each backend implements it over its own store, in its inspect_test.go.
type Inspector interface {
	// SetCommitCDate overwrites the receipt time of commit id, so the
	// replication suite can pin an ordering that otherwise depends on the
	// store's clock.
	SetCommitCDate(ctx context.Context, id string, cDate time.Time) error
	// AbuseReportCount counts the abuse reports stored (abuse.Repository
	// is append-only and has no read).
	AbuseReportCount(ctx context.Context) (int64, error)
}

const postSchema = "https://schema.example/post.json"

// ptr returns a pointer to v.
func ptr[T any](v T) *T {
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

// commitOf is the commit log entry of sd under id.
func commitOf(id string, sd concrnt.SignedDocument, owner string) record.Commit {
	return record.Commit{ID: id, IP: "127.0.0.1", Document: sd.Document, Proof: sd.Proof, Owner: owner}
}

// ownerOf is the owner of a cckv key.
func ownerOf(t *testing.T, key string) string {
	t.Helper()
	parsed, err := concrnt.ParseCCURI(key)
	require.NoError(t, err)
	return parsed.Owner
}

// recordCommit builds the commit of a plain record at key, authored by the
// key's owner; edit adjusts it before the document is rendered from it, so
// the document always says what the commit does.
func recordCommit(t *testing.T, id, key string, createdAt time.Time, edit ...func(*record.RecordCommit)) record.RecordCommit {
	t.Helper()
	owner := ownerOf(t, key)
	rc := record.RecordCommit{
		Commit:        record.Commit{ID: id, IP: "127.0.0.1", Owner: owner},
		Key:           key,
		Author:        owner,
		Schema:        postSchema,
		Distributions: []string{},
		CreatedAt:     createdAt,
	}
	for _, fn := range edit {
		fn(&rc)
	}
	var distributes *[]string
	if len(rc.Distributions) > 0 {
		distributes = &rc.Distributions
	}
	sd := SignedDocument(t, concrnt.Document[map[string]string]{
		Kind:        "record",
		Key:         key,
		Value:       map[string]string{"id": id},
		Author:      rc.Author,
		Schema:      rc.Schema,
		CreatedAt:   createdAt,
		OnUpdate:    rc.OnUpdate,
		Distributes: distributes,
	})
	rc.Document, rc.Proof = sd.Document, sd.Proof
	return rc
}

// putRecord applies a record that must be Applied and returns its commit
// and outcome.
func putRecord(t *testing.T, ctx context.Context, repo record.Repository, id, key string, createdAt time.Time, edit ...func(*record.RecordCommit)) (record.RecordCommit, record.RecordOutcome) {
	t.Helper()
	rc := recordCommit(t, id, key, createdAt, edit...)
	outcome, err := repo.ApplyRecord(ctx, rc)
	require.NoError(t, err)
	require.Equal(t, record.Applied, outcome.Status, "record %s must apply", id)
	return rc, outcome
}

// putCommit records a commit that stores nothing but its log entry (a
// delete without removals), for the suites that are about the log itself.
func putCommit(t *testing.T, ctx context.Context, repo record.Repository, id, owner, document string) {
	t.Helper()
	outcome, err := repo.ApplyDelete(ctx, record.DeleteCommit{
		Commit: &record.Commit{ID: id, IP: "127.0.0.1", Document: document, Proof: concrnt.Proof{Type: concrnt.ProofTypeNone}, Owner: owner},
	})
	require.NoError(t, err)
	require.Equal(t, record.Applied, outcome.Status, "commit %s must apply", id)
}

// commitEntry reads the commit log entry id; nil when the log has none.
func commitEntry(t *testing.T, ctx context.Context, repo record.Repository, id string) *record.CommitLogEntry {
	t.Helper()
	entries, err := repo.ListCommitLogs(ctx, record.CommitLogPage{FromID: id, UntilID: id, Limit: 1})
	require.NoError(t, err)
	if len(entries) == 0 {
		return nil
	}
	require.Equal(t, id, entries[0].ID)
	return &entries[0]
}

// commitExists reports whether the commit log holds id.
func commitExists(t *testing.T, ctx context.Context, repo record.Repository, id string) bool {
	t.Helper()
	return commitEntry(t, ctx, repo, id) != nil
}

// gcCutoff sorts above every commit id the suites use (lowercase letters,
// digits and hyphens, never starting with "z"), under any collation.
const gcCutoff = "zzzzzzzzzzzzzzzz"

// gcFlagged reports whether commit id is flagged for gc.
func gcFlagged(t *testing.T, ctx context.Context, repo record.Repository, id string) bool {
	t.Helper()
	after := ""
	for {
		page, err := repo.ListGcCandidates(ctx, gcCutoff, after, 100)
		require.NoError(t, err)
		for _, entry := range page {
			if entry.ID == id {
				return true
			}
		}
		if len(page) < 100 {
			return false
		}
		after = page[len(page)-1].ID
	}
}

// resolve reads uri; nil when it resolves to nothing.
func resolve(t *testing.T, ctx context.Context, repo record.Repository, uri string) *concrnt.SignedDocument {
	t.Helper()
	sd, err := repo.GetSignedDocument(ctx, uri)
	if errors.Is(err, domain.ErrNotFound) {
		return nil
	}
	require.NoError(t, err)
	return sd
}

// ccfsOf is the ccfs URI of commit id in owner's namespace.
func ccfsOf(owner, id string) string {
	return concrnt.ComposeCCFSURI(owner, concrnt.CCFSTypeConcrnt, id)
}

// rowKeys are the cckv keys of listing rows, in order.
func rowKeys(t *testing.T, rows []record.QueryRow) []string {
	t.Helper()
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		require.NotNil(t, row.Row.CCKV)
		keys = append(keys, *row.Row.CCKV)
	}
	return keys
}

// rowIDs are the commit ids of listing rows, read out of their ccfs URIs.
func rowIDs(t *testing.T, rows []record.QueryRow) []string {
	t.Helper()
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		require.NotNil(t, row.Row.CCFS)
		parsed, err := concrnt.ParseCCURI(*row.Row.CCFS)
		require.NoError(t, err)
		ids = append(ids, parsed.CDID)
	}
	return ids
}

// removeKeys deletes the records at keys under a delete commit id and
// returns the outcome.
func removeKeys(t *testing.T, ctx context.Context, repo record.Repository, id, owner string, keys ...string) record.DeleteOutcome {
	t.Helper()
	removals := make([]record.Removal, len(keys))
	for i, key := range keys {
		removals[i] = record.Removal{Kind: record.RemoveKey, URI: key}
	}
	sd := SignedDocument(t, concrnt.Document[string]{Kind: "delete", Value: fmt.Sprint(keys), Author: owner})
	commit := commitOf(id, sd, owner)
	outcome, err := repo.ApplyDelete(ctx, record.DeleteCommit{Commit: &commit, Removals: removals})
	require.NoError(t, err)
	require.Equal(t, record.Applied, outcome.Status)
	require.Len(t, outcome.Removed, len(keys))
	return outcome
}

// runCleanups runs every cleanup chain to its end.
func runCleanups(t *testing.T, ctx context.Context, repo record.Repository, tokens []string) {
	t.Helper()
	for _, token := range tokens {
		for steps := 0; token != ""; steps++ {
			require.Less(t, steps, 10000, "a cleanup chain must end")
			next, err := repo.Cleanup(ctx, token)
			require.NoError(t, err)
			token = next
		}
	}
}
