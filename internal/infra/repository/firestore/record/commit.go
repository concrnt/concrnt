package record

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"

	"github.com/concrnt/concrnt"
	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func (r *Repository) GetAllCommitLogs(ctx context.Context, owner string) ([]concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.GetAllCommitLogs")
	defer span.End()

	it := r.col(fs.ColCommits).Where("owner", "==", owner).OrderBy("cDate", firestore.Asc).Documents(ctx)
	defer it.Stop()

	sds := []concrnt.SignedDocument{}
	for {
		snap, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			span.RecordError(err)
			return nil, fs.QueryErr(err)
		}
		c := commitFromMap(snap.Data())
		proof, err := repoutil.ParseProof(c.Proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		sds = append(sds, concrnt.SignedDocument{Document: c.Document, Proof: proof})
	}
	return sds, nil
}

func (r *Repository) QueryCommitLogs(ctx context.Context, owner string, since, until *time.Time, limit int, order string) ([]ucrecord.QueryRow, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.QueryCommitLogs")
	defer span.End()

	q := r.col(fs.ColCommits).Query
	if owner != "" {
		q = q.Where("owner", "==", owner)
	}
	q = applyWindow(q, "cDate", firestore.DocumentID, since, until, limit, order)

	snaps, err := q.Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, fs.QueryErr(err)
	}

	rows := make([]ucrecord.QueryRow, 0, len(snaps))
	for _, snap := range snaps {
		c := commitFromMap(snap.Data())
		sd, err := repoutil.CommitDocument(c.Owner, snap.Ref.ID, c.Document, c.Proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		rows = append(rows, ucrecord.QueryRow{Row: sd, CreatedAt: c.CDate})
	}
	return rows, nil
}

// gcCandidates selects gc-flagged commits whose id sorts below cutoffID.
// Commit ids are time-prefixed CDIDs, so this is "older than the cutoff";
// the equality plus the __name__ ordering is served by the automatic index.
func (r *Repository) gcCandidates(cutoffID string) firestore.Query {
	return r.col(fs.ColCommits).
		Where("gcCandidate", "==", true).
		OrderBy(firestore.DocumentID, firestore.Asc).
		EndBefore(cutoffID)
}

func (r *Repository) CountGcCandidates(ctx context.Context, cutoffID string) (int64, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.CountGcCandidates")
	defer span.End()

	q := r.gcCandidates(cutoffID)
	res, err := q.NewAggregationQuery().WithCount("n").Get(ctx)
	if err != nil {
		span.RecordError(err)
		return 0, fs.QueryErr(err)
	}
	return aggregationCount(res)
}

func (r *Repository) ListGcCandidates(ctx context.Context, cutoffID string, afterID string, limit int) ([]ucrecord.CommitLogEntry, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ListGcCandidates")
	defer span.End()

	q := r.gcCandidates(cutoffID)
	if afterID != "" {
		q = q.StartAfter(afterID)
	}
	if limit > 0 {
		q = q.Limit(limit)
	}
	snaps, err := q.Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, fs.QueryErr(err)
	}
	return commitLogEntries(snaps)
}

// DeleteCommitLogs removes the commits and, by hand, everything the SQL
// backends drop through ON DELETE CASCADE. A commit names what it anchors,
// so the dependents are found by id: the commits are read in one batch get,
// what they anchor in a second, and a dependent goes only if it still
// points back at its commit (a key that has moved on to a newer version
// stays) and has not changed since it was read. A key removed this way
// ends its incarnation; its associations are cleaned up here, to the end.
//
// Dependents are deleted before the commits: a key never points at a
// commit that is gone, which ApplyRecord relies on. Every step is
// idempotent, so a partial run is finished by re-running. Not atomic over
// the list (offline tooling).
func (r *Repository) DeleteCommitLogs(ctx context.Context, ids []string) (int, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteCommitLogs")
	defer span.End()

	if len(ids) == 0 {
		return 0, nil
	}

	commits := newReadSet()
	for _, id := range ids {
		commits.add(r.col(fs.ColCommits).Doc(id))
	}
	if err := commits.load(ctx, r.client); err != nil {
		span.RecordError(err)
		return 0, err
	}

	type dependent struct {
		commitID string
		anchor   string
		ref      *firestore.DocumentRef
	}
	var dependents []dependent
	anchored := newReadSet()
	var existing []*firestore.DocumentRef
	for _, ref := range commits.refs {
		m, exists := commits.data(ref)
		if !exists {
			continue
		}
		existing = append(existing, ref)
		commit := commitFromMap(m)
		var anchorRef *firestore.DocumentRef
		switch commit.Anchor {
		case fs.ColRecordKeys:
			anchorRef = r.keyRef(commit.AnchorID)
		case fs.ColAssociations:
			anchorRef = r.col(fs.ColAssociations).Doc(ref.ID)
		case fs.ColAcks, fs.ColAckeds, fs.ColEntities:
			anchorRef = r.col(commit.Anchor).Doc(commit.AnchorID)
		default:
			continue
		}
		dependents = append(dependents, dependent{commitID: ref.ID, anchor: commit.Anchor, ref: anchorRef})
		anchored.add(anchorRef)
	}
	if err := anchored.load(ctx, r.client); err != nil {
		span.RecordError(err)
		return 0, err
	}

	// the uniqueness marker of an association goes with it, while it is
	// still the marker of that association
	markerOf := map[string]*firestore.DocumentRef{}
	markers := newReadSet()
	for _, d := range dependents {
		if m, exists := anchored.data(d.ref); exists && d.anchor == fs.ColAssociations {
			ref := r.uniqueRef(str(m, "targetURI"), str(m, "unique"))
			markerOf[d.commitID] = ref
			markers.add(ref)
		}
	}
	if err := markers.load(ctx, r.client); err != nil {
		span.RecordError(err)
		return 0, err
	}

	var gens []string
	b := fs.NewBulk(ctx, r.client)
	for _, d := range dependents {
		snap := anchored.snap(d.ref)
		if snap == nil {
			continue
		}
		m := snap.Data()
		switch d.anchor {
		case fs.ColRecordKeys:
			if str(m, "recordID") != d.commitID {
				continue
			}
			gens = append(gens, str(m, "gen"))
		case fs.ColAssociations:
			if marker := markers.snap(markerOf[d.commitID]); marker != nil && str(marker.Data(), "associationID") == d.commitID {
				b.DeleteIfUnchanged(marker)
			}
		default:
			if str(m, "documentID") != d.commitID {
				continue
			}
		}
		b.DeleteIfUnchanged(snap)
	}
	for _, ref := range existing {
		b.Delete(r.col(fs.ColRetainedVersions).Doc(ref.ID))
	}
	if err := b.End(); err != nil {
		span.RecordError(err)
		return 0, err
	}
	if err := r.runCleanup(ctx, gens); err != nil {
		span.RecordError(err)
		return 0, err
	}

	b = fs.NewBulk(ctx, r.client)
	for _, ref := range existing {
		b.Delete(ref)
	}
	if err := b.End(); err != nil {
		span.RecordError(err)
		return 0, err
	}
	return len(existing), nil
}

// ListCommitLogs orders, paginates and filters on the document id; cursors
// on __name__ take the bare id. Time-kind CDIDs are fixed-length lowercase
// base32 whose byte order encodes the embedded timestamp.
func (r *Repository) ListCommitLogs(ctx context.Context, page ucrecord.CommitLogPage) ([]ucrecord.CommitLogEntry, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.ListCommitLogs")
	defer span.End()

	q := r.col(fs.ColCommits).OrderBy(firestore.DocumentID, firestore.Asc)
	if page.Owner != "" {
		q = q.Where("owner", "==", page.Owner)
	}
	if page.AfterID != "" {
		q = q.StartAfter(page.AfterID)
	} else if page.FromID != "" {
		q = q.StartAt(page.FromID)
	}
	if page.UntilID != "" {
		q = q.EndAt(page.UntilID)
	}
	if page.Limit > 0 {
		q = q.Limit(page.Limit)
	}

	snaps, err := q.Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return nil, fs.QueryErr(err)
	}
	return commitLogEntries(snaps)
}

func commitLogEntries(snaps []*firestore.DocumentSnapshot) ([]ucrecord.CommitLogEntry, error) {
	entries := make([]ucrecord.CommitLogEntry, 0, len(snaps))
	for _, snap := range snaps {
		c := commitFromMap(snap.Data())
		entry, err := repoutil.CommitLogEntry(snap.Ref.ID, c.IP, c.Document, c.Proof, c.Owner, c.CDate)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// aggregationCount extracts the count() alias "n" from an aggregation result.
func aggregationCount(res firestore.AggregationResult) (int64, error) {
	var m struct {
		N int64 `firestore:"n"`
	}
	if err := res.DataTo(&m); err != nil {
		return 0, err
	}
	return m.N, nil
}
