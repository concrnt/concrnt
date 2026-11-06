package record

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"

	"github.com/concrnt/concrnt"
	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

func (r *Repository) HasCommitLog(ctx context.Context, id string) (bool, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.HasCommitLog")
	defer span.End()

	// a projection to no fields avoids pulling the 32 KiB document body
	snaps, err := r.col(fs.ColCommits).Where(firestore.DocumentID, "==", r.col(fs.ColCommits).Doc(id)).Select().Limit(1).Documents(ctx).GetAll()
	if err != nil {
		span.RecordError(err)
		return false, fs.QueryErr(err)
	}
	return len(snaps) > 0, nil
}

// CreateCommitLog stamps cDate with the server time of the commit. First
// write wins, like INSERT ... ON CONFLICT DO NOTHING: a re-delivery of the
// same id is ignored wholesale.
func (r *Repository) CreateCommitLog(ctx context.Context, tx ucrecord.RepositoryTx, id string, ip string, document string, proof any, owner string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.CreateCommitLog")
	defer span.End()

	t, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	proofBytes, err := json.Marshal(proof)
	if err != nil {
		span.RecordError(err)
		return err
	}

	ref := r.col(fs.ColCommits).Doc(id)
	_, exists, err := t.read(ref)
	if err != nil {
		span.RecordError(err)
		return err
	}
	if exists {
		return ucrecord.ErrAlreadyCommitted
	}
	t.set(ref, commitDoc{IP: ip, Document: document, Proof: string(proofBytes), Owner: owner}.toMap())
	return nil
}

func (r *Repository) MarkCommitLogGcCandidate(ctx context.Context, tx ucrecord.RepositoryTx, documentID string) error {
	ctx, span := tracer.Start(ctx, "Repository.Record.MarkCommitLogGcCandidate")
	defer span.End()

	t, err := getRecordTx(tx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	// a missing commit is a no-op (UPDATE matching zero rows)
	if err := t.updateIfExists(r.col(fs.ColCommits).Doc(documentID), []firestore.Update{{Path: "gcCandidate", Value: true}}); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

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
		proof, err := c.proof()
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
		proof, err := c.proof()
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		row := ucrecord.QueryRow{
			Row:       concrnt.SignedDocument{Document: c.Document, Proof: proof},
			CreatedAt: c.CDate,
		}
		// an ownerless commit (unrepaired legacy row) has no ccfs namespace to
		// compose; ComposeCCFSURI("") would yield a URI that misparses
		if c.Owner != "" {
			ccfs := concrnt.ComposeCCFSURI(c.Owner, concrnt.CCFSTypeConcrnt, snap.Ref.ID)
			row.Row.CCFS = &ccfs
		}
		rows = append(rows, row)
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

// deleteConcurrency bounds the parallel per-commit sweeps of DeleteCommitLogs.
const deleteConcurrency = 16

// DeleteCommitLogs removes the commits and, by hand, everything the SQL
// backends drop through ON DELETE CASCADE: the record version, the key
// pointing at it (and the associations targeting that key), the association
// and its uniqueness marker, ack / acked states and entities carrying the
// document id. Every step is idempotent, so a partial run is finished by
// re-running. Not atomic over the list (offline tooling).
func (r *Repository) DeleteCommitLogs(ctx context.Context, ids []string) (int, error) {
	ctx, span := tracer.Start(ctx, "Repository.Record.DeleteCommitLogs")
	defer span.End()

	if len(ids) == 0 {
		return 0, nil
	}

	refs := make([]*firestore.DocumentRef, 0, len(ids))
	for _, id := range ids {
		refs = append(refs, r.col(fs.ColCommits).Doc(id))
	}
	existing := 0
	for start := 0; start < len(refs); start += getAllBatch {
		snaps, err := r.client.GetAll(ctx, refs[start:min(start+getAllBatch, len(refs))])
		if err != nil {
			span.RecordError(err)
			return 0, err
		}
		for _, s := range snaps {
			if s.Exists() {
				existing++
			}
		}
	}

	// the queries run in parallel per commit, the deletes are collected and
	// handed to one BulkWriter afterwards
	targets := make([][]*firestore.DocumentRef, len(ids))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(deleteConcurrency)
	for i, id := range ids {
		g.Go(func() error {
			refs, err := r.commitDependents(gctx, id)
			if err != nil {
				return err
			}
			targets[i] = refs
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		span.RecordError(err)
		return 0, err
	}

	b := newBulk(ctx, r.client)
	for _, refs := range targets {
		for _, ref := range refs {
			b.delete(ref)
		}
	}
	if err := b.end(); err != nil {
		span.RecordError(err)
		return 0, err
	}
	return existing, nil
}

// commitDependents lists every document that goes when commit id does.
func (r *Repository) commitDependents(ctx context.Context, id string) ([]*firestore.DocumentRef, error) {
	refs := []*firestore.DocumentRef{r.col(fs.ColCommits).Doc(id), r.col(fs.ColRecords).Doc(id)}

	// the key pointing at this version goes, and with it the associations
	// targeting the key (record_keys -> associations cascade)
	keySnaps, err := r.col(fs.ColRecordKeys).Where("recordID", "==", id).Select("uri").Documents(ctx).GetAll()
	if err != nil {
		return nil, fs.QueryErr(err)
	}
	for _, keySnap := range keySnaps {
		refs = append(refs, keySnap.Ref)
		assocs, err := r.associationsOf(ctx, str(keySnap.Data(), "uri"))
		if err != nil {
			return nil, err
		}
		refs = append(refs, assocs...)
	}

	assocSnap, err := r.col(fs.ColAssociations).Doc(id).Get(ctx)
	if err != nil && !fs.IsNotFound(err) {
		return nil, err
	}
	if err == nil && assocSnap.Exists() {
		refs = append(refs, assocSnap.Ref)
		if unique := str(assocSnap.Data(), "unique"); unique != "" {
			refs = append(refs, r.col(fs.ColAssocUniques).Doc(uniqueID(unique)))
		}
	}

	for _, collection := range []string{fs.ColAcks, fs.ColAckeds, fs.ColEntities} {
		snaps, err := r.col(collection).Where("documentID", "==", id).Select().Documents(ctx).GetAll()
		if err != nil {
			return nil, fs.QueryErr(err)
		}
		for _, snap := range snaps {
			refs = append(refs, snap.Ref)
		}
	}
	return refs, nil
}

// associationsOf lists the association documents targeting a key and their
// uniqueness markers.
func (r *Repository) associationsOf(ctx context.Context, targetURI string) ([]*firestore.DocumentRef, error) {
	snaps, err := r.col(fs.ColAssociations).Where("targetURI", "==", targetURI).Select("unique").Documents(ctx).GetAll()
	if err != nil {
		return nil, fs.QueryErr(err)
	}
	return associationRefs(r.client, snaps), nil
}

func associationRefs(client *firestore.Client, snaps []*firestore.DocumentSnapshot) []*firestore.DocumentRef {
	refs := make([]*firestore.DocumentRef, 0, 2*len(snaps))
	for _, snap := range snaps {
		refs = append(refs, snap.Ref)
		if unique := str(snap.Data(), "unique"); unique != "" {
			refs = append(refs, client.Collection(fs.ColAssocUniques).Doc(uniqueID(unique)))
		}
	}
	return refs
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
		proof, err := c.proof()
		if err != nil {
			return nil, fmt.Errorf("failed to parse proof for commit %s: %w", snap.Ref.ID, err)
		}
		entries = append(entries, ucrecord.CommitLogEntry{
			ID:       snap.Ref.ID,
			IP:       c.IP,
			Document: c.Document,
			Proof:    proof,
			Owner:    c.Owner,
			CDate:    c.CDate,
		})
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
