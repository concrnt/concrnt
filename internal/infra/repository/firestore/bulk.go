package firestore

import (
	"context"
	"errors"

	"cloud.google.com/go/firestore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Bulk wraps a BulkWriter so callers get every write's outcome: the writer's
// End only flushes, the results must be awaited per job. A document is
// written at most once (the BulkWriter rejects a second write to the same
// document). Not safe for concurrent use.
type Bulk struct {
	bw   *firestore.BulkWriter
	jobs []bulkJob
	seen map[string]bool
	errs []error
}

type bulkJob struct {
	job *firestore.BulkWriterJob
	// conditional marks a write whose precondition may fail by design
	conditional bool
}

func NewBulk(ctx context.Context, client *firestore.Client) *Bulk {
	return &Bulk{bw: client.BulkWriter(ctx), seen: map[string]bool{}}
}

func (b *Bulk) add(job *firestore.BulkWriterJob, err error, conditional bool) {
	if err != nil {
		b.errs = append(b.errs, err)
		return
	}
	b.jobs = append(b.jobs, bulkJob{job: job, conditional: conditional})
}

func (b *Bulk) Delete(ref *firestore.DocumentRef) {
	if b.seen[ref.Path] {
		return
	}
	b.seen[ref.Path] = true
	job, err := b.bw.Delete(ref)
	b.add(job, err, false)
}

// DeleteIfUnchanged deletes the document only if it is still the version
// snap read. A document written or removed since is left alone, silently:
// the caller decided on what it read, and that is no longer what is there.
func (b *Bulk) DeleteIfUnchanged(snap *firestore.DocumentSnapshot) {
	if b.seen[snap.Ref.Path] {
		return
	}
	b.seen[snap.Ref.Path] = true
	job, err := b.bw.Delete(snap.Ref, firestore.LastUpdateTime(snap.UpdateTime))
	b.add(job, err, true)
}

func (b *Bulk) Update(ref *firestore.DocumentRef, updates []firestore.Update) {
	if b.seen[ref.Path] {
		return
	}
	b.seen[ref.Path] = true
	job, err := b.bw.Update(ref, updates)
	b.add(job, err, false)
}

// End flushes and waits for every write, returning the joined errors.
func (b *Bulk) End() error {
	b.bw.End()
	for _, j := range b.jobs {
		_, err := j.job.Results()
		if err == nil {
			continue
		}
		if code := status.Code(err); j.conditional && (code == codes.FailedPrecondition || code == codes.NotFound) {
			continue
		}
		b.errs = append(b.errs, err)
	}
	return errors.Join(b.errs...)
}
