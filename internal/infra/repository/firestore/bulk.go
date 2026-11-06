package firestore

import (
	"context"
	"errors"

	"cloud.google.com/go/firestore"
)

// Bulk wraps a BulkWriter so callers get every write's outcome: the writer's
// End only flushes, the results must be awaited per job. A document is
// written at most once (the BulkWriter rejects a second write to the same
// document). Not safe for concurrent use.
type Bulk struct {
	bw   *firestore.BulkWriter
	jobs []*firestore.BulkWriterJob
	seen map[string]bool
	errs []error
}

func NewBulk(ctx context.Context, client *firestore.Client) *Bulk {
	return &Bulk{bw: client.BulkWriter(ctx), seen: map[string]bool{}}
}

func (b *Bulk) add(job *firestore.BulkWriterJob, err error) {
	if err != nil {
		b.errs = append(b.errs, err)
		return
	}
	b.jobs = append(b.jobs, job)
}

func (b *Bulk) Delete(ref *firestore.DocumentRef) {
	if b.seen[ref.Path] {
		return
	}
	b.seen[ref.Path] = true
	b.add(b.bw.Delete(ref))
}

func (b *Bulk) Update(ref *firestore.DocumentRef, updates []firestore.Update) {
	if b.seen[ref.Path] {
		return
	}
	b.seen[ref.Path] = true
	b.add(b.bw.Update(ref, updates))
}

// End flushes and waits for every write, returning the joined errors.
func (b *Bulk) End() error {
	b.bw.End()
	for _, job := range b.jobs {
		if _, err := job.Results(); err != nil {
			b.errs = append(b.errs, err)
		}
	}
	return errors.Join(b.errs...)
}
