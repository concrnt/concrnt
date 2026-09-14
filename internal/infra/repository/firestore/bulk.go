package firestore

import (
	"context"
	"errors"

	"cloud.google.com/go/firestore"
)

// bulk wraps a BulkWriter so callers get every write's outcome: the writer's
// End only flushes, the results must be awaited per job. A document is
// written at most once (the BulkWriter rejects a second write to the same
// document).
type bulk struct {
	bw   *firestore.BulkWriter
	jobs []*firestore.BulkWriterJob
	seen map[string]bool
	errs []error
}

func newBulk(ctx context.Context, client *firestore.Client) *bulk {
	return &bulk{bw: client.BulkWriter(ctx), seen: map[string]bool{}}
}

func (b *bulk) add(job *firestore.BulkWriterJob, err error) {
	if err != nil {
		b.errs = append(b.errs, err)
		return
	}
	b.jobs = append(b.jobs, job)
}

func (b *bulk) delete(ref *firestore.DocumentRef) {
	if b.seen[ref.Path] {
		return
	}
	b.seen[ref.Path] = true
	b.add(b.bw.Delete(ref))
}

func (b *bulk) update(ref *firestore.DocumentRef, updates []firestore.Update) {
	if b.seen[ref.Path] {
		return
	}
	b.seen[ref.Path] = true
	b.add(b.bw.Update(ref, updates))
}

// end flushes and waits for every write, returning the joined errors.
func (b *bulk) end() error {
	b.bw.End()
	for _, job := range b.jobs {
		if _, err := job.Results(); err != nil {
			b.errs = append(b.errs, err)
		}
	}
	return errors.Join(b.errs...)
}
