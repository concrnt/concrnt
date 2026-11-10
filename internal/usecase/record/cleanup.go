package record

import (
	"context"
	"errors"
)

// JobTypeRecordCleanup is the JobQueue job type that runs the physical
// removal a delete left for later (Repository.Cleanup); its payload is a
// CleanupJob and its handler is ContinueCleanup.
const JobTypeRecordCleanup = "RecordCleanup"

// CleanupJob is one step of a deferred removal. Token is the repository's
// own and means nothing here.
type CleanupJob struct {
	Token string `json:"token"`
}

// ContinueCleanup runs the job's step and enqueues the next one, if any. A
// step is idempotent, so the queue's retry of a failed job is safe.
func (uc *Usecase) ContinueCleanup(ctx context.Context, job CleanupJob) error {
	ctx, span := tracer.Start(ctx, "Usecase.Record.ContinueCleanup")
	defer span.End()

	next, err := uc.repo.Cleanup(ctx, job.Token)
	if err != nil {
		span.RecordError(err)
		return err
	}
	if next == "" {
		return nil
	}
	return uc.jobs.Enqueue(ctx, JobTypeRecordCleanup, CleanupJob{Token: next})
}

// cleanupsScheduled hands the cleanup tokens of a delete to the job queue,
// one job per token; without a queue (offline tooling) every chain is run
// to its end here. outcome is read when the action runs, after the delete.
func (uc *Usecase) cleanupsScheduled(outcome *DeleteOutcome) PostProcessAction {
	return func(ctx context.Context) error {
		var errs error
		for _, token := range outcome.Cleanups {
			if uc.jobs != nil {
				errs = errors.Join(errs, uc.jobs.Enqueue(ctx, JobTypeRecordCleanup, CleanupJob{Token: token}))
				continue
			}
			for token != "" {
				next, err := uc.repo.Cleanup(ctx, token)
				if err != nil {
					errs = errors.Join(errs, err)
					break
				}
				token = next
			}
		}
		return errs
	}
}
