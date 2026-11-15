package record

import (
	"context"
	"errors"
	"log/slog"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
)

// JobTypeRecordRangeDelete is the JobQueue job type that continues a range
// delete too large for one write; its payload is a RangeDeleteJob and
// its handler is ContinueRangeDelete.
const JobTypeRecordRangeDelete = "RecordRangeDelete"

// RangeDeleteJob is one page of a range delete whose commit is already
// recorded: the subtree of Base after AfterURI (in key order) is removed
// rangeDeleteSyncLimit targets per ApplyDelete call, each page re-enqueueing the
// next. Retained carries whether an earlier page removed a document with
// onUpdate=retain, which keeps the delete commit itself.
type RangeDeleteJob struct {
	IP          string                 `json:"ip"`
	Requester   string                 `json:"requester"`
	Delete      concrnt.SignedDocument `json:"delete"`
	Base        string                 `json:"base"`
	IncludeSelf bool                   `json:"includeSelf"`
	AfterURI    string                 `json:"afterUri"`
	Retained    bool                   `json:"retained"`
}

// ContinueRangeDelete removes the next page of the job's subtree. A target
// whose policy denies the delete is skipped (the synchronous first page
// rejected the whole commit on that; here the commit exists already); any
// other error fails the job for the queue to retry. The last page flags the
// delete commit for gc unless a retained document kept it.
func (uc *Usecase) ContinueRangeDelete(ctx context.Context, job RangeDeleteJob) error {
	ctx, span := tracer.Start(ctx, "Usecase.Record.ContinueRangeDelete")
	defer span.End()

	requester, err := uc.GetEntity(ctx, concrnt.CCURI{Scheme: "cckv", Owner: job.Requester}.String())
	if err != nil {
		span.RecordError(err)
		return err
	}
	documentID, err := job.Delete.CDID()
	if err != nil {
		span.RecordError(err)
		return err
	}

	all, err := uc.repo.QueryRecordSubtree(ctx, job.Base, job.IncludeSelf, job.AfterURI, rangeDeleteSyncLimit+1)
	if err != nil {
		span.RecordError(err)
		return err
	}
	more := len(all) > rangeDeleteSyncLimit
	if more {
		all = all[:rangeDeleteSyncLimit]
	}
	docs, err := unmarshalTargets(all)
	if err != nil {
		span.RecordError(err)
		return err
	}

	page := deletePage{
		ip: job.IP, requester: *requester, delete: job.Delete, mode: domain.CommitModeExecute,
		authoritative: true, remoteKind: DeliveryRemoteCommit,
	}
	for i := range all {
		uri := *all[i].CCKV
		err := uc.evalDeletePolicy(ctx, *requester, docs[i], uri)
		if errors.Is(err, domain.ErrPermissionDenied) {
			slog.Info("range delete: target kept, its policy denied the delete", slog.String("key", uri), slog.String("requester", requester.ID))
			continue
		}
		if err != nil {
			span.RecordError(err)
			return err
		}
		page.targets = append(page.targets, all[i])
		page.targetDocs = append(page.targetDocs, docs[i])
		page.targetURIs = append(page.targetURIs, uri)
	}

	pagePlan, err := uc.planDeleteTargets(ctx, page)
	if err != nil {
		span.RecordError(err)
		return err
	}
	retained := job.Retained || pagePlan.retained
	if !more && !retained {
		pagePlan.gcCommitIDs = append(pagePlan.gcCommitIDs, documentID)
	}

	// the delete commit is in the log since the first page: this page only
	// removes, which is idempotent, so a retried job is safe
	if len(pagePlan.removals) > 0 || len(pagePlan.gcCommitIDs) > 0 {
		if _, err := uc.applyDeletePage(ctx, nil, pagePlan); err != nil {
			span.RecordError(err)
			return err
		}
	}

	for _, task := range pagePlan.postProcesses {
		if err := task(ctx); err != nil {
			slog.Error("failed to run post-commit action", slog.String("error", err.Error()))
			span.RecordError(err)
		}
	}

	if !more {
		return nil
	}
	next := job
	next.AfterURI = *all[len(all)-1].CCKV
	next.Retained = retained
	return uc.jobs.Enqueue(ctx, JobTypeRecordRangeDelete, next)
}
