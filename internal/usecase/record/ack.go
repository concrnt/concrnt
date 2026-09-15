package record

import (
	"context"
	"errors"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
)

func (uc *Usecase) processAck(ctx context.Context, tx RepositoryTx, ip string, from domain.Entity, to domain.Entity, doc concrnt.Document[any], sd concrnt.SignedDocument, mode domain.CommitMode) (*commitApplyResult, error) {
	ctx, span := tracer.Start(ctx, "Usecase.Record.Acknowledge")
	defer span.End()

	postProcesses := []PostProcessAction{}
	created := false

	documentID, err := sd.CDID()
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	switch doc.Kind {
	case "ack", "unack":
		if err := uc.repo.CreateCommitLog(ctx, tx, documentID, ip, sd.Document, sd.Proof, from.ID); err != nil {
			span.RecordError(err)
			return nil, err
		}
	case "acked", "unacked":
		if err := uc.repo.CreateCommitLog(ctx, tx, documentID, ip, sd.Document, sd.Proof, to.ID); err != nil {
			span.RecordError(err)
			return nil, err
		}
	}

	if uc.IsLocalEntity(ctx, &from) && (doc.Kind == "ack" || doc.Kind == "unack") { // create ack

		if doc.Kind == "ack" {
			created, err = uc.repo.Acknowledge(ctx, tx, documentID, from.ID, to.ID, doc.Schema, doc.CreatedAt)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
		} else {
			created, err = uc.repo.UnAcknowledge(ctx, tx, documentID, from.ID, to.ID, doc.Schema, doc.CreatedAt)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
		}

		// フォロー通知などを生成
		// CIP-3 §3.4: the ack's ccfs identity is held by the author's server
		ccfs := concrnt.CCURI{
			Scheme: "ccfs",
			Owner:  from.ID,
			Type:   concrnt.CCFSTypeConcrnt,
			CDID:   documentID,
		}.String()
		sd.CCFS = &ccfs

		actions, err := uc.createReferenceDistributionActions(ctx, ip, doc.Author, ccfs, from, sd, distributionsFromPtr(doc.Distributes), mode)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		postProcesses = append(postProcesses, actions...)

		// acked documentを生成・配送
		// DeriveAcked is the same derivation the receiving side re-runs to
		// verify the document-direct proof (verify.go), so the two stay in
		// lockstep by construction.
		ackedSD, err := sd.DeriveAcked()
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		// like reference distribution, only an executing commit ships the
		// acked document; a dump replay (LocalOnlyExecute) leaves the
		// associate owner's holding to that side's own dump
		if mode == domain.CommitModeExecute && uc.jobs != nil {
			// like reference distribution, ship the acker's current entity
			// document along so the target's server can refresh its copy
			fromSD, err := uc.GetSigned(ctx, from.CCKVWithHint())
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
			ackedSD.References = map[string]concrnt.SignedDocument{
				from.CCKV(): *fromSD,
			}

			postProcesses = append(postProcesses,
				func(ctx context.Context) error {
					return uc.jobs.Enqueue(ctx, JobTypeRecordDelivery, DeliveryJob{
						ResolveURI: to.CCKVWithHint(),
						Payload:    ackedSD,
						Local:      DeliveryLocalCommit,
						Remote:     DeliveryRemoteCommit,
						IP:         ip,
					})
				},
			)
		}

	} else {
		// ignore. because...
		// if remote: from should be the local entity.
		// if acked: acked document does not create ack
	}

	if uc.IsLocalEntity(ctx, &to) && (doc.Kind == "acked" || doc.Kind == "unacked") { // create acked

		switch doc.Kind {
		case "acked":
			created, err = uc.repo.Acknowledged(ctx, tx, documentID, from.ID, to.ID, doc.Schema, doc.CreatedAt)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
		case "unacked":
			created, err = uc.repo.UnAcknowledged(ctx, tx, documentID, from.ID, to.ID, doc.Schema, doc.CreatedAt)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
		default:
			err := errors.New("unsupported document kind for acked: " + doc.Kind)
			span.RecordError(err)
			return nil, err
		}

		// CIP-3 §3.4: the acked's ccfs identity is held by the associate
		// owner's server
		ccfs := concrnt.CCURI{
			Scheme: "ccfs",
			Owner:  to.ID,
			Type:   concrnt.CCFSTypeConcrnt,
			CDID:   documentID,
		}.String()
		sd.CCFS = &ccfs
	}

	return &commitApplyResult{result: &sd, noop: !created, postProcesses: postProcesses}, nil

}
