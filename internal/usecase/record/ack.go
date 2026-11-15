package record

import (
	"context"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
)

func (uc *Usecase) processAck(ctx context.Context, ip string, from domain.Entity, to domain.Entity, doc concrnt.Document[any], sd concrnt.SignedDocument, mode domain.CommitMode) (*commitApplyResult, error) {
	ctx, span := tracer.Start(ctx, "Usecase.Record.Acknowledge")
	defer span.End()

	postProcesses := []PostProcessAction{}
	var commit *AckCommit

	documentID, err := sd.CDID()
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	if uc.IsLocalEntity(ctx, &from) && (doc.Kind == "ack" || doc.Kind == "unack") { // create ack

		commit = &AckCommit{
			Commit:    Commit{ID: documentID, IP: ip, Document: sd.Document, Proof: sd.Proof, Owner: from.ID},
			Side:      AckerSide,
			From:      from.ID,
			To:        to.ID,
			Schema:    doc.Schema,
			Valid:     doc.Kind == "ack",
			CreatedAt: doc.CreatedAt,
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

		commit = &AckCommit{
			Commit:    Commit{ID: documentID, IP: ip, Document: sd.Document, Proof: sd.Proof, Owner: to.ID},
			Side:      TargetSide,
			From:      from.ID,
			To:        to.ID,
			Schema:    doc.Schema,
			Valid:     doc.Kind == "acked",
			CreatedAt: doc.CreatedAt,
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

	if commit == nil {
		return &commitApplyResult{noop: true, unapplied: &sd}, nil
	}

	apply := func(ctx context.Context) (ApplyStatus, error) {
		outcome, err := uc.repo.ApplyAck(ctx, *commit)
		return outcome.Status, err
	}

	return &commitApplyResult{apply: apply, result: &sd, unapplied: &sd, postProcesses: postProcesses}, nil

}
