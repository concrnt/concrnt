package record

import (
	"context"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
)

func (uc *Usecase) planAck(ctx context.Context, ip string, from domain.Entity, to domain.Entity, doc concrnt.Document[any], sd concrnt.SignedDocument, mode domain.CommitMode) (*commitPlan, error) {
	ctx, span := tracer.Start(ctx, "Usecase.Record.PlanAck")
	defer span.End()

	documentID, err := sd.CDID()
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// an ack-kind document moves one state, on the server that holds it:
	// ack / unack the acker's (recorded under the acker), acked / unacked
	// the target's (recorded under the target)
	var side AckSide
	var owner string
	switch {
	case (doc.Kind == "ack" || doc.Kind == "unack") && uc.IsLocalEntity(ctx, &from):
		side, owner = AckerSide, from.ID
	case (doc.Kind == "acked" || doc.Kind == "unacked") && uc.IsLocalEntity(ctx, &to):
		side, owner = TargetSide, to.ID
	default:
		// nothing to hold here:
		// if remote: from should be the local entity.
		// if acked: an acked document does not create an ack
		return &commitPlan{noop: true, unapplied: &sd}, nil
	}

	// CIP-3 §3.4: the ccfs identity of the state's document is held by the
	// server of the side it belongs to
	ccfs := concrnt.CCURI{
		Scheme: "ccfs",
		Owner:  owner,
		Type:   concrnt.CCFSTypeConcrnt,
		CDID:   documentID,
	}.String()
	sd.CCFS = &ccfs

	postProcesses := []PostProcessAction{}

	if side == AckerSide {
		// フォロー通知などを生成
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
	}

	commit := AckCommit{
		Commit:    Commit{ID: documentID, IP: ip, Document: sd.Document, Proof: sd.Proof, Owner: owner},
		Side:      side,
		From:      from.ID,
		To:        to.ID,
		Schema:    doc.Schema,
		Valid:     doc.Kind == "ack" || doc.Kind == "acked",
		CreatedAt: doc.CreatedAt,
	}
	var outcome AckOutcome
	apply := func(ctx context.Context) (ApplyStatus, error) {
		var err error
		outcome, err = uc.repo.ApplyAck(ctx, commit)
		return outcome.Status, err
	}

	postProcesses = append(postProcesses, uc.ackCountsTouched(side, from.ID, to.ID, doc.Schema, &outcome, mode))

	return &commitPlan{apply: apply, result: &sd, unapplied: &sd, postProcesses: postProcesses}, nil
}
