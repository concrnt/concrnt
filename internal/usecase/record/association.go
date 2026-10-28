package record

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/zeebo/xxh3"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/policy"
)

func (uc *Usecase) createAssociation(ctx context.Context, tx RepositoryTx, ip string, requester domain.Entity, parsed concrnt.Document[any], sd concrnt.SignedDocument, mode domain.CommitMode) (*commitApplyResult, error) {
	ctx, span := tracer.Start(ctx, "Usecase.Record.CreateAssociation")
	defer span.End()

	// CIP-9: an association is addressable only by its ccfs URI, never a key.
	if parsed.Key != "" {
		err := domain.ValidationError{Field: "key", Message: "association documents must not have a key"}
		span.RecordError(err)
		return nil, err
	}
	if parsed.AssociationVariant != nil && len(*parsed.AssociationVariant) > domain.MaxAssociationVariantSize {
		err := domain.ValidationError{Field: "associationVariant", Message: fmt.Sprintf("associationVariant exceeds the maximum size of %d bytes", domain.MaxAssociationVariantSize)}
		span.RecordError(err)
		return nil, err
	}

	stack, err := uc.repo.GetHierarchicalRecordPolicies(ctx, *parsed.Associate)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	err = uc.policy.Eval(
		ctx,
		policy.RequestContext{
			Requester: requester,
			Self:      parsed,
		},
		stack,
		policyCreateAction(parsed),
		*parsed.Associate,
	)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	target := *parsed.Associate
	targetURI, err := concrnt.ParseCCURI(*parsed.Associate)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if targetURI.Scheme != "cckv" {
		err := fmt.Errorf("invalid associate: document associate scheme must be cckv")
		span.RecordError(err)
		return nil, err
	}

	documentID, err := sd.CDID()
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	ccfs := concrnt.CCURI{
		Scheme: "ccfs",
		Owner:  targetURI.Owner,
		Type:   concrnt.CCFSTypeConcrnt,
		CDID:   documentID,
	}.String()

	isLocal, err := uc.IsLocalEntityByCCID(ctx, targetURI.Owner)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	postProcesses := []PostProcessAction{}

	requesterSD, err := uc.GetSigned(ctx, requester.CCKVWithHint())
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	if isLocal {
		uniqueKey := *parsed.Associate + parsed.Author + parsed.Schema
		if parsed.AssociationVariant != nil {
			uniqueKey += *parsed.AssociationVariant
		}
		// bodyも一意性判定に含める(v1と同じ意味論)。bodyが異なるassociation
		// (同一ユーザーからの複数リプライ等)は共存できる
		bodyBytes, err := json.Marshal(parsed.Value)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		uniqueKey += string(bodyBytes)
		uniqueHash := xxh3.HashString(uniqueKey)

		if err := uc.repo.CreateCommitLog(ctx, tx, documentID, ip, sd.Document, sd.Proof, targetURI.Owner); err != nil {
			span.RecordError(err)
			return nil, err
		}

		applied, err := uc.repo.CreateAssociation(ctx, tx, documentID, *parsed.Associate, targetURI.Owner, parsed.Author, parsed.Schema, parsed.AssociationVariant, fmt.Sprintf("%x", uniqueHash), parsed.CreatedAt)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		if !applied {
			return &commitApplyResult{result: &sd, noop: true}, nil
		}

		postProcesses = append(postProcesses, uc.associationCountsTouched(*parsed.Associate, parsed.Schema, parsed.AssociationVariant, parsed.CreatedAt, mode))

		if mode == domain.CommitModeExecute {
			actions, err := uc.createReferenceDistributionActions(ctx, ip, parsed.Author, ccfs, requester, sd, distributionsFromPtr(parsed.Distributes), mode)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
			postProcesses = append(postProcesses, actions...)
		}
	}

	// signal
	if mode == domain.CommitModeExecute {
		distributions := []string{target}

		targetSD, err := uc.repo.GetSignedDocument(ctx, target)
		if err != nil { // ないとき
			sd, ok := sd.References[target]
			if !ok {
				span.RecordError(err)
				return nil, errors.New("target document not found in references")
			}
			targetSD = &sd

			// Note: none-proof targets are deliberately rejected here — they
			// carry no verifiable authorship, so a committer-supplied inline
			// copy cannot be trusted.
			if err := targetSD.Verify(ctx, uc.resolver()); err != nil {
				span.RecordError(err)
				return nil, errors.Join(errors.New("target document failed signature verification"), err)
			}

			var targetDoc concrnt.Document[any]
			err = json.Unmarshal([]byte(targetSD.Document), &targetDoc)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}

			distributions = append(distributions, distributionsFromPtr(targetDoc.Distributes)...)
		} else { // あるとき
			dists, err := uc.repo.GetDistributions(ctx, target)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
			distributions = append(distributions, dists...)
		}

		// 各タイムラインを購読しているクライアントに、そのタイムラインにassociationが追加されたことを通知するために、
		// association本体をリモートサーバーに転送し、eventの発生を促す。
		remoteKind := DeliveryRemoteNone
		if isLocal {
			remoteKind = DeliveryRemoteCommit
		}

		for _, channel := range distributions {
			remoteSD := concrnt.SignedDocument{
				Document: sd.Document,
				Proof:    sd.Proof,
				References: map[string]concrnt.SignedDocument{
					requester.CCKV(): *requesterSD,
					target:           *targetSD,
				},
			}
			postProcesses = append(postProcesses,
				func(ctx context.Context) error {
					// only the realtime event carries visibility flags — the
					// delivery payload is the signed document remote servers
					// must verify in full, so it stays untouched
					event := uc.markEventForAnonymous(ctx, concrnt.Event{
						Type:        "associated",
						URI:         target,
						Association: &ccfs,
						Timestamp:   time.Now(),
						References: map[string]concrnt.SignedDocument{
							ccfs: sd,
						},
					})
					return uc.jobs.Enqueue(ctx, JobTypeRecordDelivery, DeliveryJob{
						ResolveURI: channel,
						Payload:    remoteSD,
						Local:      DeliveryLocalPublish,
						Remote:     remoteKind,
						Event:      &event,
					})
				},
			)
		}
	}

	sd.CCFS = &ccfs

	return &commitApplyResult{result: &sd, postProcesses: postProcesses}, nil
}
