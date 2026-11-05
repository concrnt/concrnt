package record

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/cdid"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase"
	"github.com/concrnt/concrnt/policy"
	"github.com/concrnt/concrnt/schemas"
)

// rangeDeleteSyncLimit bounds how many targets a range delete removes inside
// the commit transaction (C4: a transaction's writes stay bounded; on
// Spanner every deleted row and index entry counts against the commit's
// mutation limit). The rest of the subtree is deleted page by page by
// RecordRangeDelete jobs.
const rangeDeleteSyncLimit = 500

func (uc *Usecase) deleteRecord(ctx context.Context, tx RepositoryTx, ip string, requester domain.Entity, sd concrnt.SignedDocument, mode domain.CommitMode) (*commitApplyResult, error) {
	ctx, span := tracer.Start(ctx, "Usecase.Record.Delete")
	defer span.End()

	var deletedoc concrnt.Document[schemas.Delete]
	err := json.Unmarshal([]byte(sd.Document), &deletedoc)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	rawTarget := string(deletedoc.Value)

	rangeBase, includeSelf, isRange, err := parseRangeDeleteTarget(rawTarget)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	targetHost, err := uc.client.ResolveResourceHost(ctx, rawTarget)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	authoritative := targetHost == uc.config.FQDN

	var targets []concrnt.SignedDocument
	var targetURIs []string // per-target address (cckv/ccfs URI) deletion and policy key on
	// deferred is set when the subtree is larger than one transaction
	// removes: the first page goes here, the rest to RecordRangeDelete jobs
	deferred := false
	if authoritative {
		if isRange {
			// without a job queue (offline tooling) the whole subtree is
			// removed here, as before
			limit := 0
			if uc.jobs != nil {
				limit = rangeDeleteSyncLimit + 1
			}
			targets, err = uc.repo.QueryRecordSubtree(ctx, rangeBase, includeSelf, "", limit)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
			if len(targets) == 0 {
				err := domain.NotFoundError{Resource: rawTarget}
				span.RecordError(err)
				return nil, err
			}
			if limit > 0 && len(targets) > rangeDeleteSyncLimit {
				targets = targets[:rangeDeleteSyncLimit]
				deferred = true
			}
			for _, target := range targets {
				targetURIs = append(targetURIs, *target.CCKV)
			}
		} else {
			targetSD, err := uc.repo.GetSignedDocument(ctx, rawTarget)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
			targets = []concrnt.SignedDocument{*targetSD}
			targetURIs = []string{rawTarget}
		}
	} else {
		if isRange {
			for refURI := range sd.References {
				if (includeSelf && refURI == rangeBase) || strings.HasPrefix(refURI, rangeBase+"/") {
					targetURIs = append(targetURIs, refURI)
				}
			}
			slices.Sort(targetURIs)
		} else if _, ok := sd.References[rawTarget]; ok {
			targetURIs = []string{rawTarget}
		}
		// no matching reference: this delete concerns nothing this server can
		// act on — pass it through as a no-op success (commitlog included, so
		// a later delivery that does carry the targets is not deduplicated away)
		if len(targetURIs) == 0 {
			return &commitApplyResult{result: &sd, noop: true}, nil
		}
		for _, targetURI := range targetURIs {
			targets = append(targets, sd.References[targetURI])
		}
	}

	targetDocs, err := unmarshalTargets(targets)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// evaluate the delete policy on every authoritative target before
	// removing anything
	if authoritative {
		for i := range targets {
			if err := uc.evalDeletePolicy(ctx, requester, targetDocs[i], targetURIs[i]); err != nil {
				span.RecordError(err)
				return nil, err
			}
		}
	}

	remoteKind := DeliveryRemoteNone
	if authoritative {
		// only the authoritative server re-federates the delete; a receiving
		// server acts on its own copies and signals its own subscribers
		remoteKind = DeliveryRemoteCommit
	}

	// A delete is committed the same way whichever way it arrived: from the
	// user, or forwarded by the author's server to a distribution destination
	// on the user's behalf (CIP-4 §6.1). The commit belongs to the owner of
	// the deleted namespace (CIP-3 §3.1) — every target of a range shares the
	// base's owner, so one commit log covers them all.
	documentID, err := sd.CDID()
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	parsedBase, err := concrnt.ParseCCURI(rangeBase)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := uc.repo.CreateCommitLog(ctx, tx, documentID, ip, sd.Document, sd.Proof, parsedBase.Owner); err != nil {
		span.RecordError(err)
		return nil, err
	}

	page := deletePage{
		ip: ip, requester: requester, delete: sd, mode: mode, authoritative: authoritative, remoteKind: remoteKind,
		targets: targets, targetDocs: targetDocs, targetURIs: targetURIs,
	}
	postProcesses, deletedAny, deleteRetained, err := uc.deleteTargets(ctx, tx, page)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// CIP-4 §6.1: a delete that removed nothing here (the forwarded targets
	// had no local reference rows) is not recorded — the tx rolls back — so
	// a later delivery that does find rows is not deduplicated away
	if !deletedAny {
		return &commitApplyResult{result: &sd, noop: true}, nil
	}

	if deferred {
		// the pages to come decide the fate of the delete commit (a retained
		// target in any page keeps it)
		job := RangeDeleteJob{
			IP: ip, Requester: requester.ID, Delete: sd, Base: rangeBase, IncludeSelf: includeSelf,
			AfterURI: targetURIs[len(targetURIs)-1], Retained: deleteRetained,
		}
		postProcesses = append(postProcesses, func(ctx context.Context) error {
			return uc.jobs.Enqueue(ctx, JobTypeRecordRangeDelete, job)
		})
	} else if !deleteRetained {
		if err := uc.repo.MarkCommitLogGcCandidate(ctx, tx, documentID); err != nil {
			span.RecordError(err)
			return nil, err
		}
	}

	// targets are URI-ordered, so with includeSelf the base record itself
	// leads and becomes the reported result
	return &commitApplyResult{result: &targets[0], postProcesses: postProcesses}, nil
}

func unmarshalTargets(targets []concrnt.SignedDocument) ([]concrnt.Document[any], error) {
	targetDocs := make([]concrnt.Document[any], len(targets))
	for i, target := range targets {
		if err := json.Unmarshal([]byte(target.Document), &targetDocs[i]); err != nil {
			return nil, err
		}
	}
	return targetDocs, nil
}

// evalDeletePolicy evaluates the delete policy of one authoritative target;
// an association is governed by the policy of what it is attached to.
func (uc *Usecase) evalDeletePolicy(ctx context.Context, requester domain.Entity, targetDoc concrnt.Document[any], targetURI string) error {
	policyRoot := targetURI
	if targetDoc.Associate != nil {
		policyRoot = *targetDoc.Associate
	}
	stack, err := uc.repo.GetHierarchicalRecordPolicies(ctx, policyRoot)
	if err != nil {
		return err
	}
	return uc.policy.Eval(
		ctx,
		policy.RequestContext{Requester: requester, Self: targetDoc},
		stack,
		policyDeleteAction(targetDoc),
		targetURI,
	)
}

// deletePage is one batch of delete targets with everything deleteTargets
// needs to remove them: targets, targetDocs and targetURIs are parallel.
type deletePage struct {
	ip            string
	requester     domain.Entity
	delete        concrnt.SignedDocument
	mode          domain.CommitMode
	authoritative bool
	remoteKind    DeliveryRemoteKind
	targets       []concrnt.SignedDocument
	targetDocs    []concrnt.Document[any]
	targetURIs    []string
}

// deleteTargets removes the page's targets inside tx (the delete commit is
// already recorded there) and returns the actions to run once it commits.
// deletedAny reports that something was removed; deleteRetained that a
// removed document had onUpdate=retain, which keeps the delete commit too.
func (uc *Usecase) deleteTargets(ctx context.Context, tx RepositoryTx, page deletePage) (postProcesses []PostProcessAction, deletedAny bool, deleteRetained bool, err error) {
	ctx, span := tracer.Start(ctx, "Usecase.Record.DeleteTargets")
	defer span.End()

	sd := page.delete
	requester := page.requester
	mode := page.mode
	authoritative := page.authoritative

	// History is only kept for what asked for it: a deleted document with
	// onUpdate=retain keeps its commit and, with it, this delete's (the
	// replay needs both); everything else is flagged for gc once the
	// backdate window has passed (the replay guard then holds on its own).
	flagDeleted := func(deleted *concrnt.SignedDocument, onUpdate *string) error {
		deletedAny = true
		if onUpdate != nil && *onUpdate == "retain" {
			deleteRetained = true
			return nil
		}
		deletedID, err := deleted.CDID()
		if err != nil {
			return err
		}
		return uc.repo.MarkCommitLogGcCandidate(ctx, tx, deletedID)
	}

	postProcesses = []PostProcessAction{}
	for i := range page.targets {
		targetSD := &page.targets[i]
		targetDoc := page.targetDocs[i]
		targetURI := page.targetURIs[i]

		var removedTimeline, removedItemID string

		if authoritative {
			switch targetDoc.Kind {
			case "record":

				// capture which chunkline item this record occupies before the
				// delete cascades its record_keys row away; advertised via
				// /chunkline/removed so readers can drop it from cached chunks
				if mode == domain.CommitModeExecute && uc.kvs != nil && targetSD.CCKV != nil {
					tl, id, err := uc.repo.GetTimelineRemoval(ctx, *targetSD.CCKV)
					if err != nil {
						span.RecordError(err) // non-fatal: the deleted item just lingers in caches
					} else {
						removedTimeline, removedItemID = tl, id
					}
				}

				parsedURI, err := concrnt.ParseCCURI(targetURI)
				if err != nil {
					span.RecordError(err)
					return nil, false, false, err
				}

				switch parsedURI.Scheme {
				case "cckv":
					err = uc.repo.DeleteRecordByKey(ctx, tx, targetURI)
					if err != nil {
						span.RecordError(err)
						return nil, false, false, err
					}
					// the key's associations cascaded away with the record
					postProcesses = append(postProcesses, uc.associationCountsOfTargetInvalidated(targetURI))
				case "ccfs":
					if parsedURI.Type != concrnt.CCFSTypeConcrnt {
						err := errors.New("unsupported ccfs type for delete record: " + parsedURI.Type)
						span.RecordError(err)
						return nil, false, false, err
					}
					err = uc.repo.DeleteRecordByDocumentID(ctx, tx, parsedURI.CDID)
					if err != nil {
						span.RecordError(err)
						return nil, false, false, err
					}
				default:
					err := errors.New("unsupported document scheme for delete record: " + parsedURI.Scheme)
					span.RecordError(err)
					return nil, false, false, err
				}

				if err := flagDeleted(targetSD, targetDoc.OnUpdate); err != nil {
					span.RecordError(err)
					return nil, false, false, err
				}

			case "association":

				parsedURI, err := concrnt.ParseCCURI(targetURI)
				if err != nil {
					span.RecordError(err)
					return nil, false, false, err
				}

				if parsedURI.Scheme != "ccfs" || parsedURI.Type != concrnt.CCFSTypeConcrnt {
					err := errors.New("unsupported document scheme for delete association: " + targetURI)
					span.RecordError(err)
					return nil, false, false, err
				}

				removed, err := uc.repo.DeleteAssociation(ctx, tx, parsedURI.CDID)
				if err != nil {
					span.RecordError(err)
					return nil, false, false, err
				}
				if removed && targetDoc.Associate != nil {
					postProcesses = append(postProcesses, uc.associationCountsTouched(*targetDoc.Associate, targetDoc.Schema, targetDoc.AssociationVariant, -1, mode))
				}

				if err := flagDeleted(targetSD, nil); err != nil {
					span.RecordError(err)
					return nil, false, false, err
				}
			default:
				err := errors.New("unsupported document kind for delete: " + targetDoc.Kind)
				span.RecordError(err)
				return nil, false, false, err
			}
		}

		// sweep the reference records createReferenceDistributionActions left
		// on this server: their key is destination + "/" + the hash-based CDID
		// of the target's href (its cckv key for records, its ccfs URI for
		// keyless documents), so a row existing under that key is exactly
		// "this destination was distributed to and lives here" — remote and
		// never-delivered destinations fall out as not-found. The href is
		// re-derived from the signed target document itself, mirroring the
		// creation side, so it holds regardless of how the delete addressed
		// the target (cckv or ccfs)
		href := targetDoc.Key
		if targetDoc.Kind != "record" {
			if targetDoc.Associate == nil {
				err := errors.New("unsupported document kind for distribution sweep: " + targetDoc.Kind)
				span.RecordError(err)
				return nil, false, false, err
			}
			parsedAssociate, err := concrnt.ParseCCURI(*targetDoc.Associate)
			if err != nil {
				span.RecordError(err)
				return nil, false, false, err
			}
			documentID, err := targetSD.CDID()
			if err != nil {
				span.RecordError(err)
				return nil, false, false, err
			}
			href = concrnt.CCURI{
				Scheme: "ccfs",
				Owner:  parsedAssociate.Owner,
				Type:   concrnt.CCFSTypeConcrnt,
				CDID:   documentID,
			}.String()
		}
		refSegment := cdid.MakeHash([]byte(href)).String()
		refKeys := []string{}
		for _, dest := range distributionsFromPtr(targetDoc.Distributes) {
			refKey, err := url.JoinPath(dest, refSegment)
			if err != nil {
				slog.Error("failed to join path for distribution sweep", slog.String("destination", dest), slog.String("href", href), slog.String("error", err.Error()))
				continue
			}
			refKeys = append(refKeys, refKey)
		}
		refSDs := map[string]*concrnt.SignedDocument{}
		if len(refKeys) > 0 {
			refSDs, err = uc.repo.GetSignedDocuments(ctx, refKeys)
			if err != nil {
				span.RecordError(err)
				return nil, false, false, err
			}
		}
		for _, refKey := range refKeys {
			refSD, ok := refSDs[refKey]
			if !ok {
				continue
			}

			var refDoc concrnt.Document[any]
			err = json.Unmarshal([]byte(refSD.Document), &refDoc)
			if err != nil {
				span.RecordError(err)
				return nil, false, false, err
			}

			stack, err := uc.repo.GetHierarchicalRecordPolicies(ctx, refKey)
			if err != nil {
				span.RecordError(err)
				return nil, false, false, err
			}

			err = uc.policy.Eval(
				ctx,
				policy.RequestContext{
					Requester: requester,
					Self:      refDoc,
				},
				stack,
				policyDeleteAction(refDoc),
				refKey,
			)
			if errors.Is(err, domain.ErrPermissionDenied) {
				slog.Info("reference record kept: destination policy denied the delete", slog.String("ref_key", refKey), slog.String("requester", requester.ID))
				continue
			}
			if err != nil {
				span.RecordError(err)
				return nil, false, false, err
			}

			if mode == domain.CommitModeExecute && uc.kvs != nil {
				tl, id, err := uc.repo.GetTimelineRemoval(ctx, refKey)
				if err != nil {
					span.RecordError(err) // non-fatal: the deleted item just lingers in caches
				} else if tl != "" {
					postProcesses = append(postProcesses, func(ctx context.Context) error {
						return uc.kvs.SetAdd(ctx, usecase.RemovedItemsKey(tl), id, usecase.RemovedItemsTTL)
					})
				}
			}

			err = uc.repo.DeleteRecordByKey(ctx, tx, refKey)
			if err != nil {
				span.RecordError(err)
				return nil, false, false, err
			}
			postProcesses = append(postProcesses, uc.associationCountsOfTargetInvalidated(refKey))

			if err := flagDeleted(refSD, refDoc.OnUpdate); err != nil {
				span.RecordError(err)
				return nil, false, false, err
			}
		}

		if mode != domain.CommitModeExecute {
			continue
		}

		if uc.kvs != nil && removedTimeline != "" {
			tl, id := removedTimeline, removedItemID
			postProcesses = append(postProcesses, func(ctx context.Context) error {
				return uc.kvs.SetAdd(ctx, usecase.RemovedItemsKey(tl), id, usecase.RemovedItemsTTL)
			})
		}

		destinations := []string{targetURI}
		if targetDoc.Distributes != nil {
			destinations = append(destinations, *targetDoc.Distributes...)
		}
		for _, dest := range destinations {
			remoteSD := concrnt.SignedDocument{
				Document: sd.Document,
				Proof:    sd.Proof,
				References: map[string]concrnt.SignedDocument{
					targetURI: *targetSD,
				},
			}
			postProcesses = append(
				postProcesses,
				func(ctx context.Context) error {
					return uc.enqueueEventDelivery(ctx, DeliveryJob{
						ResolveURI: dest,
						Payload:    remoteSD,
						Local:      DeliveryLocalPublish,
						Remote:     page.remoteKind,
						Event: &concrnt.Event{
							Type:      "deleted",
							URI:       targetURI,
							Timestamp: time.Now(),
						},
					})
				},
			)
		}

		if targetDoc.Associate != nil {
			associatedURI := *targetDoc.Associate

			var associatedSD *concrnt.SignedDocument
			if authoritative {
				associatedSD, err = uc.repo.GetSignedDocument(ctx, associatedURI)
				if err != nil {
					slog.Error("failed to fetch associated document for signal", slog.String("associated_uri", associatedURI), slog.String("error", err.Error()))
					span.RecordError(err)
					return nil, false, false, err
				}
			} else {
				ref, ok := sd.References[associatedURI]
				if !ok {
					err := errors.New("associated document not found in references for remote delete")
					slog.Error("associated document not found in references for remote delete", slog.String("associated_uri", associatedURI))
					span.RecordError(err)
					return nil, false, false, err
				}
				associatedSD = &ref
			}

			var associatedDoc concrnt.Document[any]
			err = json.Unmarshal([]byte(associatedSD.Document), &associatedDoc)
			if err != nil {
				slog.Error("failed to unmarshal associated document for signal", slog.String("associated_uri", associatedURI), slog.String("error", err.Error()))
				span.RecordError(err)
				return nil, false, false, err
			}

			destinations := []string{associatedURI}
			if associatedDoc.Distributes != nil {
				destinations = append(destinations, *associatedDoc.Distributes...)
			}

			for _, dest := range destinations {
				remoteSD := concrnt.SignedDocument{
					Document: sd.Document,
					Proof:    sd.Proof,
					References: map[string]concrnt.SignedDocument{
						targetURI:     *targetSD,
						associatedURI: *associatedSD,
					},
				}
				postProcesses = append(postProcesses,
					func(ctx context.Context) error {
						return uc.enqueueEventDelivery(ctx, DeliveryJob{
							ResolveURI: dest,
							Payload:    remoteSD,
							Local:      DeliveryLocalPublish,
							Remote:     page.remoteKind,
							Event: &concrnt.Event{
								Type:      "unassociated",
								URI:       associatedURI,
								Timestamp: time.Now(),
							},
						})
					},
				)
			}
		}
	}

	return postProcesses, deletedAny, deleteRetained, nil
}

// parseRangeDeleteTarget detects the trailing-asterisk range notation on a
// delete target: "cckv://.../item*" selects item itself plus its subtree,
// "cckv://.../item/*" selects the subtree only. Matching is by path hierarchy,
// not string prefix — "item2" is never part of "item*". A target without a
// trailing '*' is a plain single-key delete (isRange=false, no validation).
func parseRangeDeleteTarget(targetURI string) (base string, includeSelf bool, isRange bool, err error) {
	switch {
	case strings.HasSuffix(targetURI, "/*"):
		base = strings.TrimSuffix(targetURI, "/*")
	case strings.HasSuffix(targetURI, "*"):
		base = strings.TrimSuffix(targetURI, "*")
		includeSelf = true
	default:
		return targetURI, false, false, nil
	}

	if strings.Contains(base, "*") {
		return "", false, true, domain.ValidationError{Field: "value", Message: "'*' is only allowed as a trailing range sentinel in a delete target"}
	}
	parsed, perr := concrnt.ParseCCURI(base)
	if perr != nil {
		return "", false, true, domain.ValidationError{Field: "value", Message: "invalid range delete target: " + perr.Error()}
	}
	if parsed.Scheme != "cckv" {
		return "", false, true, domain.ValidationError{Field: "value", Message: "range delete targets must use the cckv scheme"}
	}
	if parsed.Key == "" {
		return "", false, true, domain.ValidationError{Field: "value", Message: "range delete requires a non-empty key"}
	}
	return base, includeSelf, true, nil
}
