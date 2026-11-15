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

// rangeDeleteSyncLimit bounds how many targets one ApplyDelete call removes
// (a write stays bounded: on Spanner every deleted row and index entry
// counts against the commit's mutation limit, on Firestore against the
// commit size). A range delete removes that many with its commit; the rest
// of the subtree is deleted page by page by RecordRangeDelete jobs.
const rangeDeleteSyncLimit = 500

func (uc *Usecase) planDelete(ctx context.Context, ip string, requester domain.Entity, sd concrnt.SignedDocument, mode domain.CommitMode) (*commitPlan, error) {
	ctx, span := tracer.Start(ctx, "Usecase.Record.PlanDelete")
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
	// deferred is set when the subtree is larger than one write removes:
	// the first page goes with the commit, the rest to RecordRangeDelete jobs
	deferred := false
	if authoritative {
		if isRange {
			// without a job queue (offline tooling) the whole subtree is
			// removed here
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
			return &commitPlan{noop: true, unapplied: &sd}, nil
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
	// planning any removal
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

	page := deletePage{
		ip: ip, requester: requester, delete: sd, mode: mode, authoritative: authoritative, remoteKind: remoteKind,
		targets: targets, targetDocs: targetDocs, targetURIs: targetURIs,
	}
	pagePlan, err := uc.planDeleteTargets(ctx, page)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// CIP-4 §6.1: a delete that removes nothing here (the forwarded targets
	// have no local reference rows) is not recorded, so a later delivery
	// that does find rows is not deduplicated away
	if !pagePlan.deletedAny {
		return &commitPlan{noop: true, unapplied: &sd}, nil
	}

	postProcesses := pagePlan.postProcesses
	if deferred {
		// the pages to come decide the fate of the delete commit (a retained
		// target in any page keeps it)
		job := RangeDeleteJob{
			IP: ip, Requester: requester.ID, Delete: sd, Base: rangeBase, IncludeSelf: includeSelf,
			AfterURI: targetURIs[len(targetURIs)-1], Retained: pagePlan.retained,
		}
		postProcesses = append(postProcesses, func(ctx context.Context) error {
			return uc.jobs.Enqueue(ctx, JobTypeRecordRangeDelete, job)
		})
	} else if !pagePlan.retained {
		pagePlan.gcCommitIDs = append(pagePlan.gcCommitIDs, documentID)
	}

	commit := Commit{ID: documentID, IP: ip, Document: sd.Document, Proof: sd.Proof, Owner: parsedBase.Owner}

	// targets are URI-ordered, so with includeSelf the base record itself
	// leads and becomes the reported result
	return &commitPlan{
		apply: func(ctx context.Context) (ApplyStatus, error) {
			return uc.applyDeletePage(ctx, &commit, pagePlan)
		},
		result:        &targets[0],
		unapplied:     &sd,
		postProcesses: postProcesses,
	}, nil
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
// an association is governed by the policy of what it is attached to, and a
// record by the stack of its key however the delete addressed it (a version
// its key has moved on from has no key pointing at it to look the stack up
// by).
func (uc *Usecase) evalDeletePolicy(ctx context.Context, requester domain.Entity, targetDoc concrnt.Document[any], targetURI string) error {
	policyRoot := targetURI
	if targetDoc.Associate != nil {
		policyRoot = *targetDoc.Associate
	} else if targetDoc.Kind == "record" && targetDoc.Key != "" {
		policyRoot = targetDoc.Key
	}
	stack, err := uc.policyStack(ctx, policyRoot)
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

// deletePage is one batch of delete targets with everything
// planDeleteTargets needs to plan their removal: targets, targetDocs and
// targetURIs are parallel.
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

// deletePagePlan is one ApplyDelete call in the making.
type deletePagePlan struct {
	removals    []Removal
	gcCommitIDs []string
	// deletedAny reports that the page removes something; retained that a
	// removed document has onUpdate=retain, which keeps the delete commit
	// too.
	deletedAny bool
	retained   bool
	// postProcesses run once the page is applied; outcome is what they
	// read the result from.
	postProcesses []PostProcessAction
	outcome       *DeleteOutcome
}

// applyDeletePage is the page's one repository write. commit is the delete
// document, nil on the continuation pages of a range delete.
func (uc *Usecase) applyDeletePage(ctx context.Context, commit *Commit, plan *deletePagePlan) (ApplyStatus, error) {
	outcome, err := uc.repo.ApplyDelete(ctx, DeleteCommit{Commit: commit, Removals: plan.removals, GcCommitIDs: plan.gcCommitIDs})
	if err != nil {
		return outcome.Status, err
	}
	*plan.outcome = outcome
	return outcome.Status, nil
}

// planDeleteTargets plans the removal of the page's targets and of the
// reference records distributing them left on this server, and the actions
// to run once the page is applied. Every read happens here; nothing is
// removed.
func (uc *Usecase) planDeleteTargets(ctx context.Context, page deletePage) (*deletePagePlan, error) {
	ctx, span := tracer.Start(ctx, "Usecase.Record.PlanDeleteTargets")
	defer span.End()

	sd := page.delete
	requester := page.requester
	mode := page.mode
	authoritative := page.authoritative

	var err error
	plan := &deletePagePlan{outcome: &DeleteOutcome{}}
	plan.postProcesses = []PostProcessAction{uc.cleanupsScheduled(plan.outcome)}

	// History is only kept for what asked for it: a deleted document with
	// onUpdate=retain keeps its commit and, with it, this delete's (the
	// replay needs both); everything else is flagged for gc once the
	// backdate window has passed (the replay guard then holds on its own).
	flagDeleted := func(deleted *concrnt.SignedDocument, onUpdate *string) error {
		plan.deletedAny = true
		if onUpdate != nil && *onUpdate == "retain" {
			plan.retained = true
			return nil
		}
		deletedID, err := deleted.CDID()
		if err != nil {
			return err
		}
		plan.gcCommitIDs = append(plan.gcCommitIDs, deletedID)
		return nil
	}

	for i := range page.targets {
		targetSD := &page.targets[i]
		targetDoc := page.targetDocs[i]
		targetURI := page.targetURIs[i]

		var removedTimeline, removedItemID string

		if authoritative {
			plan.postProcesses = append(plan.postProcesses, uc.documentCacheInvalidated(&targetURI, targetSD.CCKV, targetSD.CCFS))
			switch targetDoc.Kind {
			case "record":

				// capture which chunkline item this record occupies while its
				// key is still there; advertised via
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
					return nil, err
				}

				// a deleted record may have been a layer of its subtree's
				// stacks; deletes are rare enough to drop the owner's stacks
				plan.postProcesses = append(plan.postProcesses, uc.policyStacksInvalidated(parsedURI.Owner))

				switch parsedURI.Scheme {
				case "cckv":
					plan.removals = append(plan.removals, Removal{Kind: RemoveKey, URI: targetURI})
					// the key's associations go with it
					plan.postProcesses = append(plan.postProcesses, uc.associationCountsInvalidated(targetURI))
				case "ccfs":
					if parsedURI.Type != concrnt.CCFSTypeConcrnt {
						err := errors.New("unsupported ccfs type for delete record: " + parsedURI.Type)
						span.RecordError(err)
						return nil, err
					}
					plan.removals = append(plan.removals, Removal{Kind: RemoveVersion, URI: targetDoc.Key, DocumentID: parsedURI.CDID})
				default:
					err := errors.New("unsupported document scheme for delete record: " + parsedURI.Scheme)
					span.RecordError(err)
					return nil, err
				}

				if err := flagDeleted(targetSD, targetDoc.OnUpdate); err != nil {
					span.RecordError(err)
					return nil, err
				}

			case "association":

				parsedURI, err := concrnt.ParseCCURI(targetURI)
				if err != nil {
					span.RecordError(err)
					return nil, err
				}

				if parsedURI.Scheme != "ccfs" || parsedURI.Type != concrnt.CCFSTypeConcrnt {
					err := errors.New("unsupported document scheme for delete association: " + targetURI)
					span.RecordError(err)
					return nil, err
				}

				removal := Removal{Kind: RemoveAssociation, DocumentID: parsedURI.CDID}
				if targetDoc.Associate != nil {
					removal.URI = *targetDoc.Associate
					plan.postProcesses = append(plan.postProcesses, uc.associationCountsRemoved(*targetDoc.Associate, targetDoc.Schema, targetDoc.AssociationVariant, plan.outcome, len(plan.removals), mode))
				}
				plan.removals = append(plan.removals, removal)

				if err := flagDeleted(targetSD, nil); err != nil {
					span.RecordError(err)
					return nil, err
				}
			default:
				err := errors.New("unsupported document kind for delete: " + targetDoc.Kind)
				span.RecordError(err)
				return nil, err
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
				return nil, err
			}
			parsedAssociate, err := concrnt.ParseCCURI(*targetDoc.Associate)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
			documentID, err := targetSD.CDID()
			if err != nil {
				span.RecordError(err)
				return nil, err
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
				return nil, err
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
				return nil, err
			}

			stack, err := uc.policyStack(ctx, refKey)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
			if parsedRef, err := concrnt.ParseCCURI(refKey); err == nil && policyLayerAt(stack, refKey) {
				plan.postProcesses = append(plan.postProcesses, uc.policyStacksInvalidated(parsedRef.Owner))
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
				return nil, err
			}

			if mode == domain.CommitModeExecute && uc.kvs != nil {
				tl, id, err := uc.repo.GetTimelineRemoval(ctx, refKey)
				if err != nil {
					span.RecordError(err) // non-fatal: the deleted item just lingers in caches
				} else if tl != "" {
					plan.postProcesses = append(plan.postProcesses, func(ctx context.Context) error {
						return uc.kvs.SetAdd(ctx, usecase.RemovedItemsKey(tl), id, usecase.RemovedItemsTTL)
					})
				}
			}

			plan.removals = append(plan.removals, Removal{Kind: RemoveKey, URI: refKey})
			plan.postProcesses = append(plan.postProcesses, uc.associationCountsInvalidated(refKey), uc.documentCacheInvalidated(&refKey, refSD.CCFS))

			if err := flagDeleted(refSD, refDoc.OnUpdate); err != nil {
				span.RecordError(err)
				return nil, err
			}
		}

		if mode != domain.CommitModeExecute {
			continue
		}

		if uc.kvs != nil && removedTimeline != "" {
			tl, id := removedTimeline, removedItemID
			plan.postProcesses = append(plan.postProcesses, func(ctx context.Context) error {
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
			plan.postProcesses = append(
				plan.postProcesses,
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
					return nil, err
				}
			} else {
				ref, ok := sd.References[associatedURI]
				if !ok {
					err := errors.New("associated document not found in references for remote delete")
					slog.Error("associated document not found in references for remote delete", slog.String("associated_uri", associatedURI))
					span.RecordError(err)
					return nil, err
				}
				associatedSD = &ref
			}

			var associatedDoc concrnt.Document[any]
			err = json.Unmarshal([]byte(associatedSD.Document), &associatedDoc)
			if err != nil {
				slog.Error("failed to unmarshal associated document for signal", slog.String("associated_uri", associatedURI), slog.String("error", err.Error()))
				span.RecordError(err)
				return nil, err
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
				plan.postProcesses = append(plan.postProcesses,
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

	return plan, nil
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
