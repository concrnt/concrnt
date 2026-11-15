package record

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/policy"
	"github.com/concrnt/concrnt/schemas"
)

func (uc *Usecase) planRecord(ctx context.Context, ip string, requester domain.Entity, parsed concrnt.Document[any], sd concrnt.SignedDocument, mode domain.CommitMode) (*commitPlan, error) {
	ctx, span := tracer.Start(ctx, "Usecase.Record.PlanRecord")
	defer span.End()

	stack, err := uc.policyStack(ctx, parsed.Key)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	action := "record:create"
	policySelf := parsed

	// the stored version decides the policy action; whether the key was
	// fresh when the write landed is the repository's to say (RecordOutcome)
	existingSD, err := uc.repo.GetSignedDocument(ctx, parsed.Key)
	if err == nil {
		var existingDoc concrnt.Document[any]
		err = json.Unmarshal([]byte(existingSD.Document), &existingDoc)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		action = "record:update"
		policySelf = existingDoc
	} else if !errors.Is(err, domain.ErrNotFound) {
		span.RecordError(err)
		return nil, err
	}

	err = uc.policy.Eval(
		ctx,
		policy.RequestContext{
			Requester: requester,
			Self:      policySelf,
		},
		stack,
		action,
		parsed.Key,
	)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	parsedKey, err := concrnt.ParseCCURI(parsed.Key)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	if parsedKey.Scheme != "cckv" {
		err := fmt.Errorf("invalid key: document key scheme must be cckv")
		span.RecordError(err)
		return nil, err
	}
	// '*' is reserved as the range-delete sentinel (parseRangeDeleteTarget):
	// a key containing it could never be addressed by an exact-match delete
	// unambiguously.
	if strings.Contains(parsedKey.Key, "*") {
		err := domain.ValidationError{Field: "key", Message: "record key must not contain '*'"}
		span.RecordError(err)
		return nil, err
	}
	// CIP-0 §7: a cckv key component is 1..1024 bytes. An empty key
	// (cckv://<owner>) addresses the entity itself, not a record slot.
	if parsedKey.Key == "" {
		err := domain.ValidationError{Field: "key", Message: "record key must not be empty"}
		span.RecordError(err)
		return nil, err
	}
	if len(parsedKey.Key) > domain.MaxRecordKeySize {
		err := domain.ValidationError{Field: "key", Message: fmt.Sprintf("record key exceeds the maximum size of %d bytes", domain.MaxRecordKeySize)}
		span.RecordError(err)
		return nil, err
	}

	var policies *string
	if parsed.Policy != nil {
		policyBytes, err := json.Marshal(parsed.Policy)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		policyStr := string(policyBytes)
		policies = &policyStr
	}

	distributions := []string{}
	if parsed.Distributes != nil {
		distributions = *parsed.Distributes
	}

	schema := parsed.Schema
	createdAt := parsed.CreatedAt
	var redirect *string

	if parsed.Schema == schemas.ReferenceURL {
		var refDoc concrnt.Document[schemas.Reference]
		err := json.Unmarshal([]byte(sd.Document), &refDoc)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		redirect = &refDoc.Value.Href

		refSD, ok := sd.References[refDoc.Value.Href]
		if ok {
			var targetDoc concrnt.Document[any]
			err = json.Unmarshal([]byte(refSD.Document), &targetDoc)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}
			schema = targetDoc.Schema
			createdAt = targetDoc.CreatedAt
		} else {
			if refDoc.Value.Schema != nil {
				schema = *refDoc.Value.Schema
			}
			if refDoc.Value.CreatedAt != nil {
				createdAt = *refDoc.Value.CreatedAt
			}
		}
	}

	documentID, err := sd.CDID()
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	commit := RecordCommit{
		Commit:        Commit{ID: documentID, IP: ip, Document: sd.Document, Proof: sd.Proof, Owner: parsedKey.Owner},
		Key:           parsed.Key,
		Author:        parsed.Author,
		Schema:        schema,
		OnUpdate:      parsed.OnUpdate,
		Policies:      policies,
		Distributions: distributions,
		Redirect:      redirect,
		CreatedAt:     createdAt,
	}

	var outcome RecordOutcome
	apply := func(ctx context.Context) (ApplyStatus, error) {
		var err error
		outcome, err = uc.repo.ApplyRecord(ctx, commit)
		return outcome.Status, err
	}

	resultURI := parsed.Key
	postProcesses := []PostProcessAction{}

	// the stored record is a layer of its subtree's stacks when it carries a
	// policy or distributions, and the one it replaced may have been
	if parsed.Policy != nil || parsed.Distributes != nil || policyLayerAt(stack, parsed.Key) {
		postProcesses = append(postProcesses, uc.policyStacksInvalidated(parsedKey.Owner))
	}

	postProcesses = append(postProcesses, func(ctx context.Context) error {
		if outcome.FreshKey {
			return uc.associationCountsSeededEmpty(parsed.Key)(ctx)
		}
		var previous *string
		if outcome.PreviousID != "" {
			ccfs := concrnt.ComposeCCFSURI(parsedKey.Owner, concrnt.CCFSTypeConcrnt, outcome.PreviousID)
			previous = &ccfs
		}
		return uc.documentCacheInvalidated(&parsed.Key, previous)(ctx)
	})

	if mode == domain.CommitModeExecute {
		postProcesses = append(postProcesses, func(ctx context.Context) error {
			// runs post-commit, so the anonymous read evaluation sees the
			// just-stored record's own policy
			return uc.signal.Publish(ctx, resultURI, uc.markEventForAnonymous(ctx, concrnt.Event{
				Type:       "created",
				URI:        resultURI,
				References: map[string]concrnt.SignedDocument{resultURI: sd},
				Timestamp:  createdAt,
			}))
		})
	}

	if mode == domain.CommitModeExecute && parsed.Distributes != nil {
		actions, err := uc.createReferenceDistributionActions(ctx, ip, parsed.Author, resultURI, requester, sd, *parsed.Distributes, mode)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		postProcesses = append(postProcesses, actions...)
	}

	unapplied := sd

	sd.CCKV = &parsed.Key
	ccfs := concrnt.CCURI{
		Scheme: "ccfs",
		Owner:  parsedKey.Owner,
		Type:   concrnt.CCFSTypeConcrnt,
		CDID:   documentID,
	}.String()

	sd.CCFS = &ccfs

	return &commitPlan{apply: apply, result: &sd, unapplied: &unapplied, postProcesses: postProcesses}, nil
}
