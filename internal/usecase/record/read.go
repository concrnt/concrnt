package record

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/impl/interop"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/policy"
	"github.com/concrnt/concrnt/schemas"
)

func (uc *Usecase) GetSigned(ctx context.Context, uri string) (*concrnt.SignedDocument, error) {
	ctx, span := tracer.Start(ctx, "Usecase.Record.GetSigned")
	defer span.End()

	parsed, err := concrnt.ParseCCURI(uri)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	if parsed.Scheme == "cckv" && parsed.Key == "" { // entity document
		entity, err := uc.GetEntity(ctx, uri)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		return entity.SignedDocument, nil
	} else {
		sd, err := uc.repo.GetSignedDocument(ctx, uri)
		if err != nil {
			return nil, err
		}

		var doc concrnt.Document[schemas.Reference]
		err = json.Unmarshal([]byte(sd.Document), &doc)
		if err == nil && doc.Schema == schemas.ReferenceURL {

			newLocation, err := uc.client.ResolveResourceURI(ctx, doc.Value.Href, nil)
			if err != nil {
				span.RecordError(err)
				return nil, err
			}

			return nil, domain.RedirectError{
				Location: newLocation,
				Body:     sd,
			}
		}

		err = uc.checkReadAccess(ctx, uri, *sd)
		if err != nil {
			return nil, err
		}

		return sd, nil
	}
}

func (uc *Usecase) checkReadAccess(ctx context.Context, uri string, sd concrnt.SignedDocument) error {
	requester, _ := ctx.Value(interop.RequesterCtxKey).(domain.Entity)
	return uc.checkReadAccessAs(ctx, uri, sd, requester)
}

func (uc *Usecase) checkReadAccessAs(ctx context.Context, uri string, sd concrnt.SignedDocument, requester domain.Entity) error {
	ctx, span := tracer.Start(ctx, "Usecase.Record.CheckReadAccess")
	defer span.End()

	if uri == "" {
		err := errors.New("uri is required for read access evaluation")
		span.RecordError(err)
		return err
	}

	// A key-only query row (CIP-5 §3.2.1 intermediate key) has no document;
	// its read access is record:read on the key itself, evaluated against the
	// ancestor policy stack alone.
	var doc concrnt.Document[any]
	if sd.Document == "" {
		doc = concrnt.Document[any]{Kind: "record", Key: uri}
	} else {
		err := json.Unmarshal([]byte(sd.Document), &doc)
		if err != nil {
			span.RecordError(err)
			return err
		}
	}

	// Associations have no record_keys row of their own; their policy stack is
	// rooted at the associated document (CIP-12 §5.3), same as deleteRecord.
	policyRoot := uri
	if doc.Associate != nil {
		policyRoot = *doc.Associate
	}

	stack, err := uc.repo.GetHierarchicalRecordPolicies(ctx, policyRoot)
	if err != nil {
		span.RecordError(err)
		stack = []concrnt.Policy{}
	}

	err = uc.policy.Eval(
		ctx,
		policy.RequestContext{
			Requester: requester,
			Self:      doc,
		},
		stack,
		policyReadAction(doc),
		uri,
	)
	if err != nil {
		span.RecordError(err)
		return err
	}

	return nil
}

func (uc *Usecase) markEventForAnonymous(ctx context.Context, event concrnt.Event) concrnt.Event {
	if len(event.References) == 0 {
		return event
	}
	marked := make(map[string]concrnt.SignedDocument, len(event.References))
	for uri, refSD := range event.References {
		public := uc.checkReadAccessAs(ctx, uri, refSD, domain.Entity{}) == nil
		refSD.IsPublic = &public
		// Nested references embed further documents (e.g. the distributed
		// original inside a timeline reference) that the loop above never
		// sees — apply the same anonymous evaluation to them.
		var nested map[string]concrnt.SignedDocument
		if len(refSD.References) > 0 {
			nested = make(map[string]concrnt.SignedDocument, len(refSD.References))
			for nestedURI, nestedSD := range refSD.References {
				nestedPublic := uc.checkReadAccessAs(ctx, nestedURI, nestedSD, domain.Entity{}) == nil
				nestedSD.IsPublic = &nestedPublic
				nestedSD.References = nil
				nested[nestedURI] = nestedSD
			}
		}
		refSD.References = nested
		marked[uri] = refSD
	}
	event.References = marked
	return event
}
