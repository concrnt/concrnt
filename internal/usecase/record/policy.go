package record

import (
	"context"
	"maps"

	"github.com/concrnt/concrnt"
)

// Not GetHierarchicalRecordPoliciesMulti for everything: it takes cckv keys
// only, and a ccfs URI (an association's document) has to be mapped to its
// key first.
func (uc *Usecase) policyStacks(ctx context.Context, uris []string) (map[string][]concrnt.Policy, error) {
	stacks := make(map[string][]concrnt.Policy, len(uris))
	var cckvs []string
	for _, uri := range uris {
		parsed, err := concrnt.ParseCCURI(uri)
		if err != nil {
			return nil, err
		}
		if parsed.Scheme != "cckv" {
			stack, err := uc.repo.GetHierarchicalRecordPolicies(ctx, uri)
			if err != nil {
				return nil, err
			}
			stacks[uri] = stack
			continue
		}
		cckvs = append(cckvs, uri)
	}
	if len(cckvs) == 0 {
		return stacks, nil
	}

	found, err := uc.repo.GetHierarchicalRecordPoliciesMulti(ctx, cckvs)
	if err != nil {
		return nil, err
	}
	maps.Copy(stacks, found)
	return stacks, nil
}

func policyCreateAction(doc concrnt.Document[any]) string {
	if doc.Kind == "association" {
		return "association:create"
	}
	return "record:create"
}

func policyReadAction(doc concrnt.Document[any]) string {
	if doc.Kind == "association" {
		return "association:read"
	}
	return "record:read"
}

func policyDeleteAction(doc concrnt.Document[any]) string {
	if doc.Kind == "association" {
		return "association:delete"
	}
	return "record:delete"
}
