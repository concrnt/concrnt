// Package repoutil holds the backend-independent logic of the repository
// contract: what every backend does with a row once it has fetched it, and
// how the URIs the contract is keyed by relate to each other. Nothing here
// is a model, a table or an index (CONTRACT.md C5).
package repoutil

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/chunkline"
	"github.com/concrnt/concrnt/internal/domain"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// HierarchyOf lists the ancestor chain of a cckv URI, root first, the URI
// itself last.
func HierarchyOf(cckv string) ([]string, error) {
	hierarchy := []string{}
	current := cckv
	for {
		hierarchy = append([]string{current}, hierarchy...)
		parent, err := url.JoinPath(current, "..")
		if err != nil {
			return nil, err
		}
		if parent == current {
			break
		}
		current = parent
	}
	return hierarchy, nil
}

// ParentURI is the key one segment up from uri; ok is false for a
// top-level key, whose parent is the owner root and holds no key row.
func ParentURI(uri string) (parent string, ok bool, err error) {
	parent, err = url.JoinPath(uri, "..")
	if err != nil {
		return "", false, err
	}
	parsed, err := url.Parse(parent)
	if err != nil {
		return "", false, err
	}
	if parsed.Path == "" || parsed.Path == "/" {
		return "", false, nil
	}
	return parent, true, nil
}

// RequireCCKV rejects a policy stack lookup keyed by anything but cckv.
func RequireCCKV(uris []string) error {
	for _, uri := range uris {
		parsed, err := concrnt.ParseCCURI(uri)
		if err != nil {
			return err
		}
		if parsed.Scheme != "cckv" {
			return fmt.Errorf("policy stacks are keyed by cckv, got %s", uri)
		}
	}
	return nil
}

// PolicyLayer is what a key holding a record contributes to the stacks
// below it: its policy document, if any, and its distributions.
type PolicyLayer struct {
	Policy        *string
	Distributions []string
}

// PolicyStacks builds the CIP-12 §5.3 stack (root first, the resource
// itself last) of every cckv with one fetch of the union of their ancestor
// chains; fetch returns the layers of the keys that hold a record. A level
// with neither a policy nor distributions is skipped, but one that only
// distributes stays: its virtual parents (the destination timelines'
// policies) must reach the stack.
func PolicyStacks(cckvs []string, hierarchyOf func(string) ([]string, error), fetch func(uris []string) (map[string]PolicyLayer, error)) (map[string][]concrnt.Policy, error) {
	hierarchies := make(map[string][]string, len(cckvs))
	wanted := map[string]bool{}
	uris := []string{}
	for _, cckv := range cckvs {
		if _, ok := hierarchies[cckv]; ok {
			continue
		}
		hierarchy, err := hierarchyOf(cckv)
		if err != nil {
			return nil, err
		}
		hierarchies[cckv] = hierarchy
		for _, uri := range hierarchy {
			if !wanted[uri] {
				wanted[uri] = true
				uris = append(uris, uri)
			}
		}
	}

	layers := map[string]PolicyLayer{}
	if len(uris) > 0 {
		var err error
		if layers, err = fetch(uris); err != nil {
			return nil, err
		}
	}

	// With the evaluator's strong=first-wins / weak=last-wins folding the
	// root-first order makes strong conclusions prefer the outer layers
	// and weak conclusions the resource itself.
	stacks := make(map[string][]concrnt.Policy, len(hierarchies))
	for cckv, hierarchy := range hierarchies {
		policies := []concrnt.Policy{}
		for _, uri := range hierarchy {
			layer, ok := layers[uri]
			if !ok || (layer.Policy == nil && len(layer.Distributions) == 0) {
				continue
			}
			var policyDoc concrnt.Policy
			if layer.Policy != nil {
				if err := json.Unmarshal([]byte(*layer.Policy), &policyDoc); err != nil {
					return nil, err
				}
			}
			policyDoc.Source = uri
			virtualParents := append([]string{}, layer.Distributions...)
			policyDoc.VirtualParents = &virtualParents
			policies = append(policies, policyDoc)
		}
		stacks[cckv] = policies
	}
	return stacks, nil
}

// PartitionDocumentURIs splits a GetSignedDocuments batch into the cckv
// keys and the commit ids of the concrnt ccfs URIs (idOf maps each such
// URI to its id; a ccfs URI of another type resolves to nothing).
func PartitionDocumentURIs(uris []string) (cckvs, ids []string, idOf map[string]string, err error) {
	cckvs, ids, idOf = []string{}, []string{}, map[string]string{}
	for _, uri := range uris {
		parsed, err := concrnt.ParseCCURI(uri)
		if err != nil {
			return nil, nil, nil, err
		}
		switch parsed.Scheme {
		case "cckv":
			cckvs = append(cckvs, uri)
		case "ccfs":
			if parsed.Type == concrnt.CCFSTypeConcrnt {
				ids = append(ids, parsed.CDID)
				idOf[uri] = parsed.CDID
			}
		default:
			return nil, nil, nil, fmt.Errorf("unsupported uri scheme: %s", parsed.Scheme)
		}
	}
	return cckvs, ids, idOf, nil
}

// ParseProof decodes a stored proof column; an empty one is a zero proof.
func ParseProof(proof string) (concrnt.Proof, error) {
	var parsed concrnt.Proof
	if proof == "" {
		return parsed, nil
	}
	err := json.Unmarshal([]byte(proof), &parsed)
	return parsed, err
}

// RecordDocument is the signed document of a key holding a record: both
// its cckv and the ccfs composed in the record owner's namespace.
func RecordDocument(cckv, owner, recordID, document, proof string) (concrnt.SignedDocument, error) {
	parsed, err := ParseProof(proof)
	if err != nil {
		return concrnt.SignedDocument{}, err
	}
	ccfs := concrnt.ComposeCCFSURI(owner, concrnt.CCFSTypeConcrnt, recordID)
	return concrnt.SignedDocument{CCKV: &cckv, CCFS: &ccfs, Document: document, Proof: parsed}, nil
}

// CommitDocument is the signed document of a commit log row in owner's
// namespace. An ownerless commit has no ccfs to compose
// (ComposeCCFSURI("") would yield a URI that misparses).
func CommitDocument(owner, id, document, proof string) (concrnt.SignedDocument, error) {
	parsed, err := ParseProof(proof)
	if err != nil {
		return concrnt.SignedDocument{}, err
	}
	sd := concrnt.SignedDocument{Document: document, Proof: parsed}
	if owner != "" {
		ccfs := concrnt.ComposeCCFSURI(owner, concrnt.CCFSTypeConcrnt, id)
		sd.CCFS = &ccfs
	}
	return sd, nil
}

// CommitLogEntry is the dump form of a commit log row.
func CommitLogEntry(id, ip, document, proof, owner string, cDate time.Time) (ucrecord.CommitLogEntry, error) {
	parsed, err := ParseProof(proof)
	if err != nil {
		return ucrecord.CommitLogEntry{}, fmt.Errorf("failed to parse proof for commit %s: %w", id, err)
	}
	return ucrecord.CommitLogEntry{ID: id, IP: ip, Document: document, Proof: parsed, Owner: owner, CDate: cDate.UTC()}, nil
}

// ChunkSize is the length in seconds of a chunkline chunk; the chunk of an
// item is its createdAt in unix seconds divided by it.
const ChunkSize = 600

// ChunklineManifest is the manifest every backend serves for a local
// timeline: the chunkline endpoints of this server's own API.
func ChunklineManifest(uri string, firstChunk *int64) *chunkline.Manifest {
	safeURI := url.QueryEscape(uri)
	return &chunkline.Manifest{
		Version:    "1.0",
		ChunkSize:  ChunkSize,
		FirstChunk: firstChunk,
		Descending: &chunkline.Endpoint{
			Iterator: "/api/v2/chunkline/itr/{chunk}?uri=" + safeURI,
			Body:     "/api/v2/chunkline/body/{chunk}?uri=" + safeURI,
		},
		Removed: "/api/v2/chunkline/removed?uri=" + safeURI,
	}
}

// ServerFromWellKnown is a cached server row as the usecase sees it.
func ServerFromWellKnown(tag, wellKnown string) (*domain.Server, error) {
	var wkc concrnt.WellKnownConcrnt
	if err := json.Unmarshal([]byte(wellKnown), &wkc); err != nil {
		return nil, err
	}
	return &domain.Server{TagString: tag, WellKnown: wkc}, nil
}
