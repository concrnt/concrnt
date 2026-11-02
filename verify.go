package concrnt

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/concrnt/concrnt/cdid"
	"github.com/concrnt/concrnt/schemas"
)

type DocumentResolver interface {
	ResolveSignedDocument(ctx context.Context, uri string) (SignedDocument, error)
}

// ErrSignatureVerificationFailed indicates a signed document's proof did not
// verify.
var ErrSignatureVerificationFailed = errors.New("signature verification failed")

// ErrNoneProofNotAllowed indicates a none-proof (unsigned) document reached
// Verify. None proofs carry no authorship and are only accepted by trusted
// system service accounts, which bypass Verify entirely rather than passing an
// option here.
var ErrNoneProofNotAllowed = errors.New("none proof type is not allowed")

// ErrUnsupportedProofType indicates a proof type Verify doesn't know how to
// check.
var ErrUnsupportedProofType = errors.New("unsupported or unverifiable proof type")

// ErrProofTypeNotAllowed indicates a proof type excluded by the caller's
// allowed list (VerifyWithProofTypes) or by a nested requirement.
var ErrProofTypeNotAllowed = errors.New("proof type is not allowed here")

// maxVerifyDepth bounds how many linked documents (subkey / document-reference
// proofs) Verify will follow, to protect against malicious, arbitrarily deep
// proof chains.
const maxVerifyDepth = 4

// Verify verifies a signed document's proof. For document-reference proofs
// the referenced document is looked up via the inlined References first
// (falling back to resolver) and recursively verified; for subkey proofs the
// enact document is always fetched via the resolver, never from References.
// None (unsigned) proofs never verify — trusted system service accounts skip
// Verify entirely instead.
func (sd *SignedDocument) Verify(ctx context.Context, resolver DocumentResolver) error {
	return sd.verify(ctx, resolver, maxVerifyDepth, nil)
}

// VerifyWithProofTypes verifies like Verify but additionally requires the
// top-level document's proof to be one of the allowed types. A nil allowed
// slice means no restriction. Nested documents reached during verification
// carry their own requirements (e.g. CIP-13 restricts subkey enact/revocation
// documents to ecrecover-direct) independent of this parameter.
func (sd *SignedDocument) VerifyWithProofTypes(ctx context.Context, resolver DocumentResolver, allowed []string) error {
	return sd.verify(ctx, resolver, maxVerifyDepth, allowed)
}

func (sd *SignedDocument) resolve(ctx context.Context, resolver DocumentResolver, uri string) (SignedDocument, error) {
	if ref, ok := sd.References[uri]; ok {
		return ref, nil
	}
	if resolver == nil {
		return SignedDocument{}, fmt.Errorf("no resolver available to fetch referenced document %s", uri)
	}
	return resolver.ResolveSignedDocument(ctx, uri)
}

func (sd *SignedDocument) verify(ctx context.Context, resolver DocumentResolver, depth int, allowed []string) error {
	if depth <= 0 {
		return errors.New("proof chain is too deep")
	}

	if allowed != nil && !slices.Contains(allowed, sd.Proof.Type) {
		return fmt.Errorf("%w: %s (allowed: %s)", ErrProofTypeNotAllowed, sd.Proof.Type, strings.Join(allowed, ", "))
	}

	var doc Document[any]
	err := json.Unmarshal([]byte(sd.Document), &doc)
	if err != nil {
		return errors.Join(errors.New("failed to decode document for signature verification"), err)
	}

	switch sd.Proof.Type {
	case ProofTypeEcrecover:
		if sd.Proof.Signature == nil {
			return errors.New("signature is required for ecrecover proof")
		}
		signatureBytes, err := hex.DecodeString(*sd.Proof.Signature)
		if err != nil {
			return errors.Join(errors.New("invalid signature format"), err)
		}
		err = VerifySignature([]byte(sd.Document), signatureBytes, doc.Author)
		if err != nil {
			return errors.Join(ErrSignatureVerificationFailed, err)
		}
		return nil

	case ProofTypeSubkey:
		if sd.Proof.Signature == nil {
			return errors.New("signature is required for subkey proof")
		}
		if sd.Proof.Key == nil {
			return errors.New("key is required for subkey proof")
		}

		// The enact document must always come from its authoritative server:
		// inlined copies are supplied by whoever submitted the document, so
		// trusting them would let a revoked subkey enact document be replayed
		// forever.
		if resolver == nil {
			return fmt.Errorf("no resolver available to fetch subkey document %s", *sd.Proof.Key)
		}
		subKeySD, err := resolver.ResolveSignedDocument(ctx, *sd.Proof.Key)
		if err != nil {
			return errors.Join(fmt.Errorf("failed to fetch subkey document %s", *sd.Proof.Key), err)
		}

		// CIP-13 §6 step 2: the enact / revoked-subkey document must itself be
		// signed with the entity's master key (ecrecover-direct) — a subkey
		// must not be able to enact or revoke another subkey.
		err = subKeySD.verify(ctx, resolver, depth-1, []string{ProofTypeEcrecover})
		if err != nil {
			return errors.Join(errors.New("subkey document failed verification"), err)
		}

		// CIP-13: the key resolves either to the enact document itself (the
		// subkey is currently valid) or to a revoked-subkey document embedding
		// the original enact document (the subkey was valid only between the
		// enact's createdAt and the revocation's createdAt). Anything else
		// must not pass as a subkey authorization — otherwise any owner-signed
		// document that happens to carry a value.ckid would.
		var enactDoc Document[schemas.Subkey]
		var validUntil *time.Time

		var keyDoc Document[json.RawMessage]
		err = json.Unmarshal([]byte(subKeySD.Document), &keyDoc)
		if err != nil {
			return errors.Join(errors.New("failed to decode subkey document"), err)
		}

		switch keyDoc.Schema {
		case schemas.SubkeyURL:
			err = json.Unmarshal([]byte(subKeySD.Document), &enactDoc)
			if err != nil {
				return errors.Join(errors.New("failed to decode subkey enact document"), err)
			}

		case schemas.RevokedSubkeyURL:
			var revokedDoc Document[SignedDocument]
			err = json.Unmarshal([]byte(subKeySD.Document), &revokedDoc)
			if err != nil {
				return errors.Join(errors.New("failed to decode revoked-subkey document"), err)
			}
			if revokedDoc.Author != doc.Author {
				return errors.New("revoked-subkey document author does not match signed document author")
			}

			// The embedded enact document is submitter-independent (it is part
			// of the owner-signed revocation), but it still has to verify on
			// its own so a forged enact can't be smuggled in via value. Like
			// the live enact, it must be master-key signed (CIP-13 §6 step 3).
			enactSD := revokedDoc.Value
			err = enactSD.verify(ctx, resolver, depth-1, []string{ProofTypeEcrecover})
			if err != nil {
				return errors.Join(errors.New("enact document embedded in revoked-subkey failed verification"), err)
			}
			err = json.Unmarshal([]byte(enactSD.Document), &enactDoc)
			if err != nil {
				return errors.Join(errors.New("failed to decode enact document embedded in revoked-subkey"), err)
			}
			if enactDoc.Schema != schemas.SubkeyURL {
				return errors.New("revoked-subkey document does not embed a subkey enact document")
			}
			validUntil = &revokedDoc.CreatedAt

		default:
			return errors.New("subkey proof requires a subkey enact document")
		}

		if enactDoc.Author != doc.Author {
			return errors.New("subkey document author does not match signed document author")
		}

		// CIP-13 §4.1/§6: the signed document must fall inside the subkey's
		// validity period — from the enact's createdAt up to (for revoked
		// subkeys) the revocation's createdAt.
		if doc.CreatedAt.Before(enactDoc.CreatedAt) {
			return errors.New("signed document predates the subkey enact document")
		}
		if validUntil != nil && doc.CreatedAt.After(*validUntil) {
			return errors.New("signed document postdates the subkey revocation")
		}

		signatureBytes, err := hex.DecodeString(*sd.Proof.Signature)
		if err != nil {
			return errors.Join(errors.New("invalid signature format"), err)
		}

		err = VerifySignature([]byte(sd.Document), signatureBytes, enactDoc.Value.CKID)
		if err != nil {
			return errors.Join(ErrSignatureVerificationFailed, err)
		}
		return nil

	case ProofTypeDocumentReference:

		if sd.Proof.Href == nil {
			return errors.New("href is required for document-reference proof")
		}
		if doc.Schema != schemas.ReferenceURL {
			return errors.New("document-reference proof is only allowed for reference documents")
		}

		var refDoc Document[schemas.Reference]
		err = json.Unmarshal([]byte(sd.Document), &refDoc)
		if err != nil {
			return errors.Join(errors.New("invalid reference document"), err)
		}
		if refDoc.Value.Href != *sd.Proof.Href {
			return errors.New("proof href does not match reference document href")
		}

		targetSD, err := sd.resolve(ctx, resolver, *sd.Proof.Href)
		if err != nil {
			return errors.Join(fmt.Errorf("failed to fetch referenced document %s", *sd.Proof.Href), err)
		}

		err = targetSD.verify(ctx, resolver, depth-1, nil)
		if err != nil {
			return errors.Join(errors.New("referenced document failed verification"), err)
		}

		var targetDoc Document[any]
		err = json.Unmarshal([]byte(targetSD.Document), &targetDoc)
		if err != nil {
			return errors.Join(errors.New("failed to decode referenced document"), err)
		}

		if targetDoc.Author != doc.Author {
			return errors.New("referenced document author does not match signed document author")
		}

		hrefURI, err := ParseCCURI(*sd.Proof.Href)
		if err != nil {
			return errors.Join(errors.New("invalid href for document-reference proof"), err)
		}
		switch {
		case hrefURI.Scheme == "cckv" && hrefURI.Key != "":
			targetKey, err := ParseCCURI(targetDoc.Key)
			if err != nil || targetKey.Scheme != "cckv" || targetKey.Key == "" {
				return errors.New("referenced document key is not a keyed cckv uri")
			}
			if targetKey.Owner != hrefURI.Owner || targetKey.Key != hrefURI.Key {
				return errors.New("referenced document key does not match proof href")
			}
		case hrefURI.Scheme == "cckv":
			if targetDoc.Kind != "entity" {
				return errors.New("referenced document for entity href is not an entity document")
			}
			if targetDoc.Author != hrefURI.Owner {
				return errors.New("referenced document author does not match entity href owner")
			}
		case hrefURI.Scheme == "ccfs" && hrefURI.Type == CCFSTypeConcrnt:
			hash := GetHash([]byte(targetSD.Document))
			var hash10 [10]byte
			copy(hash10[:], hash[:10])
			if cdid.New(hash10, targetDoc.CreatedAt).String() != hrefURI.CDID {
				return errors.New("referenced document cdid does not match proof href")
			}

			// the ccfs owner is whoever's server holds the document: the key
			// owner for keyed records, the associate's owner for associations
			// (CIP-3 §3.3) and the author for everything else, acks included
			// (CIP-3 §3.4)
			expectedOwner := targetDoc.Author
			if targetDoc.Key != "" {
				targetKey, err := ParseCCURI(targetDoc.Key)
				if err != nil {
					return errors.Join(errors.New("referenced document key is not a valid cc uri"), err)
				}
				expectedOwner = targetKey.Owner
			} else if targetDoc.Kind == "association" && targetDoc.Associate != nil {
				targetAssociate, err := ParseCCURI(*targetDoc.Associate)
				if err != nil {
					return errors.Join(errors.New("referenced document associate is not a valid cc uri"), err)
				}
				expectedOwner = targetAssociate.Owner
			}
			if expectedOwner != hrefURI.Owner {
				return errors.New("referenced document owner does not match proof href owner")
			}
		default:
			return fmt.Errorf("document-reference proof href must be a cckv or ccfs concrnt uri, got %s", *sd.Proof.Href)
		}
		return nil

	case ProofTypeDocumentDirect:
		if sd.Proof.Document == nil || sd.Proof.Proof == nil {
			return errors.New("embedded document and proof are required for document-direct proof")
		}

		embedded := SignedDocument{
			Document: *sd.Proof.Document,
			Proof:    *sd.Proof.Proof,
		}

		if err := embedded.verify(ctx, resolver, depth-1, []string{ProofTypeEcrecover, ProofTypeSubkey}); err != nil {
			return errors.Join(errors.New("embedded document failed verification"), err)
		}

		self, err := sd.ParsedDocument()
		if err != nil {
			return errors.Join(errors.New("failed to parse signed document for document-direct proof"), err)
		}

		switch self.Kind {
		case "acked", "unacked":
			// CIP-10 §5.2: the document must be byte-equal to the canonical
			// derivation of the embedded ack/unack — that single comparison
			// binds the kind correspondence (acked→ack, unacked→unack) and
			// every other field (author, schema, createdAt, associate, value)
			// at once, so a valid ack cannot be re-purposed.
			expected, err := embedded.DeriveAcked()
			if err != nil {
				return errors.Join(errors.New("embedded document is not an ack/unack document for document-direct proof"), err)
			}

			if sd.Document != expected.Document {
				return errors.New("signed document does not match the derivation of the embedded document for document-direct proof")
			}

			return nil

		default:
			return ErrProofTypeNotAllowed
		}

	case ProofTypeNone:
		return ErrNoneProofNotAllowed

	default:
		return fmt.Errorf("%w: %s", ErrUnsupportedProofType, sd.Proof.Type)
	}
}
