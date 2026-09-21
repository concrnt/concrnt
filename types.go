package concrnt

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/concrnt/concrnt/cdid"
)

const (
	ProofTypeEcrecover         = "concrnt-ecrecover-direct"
	ProofTypeDocumentReference = "document-reference"
	ProofTypeDocumentDirect    = "document-direct"
	ProofTypeSubkey            = "concrnt-ecrecover-subkey"
	ProofTypeNone              = "none"
)

type SoftwareInfo struct {
	Version      string `json:"version"`
	BuildMachine string `json:"buildMachine"`
	BuildTime    string `json:"buildTime"`
	GoVersion    string `json:"goVersion"`
}

type WellKnownConcrnt struct {
	Version      string            `json:"version"`
	Domain       string            `json:"domain"`
	CSID         string            `json:"csid"`
	Layer        string            `json:"layer"`
	Dimension    string            `json:"dimension"` // for backwards compatibility
	Endpoints    map[string]string `json:"endpoints"`
	SoftwareInfo SoftwareInfo      `json:"softwareInfo"`
	Meta         map[string]any    `json:"meta,omitempty"`
}

type PolicyEntry struct {
	URL      *string            `json:"url"`
	Params   *map[string]any    `json:"params,omitempty"`
	Defaults *map[string]string `json:"defaults,omitempty"`

	// Errored marks a policy entry whose referenced policy couldn't be
	// resolved (e.g. the record or URL failed to fetch), so evaluation falls
	// back to the entry's Defaults instead of silently treating it as no
	// policy. In-process marker only — kept off the wire so remote documents
	// can't set it.
	Errored bool `json:"-"`
}

type Policy struct {
	Entries []PolicyEntry `json:"entries"`

	// internal use only
	VirtualParents *[]string `json:"-"`
	Source         string    `json:"-"`
}

type Document[T any] struct {
	Kind string `json:"kind"` // entity / record / association / delete / ack / unack / acked / unacked

	// CIP-1
	Key   string `json:"key"`
	Value T      `json:"value"`

	Author string `json:"author"`

	Schema string `json:"schema,omitempty"`

	CreatedAt time.Time `json:"createdAt"`

	OnUpdate *string `json:"onUpdate,omitempty"` // forget(default), retain. ignored for kind=entity (always retained, CIP-0 §8.4)

	// CIP-7
	Distributes *[]string `json:"distributes,omitempty"`

	// CIP-9
	Associate          *string `json:"associate,omitempty"`
	AssociationVariant *string `json:"associationVariant,omitempty"`

	// CIP-12
	Policy *Policy `json:"policy,omitempty"`
}

type LegacyDocument struct {
	ID        string    `json:"id"`
	Author    string    `json:"author"`
	Owner     *string   `json:"owner,omitempty"`
	Schema    string    `json:"schema,omitempty"`
	Document  string    `json:"document"`
	Signature string    `json:"signature"`
	CDate     time.Time `json:"cdate"`
}

type SchemaDeleteType string

type Proof struct {
	Type      string  `json:"type"`
	Signature *string `json:"signature,omitempty"`
	Href      *string `json:"href,omitempty"`
	Key       *string `json:"key,omitempty"`
	Document  *string `json:"document,omitempty"`
	Proof     *Proof  `json:"proof,omitempty"`
}

// SignedDocument is a document with its proof. A query row for an
// intermediate key (CIP-5 §3.2.1) carries only CCKV: Document and Proof are
// omitted from the wire form when empty so such key-only entries serialize
// as {"cckv": "..."}.
type SignedDocument struct {
	CCKV       *string                   `json:"cckv,omitempty"`
	CCFS       *string                   `json:"ccfs,omitempty"`
	Document   string                    `json:"document,omitempty"`
	Proof      Proof                     `json:"proof,omitzero"`
	References map[string]SignedDocument `json:"references,omitempty"`

	// IsPublic is an internal, publish-time annotation (not part of the CCAPI
	// wire format): whether an anonymous requester may read this document
	// (CIP-11 §3.2 baseline). nil means unevaluated, which consumers must
	// treat as not public. It exists only on the redis pubsub Event contract;
	// Event.PublicView strips it at the websocket edge and it is never part of
	// commit responses, query results or federation payloads.
	IsPublic *bool `json:"isPublic,omitempty"`
}

func (sd *SignedDocument) ParsedDocument() (Document[any], error) {
	var doc Document[any]
	err := json.Unmarshal([]byte(sd.Document), &doc)
	return doc, err
}

func (sd *SignedDocument) CDID() (string, error) {
	doc, err := sd.ParsedDocument()
	if err != nil {
		return "", err
	}

	hash := GetHash([]byte(sd.Document))
	var hash10 [10]byte
	copy(hash10[:], hash[:10])
	return cdid.New(hash10, doc.CreatedAt).String(), nil
}

// DeriveAcked derives the acked/unacked document the author's server ships
// to the associate owner's server for an ack/unack (CIP-10 §5.2): the same
// document with only kind replaced, re-serialized in the canonical field set,
// carrying the original signed document in a document-direct proof. The
// derivation is deterministic (same ack → same CDID), which is what makes a
// retried or backfilled acked commit an idempotent no-op.
func (sd *SignedDocument) DeriveAcked() (SignedDocument, error) {
	doc, err := sd.ParsedDocument()
	if err != nil {
		return SignedDocument{}, err
	}

	switch doc.Kind {
	case "ack":
		doc.Kind = "acked"
	case "unack":
		doc.Kind = "unacked"
	default:
		return SignedDocument{}, fmt.Errorf("acked documents can only be derived from ack/unack documents, got kind %q", doc.Kind)
	}

	derived, err := json.Marshal(doc)
	if err != nil {
		return SignedDocument{}, err
	}

	original := sd.Document
	originalProof := sd.Proof
	return SignedDocument{
		Document: string(derived),
		Proof: Proof{
			Type:     ProofTypeDocumentDirect,
			Document: &original,
			Proof:    &originalProof,
		},
	}, nil
}

// QueryResult is the paged envelope returned by the query/associations/
// acknowledges endpoints (CIP-5 §3.2). Prev and Next are cursors holding the
// server-side sort key of the boundary rows: an RFC3339Nano datetime for
// orderby=createdAt (the default everywhere) or the cckv URI for
// orderby=key (CIP-5 §3.3). nil means no more rows in that direction.
type QueryResult struct {
	Items []SignedDocument `json:"items"`
	Prev  *string          `json:"prev"`
	Next  *string          `json:"next"`
}

type RegisterRequest struct {
	SignedDocument
	Meta        any     `json:"meta,omitempty"`
	InviteToken *string `json:"inviteToken,omitempty"`
}

type Event struct {
	Type        string                    `json:"type"`
	Source      string                    `json:"source"`
	URI         string                    `json:"uri"`
	Association *string                   `json:"association,omitempty"`
	References  map[string]SignedDocument `json:"documents,omitempty"`
	Timestamp   time.Time                 `json:"timestamp"`
}

// PublicView returns a copy of the event suitable for unauthenticated
// consumers (CIP-11 §3.2): only documents marked anonymously readable are
// kept, references nested deeper than one level are removed outright, and the
// internal IsPublic flags are stripped. Unmarked (nil) documents are dropped
// (fail closed). The receiver is not mutated.
func (e Event) PublicView() Event {
	if len(e.References) == 0 {
		return e
	}
	readable := make(map[string]SignedDocument, len(e.References))
	for uri, sd := range e.References {
		if sd.IsPublic == nil || !*sd.IsPublic {
			continue
		}
		sd.IsPublic = nil
		var kept map[string]SignedDocument
		for nestedURI, nestedSD := range sd.References {
			if nestedSD.IsPublic == nil || !*nestedSD.IsPublic {
				continue
			}
			nestedSD.IsPublic = nil
			nestedSD.References = nil
			if kept == nil {
				kept = make(map[string]SignedDocument, len(sd.References))
			}
			kept[nestedURI] = nestedSD
		}
		sd.References = kept
		readable[uri] = sd
	}
	e.References = readable
	return e
}

// MarkAllPublic returns a copy of the event with every document (nested
// included) flagged anonymously readable. Used when republishing events
// received from a remote server's public realtime feed: whatever arrived has
// already passed the remote edge's anonymous filter, so inbound content is
// public by definition. The receiver is not mutated.
func (e Event) MarkAllPublic() Event {
	if len(e.References) == 0 {
		return e
	}
	marked := make(map[string]SignedDocument, len(e.References))
	for uri, sd := range e.References {
		public := true
		sd.IsPublic = &public
		if len(sd.References) > 0 {
			nested := make(map[string]SignedDocument, len(sd.References))
			for nestedURI, nestedSD := range sd.References {
				nestedPublic := true
				nestedSD.IsPublic = &nestedPublic
				nested[nestedURI] = nestedSD
			}
			sd.References = nested
		}
		marked[uri] = sd
	}
	e.References = marked
	return e
}

// StripInternalFlags removes the internal IsPublic annotations at every
// nesting level. Called on inbound documents so the spec-external flag never
// echoes back on API responses. The receiver is not mutated.
func (sd SignedDocument) StripInternalFlags() SignedDocument {
	sd.IsPublic = nil
	if len(sd.References) > 0 {
		refs := make(map[string]SignedDocument, len(sd.References))
		for uri, ref := range sd.References {
			refs[uri] = ref.StripInternalFlags()
		}
		sd.References = refs
	}
	return sd
}

// NotificationPayload is the minimal push-notification body delivered to devices
// (through webpush-relay). It replaces sending the whole Event, whose embedded
// SignedDocuments blow past the 4096-byte WebPush/FCM limit. The client resolves
// URI (a ccfs association document) via /api/v2/resolve to render the rest.
const (
	// NotificationTypeNotification is a regular push: render and show it.
	NotificationTypeNotification = "notification"
	// NotificationTypeCounterReset tells the device the owner has looked at
	// their notifications elsewhere: clear the app icon badge, show nothing.
	NotificationTypeCounterReset = "counter-reset"
	// NotificationTopicCounterReset is the RFC 8030 Topic header carried by a
	// counter-reset push. The body is encrypted, so this plaintext header is
	// what lets webpush-relay turn it into a badge-only APNs notification.
	NotificationTopicCounterReset = "counter-reset"
)

type NotificationPayload struct {
	Type      string    `json:"type"`
	URI       string    `json:"uri,omitempty"`
	Schema    string    `json:"schema,omitempty"`
	Author    string    `json:"author,omitempty"`
	CreatedAt time.Time `json:"createdAt,omitzero"`
	// Badge is the owner's unread counter after this notification was counted
	// (0 when the counter could not be read); devices mirror it onto the app icon.
	Badge int64 `json:"badge"`
}

type RealtimeRequest struct {
	Type     string   `json:"type"`
	Prefixes []string `json:"prefixes"`
}

type AbuseReport struct {
	TargetURI string `json:"target" gorm:"type:text"`
	Body      string `json:"body" gorm:"type:text"`
}
