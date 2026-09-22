package concrnt

import (
	"encoding/json"
	"testing"
	"time"
)

func boolPtr(b bool) *bool { return &b }

func TestEventPublicView(t *testing.T) {
	event := Event{
		Type: "created",
		URI:  "cckv://owner/timelines/home/items/x",
		References: map[string]SignedDocument{
			"cckv://owner/timelines/home/items/x": {
				Document: "item",
				IsPublic: boolPtr(true),
				References: map[string]SignedDocument{
					"cckv://owner/posts/public": {
						Document: "public",
						IsPublic: boolPtr(true),
						References: map[string]SignedDocument{
							"cckv://owner/posts/deep": {Document: "deep", IsPublic: boolPtr(true)},
						},
					},
					"cckv://owner/posts/protected": {Document: "protected", IsPublic: boolPtr(false)},
					"cckv://owner/posts/unmarked":  {Document: "unmarked"},
				},
			},
			"cckv://owner/posts/hidden":   {Document: "hidden", IsPublic: boolPtr(false)},
			"cckv://owner/posts/unmarked": {Document: "unmarked"},
		},
	}

	public := event.PublicView()

	if len(public.References) != 1 {
		t.Fatalf("only the public document must survive, got %v", public.References)
	}
	item, ok := public.References["cckv://owner/timelines/home/items/x"]
	if !ok {
		t.Fatalf("public document must survive, got %v", public.References)
	}
	if item.IsPublic != nil {
		t.Fatal("the internal flag must be stripped from the public view")
	}
	if len(item.References) != 1 {
		t.Fatalf("only the public nested reference must survive, got %v", item.References)
	}
	nested, ok := item.References["cckv://owner/posts/public"]
	if !ok {
		t.Fatalf("public nested reference must survive, got %v", item.References)
	}
	if nested.IsPublic != nil {
		t.Fatal("the internal flag must be stripped from nested references")
	}
	if nested.References != nil {
		t.Fatalf("depth-2 references must be removed outright, got %v", nested.References)
	}

	// the receiver must stay intact — internal consumers share it
	if event.References["cckv://owner/posts/hidden"].Document != "hidden" {
		t.Fatal("PublicView must not mutate the receiver")
	}
	if event.References["cckv://owner/timelines/home/items/x"].IsPublic == nil {
		t.Fatal("PublicView must not strip flags on the receiver")
	}
	if len(event.References["cckv://owner/timelines/home/items/x"].References) != 3 {
		t.Fatal("PublicView must not filter the receiver's nested references")
	}
}

func TestEventMarkAllPublic(t *testing.T) {
	event := Event{
		Type: "created",
		References: map[string]SignedDocument{
			"cckv://owner/posts/a": {
				Document: "a",
				References: map[string]SignedDocument{
					"cckv://owner/posts/b": {Document: "b"},
				},
			},
		},
	}

	marked := event.MarkAllPublic()

	top := marked.References["cckv://owner/posts/a"]
	if top.IsPublic == nil || !*top.IsPublic {
		t.Fatalf("top-level document must be flagged public, got %+v", top.IsPublic)
	}
	nested := top.References["cckv://owner/posts/b"]
	if nested.IsPublic == nil || !*nested.IsPublic {
		t.Fatalf("nested document must be flagged public, got %+v", nested.IsPublic)
	}

	if event.References["cckv://owner/posts/a"].IsPublic != nil {
		t.Fatal("MarkAllPublic must not mutate the receiver")
	}
	if event.References["cckv://owner/posts/a"].References["cckv://owner/posts/b"].IsPublic != nil {
		t.Fatal("MarkAllPublic must not mutate the receiver's nested references")
	}
}

func TestSignedDocumentStripInternalFlags(t *testing.T) {
	sd := SignedDocument{
		Document: "top",
		IsPublic: boolPtr(true),
		References: map[string]SignedDocument{
			"cckv://owner/posts/a": {
				Document: "a",
				IsPublic: boolPtr(false),
				References: map[string]SignedDocument{
					"cckv://owner/posts/b": {Document: "b", IsPublic: boolPtr(true)},
				},
			},
		},
	}

	stripped := sd.StripInternalFlags()

	if stripped.IsPublic != nil {
		t.Fatal("top-level flag must be stripped")
	}
	ref := stripped.References["cckv://owner/posts/a"]
	if ref.IsPublic != nil {
		t.Fatal("nested flag must be stripped")
	}
	if ref.References["cckv://owner/posts/b"].IsPublic != nil {
		t.Fatal("deeply nested flag must be stripped")
	}
	if ref.References["cckv://owner/posts/b"].Document != "b" {
		t.Fatal("documents must be preserved")
	}

	if sd.IsPublic == nil {
		t.Fatal("StripInternalFlags must not mutate the receiver")
	}
	if sd.References["cckv://owner/posts/a"].IsPublic == nil {
		t.Fatal("StripInternalFlags must not mutate the receiver's references")
	}
}

// Proof round-trips its nested embedding through JSON unchanged.
func TestProofDocumentDirectJSONRoundTrip(t *testing.T) {
	original, _ := signedTestAck(t, "ack")
	mirror := mirrorOf(t, original)

	encoded, err := json.Marshal(mirror.Proof)
	if err != nil {
		t.Fatalf("marshal proof: %v", err)
	}
	var decoded Proof
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal proof: %v", err)
	}
	if decoded.Type != ProofTypeDocumentDirect || decoded.Document == nil || *decoded.Document != original.Document {
		t.Fatalf("embedded document did not survive the round trip: %+v", decoded)
	}
	if decoded.Proof == nil || decoded.Proof.Type != original.Proof.Type || decoded.Proof.Signature == nil || *decoded.Proof.Signature != *original.Proof.Signature {
		t.Fatalf("embedded proof did not survive the round trip: %+v", decoded.Proof)
	}
}

// CDID derives the content+time document id Commit stores a document under:
// the time-prefixed CDID of createdAt with the first 10 bytes of the document
// hash — lexicographically ordered by createdAt with a deterministic content
// tiebreaker.
func TestSignedDocumentCDIDAndParsedDocument(t *testing.T) {
	ccid, priv := newTestIdentity(t)
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	newDoc := func(createdAt time.Time, foo string) SignedDocument {
		return signDocument(t, Document[testRecordValue]{
			Kind:      "record",
			Key:       "cckv://" + ccid + "/example",
			Value:     testRecordValue{Foo: foo},
			Author:    ccid,
			CreatedAt: createdAt,
		}, priv)
	}

	sd := newDoc(at, "bar")
	doc, err := sd.ParsedDocument()
	if err != nil {
		t.Fatalf("ParsedDocument returned error: %v", err)
	}
	if doc.Kind != "record" || doc.Author != ccid || !doc.CreatedAt.Equal(at) {
		t.Fatalf("unexpected parsed document: %+v", doc)
	}

	id, err := sd.CDID()
	if err != nil {
		t.Fatalf("CDID returned error: %v", err)
	}
	again, _ := sd.CDID()
	if id != again {
		t.Fatal("CDID must be deterministic")
	}
	other := newDoc(at, "baz")
	sameTime, _ := other.CDID()
	if sameTime == id {
		t.Fatal("documents with different content must not share a CDID")
	}
	laterSD := newDoc(at.Add(time.Second), "bar")
	later, _ := laterSD.CDID()
	if !(later > id) {
		t.Fatalf("a later createdAt must sort above: %s !> %s", later, id)
	}

	broken := SignedDocument{Document: "{not json"}
	if _, err := broken.CDID(); err == nil {
		t.Fatal("CDID of an unparseable document must fail")
	}
}

// CIP-5 §3.2.1: a key-only query entry (intermediate key) serializes as just
// its cckv, while a document-bearing entry keeps document and proof.
func TestSignedDocumentKeyOnlyJSON(t *testing.T) {
	key := "cckv://con1owner/app/posts"
	keyOnly, err := json.Marshal(SignedDocument{CCKV: &key})
	if err != nil {
		t.Fatal(err)
	}
	if string(keyOnly) != `{"cckv":"cckv://con1owner/app/posts"}` {
		t.Fatalf("key-only entry must carry only cckv, got %s", keyOnly)
	}

	full, err := json.Marshal(SignedDocument{CCKV: &key, Document: `{"kind":"record"}`, Proof: Proof{Type: ProofTypeNone}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(full, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"cckv", "document", "proof"} {
		if _, ok := decoded[field]; !ok {
			t.Fatalf("document-bearing entry must keep %q, got %s", field, full)
		}
	}
}

func TestSignedDocumentWithMetaJSON(t *testing.T) {
	sd := SignedDocument{
		Document: `{"kind":"record"}`,
		Proof:    Proof{Type: "none"},
	}
	line, err := json.Marshal(SignedDocumentWithMeta{
		SignedDocument: sd,
		Meta:           map[string]any{"id": "abc", "ip": "127.0.0.1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	var top map[string]json.RawMessage
	if err := json.Unmarshal(line, &top); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"document", "proof", "meta"} {
		if _, ok := top[k]; !ok {
			t.Fatalf("missing top-level key %q in %s", k, line)
		}
	}
	if len(top) != 3 {
		t.Fatalf("expected exactly document/proof/meta, got %s", line)
	}

	// A plain SignedDocument reader must see the same document and proof and
	// ignore meta.
	var back SignedDocument
	if err := json.Unmarshal(line, &back); err != nil {
		t.Fatal(err)
	}
	if back.Document != sd.Document || back.Proof.Type != sd.Proof.Type {
		t.Fatalf("round trip mismatch: %+v", back)
	}
}
