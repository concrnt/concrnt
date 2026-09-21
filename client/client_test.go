package client

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/patrickmn/go-cache"
)

type testRecordValue struct {
	Foo string `json:"foo"`
}

// newTestIdentity generates a fresh secp256k1 key pair and its CCID, for
// building signed documents in tests.
func newTestIdentity(t *testing.T) (ccid string, privKeyHex string) {
	t.Helper()

	priv := make([]byte, 32)
	if _, err := rand.Read(priv); err != nil {
		t.Fatalf("generate private key: %v", err)
	}
	privKeyHex = hex.EncodeToString(priv)

	ccid, err := concrnt.PrivKeyToAddr(privKeyHex, "con")
	if err != nil {
		t.Fatalf("derive ccid: %v", err)
	}
	return ccid, privKeyHex
}

// newSignedRecord builds an ecrecover-proof signed document authored by ccid.
func newSignedRecord(t *testing.T, ccid, privKeyHex, key, foo string) concrnt.SignedDocument {
	t.Helper()

	doc := concrnt.Document[testRecordValue]{
		Kind:      "record",
		Key:       key,
		Value:     testRecordValue{Foo: foo},
		Author:    ccid,
		Schema:    "https://schema.concrnt.test/example.json",
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	docBytes, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal document: %v", err)
	}

	sigBytes, err := concrnt.SignBytes(docBytes, privKeyHex)
	if err != nil {
		t.Fatalf("sign document: %v", err)
	}
	signature := hex.EncodeToString(sigBytes)

	return concrnt.SignedDocument{
		Document: string(docBytes),
		Proof: concrnt.Proof{
			Type:      concrnt.ProofTypeEcrecover,
			Signature: &signature,
		},
	}
}

// newResolveTestServer returns a test client and server serving sd at the
// resolve endpoint for the given uri.
func newResolveTestServer(t *testing.T, domain string, resources map[string]concrnt.SignedDocument) *Client {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/concrnt":
			wkc := concrnt.WellKnownConcrnt{
				Version: "2.0",
				Domain:  domain,
				CSID:    "ccs1example",
				Layer:   "concrnt",
				Endpoints: map[string]string{
					"net.concrnt.core.resolve": "/resolve?uri={uri}",
				},
			}
			if err := json.NewEncoder(w).Encode(wkc); err != nil {
				t.Fatalf("encode well-known: %v", err)
			}
		case "/resolve":
			uriParam, err := url.QueryUnescape(r.URL.Query().Get("uri"))
			if err != nil {
				t.Fatalf("unescape uri: %v", err)
			}
			sd, ok := resources[uriParam]
			if !ok {
				http.NotFound(w, r)
				return
			}
			if err := json.NewEncoder(w).Encode(sd); err != nil {
				t.Fatalf("encode resource: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	cl := New(domain)
	cl.AddHostRemapping(domain, server.URL)
	return cl
}

func TestGetRecordVerifiesValidSignature(t *testing.T) {
	t.Parallel()

	const domain = "example.test"
	ccid, priv := newTestIdentity(t)
	uri := "cckv://" + ccid + "/example"
	sd := newSignedRecord(t, ccid, priv, uri, "bar")

	cl := newResolveTestServer(t, domain, map[string]concrnt.SignedDocument{uri: sd})

	var got concrnt.Document[testRecordValue]
	err := cl.GetRecord(context.Background(), uri, &Options{Resolver: domain}, &got)
	if err != nil {
		t.Fatalf("GetRecord returned error: %v", err)
	}
	if got.Value.Foo != "bar" {
		t.Fatalf("GetRecord decoded Foo = %q, want %q", got.Value.Foo, "bar")
	}
}

func TestGetRecordRejectsTamperedDocument(t *testing.T) {
	t.Parallel()

	const domain = "example.test"
	ccid, priv := newTestIdentity(t)
	uri := "cckv://" + ccid + "/example"
	sd := newSignedRecord(t, ccid, priv, uri, "bar")

	// tamper with the document after signing, keeping the original signature
	tampered := strings.Replace(sd.Document, "bar", "evil", 1)
	if tampered == sd.Document {
		t.Fatal("tampering did not change the document")
	}
	sd.Document = tampered

	cl := newResolveTestServer(t, domain, map[string]concrnt.SignedDocument{uri: sd})

	var got testRecordValue
	err := cl.GetRecord(context.Background(), uri, &Options{Resolver: domain}, &got)
	if err == nil {
		t.Fatal("GetRecord returned nil error for tampered document")
	}
	if !errors.Is(err, concrnt.ErrSignatureVerificationFailed) {
		t.Fatalf("GetRecord returned error %v, want ErrSignatureVerificationFailed", err)
	}
}

func TestGetRecordSkipVerifyBypassesSignatureCheck(t *testing.T) {
	t.Parallel()

	const domain = "example.test"
	ccid, priv := newTestIdentity(t)
	uri := "cckv://" + ccid + "/example"
	sd := newSignedRecord(t, ccid, priv, uri, "bar")
	sd.Document = strings.Replace(sd.Document, "bar", "evil", 1)

	cl := newResolveTestServer(t, domain, map[string]concrnt.SignedDocument{uri: sd})

	var got concrnt.Document[testRecordValue]
	err := cl.GetRecord(context.Background(), uri, &Options{Resolver: domain, SkipVerify: true}, &got)
	if err != nil {
		t.Fatalf("GetRecord with SkipVerify returned error: %v", err)
	}
	if got.Value.Foo != "evil" {
		t.Fatalf("GetRecord decoded Foo = %q, want %q", got.Value.Foo, "evil")
	}
}

func TestVerifyWithClientResolverRejectsNoneProof(t *testing.T) {
	t.Parallel()

	ccid, _ := newTestIdentity(t)
	doc := concrnt.Document[testRecordValue]{
		Kind:      "record",
		Key:       "cckv://" + ccid + "/example",
		Value:     testRecordValue{Foo: "bar"},
		Author:    ccid,
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	docBytes, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal document: %v", err)
	}

	sd := concrnt.SignedDocument{
		Document: string(docBytes),
		Proof:    concrnt.Proof{Type: concrnt.ProofTypeNone},
	}

	cl := New("example.test")
	err = sd.Verify(context.Background(), cl)
	if err == nil {
		t.Fatal("Verify returned nil error for none proof")
	}
}

func TestQuery(t *testing.T) {
	t.Parallel()

	const domain = "example.test"
	since := time.Date(2026, 5, 16, 10, 11, 12, 123456789, time.FixedZone("JST", 9*60*60))
	until := time.Date(2026, 5, 16, 11, 12, 13, 987654321, time.FixedZone("JST", 9*60*60))

	cckv := "cckv://con1example/concrnt.world/profiles/main"
	want := []concrnt.SignedDocument{
		{
			CCKV:     &cckv,
			Document: `{"kind":"record","key":"cckv://con1example/concrnt.world/profiles/main","value":{"username":"tester"},"author":"con1example","schema":"https://schema.concrnt.world/p/main.json","createdAt":"2026-05-16T00:00:00Z"}`,
			Proof:    concrnt.Proof{Type: concrnt.ProofTypeNone},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/concrnt":
			wkc := concrnt.WellKnownConcrnt{
				Version: "2.0",
				Domain:  domain,
				CSID:    "ccs1example",
				Layer:   "concrnt",
				Endpoints: map[string]string{
					"net.concrnt.core.query": "/query{?prefix,schema,since,until,limit,order,parent}",
				},
			}
			if err := json.NewEncoder(w).Encode(wkc); err != nil {
				t.Fatalf("encode well-known: %v", err)
			}
		case "/query":
			query := r.URL.Query()
			assertQueryParam(t, query.Get("prefix"), "cckv://")
			assertQueryParam(t, query.Get("schema"), "https://schema.concrnt.world/p/main.json")
			assertQueryParam(t, query.Get("since"), since.UTC().Format(time.RFC3339Nano))
			assertQueryParam(t, query.Get("until"), until.UTC().Format(time.RFC3339Nano))
			assertQueryParam(t, query.Get("limit"), "100")
			assertQueryParam(t, query.Get("order"), "asc")
			assertQueryParam(t, r.Header.Get("User-Agent"), "concrnt-test/dev (Concrnt)")
			assertQueryParam(t, r.Header.Get("Accept"), "application/json")

			prev := until.Format(time.RFC3339Nano)
			next := until.Add(-time.Hour).Format(time.RFC3339Nano)
			if err := json.NewEncoder(w).Encode(concrnt.QueryResult{Items: want, Prev: &prev, Next: &next}); err != nil {
				t.Fatalf("encode query response: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cl := New(domain)
	cl.SetUserAgent("concrnt-test", "dev")
	cl.AddHostRemapping(domain, server.URL)

	got, err := cl.Query(context.Background(), domain, QueryParams{
		Prefix: "cckv://",
		Schema: "https://schema.concrnt.world/p/main.json",
		Since:  &since,
		Until:  &until,
		Limit:  100,
		Order:  "asc",
	})
	if err != nil {
		t.Fatalf("Query returned error: %v", err)
	}

	if len(got.Items) != 1 {
		t.Fatalf("Query returned %d results, want 1", len(got.Items))
	}
	if got.Items[0].CCKV == nil || *got.Items[0].CCKV != cckv {
		t.Fatalf("Query returned CCKV %v, want %q", got.Items[0].CCKV, cckv)
	}
	if got.Items[0].Document != want[0].Document {
		t.Fatalf("Query returned document %q, want %q", got.Items[0].Document, want[0].Document)
	}
	if got.Prev == nil || *got.Prev != until.Format(time.RFC3339Nano) {
		t.Fatalf("Query returned prev %v, want %v", got.Prev, until.Format(time.RFC3339Nano))
	}
	if got.Next == nil || *got.Next != until.Add(-time.Hour).Format(time.RFC3339Nano) {
		t.Fatalf("Query returned next %v, want %v", got.Next, until.Add(-time.Hour).Format(time.RFC3339Nano))
	}
}

// orderby=key pages by cckv: the key cursors are sent verbatim as since/until
// and the response cursors are keys (CIP-5 §3.3).
func TestQueryOrderByKey(t *testing.T) {
	t.Parallel()

	const domain = "example.test"
	folder := "cckv://con1example/concrnt.world/profiles/main/posts"
	doc := "cckv://con1example/concrnt.world/profiles/main/lists"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/concrnt":
			wkc := concrnt.WellKnownConcrnt{
				Version: "2.0",
				Domain:  domain,
				CSID:    "ccs1example",
				Layer:   "concrnt",
				Endpoints: map[string]string{
					"net.concrnt.core.query": "/query{?prefix,schema,since,until,limit,order,parent,author,orderby}",
				},
			}
			if err := json.NewEncoder(w).Encode(wkc); err != nil {
				t.Fatalf("encode well-known: %v", err)
			}
		case "/query":
			query := r.URL.Query()
			assertQueryParam(t, query.Get("parent"), "cckv://con1example/concrnt.world/profiles/main")
			assertQueryParam(t, query.Get("orderby"), "key")
			assertQueryParam(t, query.Get("since"), doc)
			assertQueryParam(t, query.Get("until"), "")
			assertQueryParam(t, query.Get("order"), "asc")

			items := []concrnt.SignedDocument{
				{CCKV: &doc, Document: `{"kind":"record"}`, Proof: concrnt.Proof{Type: concrnt.ProofTypeNone}},
				{CCKV: &folder},
			}
			if err := json.NewEncoder(w).Encode(concrnt.QueryResult{Items: items, Prev: &doc, Next: &folder}); err != nil {
				t.Fatalf("encode query response: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cl := New(domain)
	cl.AddHostRemapping(domain, server.URL)

	got, err := cl.Query(context.Background(), domain, QueryParams{
		Parent:   "cckv://con1example/concrnt.world/profiles/main",
		OrderBy:  "key",
		SinceKey: doc,
		Order:    "asc",
	})
	if err != nil {
		t.Fatalf("Query returned error: %v", err)
	}
	if len(got.Items) != 2 {
		t.Fatalf("Query returned %d results, want 2", len(got.Items))
	}
	if got.Items[1].CCKV == nil || *got.Items[1].CCKV != folder || got.Items[1].Document != "" {
		t.Fatalf("key-only entry not decoded as such: %+v", got.Items[1])
	}
	if got.Next == nil || *got.Next != folder {
		t.Fatalf("Query returned next %v, want %q", got.Next, folder)
	}
}

func TestQueryRejectsOrderByKeyWithPrefix(t *testing.T) {
	t.Parallel()

	cl := New("example.test")
	_, err := cl.Query(context.Background(), "example.test", QueryParams{
		Prefix:  "cckv://",
		OrderBy: "key",
	})
	if err == nil {
		t.Fatal("Query returned nil error")
	}
}

func TestQueryRejectsPrefixAndParent(t *testing.T) {
	t.Parallel()

	cl := New("example.test")
	_, err := cl.Query(context.Background(), "example.test", QueryParams{
		Prefix: "cckv://",
		Parent: "cckv://con1example",
	})
	if err == nil {
		t.Fatal("Query returned nil error")
	}
}

func TestQueryEndpointMissing(t *testing.T) {
	t.Parallel()

	const domain = "example.test"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wkc := concrnt.WellKnownConcrnt{
			Version:   "2.0",
			Domain:    domain,
			CSID:      "ccs1example",
			Layer:     "concrnt",
			Endpoints: map[string]string{},
		}
		if err := json.NewEncoder(w).Encode(wkc); err != nil {
			t.Fatalf("encode well-known: %v", err)
		}
	}))
	defer server.Close()

	cl := New(domain)
	cl.AddHostRemapping(domain, server.URL)

	_, err := cl.Query(context.Background(), domain, QueryParams{Prefix: "cckv://"})
	if !errors.Is(err, ErrEndpointMissing) {
		t.Fatalf("Query returned error %v, want ErrEndpointMissing", err)
	}
}

func TestQuerySkipsOfflineDomain(t *testing.T) {
	t.Parallel()

	const domain = "example.test"
	cl := &Client{
		client:     &http.Client{},
		cache:      cache.New(10*time.Minute, 15*time.Minute),
		lastFailed: map[string]time.Time{domain: time.Now()},
		failCount:  make(map[string]int),
	}
	cl.cache.Set("server:"+domain, concrnt.WellKnownConcrnt{
		Version: "2.0",
		Domain:  domain,
		CSID:    "ccs1example",
		Layer:   "concrnt",
		Endpoints: map[string]string{
			"net.concrnt.core.query": "/query{?prefix}",
		},
	}, cache.DefaultExpiration)

	_, err := cl.Query(context.Background(), domain, QueryParams{Prefix: "cckv://"})
	if err == nil || err.Error() != "Domain is offline" {
		t.Fatalf("Query returned error %v, want Domain is offline", err)
	}
}

func TestCall(t *testing.T) {
	t.Parallel()

	const domain = "example.test"
	want := []any{
		map[string]any{"document": `{"kind":"ack"}`},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/concrnt":
			wkc := concrnt.WellKnownConcrnt{
				Version: "2.0",
				Domain:  domain,
				CSID:    "ccs1example",
				Layer:   "concrnt",
				Endpoints: map[string]string{
					"net.concrnt.test.list": "/list{?from,to}",
				},
			}
			if err := json.NewEncoder(w).Encode(wkc); err != nil {
				t.Fatalf("encode well-known: %v", err)
			}
		case "/list":
			query := r.URL.Query()
			assertQueryParam(t, query.Get("from"), "con1alice")
			assertQueryParam(t, query.Get("to"), "con1bob")
			assertQueryParam(t, r.Header.Get("Accept"), "application/json")

			if err := json.NewEncoder(w).Encode(want); err != nil {
				t.Fatalf("encode call response: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cl := New(domain)
	cl.AddHostRemapping(domain, server.URL)

	var got any
	err := cl.Call(context.Background(), domain, "net.concrnt.test.list", map[string]string{
		"from": "con1alice",
		"to":   "con1bob",
	}, nil, &got)
	if err != nil {
		t.Fatalf("Call returned error: %v", err)
	}

	gotList, ok := got.([]any)
	if !ok {
		t.Fatalf("Call decoded %T, want []any", got)
	}
	if len(gotList) != 1 {
		t.Fatalf("Call returned %d results, want 1", len(gotList))
	}
	gotEntry, ok := gotList[0].(map[string]any)
	if !ok || gotEntry["document"] != `{"kind":"ack"}` {
		t.Fatalf("Call returned entry %v, want %v", gotList[0], want[0])
	}
}

func TestCallEndpointMissing(t *testing.T) {
	t.Parallel()

	const domain = "example.test"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wkc := concrnt.WellKnownConcrnt{
			Version:   "2.0",
			Domain:    domain,
			CSID:      "ccs1example",
			Layer:     "concrnt",
			Endpoints: map[string]string{},
		}
		if err := json.NewEncoder(w).Encode(wkc); err != nil {
			t.Fatalf("encode well-known: %v", err)
		}
	}))
	defer server.Close()

	cl := New(domain)
	cl.AddHostRemapping(domain, server.URL)

	var got any
	err := cl.Call(context.Background(), domain, "net.concrnt.test.list", map[string]string{}, nil, &got)
	if !errors.Is(err, ErrEndpointMissing) {
		t.Fatalf("Call returned error %v, want ErrEndpointMissing", err)
	}
}

func TestGetResourceBatchUsesBatchEndpoint(t *testing.T) {
	t.Parallel()

	const domain = "example.test"
	uris := []string{
		"cckv://example.test/concrnt.world/profiles/main",
		"cckv://example.test/concrnt.world/profiles/sub",
	}
	resources := map[string]map[string]string{
		uris[0]: {"name": "main"},
		uris[1]: {"name": "sub"},
	}

	var batchHits atomic.Int64
	var handler http.Handler
	handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/concrnt":
			wkc := concrnt.WellKnownConcrnt{
				Version: "2.0",
				Domain:  domain,
				CSID:    "ccs1example",
				Layer:   "concrnt",
				Endpoints: map[string]string{
					"net.concrnt.core.resolve": "/resolve?uri={uri}",
					"net.concrnt.core.batch":   "/batch",
				},
			}
			if err := json.NewEncoder(w).Encode(wkc); err != nil {
				t.Fatalf("encode well-known: %v", err)
			}
		case "/resolve":
			uriParam, err := url.QueryUnescape(r.URL.Query().Get("uri"))
			if err != nil {
				t.Fatalf("unescape uri: %v", err)
			}
			resource, ok := resources[uriParam]
			if !ok {
				http.NotFound(w, r)
				return
			}
			if r.Header.Get("Accept") != "application/json" {
				t.Fatalf("Accept = %q, want application/json", r.Header.Get("Accept"))
			}
			if err := json.NewEncoder(w).Encode(resource); err != nil {
				t.Fatalf("encode resource: %v", err)
			}
		case "/batch":
			batchHits.Add(1)
			serveTestBatch(t, handler, w, r)
		default:
			http.NotFound(w, r)
		}
	})

	server := httptest.NewServer(handler)
	defer server.Close()

	cl := New(domain)
	cl.AddHostRemapping(domain, server.URL)

	got := make([]map[string]string, len(uris))
	results := []any{&got[0], &got[1]}
	err := cl.GetResourceBatch(context.Background(), uris, "application/json", nil, results)
	if err != nil {
		t.Fatalf("GetResourceBatch returned error: %v", err)
	}

	if batchHits.Load() != 1 {
		t.Fatalf("batch endpoint hit %d times, want 1", batchHits.Load())
	}
	for i, uri := range uris {
		if got[i]["name"] != resources[uri]["name"] {
			t.Fatalf("resource %d name = %q, want %q", i, got[i]["name"], resources[uri]["name"])
		}
	}
}

func TestGetResourceBatchDecodesLargeBatchResponse(t *testing.T) {
	t.Parallel()

	const domain = "example.test"
	uris := []string{
		"cckv://example.test/concrnt.world/profiles/main",
		"cckv://example.test/concrnt.world/profiles/sub",
	}
	resources := map[string]map[string]string{
		uris[0]: {"name": "main", "body": strings.Repeat("a", 4096)},
		uris[1]: {"name": "sub", "body": strings.Repeat("b", 4096)},
	}

	var handler http.Handler
	handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/concrnt":
			wkc := concrnt.WellKnownConcrnt{
				Version: "2.0",
				Domain:  domain,
				CSID:    "ccs1example",
				Layer:   "concrnt",
				Endpoints: map[string]string{
					"net.concrnt.core.resolve": "/resolve?uri={uri}",
					"net.concrnt.core.batch":   "/batch",
				},
			}
			if err := json.NewEncoder(w).Encode(wkc); err != nil {
				t.Fatalf("encode well-known: %v", err)
			}
		case "/resolve":
			uriParam, err := url.QueryUnescape(r.URL.Query().Get("uri"))
			if err != nil {
				t.Fatalf("unescape uri: %v", err)
			}
			resource, ok := resources[uriParam]
			if !ok {
				http.NotFound(w, r)
				return
			}
			if err := json.NewEncoder(w).Encode(resource); err != nil {
				t.Fatalf("encode resource: %v", err)
			}
		case "/batch":
			serveTestBatch(t, handler, w, r)
		default:
			http.NotFound(w, r)
		}
	})

	server := httptest.NewServer(handler)
	defer server.Close()

	cl := New(domain)
	cl.AddHostRemapping(domain, server.URL)

	got := make([]map[string]string, len(uris))
	results := []any{&got[0], &got[1]}
	err := cl.GetResourceBatch(context.Background(), uris, "application/json", nil, results)
	if err != nil {
		t.Fatalf("GetResourceBatch returned error: %v", err)
	}

	for i, uri := range uris {
		if got[i]["body"] != resources[uri]["body"] {
			t.Fatalf("resource %d body length = %d, want %d", i, len(got[i]["body"]), len(resources[uri]["body"]))
		}
	}
}

func TestGetResourceBatchFallsBackWhenBatchEndpointMissing(t *testing.T) {
	t.Parallel()

	const domain = "example.test"
	uris := []string{
		"cckv://example.test/item/0",
		"cckv://example.test/item/1",
		"cckv://example.test/item/2",
		"cckv://example.test/item/3",
		"cckv://example.test/item/4",
		"cckv://example.test/item/5",
		"cckv://example.test/item/6",
	}

	var current atomic.Int64
	var maxConcurrent atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/concrnt":
			wkc := concrnt.WellKnownConcrnt{
				Version: "2.0",
				Domain:  domain,
				CSID:    "ccs1example",
				Layer:   "concrnt",
				Endpoints: map[string]string{
					"net.concrnt.core.resolve": "/resolve?uri={uri}",
				},
			}
			if err := json.NewEncoder(w).Encode(wkc); err != nil {
				t.Fatalf("encode well-known: %v", err)
			}
		case "/resolve":
			now := current.Add(1)
			for {
				seen := maxConcurrent.Load()
				if now <= seen || maxConcurrent.CompareAndSwap(seen, now) {
					break
				}
			}
			defer current.Add(-1)

			time.Sleep(25 * time.Millisecond)

			uriParam, err := url.QueryUnescape(r.URL.Query().Get("uri"))
			if err != nil {
				t.Fatalf("unescape uri: %v", err)
			}
			if err := json.NewEncoder(w).Encode(map[string]string{"uri": uriParam}); err != nil {
				t.Fatalf("encode resource: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cl := New(domain)
	cl.AddHostRemapping(domain, server.URL)

	got := make([]map[string]string, len(uris))
	results := make([]any, len(uris))
	for i := range got {
		results[i] = &got[i]
	}

	err := cl.GetResourceBatch(context.Background(), uris, "application/json", nil, results)
	if err != nil {
		t.Fatalf("GetResourceBatch returned error: %v", err)
	}

	if maxConcurrent.Load() <= 1 {
		t.Fatalf("fallback max concurrency = %d, want > 1", maxConcurrent.Load())
	}
	if maxConcurrent.Load() > batchFallbackConcurrency {
		t.Fatalf("fallback max concurrency = %d, want <= %d", maxConcurrent.Load(), batchFallbackConcurrency)
	}
	for i, uri := range uris {
		if got[i]["uri"] != uri {
			t.Fatalf("resource %d uri = %q, want %q", i, got[i]["uri"], uri)
		}
	}
}

func TestTimeoutMarksDomainOffline(t *testing.T) {
	t.Parallel()

	const domain = "example.test"
	cl := &Client{
		lastFailed: make(map[string]time.Time),
		failCount:  make(map[string]int),
	}

	cl.markOfflineIfTimeout(domain, "testing", timeoutError{})

	if cl.IsOnline(domain) {
		t.Fatal("domain is online after timeout")
	}
}

func serveTestBatch(t *testing.T, handler http.Handler, w http.ResponseWriter, r *http.Request) {
	t.Helper()

	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		t.Fatalf("parse batch content type: %v", err)
	}
	if !strings.EqualFold(mediaType, "multipart/mixed") {
		t.Fatalf("batch content type = %q, want multipart/mixed", mediaType)
	}
	boundary := params["boundary"]
	if boundary == "" {
		t.Fatal("missing batch boundary")
	}

	w.Header().Set("Content-Type", "multipart/mixed; boundary="+boundary)
	mw := multipart.NewWriter(w)
	if err := mw.SetBoundary(boundary); err != nil {
		t.Fatalf("set response boundary: %v", err)
	}
	defer mw.Close()

	mr := multipart.NewReader(r.Body, boundary)
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read batch part: %v", err)
		}

		contentID := part.Header.Get("Content-ID")
		req, err := http.ReadRequest(bufio.NewReader(part))
		if err != nil {
			t.Fatalf("read batch request: %v", err)
		}

		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)
		resp := recorder.Result()

		pw, err := mw.CreatePart(map[string][]string{
			"Content-Type": {"application/http"},
			"Content-ID":   {contentID},
		})
		if err != nil {
			t.Fatalf("create batch response part: %v", err)
		}
		if err := resp.Write(pw); err != nil {
			t.Fatalf("write batch response: %v", err)
		}
		resp.Body.Close()
	}
}

func assertQueryParam(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

var _ interface {
	error
	Timeout() bool
	Temporary() bool
} = timeoutError{}

func TestBatchGetIsolatesDomainFailures(t *testing.T) {
	t.Parallel()

	const alive = "alive.test"
	const broken = "broken.test"
	const offline = "offline.test"

	newHandler := func(domain string, batchStatus int) http.Handler {
		var handler http.Handler
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/.well-known/concrnt":
				wkc := concrnt.WellKnownConcrnt{
					Version: "2.0",
					Domain:  domain,
					CSID:    "ccs1" + domain,
					Layer:   "concrnt",
					Endpoints: map[string]string{
						"net.concrnt.core.batch": "/batch",
					},
				}
				if err := json.NewEncoder(w).Encode(wkc); err != nil {
					t.Fatalf("encode well-known: %v", err)
				}
			case "/batch":
				if batchStatus != http.StatusOK {
					http.Error(w, "boom", batchStatus)
					return
				}
				serveTestBatch(t, handler, w, r)
			default:
				if strings.HasPrefix(r.URL.Path, "/item/") {
					fmt.Fprint(w, strings.TrimPrefix(r.URL.Path, "/item/"))
					return
				}
				http.NotFound(w, r)
			}
		})
		return handler
	}

	aliveServer := httptest.NewServer(newHandler(alive, http.StatusOK))
	defer aliveServer.Close()
	brokenServer := httptest.NewServer(newHandler(broken, http.StatusInternalServerError))
	defer brokenServer.Close()

	cl := New(alive)
	cl.AddHostRemapping(alive, aliveServer.URL)
	cl.AddHostRemapping(broken, brokenServer.URL)
	cl.markOffline(offline)

	responses, err := cl.BatchGet(context.Background(), map[string]map[string]string{
		alive: {
			"a1": "https://" + alive + "/item/a1",
			"a2": "https://" + alive + "/item/a2",
		},
		broken: {
			"b1": "https://" + broken + "/item/b1",
		},
		offline: {
			"o1": "https://" + offline + "/item/o1",
		},
	})
	if err != nil {
		t.Fatalf("BatchGet returned error: %v", err)
	}

	for _, key := range []string{"a1", "a2"} {
		resp, ok := responses[key]
		if !ok {
			t.Fatalf("missing response for %s", key)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("read body for %s: %v", key, err)
		}
		if resp.StatusCode != http.StatusOK || string(body) != key {
			t.Fatalf("response for %s = %d %q, want 200 %q", key, resp.StatusCode, body, key)
		}
	}
	for _, key := range []string{"b1", "o1"} {
		if _, ok := responses[key]; ok {
			t.Fatalf("unexpected response for %s", key)
		}
	}
}
