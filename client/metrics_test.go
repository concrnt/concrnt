package client

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRoundTripRecordsPeerRequests(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	defer srv.Close()

	cl := &Client{
		client:     &http.Client{},
		cache:      cache.New(10*time.Minute, 15*time.Minute),
		lastFailed: make(map[string]time.Time),
		failCount:  make(map[string]int),
		remappings: make(map[string]*url.URL),
		transport:  http.DefaultTransport,
	}
	cl.client.Transport = cl
	// the metric must carry the logical peer host, not where a remap sent it
	cl.AddHostRemapping("peer.test", srv.URL)

	before := testutil.ToFloat64(peerRequests.WithLabelValues("peer.test", "418"))
	resp, err := cl.client.Get("http://peer.test/anything")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()
	if got := testutil.ToFloat64(peerRequests.WithLabelValues("peer.test", "418")); got != before+1 {
		t.Fatalf("peer_requests_total{host=peer.test,code=418} = %v, want %v", got, before+1)
	}

	// a connection failure is counted as code="error"
	cl.AddHostRemapping("down.test", "http://127.0.0.1:1")
	before = testutil.ToFloat64(peerRequests.WithLabelValues("down.test", "error"))
	if _, err := cl.client.Get("http://down.test/"); err == nil {
		t.Fatal("expected connection error")
	}
	if got := testutil.ToFloat64(peerRequests.WithLabelValues("down.test", "error")); got != before+1 {
		t.Fatalf("peer_requests_total{host=down.test,code=error} = %v, want %v", got, before+1)
	}
}

func TestPeerOfflineCollector(t *testing.T) {
	cl := &Client{
		lastFailed: map[string]time.Time{"gone.test": time.Now()},
		failCount:  make(map[string]int),
	}
	expected := `
# HELP concrnt_peer_offline Peer hosts currently considered offline by the federation client; the value is always 1.
# TYPE concrnt_peer_offline gauge
concrnt_peer_offline{host="gone.test"} 1
`
	if err := testutil.CollectAndCompare(newPeerOfflineCollector(cl), strings.NewReader(expected)); err != nil {
		t.Fatal(err)
	}

	// recovery makes the series disappear rather than flip to 0
	delete(cl.lastFailed, "gone.test")
	if n := testutil.CollectAndCount(newPeerOfflineCollector(cl)); n != 0 {
		t.Fatalf("expected no series after recovery, got %d", n)
	}
}
