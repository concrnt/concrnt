package repotest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
)

// PeerDomain is the remote server PeerClient answers for.
const PeerDomain = "peer.example"

// PeerClient returns a client whose requests to PeerDomain land on an
// httptest server that serves the returned well-known document, so a
// server.Repository under test can resolve and cache it without network.
func PeerClient(t *testing.T) (*client.Client, concrnt.WellKnownConcrnt) {
	t.Helper()

	wkc := concrnt.WellKnownConcrnt{
		Version: "2.0",
		Domain:  PeerDomain,
		CSID:    "ccs1peerpeerpeerpeerpeerpeerpeerpeerpeerpe",
		Endpoints: map[string]string{
			"net.concrnt.core.resolve": "/api/v2/resolve?uri={uri}",
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/concrnt" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(wkc)
	}))
	t.Cleanup(srv.Close)

	cl := client.New(PeerDomain)
	cl.AddHostRemapping(PeerDomain, srv.URL)
	return cl, wkc
}

// RunWorldSuite runs the contract tests of the stores beside the record
// store: server.Repository, notification.Repository and abuse.Repository.
// open must return a fresh, isolated backend per call whose Server
// repository is wired to PeerClient; it is called once per top-level test.
func RunWorldSuite(t *testing.T, open func(t *testing.T) Backend) {
	t.Run("ServerResolveCachesWellKnown", func(t *testing.T) { testServerResolveCachesWellKnown(t, open(t)) })
	t.Run("NotificationSubscriptionLifecycle", func(t *testing.T) { testNotificationSubscriptionLifecycle(t, open(t)) })
	t.Run("AbuseReportInsert", func(t *testing.T) { testAbuseReportInsert(t, open(t)) })
}

// Resolving a domain fetches its well-known once and caches it; from then
// on the cached row answers lookups by domain and by csid (a csid never
// seen needs a hint and is otherwise not found), and List enumerates it.
func testServerResolveCachesWellKnown(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Server

	_, err := repo.Resolve(ctx, "ccs1peerpeerpeerpeerpeerpeerpeerpeerpeerpe", nil)
	require.True(t, errors.Is(err, domain.ErrNotFound), "an uncached csid without a hint is not found, got %v", err)

	servers, err := repo.List(ctx)
	require.NoError(t, err)
	require.Empty(t, servers)

	sv, err := repo.Resolve(ctx, PeerDomain, nil)
	require.NoError(t, err)
	require.Equal(t, PeerDomain, sv.Domain())
	require.Equal(t, "ccs1peerpeerpeerpeerpeerpeerpeerpeerpeerpe", sv.CSID())
	require.Equal(t, "/api/v2/resolve?uri={uri}", sv.WellKnown.Endpoints["net.concrnt.core.resolve"])

	sv, err = repo.Resolve(ctx, "ccs1peerpeerpeerpeerpeerpeerpeerpeerpeerpe", nil)
	require.NoError(t, err, "the cached row answers the csid lookup")
	require.Equal(t, PeerDomain, sv.Domain())

	servers, err = repo.List(ctx)
	require.NoError(t, err)
	require.Len(t, servers, 1)
	require.Equal(t, PeerDomain, servers[0].Domain)
}

// A subscription is keyed by (vendorID, owner): Subscribe upserts, Get
// reports NotFound for an unknown pair, List enumerates every row, Delete is
// idempotent.
func testNotificationSubscriptionLifecycle(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Notification

	_, err := repo.Get(ctx, "vendor-a", "con1owner")
	require.True(t, errors.Is(err, domain.ErrNotFound), "expected not found, got %v", err)

	at := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	first := domain.NotificationSubscription{
		VendorID:     "vendor-a",
		Owner:        "con1owner",
		Schemas:      []string{"https://schema.example/like.json"},
		Prefixes:     []string{"cckv://con1owner/timeline"},
		Subscription: `{"endpoint":"https://push.example/1"}`,
		CDate:        at,
		MDate:        at,
	}
	saved, err := repo.Subscribe(ctx, first)
	require.NoError(t, err)
	require.Equal(t, first.VendorID, saved.VendorID)
	require.Equal(t, first.Owner, saved.Owner)

	other := first
	other.Owner = "con1other"
	other.Subscription = `{"endpoint":"https://push.example/2"}`
	_, err = repo.Subscribe(ctx, other)
	require.NoError(t, err)

	got, err := repo.Get(ctx, "vendor-a", "con1owner")
	require.NoError(t, err)
	require.Equal(t, first.Schemas, got.Schemas)
	require.Equal(t, first.Prefixes, got.Prefixes)
	require.Equal(t, first.Subscription, got.Subscription)
	require.True(t, got.CDate.Equal(at))

	// upsert: the same (vendorID, owner) replaces the row
	updated := first
	updated.Schemas = []string{}
	updated.Prefixes = []string{"cckv://con1owner/timeline", "cckv://con1owner/dm"}
	updated.Subscription = `{"endpoint":"https://push.example/1b"}`
	_, err = repo.Subscribe(ctx, updated)
	require.NoError(t, err)

	got, err = repo.Get(ctx, "vendor-a", "con1owner")
	require.NoError(t, err)
	require.Empty(t, got.Schemas)
	require.Equal(t, updated.Prefixes, got.Prefixes)
	require.Equal(t, updated.Subscription, got.Subscription)

	all, err := repo.List(ctx)
	require.NoError(t, err)
	require.Len(t, all, 2)

	require.NoError(t, repo.Delete(ctx, "vendor-a", "con1owner"))
	require.NoError(t, repo.Delete(ctx, "vendor-a", "con1owner"), "delete is idempotent")

	_, err = repo.Get(ctx, "vendor-a", "con1owner")
	require.True(t, errors.Is(err, domain.ErrNotFound))

	all, err = repo.List(ctx)
	require.NoError(t, err)
	require.Len(t, all, 1)
	require.Equal(t, "con1other", all[0].Owner)
}

// Abuse reports are append-only: every CreateAbuseReport adds a row.
func testAbuseReportInsert(t *testing.T, be Backend) {
	ctx := context.Background()
	repo := be.Abuse

	for i := range 2 {
		require.NoError(t, repo.CreateAbuseReport(ctx, &concrnt.AbuseReport{
			TargetURI: "cckv://con1owner/timeline/post-1",
			Body:      "spam",
		}, "con1reporter", "127.0.0.1"))
		count, err := be.Inspect.AbuseReportCount(ctx)
		require.NoError(t, err)
		require.EqualValues(t, i+1, count)
	}
}
