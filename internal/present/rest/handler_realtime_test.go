package rest

// Tests for the realtime websocket session protocol (CIP-11 §3.1):
//
//  1. Every subscribe/listen/unlisten is acknowledged with the subscription
//     list now in effect, a re-sent identical list (the client heartbeat) is
//     acknowledged without re-subscribing, and unknown types are ignored.
//  2. Events from the pubsub are delivered on the socket.
//  3. A quiet session is never dropped by the server (no ping / read
//     deadline): liveness is the client's job via the subscribe round trip.

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/usecase/subscription"
)

type noopEnsurer struct{}

func (noopEnsurer) EnsureSubscriptions(context.Context, []string) {}

// fakePubsub hands the latest response channel to the test so it can inject
// events as if they came from redis.
type fakePubsub struct {
	mu         sync.Mutex
	subscribes int
	prefixes   []string
	response   chan<- concrnt.Event
}

func (f *fakePubsub) Publish(context.Context, string, concrnt.Event) error { return nil }
func (f *fakePubsub) SubscribeAll(context.Context, chan<- concrnt.Event) error {
	return nil
}
func (f *fakePubsub) Subscribe(_ context.Context, prefixes []string, response chan<- concrnt.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subscribes++
	f.prefixes = prefixes
	f.response = response
	return nil
}

func newRealtimeTestServer(t *testing.T) (*httptest.Server, *fakePubsub) {
	t.Helper()
	ps := &fakePubsub{}
	h := &Handler{subscribe: subscription.New(noopEnsurer{}, ps)}
	e := echo.New()
	e.HideBanner = true
	e.GET("/realtime", h.handleRealtime)
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)
	return srv, ps
}

func dialRealtime(t *testing.T, srv *httptest.Server) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/realtime"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func readSubscribed(t *testing.T, conn *websocket.Conn) []string {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	var reply concrnt.RealtimeSubscribed
	require.NoError(t, conn.ReadJSON(&reply))
	require.Equal(t, "subscribed", reply.Type)
	return reply.Prefixes
}

func subscribeCount(ps *fakePubsub) int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.subscribes
}

func TestRealtimeSubscribeIsAcknowledged(t *testing.T) {
	srv, ps := newRealtimeTestServer(t)
	conn := dialRealtime(t, srv)

	require.NoError(t, conn.WriteJSON(concrnt.RealtimeRequest{Type: "listen", Prefixes: []string{"cckv://a/x", "cckv://b/y"}}))
	require.Equal(t, []string{"cckv://a/x", "cckv://b/y"}, readSubscribed(t, conn))
	require.Eventually(t, func() bool { return subscribeCount(ps) == 1 }, 2*time.Second, 10*time.Millisecond)

	// the same list re-sent (client heartbeat) is acknowledged but does not
	// tear down and re-create the pubsub subscription
	require.NoError(t, conn.WriteJSON(concrnt.RealtimeRequest{Type: "listen", Prefixes: []string{"cckv://b/y", "cckv://a/x"}}))
	require.Equal(t, []string{"cckv://b/y", "cckv://a/x"}, readSubscribed(t, conn))
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, subscribeCount(ps))

	// subscribe replaces the whole list; unlisten carrying the shrunk list is the same operation
	require.NoError(t, conn.WriteJSON(concrnt.RealtimeRequest{Type: "subscribe", Prefixes: []string{"cckv://b/y"}}))
	require.Equal(t, []string{"cckv://b/y"}, readSubscribed(t, conn))
	require.Eventually(t, func() bool { return subscribeCount(ps) == 2 }, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, conn.WriteJSON(concrnt.RealtimeRequest{Type: "unlisten", Prefixes: []string{}}))
	// an empty list is acknowledged as [] (not null)
	require.Equal(t, []string{}, readSubscribed(t, conn))

	// unknown types and heartbeats are ignored without dropping the session
	require.NoError(t, conn.WriteJSON(map[string]any{"type": "bogus"}))
	require.NoError(t, conn.WriteJSON(map[string]any{"type": "h"}))
	require.NoError(t, conn.WriteJSON(map[string]any{"type": "listen", "prefixes": []string{"cckv://c/z"}}))
	require.Equal(t, []string{"cckv://c/z"}, readSubscribed(t, conn))
}

func TestRealtimeDeliversEvents(t *testing.T) {
	srv, ps := newRealtimeTestServer(t)
	conn := dialRealtime(t, srv)

	require.NoError(t, conn.WriteJSON(concrnt.RealtimeRequest{Type: "subscribe", Prefixes: []string{"cckv://a/x"}}))
	// the acknowledgement guarantees the subscribe reached the usecase
	readSubscribed(t, conn)

	require.Eventually(t, func() bool {
		ps.mu.Lock()
		defer ps.mu.Unlock()
		return ps.response != nil
	}, 2*time.Second, 10*time.Millisecond)
	ps.mu.Lock()
	response := ps.response
	ps.mu.Unlock()

	go func() {
		response <- concrnt.Event{Type: "created", Source: "cckv://a/x/1", URI: "cckv://a/x/1"}
	}()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, raw, err := conn.ReadMessage()
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Equal(t, "created", got["type"])
	require.Equal(t, "cckv://a/x/1", got["source"])
}

func TestRealtimeKeepsQuietSessions(t *testing.T) {
	srv, _ := newRealtimeTestServer(t)
	conn := dialRealtime(t, srv)

	// no frames in either direction for a while, then the session still answers
	time.Sleep(500 * time.Millisecond)
	require.NoError(t, conn.WriteJSON(map[string]any{"type": "listen", "prefixes": []string{"cckv://a/x"}}))
	require.Equal(t, []string{"cckv://a/x"}, readSubscribed(t, conn))
}
