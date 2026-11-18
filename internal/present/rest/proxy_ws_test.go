package rest

// These tests validate two properties the atproto bridge relies on when it
// sits behind the concrnt gateway on a NoAuth path:
//
//  1. WebSocket upgrades (the atproto firehose) survive the reverse proxy,
//     including the otelhttp-wrapped transport used in proxy.go.
//  2. A client-supplied cc-requester header is stripped by the proxy handler
//     and never reaches the backend (identity spoofing prevention).
//
// The proxy pipeline here mirrors proxy.go: NewSingleHostReverseProxy with the
// same Director and otelhttp transport, wrapped in an echo handler that sanitizes
// the requester headers.

import (
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v5"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/concrnt/concrnt/impl/interop"
)

// newGatewayProxy builds an echo server that proxies to backendHost:backendPort
// exactly like proxy.go (PreservePath + otelhttp transport + header sanitize).
func newGatewayProxy(t *testing.T, backendHost string, backendPort int) *httptest.Server {
	t.Helper()
	targetURL, _ := url.Parse("http://" + backendHost + ":" + strconv.Itoa(backendPort))
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = targetURL.Scheme
		req.URL.Host = targetURL.Host
		left := targetURL.Path
		if !strings.HasSuffix(left, "/") {
			left += "/"
		}
		req.URL.Path = left + strings.TrimPrefix(req.URL.Path, "/")
		_ = path.Join // keep parity with proxy.go import surface
	}
	proxy.Transport = otelhttp.NewTransport(http.DefaultTransport)

	e := echo.New()
	e.Any("/*", func(c *echo.Context) error {
		c.Request().Header.Del(interop.RequesterHeader)
		c.Request().Header.Del(interop.RequesterTagHeader)
		proxy.ServeHTTP(c.Response(), c.Request())
		return nil
	})
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)
	return srv
}

func TestGatewayProxiesWebSocket(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/xrpc/com.atproto.sync.subscribeRepos" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.BinaryMessage, []byte("firehose-frame"))
		time.Sleep(100 * time.Millisecond)
	}))
	defer backend.Close()

	host, portStr, _ := strings.Cut(strings.TrimPrefix(backend.URL, "http://"), ":")
	port, _ := strconv.Atoi(portStr)
	gw := newGatewayProxy(t, host, port)

	wsURL := "ws" + strings.TrimPrefix(gw.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos"
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		t.Fatalf("WS dial through gateway failed (status %d): %v", code, err)
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	mt, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("reading frame through gateway failed: %v", err)
	}
	if mt != websocket.BinaryMessage || string(msg) != "firehose-frame" {
		t.Fatalf("unexpected frame: type=%d body=%q", mt, msg)
	}
}

func TestGatewayStripsSpoofedRequester(t *testing.T) {
	var seen string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get(interop.RequesterHeader)
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	host, portStr, _ := strings.Cut(strings.TrimPrefix(backend.URL, "http://"), ":")
	port, _ := strconv.Atoi(portStr)
	gw := newGatewayProxy(t, host, port)

	req, _ := http.NewRequest(http.MethodGet, gw.URL+"/xrpc/_health", nil)
	req.Header.Set(interop.RequesterHeader, `{"ccid":"con1attacker"}`)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if seen != "" {
		t.Fatalf("spoofed cc-requester reached backend: %q", seen)
	}
}
