package rest

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"sync"
	"testing"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/chunkline"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	chunklineuc "github.com/concrnt/concrnt/internal/usecase/chunkline"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

type batchChunklineRepo struct {
	record.Repository
	mu      sync.Mutex
	calls   []batchChunklineLookupCall
	missing map[string]bool
}

type batchChunklineLookupCall struct {
	uris    []string
	chunkID int64
}

func (r *batchChunklineRepo) GetChunklineManifest(ctx context.Context, uri string) (*chunkline.Manifest, error) {
	return nil, fmt.Errorf("not implemented")
}

func (r *batchChunklineRepo) LookupLocalItrs(ctx context.Context, uris []string, chunkID int64) (map[string]int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, batchChunklineLookupCall{
		uris:    append([]string(nil), uris...),
		chunkID: chunkID,
	})

	results := make(map[string]int64, len(uris))
	for i, uri := range uris {
		if r.missing[uri] {
			continue
		}
		results[uri] = chunkID - int64(i)
	}
	return results, nil
}

func (r *batchChunklineRepo) LoadLocalBody(ctx context.Context, uri string, chunkID int64) ([]chunkline.BodyItem, error) {
	return nil, fmt.Errorf("not implemented")
}

func TestBatchHandlerAggregatesChunklineItrRequests(t *testing.T) {
	repo := &batchChunklineRepo{}
	handler := NewHandler(
		domain.Config{},
		nil,
		nil,
		chunklineuc.New(repo, nil, nil, nil),
		nil,
		nil,
		nil,
		nil,
	)

	app := echo.New()
	handler.RegisterRoutes(app, app.Group(""))

	server := httptest.NewServer(app)
	t.Cleanup(server.Close)

	requests := map[string]*http.Request{}
	req0, err := http.NewRequest("GET", server.URL+apiPrefix+"/chunkline/itr/100?uri=cckv://example.test/timeline/0", nil)
	require.NoError(t, err)
	requests["0"] = req0
	req1, err := http.NewRequest("GET", server.URL+apiPrefix+"/chunkline/itr/100?uri=cckv://example.test/timeline/1", nil)
	require.NoError(t, err)
	requests["1"] = req1

	batchPath, err := concrnt.RenderURITemplate(Endpoints["net.concrnt.core.batch"], map[string]string{})
	require.NoError(t, err)

	responses, err := client.DoBatchRequestWithClient(context.Background(), server.Client(), server.URL+batchPath, requests)
	require.NoError(t, err)

	require.Len(t, responses, 2)
	require.Equal(t, http.StatusOK, responses["0"].StatusCode)
	require.Equal(t, http.StatusOK, responses["1"].StatusCode)

	require.Len(t, repo.calls, 1)
	require.Equal(t, int64(100), repo.calls[0].chunkID)
	require.ElementsMatch(t, []string{
		"cckv://example.test/timeline/0",
		"cckv://example.test/timeline/1",
	}, repo.calls[0].uris)
}

// A timeline with no iterator (absent from the lookup result) must yield a
// 404, not a 200 "0", both on the plain endpoint and inside a batch part.
func TestChunklineItrMissingIsNotFound(t *testing.T) {
	present := "cckv://example.test/timeline/0"
	absent := "cckv://example.test/timeline/empty"

	repo := &batchChunklineRepo{missing: map[string]bool{absent: true}}
	handler := NewHandler(
		domain.Config{},
		nil,
		nil,
		chunklineuc.New(repo, nil, nil, nil),
		nil,
		nil,
		nil,
		nil,
	)

	app := echo.New()
	handler.RegisterRoutes(app, app.Group(""))

	server := httptest.NewServer(app)
	t.Cleanup(server.Close)

	resp, err := server.Client().Get(server.URL + apiPrefix + "/chunkline/itr/100?uri=" + absent)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	requests := map[string]*http.Request{}
	req0, err := http.NewRequest("GET", server.URL+apiPrefix+"/chunkline/itr/100?uri="+present, nil)
	require.NoError(t, err)
	requests["0"] = req0
	req1, err := http.NewRequest("GET", server.URL+apiPrefix+"/chunkline/itr/100?uri="+absent, nil)
	require.NoError(t, err)
	requests["1"] = req1

	batchPath, err := concrnt.RenderURITemplate(Endpoints["net.concrnt.core.batch"], map[string]string{})
	require.NoError(t, err)

	responses, err := client.DoBatchRequestWithClient(context.Background(), server.Client(), server.URL+batchPath, requests)
	require.NoError(t, err)

	require.Len(t, responses, 2)
	require.Equal(t, http.StatusOK, responses["0"].StatusCode)
	require.Equal(t, http.StatusNotFound, responses["1"].StatusCode)
}

var _ record.Repository = (*batchChunklineRepo)(nil)

// A batch carrying more than maxBatchParts application/http parts is rejected
// with a 400 before any part is dispatched.
func TestBatchHandlerRejectsTooManyParts(t *testing.T) {
	repo := &batchChunklineRepo{}
	handler := NewHandler(
		domain.Config{},
		nil,
		nil,
		chunklineuc.New(repo, nil, nil, nil),
		nil,
		nil,
		nil,
		nil,
	)

	app := echo.New()
	handler.RegisterRoutes(app, app.Group(""))

	server := httptest.NewServer(app)
	t.Cleanup(server.Close)

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for i := 0; i <= maxBatchParts; i++ {
		pw, err := mw.CreatePart(textproto.MIMEHeader{
			"Content-Type": {"application/http"},
			"Content-ID":   {fmt.Sprintf("%d", i)},
		})
		require.NoError(t, err)
		_, err = fmt.Fprintf(pw, "GET %s/chunkline/itr/100?uri=cckv://example.test/timeline/0 HTTP/1.1\r\nHost: example.test\r\n\r\n", apiPrefix)
		require.NoError(t, err)
	}
	require.NoError(t, mw.Close())

	batchPath, err := concrnt.RenderURITemplate(Endpoints["net.concrnt.core.batch"], map[string]string{})
	require.NoError(t, err)

	resp, err := server.Client().Post(server.URL+batchPath, "multipart/mixed; boundary="+mw.Boundary(), &buf)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Empty(t, repo.calls, "no part should be dispatched")
}

// Two parts sharing a Content-ID are rejected with an outer 400 before any
// part is dispatched (CIP-14 §5: Content-IDs must be unique within a batch).
func TestBatchHandlerRejectsDuplicateContentID(t *testing.T) {
	repo := &batchChunklineRepo{}
	handler := NewHandler(
		domain.Config{},
		nil,
		nil,
		chunklineuc.New(repo, nil, nil, nil),
		nil,
		nil,
		nil,
		nil,
	)

	app := echo.New()
	handler.RegisterRoutes(app, app.Group(""))

	server := httptest.NewServer(app)
	t.Cleanup(server.Close)

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for i := 0; i < 2; i++ {
		pw, err := mw.CreatePart(textproto.MIMEHeader{
			"Content-Type": {"application/http"},
			"Content-ID":   {"dup"},
		})
		require.NoError(t, err)
		_, err = fmt.Fprintf(pw, "GET %s/chunkline/itr/100?uri=cckv://example.test/timeline/%d HTTP/1.1\r\nHost: example.test\r\n\r\n", apiPrefix, i)
		require.NoError(t, err)
	}
	require.NoError(t, mw.Close())

	batchPath, err := concrnt.RenderURITemplate(Endpoints["net.concrnt.core.batch"], map[string]string{})
	require.NoError(t, err)

	resp, err := server.Client().Post(server.URL+batchPath, "multipart/mixed; boundary="+mw.Boundary(), &buf)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Empty(t, repo.calls, "no part should be dispatched")
}

// A part without a Content-ID is rejected with an outer 400 before any part
// is dispatched.
func TestBatchHandlerRejectsMissingContentID(t *testing.T) {
	repo := &batchChunklineRepo{}
	handler := NewHandler(
		domain.Config{},
		nil,
		nil,
		chunklineuc.New(repo, nil, nil, nil),
		nil,
		nil,
		nil,
		nil,
	)

	app := echo.New()
	handler.RegisterRoutes(app, app.Group(""))

	server := httptest.NewServer(app)
	t.Cleanup(server.Close)

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	pw, err := mw.CreatePart(textproto.MIMEHeader{
		"Content-Type": {"application/http"},
	})
	require.NoError(t, err)
	_, err = fmt.Fprintf(pw, "GET %s/chunkline/itr/100?uri=cckv://example.test/timeline/0 HTTP/1.1\r\nHost: example.test\r\n\r\n", apiPrefix)
	require.NoError(t, err)
	require.NoError(t, mw.Close())

	batchPath, err := concrnt.RenderURITemplate(Endpoints["net.concrnt.core.batch"], map[string]string{})
	require.NoError(t, err)

	resp, err := server.Client().Post(server.URL+batchPath, "multipart/mixed; boundary="+mw.Boundary(), &buf)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Empty(t, repo.calls, "no part should be dispatched")
}

// Parts must run under the outer request's authentication context: an
// Authorization header smuggled inside a part is stripped before dispatch,
// and the outer request's identity is shared with every part.
func TestBatchHandlerIgnoresPartAuthorization(t *testing.T) {
	type requesterKey struct{}

	app := echo.New()
	app.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			auth := c.Request().Header.Get("Authorization")
			if strings.HasPrefix(auth, "Bearer ") {
				ctx := context.WithValue(c.Request().Context(), requesterKey{}, strings.TrimPrefix(auth, "Bearer "))
				c.SetRequest(c.Request().WithContext(ctx))
			}
			return next(c)
		}
	})
	app.GET("/whoami", func(c echo.Context) error {
		requester, _ := c.Request().Context().Value(requesterKey{}).(string)
		if requester == "" {
			requester = "guest"
		}
		return c.String(http.StatusOK, requester)
	})
	app.POST("/batch", batchHandler(app, "example.test"))

	server := httptest.NewServer(app)
	t.Cleanup(server.Close)

	doBatch := func(outerAuth string, partAuth string) string {
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		pw, err := mw.CreatePart(textproto.MIMEHeader{
			"Content-Type": {"application/http"},
			"Content-ID":   {"0"},
		})
		require.NoError(t, err)
		partReq := "GET /whoami HTTP/1.1\r\nHost: example.test\r\n"
		if partAuth != "" {
			partReq += "Authorization: Bearer " + partAuth + "\r\n"
		}
		partReq += "\r\n"
		_, err = pw.Write([]byte(partReq))
		require.NoError(t, err)
		require.NoError(t, mw.Close())

		req, err := http.NewRequest("POST", server.URL+"/batch", &buf)
		require.NoError(t, err)
		req.Header.Set("Content-Type", "multipart/mixed; boundary="+mw.Boundary())
		if outerAuth != "" {
			req.Header.Set("Authorization", "Bearer "+outerAuth)
		}
		resp, err := server.Client().Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		_, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
		require.NoError(t, err)
		mr := multipart.NewReader(resp.Body, params["boundary"])
		part, err := mr.NextPart()
		require.NoError(t, err)
		pres, err := http.ReadResponse(bufio.NewReader(part), nil)
		require.NoError(t, err)
		defer pres.Body.Close()
		require.Equal(t, http.StatusOK, pres.StatusCode)
		body, err := io.ReadAll(pres.Body)
		require.NoError(t, err)
		return string(body)
	}

	require.Equal(t, "guest", doBatch("", "alice"), "a part's own Authorization header must be ignored")
	require.Equal(t, "alice", doBatch("alice", ""), "the outer authentication context must be shared with parts")
}

// An absolute-URI request target is accepted only when its host matches the
// server's configured FQDN (and is then processed as origin-form); a part
// aimed at another host gets a part-level 421 without being dispatched.
func TestBatchHandlerAbsoluteFormTargets(t *testing.T) {
	repo := &batchChunklineRepo{}
	handler := NewHandler(
		domain.Config{FQDN: "example.test"},
		nil,
		nil,
		chunklineuc.New(repo, nil, nil, nil),
		nil,
		nil,
		nil,
		nil,
	)

	app := echo.New()
	handler.RegisterRoutes(app, app.Group(""))

	server := httptest.NewServer(app)
	t.Cleanup(server.Close)

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	targets := map[string]string{
		"0": "https://example.test" + apiPrefix + "/chunkline/itr/100?uri=cckv://example.test/timeline/0",
		"1": "https://other.example" + apiPrefix + "/chunkline/itr/100?uri=cckv://example.test/timeline/1",
		"2": apiPrefix + "/chunkline/itr/100?uri=cckv://example.test/timeline/2",
	}
	for _, contentID := range []string{"0", "1", "2"} {
		pw, err := mw.CreatePart(textproto.MIMEHeader{
			"Content-Type": {"application/http"},
			"Content-ID":   {contentID},
		})
		require.NoError(t, err)
		_, err = fmt.Fprintf(pw, "GET %s HTTP/1.1\r\nHost: example.test\r\n\r\n", targets[contentID])
		require.NoError(t, err)
	}
	require.NoError(t, mw.Close())

	batchPath, err := concrnt.RenderURITemplate(Endpoints["net.concrnt.core.batch"], map[string]string{})
	require.NoError(t, err)

	resp, err := server.Client().Post(server.URL+batchPath, "multipart/mixed; boundary="+mw.Boundary(), &buf)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	_, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	require.NoError(t, err)

	statuses := map[string]int{}
	mr := multipart.NewReader(resp.Body, params["boundary"])
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		pres, err := http.ReadResponse(bufio.NewReader(part), nil)
		require.NoError(t, err)
		statuses[part.Header.Get("Content-ID")] = pres.StatusCode
		pres.Body.Close()
	}

	require.Equal(t, http.StatusOK, statuses["0"], "matching-host absolute-form part must be processed")
	require.Equal(t, http.StatusMisdirectedRequest, statuses["1"], "foreign-host absolute-form part must be rejected")
	require.Equal(t, http.StatusOK, statuses["2"], "origin-form part must be processed as before")

	require.Len(t, repo.calls, 1)
	require.ElementsMatch(t, []string{
		"cckv://example.test/timeline/0",
		"cckv://example.test/timeline/2",
	}, repo.calls[0].uris, "the rejected part must not reach the repository")
}
