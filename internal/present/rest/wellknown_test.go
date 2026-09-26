package rest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/config"
)

func serveWellKnown(t *testing.T, entries map[string]config.WellKnownEntry, path string) *httptest.ResponseRecorder {
	t.Helper()
	docs, err := RenderWellKnown(entries)
	require.NoError(t, err)
	e := echo.New()
	NewWellKnownHandler(nil, nil, docs).RegisterRoutes(e)
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	return rec
}

func TestAdditionalWellKnownString(t *testing.T) {
	rec := serveWellKnown(t, map[string]config.WellKnownEntry{
		"my-service": {Type: "string", Value: "hello\n"},
	}, "/.well-known/my-service")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "text/plain; charset=utf-8", rec.Header().Get("Content-Type"))
	require.Equal(t, "hello\n", rec.Body.String())
}

func TestAdditionalWellKnownJSONFromYAMLMapping(t *testing.T) {
	// go-yaml v2 decodes mappings as map[interface{}]interface{}
	value := map[interface{}]interface{}{
		"links": []interface{}{
			map[interface{}]interface{}{"rel": "self", "href": "https://example.com/"},
		},
		"count": 1,
	}
	rec := serveWellKnown(t, map[string]config.WellKnownEntry{
		"my-json": {Type: "json", Value: value},
	}, "/.well-known/my-json")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var got map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, map[string]any{
		"links": []any{map[string]any{"rel": "self", "href": "https://example.com/"}},
		"count": float64(1),
	}, got)
}

func TestAdditionalWellKnownJSONFromString(t *testing.T) {
	rec := serveWellKnown(t, map[string]config.WellKnownEntry{
		"raw": {Type: "json", Value: `{"a": 1}`},
	}, "/.well-known/raw")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	require.Equal(t, `{"a": 1}`, rec.Body.String())
}

func TestAdditionalWellKnownUnknownPathIs404(t *testing.T) {
	rec := serveWellKnown(t, map[string]config.WellKnownEntry{
		"raw": {Type: "json", Value: `{}`},
	}, "/.well-known/other")
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRenderWellKnownRejectsInvalidEntries(t *testing.T) {
	cases := map[string]map[string]config.WellKnownEntry{
		"empty name":     {"": {Type: "string", Value: "x"}},
		"slash in name":  {"a/b": {Type: "string", Value: "x"}},
		"reserved name":  {"concrnt": {Type: "string", Value: "x"}},
		"unknown type":   {"x": {Type: "xml", Value: "<a/>"}},
		"non-string str": {"x": {Type: "string", Value: 1}},
		"invalid json":   {"x": {Type: "json", Value: "{"}},
	}
	for name, entries := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := RenderWellKnown(entries)
			require.Error(t, err)
		})
	}
}
