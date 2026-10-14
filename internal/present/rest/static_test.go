package rest

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt/internal/infra/config"
)

func serveStatic(t *testing.T, files config.Static, path string) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	RegisterStaticFiles(e.Group(""), files)
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	return rec
}

func TestStaticFilesServeConfiguredPaths(t *testing.T) {
	dir := t.TempDir()
	robots := filepath.Join(dir, "my-robots.txt")
	require.NoError(t, os.WriteFile(robots, []byte("User-agent: concrnt-crawler\nDisallow: /\n"), 0o644))
	template := filepath.Join(dir, "template.json")
	require.NoError(t, os.WriteFile(template, []byte(`{"type":"object"}`), 0o644))

	files := config.Static{RobotsTxt: robots, RegisterTemplate: template}

	rec := serveStatic(t, files, "/robots.txt")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "text/plain; charset=utf-8", rec.Header().Get("Content-Type"))
	require.Equal(t, "User-agent: concrnt-crawler\nDisallow: /\n", rec.Body.String())

	rec = serveStatic(t, files, "/register-template")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	require.Equal(t, `{"type":"object"}`, rec.Body.String())
}

func TestStaticFilesAnswer404WhenTheFileIsMissing(t *testing.T) {
	files := config.Static{TOS: filepath.Join(t.TempDir(), "absent.txt")}
	require.Equal(t, http.StatusNotFound, serveStatic(t, files, "/tos").Code)
	// an unset entry points at the default location, absent in this test
	require.Equal(t, http.StatusNotFound, serveStatic(t, files, "/code-of-conduct").Code)
}

func TestStaticDefaults(t *testing.T) {
	got := config.Static{TOS: "/custom/tos.txt"}.WithDefaults()
	require.Equal(t, config.Static{
		TOS:              "/custom/tos.txt",
		CodeOfConduct:    "/etc/concrnt/static/code-of-conduct.txt",
		RegisterTemplate: "/etc/concrnt/static/register-template.json",
		RobotsTxt:        "/etc/concrnt/static/robots.txt",
	}, got)
}
