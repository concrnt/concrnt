package rest

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/concrnt/concrnt/internal/infra/config"
)

// RegisterStaticFiles serves the configured static files under their fixed
// paths. The content type follows the file extension (.txt text/plain, .json
// application/json) and a missing file answers 404, so an operator opts out
// of a document by not providing it.
func RegisterStaticFiles(g *echo.Group, files config.Static) {
	files = files.WithDefaults()
	routes := map[string]string{
		"/tos":               files.TOS,
		"/code-of-conduct":   files.CodeOfConduct,
		"/register-template": files.RegisterTemplate,
		"/robots.txt":        files.RobotsTxt,
	}
	for route, path := range routes {
		g.GET(route, func(c echo.Context) error {
			return c.File(path)
		})
		g.OPTIONS(route, func(c echo.Context) error {
			return c.NoContent(http.StatusOK)
		})
	}
}
