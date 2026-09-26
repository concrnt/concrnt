package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	echomiddleware "github.com/labstack/echo/v4/middleware"

	"github.com/concrnt/concrnt/internal/infra/config"
	"github.com/concrnt/concrnt/internal/present/rest/presenter"
	"github.com/concrnt/concrnt/internal/usecase/server"
)

// WellKnownDocument is a pre-rendered static /.well-known/<name> response.
type WellKnownDocument struct {
	ContentType string
	Body        []byte
}

type WellKnownHandler struct {
	server     *server.Usecase
	meta       map[string]any
	additional map[string]WellKnownDocument
}

func NewWellKnownHandler(
	server *server.Usecase,
	meta map[string]any,
	additional map[string]WellKnownDocument,
) *WellKnownHandler {
	return &WellKnownHandler{
		server:     server,
		meta:       meta,
		additional: additional,
	}
}

func (p *WellKnownHandler) RegisterRoutes(e *echo.Echo) {
	w := e.Group("", echomiddleware.CORS())
	w.GET("/.well-known/concrnt", p.handleWellKnown)
	for name, doc := range p.additional {
		w.GET("/.well-known/"+name, func(c echo.Context) error {
			return c.Blob(http.StatusOK, doc.ContentType, doc.Body)
		})
	}
}

func (p *WellKnownHandler) handleWellKnown(c echo.Context) error {
	server, err := p.server.GetThisServer()
	if err != nil {
		return presenter.InternalError(c, err)
	}

	wellknown := server.WellKnown
	wellknown.Meta = p.meta

	return presenter.OK(c, wellknown)
}

// RenderWellKnown turns the additionalWellKnown config section into
// ready-to-serve documents, rejecting names that cannot form a single path
// segment, the reserved "concrnt" name, unknown types and malformed bodies.
func RenderWellKnown(entries map[string]config.WellKnownEntry) (map[string]WellKnownDocument, error) {
	docs := make(map[string]WellKnownDocument, len(entries))
	for name, entry := range entries {
		if name == "" || strings.Contains(name, "/") {
			return nil, fmt.Errorf("invalid well-known name %q", name)
		}
		if name == "concrnt" {
			return nil, fmt.Errorf("well-known name %q is reserved", name)
		}

		var doc WellKnownDocument
		switch entry.Type {
		case "string":
			s, ok := entry.Value.(string)
			if !ok {
				return nil, fmt.Errorf("well-known %q: value of type string must be a string", name)
			}
			doc = WellKnownDocument{ContentType: "text/plain; charset=utf-8", Body: []byte(s)}
		case "json":
			var body []byte
			if s, ok := entry.Value.(string); ok {
				if !json.Valid([]byte(s)) {
					return nil, fmt.Errorf("well-known %q: value is not valid json", name)
				}
				body = []byte(s)
			} else {
				b, err := json.Marshal(normalizeYAML(entry.Value))
				if err != nil {
					return nil, fmt.Errorf("well-known %q: %w", name, err)
				}
				body = b
			}
			doc = WellKnownDocument{ContentType: "application/json", Body: body}
		default:
			return nil, fmt.Errorf("well-known %q: unknown type %q (want string or json)", name, entry.Type)
		}
		docs[name] = doc
	}
	return docs, nil
}

// normalizeYAML converts the map[interface{}]interface{} trees produced by
// go-yaml v2 into map[string]any so encoding/json can marshal them.
func normalizeYAML(v any) any {
	switch t := v.(type) {
	case map[interface{}]interface{}:
		m := make(map[string]any, len(t))
		for k, val := range t {
			m[fmt.Sprint(k)] = normalizeYAML(val)
		}
		return m
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, val := range t {
			m[k] = normalizeYAML(val)
		}
		return m
	case []any:
		l := make([]any, len(t))
		for i, val := range t {
			l[i] = normalizeYAML(val)
		}
		return l
	default:
		return v
	}
}
