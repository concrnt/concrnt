package rest

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// maxBatchParts caps how many application/http parts a single batch request
// may carry, bounding the fan-out a single request can trigger.
const maxBatchParts = 1024

type batchRequestPart struct {
	contentID string
	request   *http.Request
}

type batchCustomHandler struct {
	match  func(*http.Request) bool
	handle func(req *http.Request, parts []batchRequestPart) map[string]*http.Response
}

func batchHandler(app *echo.Echo, fqdn string, customHandlers ...batchCustomHandler) echo.HandlerFunc {
	return func(c *echo.Context) error {
		req := c.Request()
		ct := req.Header.Get("Content-Type")
		mediaType, params, err := mime.ParseMediaType(ct)
		if err != nil || !strings.EqualFold(mediaType, "multipart/mixed") {
			fmt.Println("Invalid Content-Type:", ct)
			return echo.NewHTTPError(http.StatusBadRequest, "Content-Type must be multipart/mixed")
		}
		boundary := params["boundary"]
		if boundary == "" {
			fmt.Println("Missing boundary in Content-Type")
			return echo.NewHTTPError(http.StatusBadRequest, "missing boundary")
		}

		mr := multipart.NewReader(req.Body, boundary)

		parts := make([]batchRequestPart, 0)
		seenIDs := make(map[string]bool)
		responses := make(map[string]*http.Response)

		for {
			part, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				return echo.NewHTTPError(http.StatusBadRequest, "failed to read multipart body")
			}

			contentType := part.Header.Get("Content-Type")
			if !strings.EqualFold(contentType, "application/http") {
				continue
			}
			contentID := part.Header.Get("Content-ID")
			if contentID == "" {
				return echo.NewHTTPError(http.StatusBadRequest, "part is missing a Content-ID")
			}
			if seenIDs[contentID] {
				return echo.NewHTTPError(http.StatusBadRequest, "duplicate Content-ID: "+contentID)
			}
			seenIDs[contentID] = true

			reader := bufio.NewReader(part)

			pr, err := http.ReadRequest(reader)
			if err != nil {
				fmt.Println("Failed to parse part as HTTP request:", err)
				return echo.NewHTTPError(http.StatusBadRequest, "failed to parse part as HTTP request")
			}

			if len(parts) >= maxBatchParts {
				return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("batch exceeds the maximum of %d parts", maxBatchParts))
			}

			// Parts run under the outer request's authentication context
			// (inherited via WithContext below); credentials carried inside
			// a part must not be re-evaluated by the middleware chain when
			// the part is dispatched.
			pr.Header.Del("Authorization")
			pr.Header.Del("Captcha")
			pr.Header.Del("Cookie")

			pr = pr.WithContext(req.Context())

			if pr.URL.Scheme != "" || pr.URL.Host != "" {
				if !strings.EqualFold(pr.URL.Host, fqdn) {
					responses[contentID] = newBatchTextResponse(http.StatusMisdirectedRequest, "request target host does not match this server")
					parts = append(parts, batchRequestPart{
						contentID: contentID,
						request:   pr,
					})
					continue
				}
				pr.Host = pr.URL.Host
				pr.URL.Scheme = ""
				pr.URL.Host = ""
				pr.RequestURI = pr.URL.RequestURI()
			}

			parts = append(parts, batchRequestPart{
				contentID: contentID,
				request:   pr,
			})
		}

		// The multipart writer is only set up once parsing has fully
		// succeeded: its deferred Close would otherwise commit a 200
		// response and swallow any parse-error status returned above.
		c.Response().Header().Set("Content-Type", "multipart/mixed; boundary="+boundary)
		mw := multipart.NewWriter(c.Response())
		mw.SetBoundary(boundary)
		defer mw.Close()

		handlerParts := make([][]batchRequestPart, len(customHandlers))
		defaultParts := make([]batchRequestPart, 0, len(parts))

		for _, part := range parts {
			if _, done := responses[part.contentID]; done {
				continue
			}
			handled := false
			for i, handler := range customHandlers {
				if handler.match(part.request) {
					handlerParts[i] = append(handlerParts[i], part)
					handled = true
					break
				}
			}
			if !handled {
				defaultParts = append(defaultParts, part)
			}
		}

		for i, parts := range handlerParts {
			if len(parts) == 0 {
				continue
			}
			for contentID, resp := range customHandlers[i].handle(req, parts) {
				responses[contentID] = resp
			}
		}

		for _, part := range defaultParts {
			recorder := NewResponseRecorder()
			app.ServeHTTP(recorder, part.request)
			responses[part.contentID] = recorder.Result()
		}

		for _, part := range parts {
			pw, err := mw.CreatePart(map[string][]string{
				"Content-Type": {"application/http"},
				"Content-ID":   {part.contentID},
			})
			if err != nil {
				return echo.NewHTTPError(http.StatusInternalServerError, "failed to create multipart part")
			}

			result, ok := responses[part.contentID]
			if !ok {
				result = newBatchTextResponse(http.StatusInternalServerError, "batch handler did not return a response")
			}
			if err := result.Write(pw); err != nil {
				return echo.NewHTTPError(http.StatusInternalServerError, "failed to write multipart part")
			}
		}

		return nil
	}
}

type responseRecorder struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func NewResponseRecorder() *responseRecorder {
	return &responseRecorder{
		header: make(http.Header),
		status: http.StatusOK,
	}
}

func (r *responseRecorder) Header() http.Header {
	return r.header
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	return r.body.Write(b)
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	r.status = statusCode
}

func (r *responseRecorder) Result() *http.Response {
	return &http.Response{
		StatusCode:    r.status,
		Status:        fmt.Sprintf("%d %s", r.status, http.StatusText(r.status)),
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        r.header,
		Body:          io.NopCloser(bytes.NewReader(r.body.Bytes())),
		ContentLength: int64(r.body.Len()),
	}
}

func newBatchTextResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode:    status,
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{"Content-Type": {"text/plain; charset=UTF-8"}},
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}
