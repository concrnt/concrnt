package presenter

import (
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v5"
)

type errorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code,omitempty"`
}

// errorCode extracts the machine-readable code from domain errors that carry
// one (via an ErrorCode method); empty when the error has none.
func errorCode(err error) string {
	var coded interface{ ErrorCode() string }
	if errors.As(err, &coded) {
		return coded.ErrorCode()
	}
	return ""
}

// OK wraps a successful response.
func OK(c *echo.Context, payload any) error {
	return c.JSON(http.StatusOK, payload)
}

func BadRequest(c *echo.Context, err error) error {
	slog.Warn("bad request", slog.String("error", err.Error()))
	return c.JSON(http.StatusBadRequest, errorResponse{Error: err.Error()})
}

func BadRequestMessage(c *echo.Context, msg string) error {
	slog.Warn("bad request", slog.String("error", msg))
	return c.JSON(http.StatusBadRequest, errorResponse{Error: msg})
}

func NotFound(c *echo.Context, msg string) error {
	slog.Info("not found", slog.String("error", msg))
	return c.JSON(http.StatusNotFound, errorResponse{Error: msg})
}

// NotFoundError renders a 404 from a domain error, carrying the error's
// machine-readable code (if it has one) alongside the message.
func NotFoundError(c *echo.Context, err error) error {
	slog.Info("not found", slog.String("error", err.Error()))
	return c.JSON(http.StatusNotFound, errorResponse{Error: err.Error(), Code: errorCode(err)})
}

func InternalError(c *echo.Context, err error) error {
	slog.Error("internal error", slog.String("error", err.Error()))
	return c.JSON(http.StatusInternalServerError, errorResponse{Error: err.Error()})
}

func Forbidden(c *echo.Context, msg string) error {
	slog.Warn("forbidden", slog.String("error", msg))
	return c.JSON(http.StatusForbidden, errorResponse{Error: msg})
}

// TooManyRequests renders a 429, telling the caller when to come back.
func TooManyRequests(c *echo.Context, err error, retryAfter time.Duration) error {
	slog.Warn("too many requests", slog.String("error", err.Error()))
	seconds := max(1, int(math.Ceil(retryAfter.Seconds())))
	c.Response().Header().Set(echo.HeaderRetryAfter, strconv.Itoa(seconds))
	return c.JSON(http.StatusTooManyRequests, errorResponse{Error: err.Error()})
}

func Redirect(c *echo.Context, location string, body any) error {
	slog.Info("redirecting", slog.String("location", location))
	c.Response().Header().Set(echo.HeaderLocation, location)
	return c.JSON(http.StatusFound, body)
}
