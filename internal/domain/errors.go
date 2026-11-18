package domain

import (
	"fmt"
	"time"
)

// Machine-readable error codes carried on API error responses, namespaced
// like the CCAPI endpoint keys. Both the code and the message are API
// contract once introduced.
const (
	ErrorCodeRegistrationNotFound = "net.concrnt.errors.registration-not-found"
)

// NotFoundError represents a missing resource. Message, when set, overrides
// the derived text — for application-level responses whose exact wording is
// part of the API contract (e.g. "Registration Not Found"). Code, when set,
// is surfaced by the presenter as the response's machine-readable code.
type NotFoundError struct {
	Resource string
	Message  string
	Code     string
}

// ErrorCode exposes the machine-readable code to the presenter layer.
func (e NotFoundError) ErrorCode() string {
	return e.Code
}

func (e NotFoundError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.Resource == "" {
		return "not found"
	}
	return fmt.Sprintf("%s not found", e.Resource)
}

// Is enables errors.Is matching on NotFoundError.
func (e NotFoundError) Is(target error) bool {
	_, ok := target.(NotFoundError)
	if ok {
		return true
	}
	_, ok = target.(*NotFoundError)
	return ok
}

// ErrNotFound is the sentinel error for missing resources.
var ErrNotFound = NotFoundError{}

type PermissionError struct {
	Reason string
}

var ErrPermissionDenied = PermissionError{}

func (e PermissionError) Error() string {
	if e.Reason == "" {
		return "permission denied"
	}
	return fmt.Sprintf("permission denied: %s", e.Reason)
}

// Is enables errors.Is matching on PermissionError.
func (e PermissionError) Is(target error) bool {
	_, ok := target.(PermissionError)
	if ok {
		return true
	}
	_, ok = target.(*PermissionError)
	return ok
}

type RedirectError struct {
	Location string
	Body     any
}

func (e RedirectError) Error() string {
	return fmt.Sprintf("redirect to %s", e.Location)
}

func (e RedirectError) Is(target error) bool {
	_, ok := target.(RedirectError)
	if ok {
		return true
	}
	_, ok = target.(*RedirectError)
	return ok
}

var ErrRedirect = RedirectError{}

type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	if e.Field == "" {
		return fmt.Sprintf("validation error: %s", e.Message)
	}
	return fmt.Sprintf("validation error on field '%s': %s", e.Field, e.Message)
}

// Is enables errors.Is matching on ValidationError.
func (e ValidationError) Is(target error) bool {
	_, ok := target.(ValidationError)
	if ok {
		return true
	}
	_, ok = target.(*ValidationError)
	return ok
}

var ErrValidation = ValidationError{}

// RateLimitError rejects a request that exceeds a rate limit. RetryAfter is
// how long the caller should wait before trying again.
type RateLimitError struct {
	Reason     string
	RetryAfter time.Duration
}

func (e RateLimitError) Error() string {
	if e.Reason == "" {
		return "rate limit exceeded"
	}
	return fmt.Sprintf("rate limit exceeded: %s", e.Reason)
}

// Is enables errors.Is matching on RateLimitError.
func (e RateLimitError) Is(target error) bool {
	_, ok := target.(RateLimitError)
	if ok {
		return true
	}
	_, ok = target.(*RateLimitError)
	return ok
}

var ErrRateLimit = RateLimitError{}
