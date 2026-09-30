package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
)

// Sentinel errors that callers can match with errors.Is against the error
// returned by any Client method. Each maps to one or more wire.Error codes
// via (*APIError).Is; see mapErr in pkg/api for the server-side mapping this
// mirrors.
//
// client_closed_request (499) has no dedicated sentinel: it means the server
// observed the caller's own request context end mid-request, which the
// caller already knows from the ctx.Err() its own call returned.
var (
	// ErrBadRequest means the request was malformed (400), or named a method
	// the route does not support (405, method_not_allowed).
	ErrBadRequest = errors.New("client: bad request")
	// ErrUnauthorized means the API key was missing or invalid (401).
	ErrUnauthorized = errors.New("client: unauthorized")
	// ErrNotFound means the pool or resource does not exist (404).
	ErrNotFound = errors.New("client: not found")
	// ErrInsufficient means fewer than min_count outputs could be reserved (409).
	ErrInsufficient = errors.New("client: insufficient")
	// ErrExhausted means no fuel is available right now (503).
	ErrExhausted = errors.New("client: exhausted")
	// ErrBackpressure means the server is shedding load (503).
	ErrBackpressure = errors.New("client: backpressure")
	// ErrUnavailable means the server (or the transport to it) is unavailable:
	// this covers pkg/api's "unavailable"/"internal" codes, any 5xx response
	// whose body could not be parsed as a wire.Error ("http_5xx"), and a
	// transport-level failure (dial, timeout, dropped connection) that was
	// not caused by the caller's own context ending.
	ErrUnavailable = errors.New("client: unavailable")
)

// maxErrorBodyBytes bounds how much of a non-2xx response body is read while
// building an *APIError, so a misbehaving server cannot make the client
// buffer an unbounded amount of memory.
const maxErrorBodyBytes = 64 << 10 // 64 KiB

// APIError is a non-2xx response from the FuelKeeper API, decoded from its
// wire.Error envelope.
type APIError struct {
	Status     int    // HTTP status code
	Code       string // wire.Error.Code, or "http_<status>" when the body could not be parsed
	Message    string
	RetryAfter int // seconds, 0 if the response carried none
}

// Error implements the error interface.
func (e *APIError) Error() string {
	return fmt.Sprintf("fuelkeeper: %s (code=%s, status=%d)", e.Message, e.Code, e.Status)
}

// Is reports whether target is one of the package sentinels that e's Code
// maps to, so callers can use errors.Is(err, client.ErrNotFound) and similar
// instead of comparing e.Code directly.
func (e *APIError) Is(target error) bool {
	switch target {
	case ErrBadRequest:
		return e.Code == "bad_request" || e.Code == "method_not_allowed"
	case ErrUnauthorized:
		return e.Code == "unauthorized"
	case ErrNotFound:
		return e.Code == "not_found" || e.Code == "unknown_pool"
	case ErrInsufficient:
		return e.Code == "insufficient"
	case ErrExhausted:
		return e.Code == "exhausted"
	case ErrBackpressure:
		return e.Code == "backpressure"
	case ErrUnavailable:
		// "http_5xx" is newAPIError's fallback code for a 5xx response whose
		// body could not be parsed as a wire.Error (e.g. an HTML error page
		// from a proxy in front of FuelKeeper): that is unavailability too.
		return e.Code == "unavailable" || e.Code == "internal" ||
			(e.Status >= http.StatusInternalServerError && strings.HasPrefix(e.Code, "http_"))
	default:
		return false
	}
}

// IsRetryable reports whether a client is likely to succeed by retrying err
// after a short delay.
//
// For an *APIError: a positive RetryAfter, a 503/409/429 status, or any 5xx
// whose body could not be parsed as a wire.Error (Code "http_5xx" -- see
// (*APIError).Is).
//
// Otherwise: whether err wraps ErrUnavailable. This intentionally does not
// special-case context.Canceled/context.DeadlineExceeded: wrapTransportErr
// never wraps ErrUnavailable around the caller's own context ending (it
// returns ctx.Err() directly instead), so errors.Is(err, ErrUnavailable) is
// already false for that case -- including when err also happens to wrap
// context.DeadlineExceeded because it was the *client's own* per-request
// timeout (http.Client.Timeout), rather than the caller's ctx, that expired.
func IsRetryable(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.RetryAfter > 0 ||
			apiErr.Status == http.StatusServiceUnavailable ||
			apiErr.Status == http.StatusConflict ||
			apiErr.Status == http.StatusTooManyRequests ||
			(apiErr.Status >= http.StatusInternalServerError && strings.HasPrefix(apiErr.Code, "http_"))
	}
	return errors.Is(err, ErrUnavailable)
}

// newAPIError builds an *APIError from a non-2xx/non-304 response: status,
// the wire.Error decoded from body (at most maxErrorBodyBytes; a body that
// fails to decode, or lacks a code, falls back to "http_<status>"), and
// RetryAfter taken from the body or, if the body carried none, from the
// response's Retry-After header.
func newAPIError(status int, body []byte, header http.Header) *APIError {
	var we wire.Error
	_ = json.Unmarshal(body, &we) // a decode failure leaves we zero-valued, handled below
	if we.Code == "" {
		we.Code = fmt.Sprintf("http_%d", status)
	}
	if we.Message == "" {
		we.Message = http.StatusText(status)
	}
	if we.RetryAfter == 0 {
		if ra, err := strconv.Atoi(header.Get("Retry-After")); err == nil {
			we.RetryAfter = ra
		}
	}
	if we.RetryAfter < 0 {
		we.RetryAfter = 0
	}
	return &APIError{Status: status, Code: we.Code, Message: we.Message, RetryAfter: we.RetryAfter}
}

// wrapTransportErr maps a transport-level failure (dial/connection/timeout,
// as opposed to an HTTP response) to the caller's own context error when that
// context is already done -- so errors.Is(err, context.Canceled) holds for a
// caller-cancelled request instead of it looking like a server outage -- or
// to ErrUnavailable otherwise.
func wrapTransportErr(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("%w: %w", ctxErr, err)
	}
	return fmt.Errorf("%w: %w", ErrUnavailable, err)
}
