package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// statusClientClosedRequest is the (non-standard, but widely used) status for
// a request whose client disconnected before a response was sent. net/http
// has no constant for it.
const statusClientClosedRequest = 499

// retryAfterSeconds is the fixed Retry-After hint attached to every retryable
// error. FuelKeeper has no better estimate to offer than "try again shortly".
const retryAfterSeconds = 1

// mapErr maps a pool/store/context error to an HTTP status, a stable error
// code and, for retryable errors, the number of seconds a client should wait
// before retrying (0 when the error is not retryable).
//
// Deviation from the original plan: a request whose client went away
// (context.Canceled) maps to 499 client_closed_request with no Retry-After,
// not 503 unavailable; context.DeadlineExceeded still maps to 503
// unavailable.
func mapErr(err error) (status int, code string, retryAfter int) {
	switch {
	case errors.Is(err, context.Canceled):
		return statusClientClosedRequest, "client_closed_request", 0
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusServiceUnavailable, "unavailable", retryAfterSeconds
	case errors.Is(err, pool.ErrBadRequest), errors.Is(err, store.ErrInvalidArgument):
		return http.StatusBadRequest, "bad_request", 0
	case errors.Is(err, pool.ErrUnknownPool):
		return http.StatusNotFound, "unknown_pool", 0
	case errors.Is(err, store.ErrNotFound):
		return http.StatusNotFound, "not_found", 0
	case errors.Is(err, pool.ErrInsufficient):
		return http.StatusConflict, "insufficient", retryAfterSeconds
	case errors.Is(err, pool.ErrExhausted):
		return http.StatusServiceUnavailable, "exhausted", retryAfterSeconds
	case errors.Is(err, pool.ErrBackpressure):
		return http.StatusServiceUnavailable, "backpressure", retryAfterSeconds
	case errors.Is(err, pool.ErrUnavailable), errors.Is(err, store.ErrUnavailable):
		return http.StatusServiceUnavailable, "unavailable", retryAfterSeconds
	default:
		return http.StatusInternalServerError, "internal", 0
	}
}

// writeJSON writes v as the JSON response body with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError maps err to an HTTP status and a stable error code, sets
// Retry-After when the error is retryable, and writes the wire.Error
// envelope. Internal and unavailable errors never leak their detail (a
// wrapped DSN, host, credential or SQL state, for example) to the client: a
// fixed, safe message is sent instead. Callers that want the real error
// logged should do so themselves before or instead of calling this directly
// (see (*server).writeError).
func writeError(w http.ResponseWriter, err error) {
	status, code, retryAfter := mapErr(err)
	msg := err.Error()
	switch code {
	case "internal":
		msg = "internal error"
	case "unavailable":
		msg = "store temporarily unavailable"
	}
	if retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	}
	writeJSON(w, status, wire.Error{Code: code, Message: msg, RetryAfter: retryAfter})
}
