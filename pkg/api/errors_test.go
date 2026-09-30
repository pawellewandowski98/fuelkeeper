package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
)

func TestMapErr(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
		retry  bool
	}{
		{pool.ErrBadRequest, 400, "bad_request", false},
		{errors.Join(pool.ErrBadRequest, errors.New("x")), 400, "bad_request", false},
		{store.ErrInvalidArgument, 400, "bad_request", false},
		{pool.ErrUnknownPool, 404, "unknown_pool", false},
		{treasury.ErrBadFunding, 400, "bad_funding", false},
		{errors.Join(treasury.ErrBadFunding, errors.New("derivation mismatch")), 400, "bad_funding", false},
		{store.ErrNotFound, 404, "not_found", false},
		{pool.ErrInsufficient, 409, "insufficient", true},
		{pool.ErrExhausted, 503, "exhausted", true},
		{pool.ErrBackpressure, 503, "backpressure", true},
		{pool.ErrUnavailable, 503, "unavailable", true},
		{store.ErrUnavailable, 503, "unavailable", true},
		{context.DeadlineExceeded, 503, "unavailable", true},
		{context.Canceled, 499, "client_closed_request", false},
		{errors.New("boom"), 500, "internal", false},
	}
	for _, tc := range cases {
		st, code, retry := mapErr(tc.err)
		assert.Equal(t, tc.status, st, tc.err)
		assert.Equal(t, tc.code, code, tc.err)
		assert.Equal(t, tc.retry, retry > 0, tc.err)
	}
	rec := httptest.NewRecorder()
	writeError(rec, pool.ErrExhausted)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))
	assert.JSONEq(t, `{"code":"exhausted","message":"pool: exhausted","retry_after":1}`, rec.Body.String())
	rec = httptest.NewRecorder()
	writeError(rec, errors.New("secret detail"))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.NotContains(t, rec.Body.String(), "secret detail", "internal errors are not leaked")
}

// TestWriteErrorDoesNotLeakUnavailableDetail guards against a wrapped
// store.ErrUnavailable carrying connection detail (DSN host, user, password)
// through to the client: writeError must always answer 503 with a fixed,
// safe message.
func TestWriteErrorDoesNotLeakUnavailableDetail(t *testing.T) {
	err := fmt.Errorf("%w: dial tcp 10.1.2.3:5432: password authentication failed", store.ErrUnavailable)
	rec := httptest.NewRecorder()
	writeError(rec, err)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.NotContains(t, rec.Body.String(), "10.1.2.3")
	assert.NotContains(t, rec.Body.String(), "password")
	assert.JSONEq(t, `{"code":"unavailable","message":"store temporarily unavailable","retry_after":1}`, rec.Body.String())
}
