package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBearerAuth(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	h := requireAPIKey([]string{"secret-1", "secret-2"}, ok)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/pools", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), `"unauthorized"`)

	req.Header.Set("Authorization", "Bearer secret-2")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	req.Header.Set("Authorization", "Bearer nope")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	req.Header.Set("Authorization", "bearer secret-1") // scheme is case-insensitive per RFC 7235
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	open := requireAPIKey(nil, ok)
	rec = httptest.NewRecorder()
	open.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/pools", nil))
	assert.Equal(t, http.StatusNoContent, rec.Code, "no keys configured → open")
}
