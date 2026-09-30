package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
)

// bearerPrefix is the "Bearer" auth scheme, matched case-insensitively per RFC
// 7235 §2.1 ("auth-scheme ... is case-insensitive").
const bearerPrefix = "bearer "

// requireAPIKey wraps next so that every request must carry
// "Authorization: Bearer <key>" for one of keys. An empty/nil keys leaves the
// endpoint open (no auth configured).
//
// Every key is hashed once, up front, rather than per request. A request's
// key is then checked against every hash, and the per-key results are OR-ed
// together (match |= ...) instead of returning on the first match: the
// comparison's timing therefore never depends on which key, if any, matched,
// on top of subtle.ConstantTimeCompare's own constant-time guarantee for each
// individual comparison.
func requireAPIKey(keys []string, next http.HandlerFunc) http.HandlerFunc {
	if len(keys) == 0 {
		return next
	}
	hashes := make([][sha256.Size]byte, len(keys))
	for i, k := range keys {
		hashes[i] = sha256.Sum256([]byte(k))
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if key, ok := bearerToken(r.Header.Get("Authorization")); ok {
			got := sha256.Sum256([]byte(key))
			match := 0
			for _, h := range hashes {
				match |= subtle.ConstantTimeCompare(got[:], h[:])
			}
			if match == 1 {
				next.ServeHTTP(w, r)
				return
			}
		}
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeJSON(w, http.StatusUnauthorized, wire.Error{Code: "unauthorized", Message: "missing or invalid API key"})
	}
}

// bearerToken extracts the token from an "Authorization: Bearer <token>"
// header value, matching the scheme case-insensitively.
func bearerToken(authHeader string) (string, bool) {
	if len(authHeader) <= len(bearerPrefix) {
		return "", false
	}
	if !strings.EqualFold(authHeader[:len(bearerPrefix)], bearerPrefix) {
		return "", false
	}
	return authHeader[len(bearerPrefix):], true
}
