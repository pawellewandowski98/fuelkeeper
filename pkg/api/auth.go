package api

import (
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
// API open (no auth configured). Keys are compared in constant time.
func requireAPIKey(keys []string, next http.Handler) http.Handler {
	if len(keys) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if key, ok := bearerToken(r.Header.Get("Authorization")); ok {
			for _, want := range keys {
				if subtle.ConstantTimeCompare([]byte(key), []byte(want)) == 1 {
					next.ServeHTTP(w, r)
					return
				}
			}
		}
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeJSON(w, http.StatusUnauthorized, wire.Error{Code: "unauthorized", Message: "missing or invalid API key"})
	})
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
