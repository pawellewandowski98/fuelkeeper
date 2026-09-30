//go:build mongodb

package mongodb_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/mongodb"
)

// TestNew_ConnectErrorIncludesHost reproduces the review's LOW finding 7: a
// connection failure from New must be prefixed with "mongodb: connect
// <host>" so it is identifiable in logs/metrics without needing to inspect
// the wrapped driver error. This does not require FUELKEEPER_TEST_MONGODB_URI
// (it deliberately targets an address nothing listens on).
func TestNew_ConnectErrorIncludesHost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := mongodb.New(ctx, mongodb.Config{
		URI:      "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=500&connectTimeoutMS=500",
		Database: "fk_unreachable",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mongodb: connect 127.0.0.1:1", "connect errors must be prefixed with the host for identifiability in logs")
	assert.ErrorIs(t, err, store.ErrUnavailable)
}

// TestNew_MalformedURIErrorNeverLeaksCredentials reproduces the review's
// IMPORTANT finding: a malformed mongodb:// URI whose userinfo has an
// unescaped special character (an unescaped "/" in the password, or a
// second unescaped "@" as in "user:p@ss@host") must fail with an error that
// names the real host and never any fragment of the username or password.
// The driver rejects both URIs synchronously in mongo.Connect (a URI parse
// error), before any network I/O, so this needs no running mongod.
func TestNew_MalformedURIErrorNeverLeaksCredentials(t *testing.T) {
	cases := []struct {
		name          string
		uri           string
		wantHost      string
		mustNotAppear []string
	}{
		{
			name:          "unescaped slash in password",
			uri:           "mongodb://alice:1234/s3cretPW@localhost:27017/",
			wantHost:      "localhost:27017",
			mustNotAppear: []string{"alice", "1234", "s3cretPW"},
		},
		{
			name:          "unescaped @ in password (user:p@ss@host style)",
			uri:           "mongodb://bob:p@ssword@host:27017/db",
			wantHost:      "host:27017",
			mustNotAppear: []string{"bob", "ssword"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := mongodb.New(ctx, mongodb.Config{URI: tc.uri, Database: "fk_malformed"})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "mongodb: connect "+tc.wantHost)
			for _, secret := range tc.mustNotAppear {
				assert.NotContains(t, err.Error(), secret, "connect error must never leak credentials")
			}
		})
	}
}
