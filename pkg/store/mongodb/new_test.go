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
