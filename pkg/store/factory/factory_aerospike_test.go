//go:build aerospike

package factory_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/aerospike"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/factory"
)

// factoryTestSetPrefix is the FIXED set-name prefix shared with
// pkg/store/aerospike's own tests: Aerospike never frees a set name and caps
// a namespace at ~4K of them, so this test must never generate a new one. It
// isolates by a random KeyPrefix (below) instead, on the shared CE server the
// aerospike package's own tests already use (see
// pkg/store/aerospike/aerospike_test.go's testSetPrefix).
const factoryTestSetPrefix = "fk_"

// randKeyPrefix returns a fresh tenant KeyPrefix.
func randKeyPrefix() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return "tf" + hex.EncodeToString(b[:])
}

// TestNewAerospike opens an aerospike-backed store through the factory
// against FUELKEEPER_TEST_AEROSPIKE_HOST (skipped when unset), isolated by a
// fresh random KeyPrefix on the fixed shared test sets (no new set names),
// in availability mode (the shared server is Community Edition). It runs a
// tiny smoke: Capabilities, Stats on an empty pool, then Close, and Truncates
// its own records on cleanup.
func TestNewAerospike(t *testing.T) {
	host := os.Getenv("FUELKEEPER_TEST_AEROSPIKE_HOST")
	if host == "" {
		t.Skip("FUELKEEPER_TEST_AEROSPIKE_HOST not set")
	}
	ctx := context.Background()

	s, err := factory.New(ctx, factory.Config{
		Backend: factory.BackendAerospike,
		Aerospike: factory.AerospikeConfig{
			Hosts:                 []string{host},
			Namespace:             "test",
			SetPrefix:             factoryTestSetPrefix,
			KeyPrefix:             randKeyPrefix(),
			AllowAvailabilityMode: true,
			PoolSize:              8,
		},
	}, time.Now)
	require.NoError(t, err)

	as, ok := s.(*aerospike.Store)
	require.True(t, ok, "factory must return the concrete *aerospike.Store so tests can Truncate it")
	t.Cleanup(func() { require.NoError(t, as.Truncate(), "truncate") })

	require.True(t, s.Capabilities().MultiProcess)
	stats, err := s.Stats(ctx, "factory-smoke-pool")
	require.NoError(t, err)
	require.Equal(t, store.Stats{}, stats, "empty pool must report zero counts")
	require.NoError(t, s.Close())
}
