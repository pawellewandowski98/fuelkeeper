package pebble_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	pebblestore "github.com/bsv-blockchain/fuelkeeper/pkg/store/pebble"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// BenchmarkStore runs the shared Reserve/AcquireShard/Quarantine
// microbenchmark suite (storetest.RunBenchmarks) against pebble. See
// bench_test.go for pebble-specific lower-level Reserve lookup benchmarks.
func BenchmarkStore(b *testing.B) {
	storetest.RunBenchmarks(b, func(b *testing.B) store.Store {
		s, err := pebblestore.Open(context.Background(), pebblestore.Config{Path: b.TempDir(), NoSync: true})
		require.NoError(b, err)
		return s
	})
}
