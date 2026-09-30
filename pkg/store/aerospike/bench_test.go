//go:build aerospike

package aerospike

import (
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func BenchmarkStore(b *testing.B) {
	host := os.Getenv("FUELKEEPER_TEST_AEROSPIKE_HOST")
	if host == "" {
		host = os.Getenv("AERO_HOST")
	}
	if host == "" {
		b.Skip("FUELKEEPER_TEST_AEROSPIKE_HOST not set")
	}
	storetest.RunBenchmarks(b, func(b *testing.B) store.Store {
		return isolatedStoreTB(b, host, time.Now)
	})
}
