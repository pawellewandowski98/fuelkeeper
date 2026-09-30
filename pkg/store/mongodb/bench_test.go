//go:build mongodb

package mongodb_test

import (
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func BenchmarkStore(b *testing.B) {
	uri := os.Getenv("FUELKEEPER_TEST_MONGODB_URI")
	if uri == "" {
		b.Skip("FUELKEEPER_TEST_MONGODB_URI not set")
	}
	storetest.RunBenchmarks(b, func(b *testing.B) store.Store {
		return isolatedStoreTB(b, uri, time.Now)
	})
}
