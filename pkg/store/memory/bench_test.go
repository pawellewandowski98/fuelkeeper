package memory_test

import (
	"testing"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func BenchmarkStore(b *testing.B) {
	storetest.RunBenchmarks(b, func(*testing.B) store.Store { return memory.New() })
}
