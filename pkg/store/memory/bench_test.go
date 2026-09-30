package memory_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func BenchmarkStore(b *testing.B) {
	storetest.RunBenchmarks(b, func(*testing.B) store.Store { return memory.New() })
}

// BenchmarkStats measures Stats at 200k rows in one pool: the size a long-
// running pool reaches once spent/invalid rows accumulate, since Stats never
// prunes them. This is the call the minter polls once a second and the API
// serves from GET /v1/pools (pkg/pool/service.go, pkg/api/handlers.go), so
// its cost directly eats into the reserve-throughput budget on the same
// store. Before the per-pool per-state counters (counts.go), Stats scanned
// every row in s.rows under the global lock on every call, making it O(rows)
// and serialized with every other Store method on the same store.
func BenchmarkStats(b *testing.B) {
	ctx := context.Background()
	s := memory.New()
	const (
		totalRows = 200_000
		shardSize = 50
	)
	for i := 0; i < totalRows/shardSize; i++ {
		shard, rows := storetest.BuildShard("bench", shardSize, store.ShardMined)
		require.NoError(b, s.PutShard(ctx, shard, rows))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Stats(ctx, "bench"); err != nil {
			b.Fatal(err)
		}
	}
}
