package pebble_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	pebblestore "github.com/bsv-blockchain/fuelkeeper/pkg/store/pebble"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// build100kRowStore mints enough shards to reach 100,000 rows in one pool,
// for BenchmarkReserveRowLookup.
func build100kRowStore(b *testing.B) (*pebblestore.Store, []store.Outpoint) {
	b.Helper()
	s, err := pebblestore.Open(context.Background(), pebblestore.Config{Path: b.TempDir(), NoSync: true})
	require.NoError(b, err)
	b.Cleanup(func() { _ = s.Close() })

	const shardSize = 50
	const shardCount = 2000 // 2000 * 50 = 100,000 rows
	ctx := context.Background()
	ops := make([]store.Outpoint, 0, shardSize*shardCount)
	for i := 0; i < shardCount; i++ {
		// storetest.MintShard requires a *testing.T, not the *testing.B this
		// helper is built for, so build and insert the shard directly.
		shard, rows := storetest.BuildShard("bench", shardSize, store.ShardMined)
		require.NoError(b, s.PutShard(ctx, shard, rows))
		for _, r := range rows {
			ops = append(ops, r.Outpoint)
		}
	}
	return s, ops
}

// BenchmarkReserveRowLookup measures Reserve's row-resolution step in
// isolation, against a 100k-row store, at the batch sizes Reserve actually
// sees in practice (up to its MaxBatch of 1000): BenchLoadRowsPointGet is one
// pebble.DB.Get per op, the strategy Reserve used unconditionally before this
// change; BenchLoadRowsSeek is the single sorted forward-iterator walk it now
// uses above reserveSeekThreshold (16). Report with:
//
//	go test -run='^$' -bench=BenchmarkReserveRowLookup -benchmem ./pkg/store/pebble/
func BenchmarkReserveRowLookup(b *testing.B) {
	s, ops := build100kRowStore(b)
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic benchmark sampling, not a security use

	for _, n := range []int{10, 16, 100, 500, 1000} {
		batch := make([]store.Outpoint, n)
		for i := range batch {
			batch[i] = ops[rng.Intn(len(ops))]
		}
		sorted := store.SortOutpoints(batch)

		b.Run(fmt.Sprintf("PointGet/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := s.BenchLoadRowsPointGet(sorted)
				require.NoError(b, err)
			}
		})
		b.Run(fmt.Sprintf("Seek/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err := s.BenchLoadRowsSeek(sorted)
				require.NoError(b, err)
			}
		})
	}
}
