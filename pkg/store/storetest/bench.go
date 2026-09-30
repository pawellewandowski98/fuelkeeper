package storetest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// benchPool is the pool every RunBenchmarks sub-benchmark mints and operates in.
const benchPool = "bench"

// mintMany mints shards shard count times, each holding size mined rows in
// benchPool, and returns every row across all of them combined. It is
// mintShardTB's testing.B-safe caller for minting a whole working set at once.
func mintMany(tb testing.TB, s store.Store, shards, size int) []store.Row {
	tb.Helper()
	rows := make([]store.Row, 0, shards*size)
	for i := 0; i < shards; i++ {
		_, rs := mintShardTB(tb, s, benchPool, size, store.ShardMined)
		rows = append(rows, rs...)
	}
	return rows
}

// RunBenchmarks runs the store microbenchmark suite -- Reserve, AcquireShard
// and Quarantine, the three operations on FuelKeeper's hot path -- against a
// store built by newStore. newStore is called once per sub-benchmark, with
// that sub-benchmark's own *testing.B, so a backend whose stores need
// per-run isolation (a fresh schema, database or key prefix) can build one
// there; the store is closed via b.Cleanup at the end of its sub-benchmark.
//
// A backend's bench_test.go calls this from a BenchmarkStore function, e.g.:
//
//	func BenchmarkStore(b *testing.B) {
//		storetest.RunBenchmarks(b, func(*testing.B) store.Store { return memory.New() })
//	}
func RunBenchmarks(b *testing.B, newStore func(b *testing.B) store.Store) {
	b.Helper()
	for _, n := range []int{1, 16, 64, 256} {
		b.Run(fmt.Sprintf("Reserve/batch=%d", n), func(b *testing.B) {
			benchmarkReserve(b, newStore(b), n)
		})
	}
	b.Run("AcquireShard", func(b *testing.B) {
		benchmarkAcquireShard(b, newStore(b))
	})
	b.Run("Quarantine/1000", func(b *testing.B) {
		benchmarkQuarantine(b, newStore(b))
	})
}

// reserveMintShards and reserveMintSize size benchmarkReserve's working set:
// 20 shards of 1000 rows (PutShard's own per-call maximum) each, 20,000 rows,
// re-minted whenever a run drains it.
const (
	reserveMintShards = 20
	reserveMintSize   = 1000
)

// benchmarkReserve measures Reserve at a fixed batch size: it reserves
// successive n-row slices of a minted working set under a fresh token and a
// 1h expiry, re-minting the working set (off the clock) whenever it runs out.
func benchmarkReserve(b *testing.B, s store.Store, n int) {
	b.Helper()
	b.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	exp := time.Now().Add(time.Hour)

	rows := mintMany(b, s, reserveMintShards, reserveMintSize)
	idx := 0

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if idx+n > len(rows) {
			b.StopTimer()
			rows = mintMany(b, s, reserveMintShards, reserveMintSize)
			idx = 0
			b.StartTimer()
		}
		batch := rows[idx : idx+n]
		idx += n

		applied, err := s.Reserve(ctx, benchPool, ReserveOps(batch, store.NewToken(), exp))
		if err != nil {
			b.Fatalf("Reserve: %v", err)
		}
		if len(applied) != n {
			b.Fatalf("Reserve: applied %d of %d ops", len(applied), n)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*n)/b.Elapsed().Seconds(), "rows/s")
}

// acquireMintShards and acquireMintSize size benchmarkAcquireShard's working
// set: 200 shards of 10 rows each, re-minted whenever a run drains it.
const (
	acquireMintShards = 200
	acquireMintSize   = 10
	acquireHolder     = "bench-holder"
)

// benchmarkAcquireShard measures AcquireShard: it repeatedly claims the
// oldest free mined shard under one holder, re-minting the working set (off
// the clock) whenever nothing is left to acquire.
func benchmarkAcquireShard(b *testing.B, s store.Store) {
	b.Helper()
	b.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	statuses := []store.ShardStatus{store.ShardMined}

	mintMany(b, s, acquireMintShards, acquireMintSize)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		now := time.Now()
		until := now.Add(time.Hour)
		sh, err := s.AcquireShard(ctx, benchPool, acquireHolder, now, until, statuses, nil)
		if err != nil {
			b.Fatalf("AcquireShard: %v", err)
		}
		if sh == nil {
			b.StopTimer()
			mintMany(b, s, acquireMintShards, acquireMintSize)
			b.StartTimer()

			now = time.Now()
			until = now.Add(time.Hour)
			sh, err = s.AcquireShard(ctx, benchPool, acquireHolder, now, until, statuses, nil)
			if err != nil {
				b.Fatalf("AcquireShard: %v", err)
			}
			if sh == nil {
				b.Fatal("AcquireShard: no shard available after minting more")
			}
		}
	}
}

// quarantineMintShards and quarantineMintSize size each iteration's rows: 2
// shards of 500 rows, 1000 total, matching quarantineLimit.
const (
	quarantineMintShards = 2
	quarantineMintSize   = 500
	quarantineLimit      = 1000
)

// benchmarkQuarantine measures Quarantine moving a full 1000-row batch of
// already-expired reservations to quarantined. Minting the shards and
// reserving them with a past expiry happens off the clock each iteration, so
// only the Quarantine call itself is timed.
func benchmarkQuarantine(b *testing.B, s store.Store) {
	b.Helper()
	b.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		rows := mintMany(b, s, quarantineMintShards, quarantineMintSize)
		exp := time.Now().Add(-time.Hour)
		applied, err := s.Reserve(ctx, benchPool, ReserveOps(rows, store.NewToken(), exp))
		if err != nil {
			b.Fatalf("Reserve (setup): %v", err)
		}
		if len(applied) != len(rows) {
			b.Fatalf("Reserve (setup): applied %d of %d ops", len(applied), len(rows))
		}
		b.StartTimer()

		n, err := s.Quarantine(ctx, benchPool, time.Now(), quarantineLimit)
		if err != nil {
			b.Fatalf("Quarantine: %v", err)
		}
		if n != len(rows) {
			b.Fatalf("Quarantine: moved %d of %d rows", n, len(rows))
		}
	}
}
