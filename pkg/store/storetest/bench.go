package storetest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

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
// Some factories (e.g. mongodb/aerospike's isolatedStoreTB) already register
// their own Close cleanup; Close is documented idempotent (store.go), so that
// and this package's own b.Cleanup(Close) running back to back is safe.
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
	for _, leased := range []int{0, 100, 1000} {
		b.Run(fmt.Sprintf("AcquireShard/leased=%d", leased), func(b *testing.B) {
			benchmarkAcquireShard(b, newStore(b), leased)
		})
	}
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
// Reserve resolves each op by outpoint (a point lookup/CAS, not a scan), so,
// unlike AcquireShard and Quarantine below, its cost does not grow with how
// much the working set has accumulated across re-mints.
func benchmarkReserve(b *testing.B, s store.Store, n int) {
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

// acquireRingSize (K) bounds how many shards benchmarkAcquireShard's own
// holder keeps leased at once: once it holds K, it releases the oldest
// (off the clock) before every further acquire, so the working set it churns
// through never grows -- unlike a holder that never releases, whose calls
// would walk past an ever-larger prefix of shards it already holds on a
// backend (e.g. pebble) whose AcquireShard scans its lease index in order
// and skips busy entries one by one.
//
// acquireFreeShards is how many mined shards are minted for that holder to
// cycle through; acquireRingSize is both necessary (fewer and the initial,
// release-free fill phase runs out) and sufficient (the steady state after
// that only ever needs the one shard just released), so a small buffer on
// top is just slack, not a requirement.
const (
	acquireRingSize     = 16
	acquireFreeShards   = acquireRingSize + 4
	acquireShardSize    = 1
	acquireHolder       = "bench-holder"
	acquireOtherHolder  = "bench-leased-holder"
	acquireLeaseHorizon = 100 * time.Hour // far past any run's length, so a pre-lease never expires mid-benchmark
)

// preLeaseShards mints n shards and, under a holder distinct from
// benchmarkAcquireShard's own, leases every one of them for
// acquireLeaseHorizon so they never come free again during the run. Since
// AcquireShard always claims the single oldest eligible shard and this runs
// before any other shard exists in the pool, the n shards it mints here are
// exactly the n oldest in the pool -- a permanently-busy prefix that every
// later AcquireShard call under benchmarkAcquireShard's own holder must walk
// past on a scanning backend, making "leased" a documented, fixed dimension
// of that call's cost instead of something that silently grows with b.N.
func preLeaseShards(b *testing.B, s store.Store, n int) {
	b.Helper()
	if n == 0 {
		return
	}
	mintMany(b, s, n, acquireShardSize)

	ctx := context.Background()
	statuses := []store.ShardStatus{store.ShardMined}
	for i := 0; i < n; i++ {
		now := time.Now()
		sh, err := s.AcquireShard(ctx, benchPool, acquireOtherHolder, now, now.Add(acquireLeaseHorizon), statuses, nil)
		if err != nil {
			b.Fatalf("AcquireShard (pre-lease setup): %v", err)
		}
		if sh == nil {
			b.Fatalf("AcquireShard (pre-lease setup): no free shard at i=%d of %d", i, n)
		}
	}
}

// benchmarkAcquireShard measures AcquireShard against a pool with leased
// permanently-busy shards ahead of a small, steadily-recycled working
// set (see acquireRingSize and preLeaseShards): it repeatedly claims the
// oldest free mined shard under one holder, releasing its own oldest held
// lease first whenever it already holds acquireRingSize of them, so neither
// the busy prefix nor its own working set grows over the run.
func benchmarkAcquireShard(b *testing.B, s store.Store, leased int) {
	b.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	statuses := []store.ShardStatus{store.ShardMined}

	preLeaseShards(b, s, leased)
	mintMany(b, s, acquireFreeShards, acquireShardSize)

	var ring [acquireRingSize]chainhash.Hash
	ringLen, ringHead := 0, 0

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if ringLen == acquireRingSize {
			b.StopTimer()
			if err := s.ReleaseShard(ctx, benchPool, ring[ringHead], acquireHolder); err != nil {
				b.Fatalf("ReleaseShard: %v", err)
			}
			b.StartTimer()
		}

		now := time.Now()
		until := now.Add(time.Hour)
		sh, err := s.AcquireShard(ctx, benchPool, acquireHolder, now, until, statuses, nil)
		if err != nil {
			b.Fatalf("AcquireShard: %v", err)
		}
		if sh == nil {
			b.Fatalf("AcquireShard: no free shard at iteration %d (leased=%d)", i, leased)
		}
		if sh.Owner != acquireHolder || sh.Status != store.ShardMined {
			b.Fatalf("AcquireShard: got owner=%q status=%v, want owner=%q status=%v", sh.Owner, sh.Status, acquireHolder, store.ShardMined)
		}

		if ringLen == acquireRingSize {
			ring[ringHead] = sh.TxID
			ringHead = (ringHead + 1) % acquireRingSize
		} else {
			ring[ringLen] = sh.TxID
			ringLen++
		}
	}
}

// quarantineMintShards and quarantineMintSize size the one working set
// benchmarkQuarantine mints, up front, outside the timed loop: 2 shards of
// 500 rows, 1000 total, matching quarantineLimit.
const (
	quarantineMintShards = 2
	quarantineMintSize   = 500
	quarantineLimit      = 1000
)

// benchmarkQuarantine measures Quarantine moving a full 1000-row batch of
// already-expired reservations to quarantined. The 1000 rows are minted once;
// each iteration reserves them with a past expiry (off the clock), times only
// the Quarantine call, then resolves them all straight back to available (off
// the clock) so the next iteration starts from the same 1000 rows -- the
// store's size never grows with b.N.
func benchmarkQuarantine(b *testing.B, s store.Store) {
	b.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	rows := mintMany(b, s, quarantineMintShards, quarantineMintSize)
	ops := Ops(rows)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
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

		b.StopTimer()
		resolved, err := s.Resolve(ctx, benchPool, ops, store.StateAvailable, nil)
		if err != nil {
			b.Fatalf("Resolve (teardown): %v", err)
		}
		if resolved != len(rows) {
			b.Fatalf("Resolve (teardown): resolved %d of %d rows", resolved, len(rows))
		}
		b.StartTimer()
	}
}
