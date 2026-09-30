package pool_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func mkRows(txid store.Outpoint, n int) []store.Row { // helper: n rows of one shard
	rows := make([]store.Row, n)
	for i := range rows {
		rows[i] = store.Row{Outpoint: store.Outpoint{TxID: txid.TxID, Vout: uint32(i)}, Pool: "main", Satoshis: 50} //nolint:gosec // small test counts
	}
	return rows
}

func owned(created time.Time) pool.OwnedShard {
	return pool.OwnedShard{TxID: storetest.NewTxID(), Status: store.ShardMined, Version: 1, CreatedAt: created}
}

func assertDistinctOutpoints(t *testing.T, rows []store.Row) {
	t.Helper()
	seen := map[store.Outpoint]bool{}
	for _, r := range rows {
		assert.False(t, seen[r.Outpoint], "duplicate outpoint %v", r.Outpoint)
		seen[r.Outpoint] = true
	}
}

func TestAllocatorOrderAndPop(t *testing.T) {
	a := pool.NewAllocator()
	t0 := storetest.Start
	newer := owned(t0.Add(time.Minute))
	older := owned(t0)
	require.True(t, a.AddShard(newer))
	require.True(t, a.AddShard(older))
	require.False(t, a.AddShard(older), "duplicate shard ignored")

	rowsOld := mkRows(store.Outpoint{TxID: older.TxID}, 3)
	rowsNew := mkRows(store.Outpoint{TxID: newer.TxID}, 3)
	assert.Equal(t, 3, a.Add(older.TxID, rowsOld))
	assert.Equal(t, 3, a.Add(newer.TxID, rowsNew))
	assert.Equal(t, 0, a.Add(older.TxID, rowsOld), "known rows are not re-added")
	assert.Equal(t, 0, a.Add(storetest.NewTxID(), rowsOld), "unknown shard ignored")
	assert.Equal(t, 0, a.Add(older.TxID, rowsNew), "rows of another shard are ignored")
	assert.Equal(t, 6, a.Available())

	got := a.Pop(4)
	require.Len(t, got, 4)
	assert.Equal(t, older.TxID, got[0].TxID, "oldest shard drains first")
	assert.Equal(t, older.TxID, got[2].TxID)
	assert.Equal(t, newer.TxID, got[3].TxID)
	assert.Equal(t, 2, a.Available())
	assert.Equal(t, 0, a.ShardAvailable(older.TxID))

	assert.Equal(t, 0, a.Add(older.TxID, rowsOld), "popped rows are still known (in flight)")
	a.Forget(storetest.Ops(rowsOld[:1]))
	assert.Equal(t, 1, a.Add(older.TxID, rowsOld), "forgotten row can be re-added by a rescan")

	a.Return(got[3:4])
	assert.Equal(t, 4, a.Available())
	next := a.Pop(1)
	require.Len(t, next, 1)
	assert.Equal(t, rowsOld[0].Outpoint, next[0].Outpoint, "older shard still first")
	next = a.Pop(1)
	assert.Equal(t, got[3].Outpoint, next[0].Outpoint, "returned row goes to the front of its shard")

	assert.Empty(t, a.Pop(0))
	shards := a.Shards()
	require.Len(t, shards, 2)
	assert.Equal(t, older.TxID, shards[0].TxID)

	dropped := a.RemoveShard(newer.TxID)
	assert.Equal(t, 2, dropped, "new1 and new2 were still queued")
	assert.False(t, a.Has(newer.TxID))
	assert.Equal(t, 0, a.Add(newer.TxID, rowsNew))
	a.Return(rowsNew[:1]) // shard gone: nothing to return to, and the row is forgotten
	assert.Equal(t, 0, a.Available(), "no leak")
	require.True(t, a.AddShard(newer))
	assert.Equal(t, 3, a.Add(newer.TxID, rowsNew), "after RemoveShard the rows are no longer known")

	a.UpdateHead(newer.TxID, store.ShardMined, 7)
	for _, s := range a.Shards() {
		if s.TxID == newer.TxID {
			assert.Equal(t, int64(7), s.Version)
		}
	}
}

func TestAllocatorReturnPreservesOrder(t *testing.T) {
	a := pool.NewAllocator()
	sh := owned(storetest.Start)
	require.True(t, a.AddShard(sh))
	rows := mkRows(store.Outpoint{TxID: sh.TxID}, 5)
	a.Add(sh.TxID, rows)
	got := a.Pop(3) // rows 0,1,2
	a.Return(got)   // back to the front, in original order
	again := a.Pop(5)
	require.Len(t, again, 5)
	for i, r := range again {
		assert.Equal(t, uint32(i), r.Vout, "order after Return matches original order") //nolint:gosec // small
	}
}

func TestAllocatorNoDoubleInFlight(t *testing.T) {
	a := pool.NewAllocator()
	sh := owned(storetest.Start)
	a.AddShard(sh)
	rows := mkRows(store.Outpoint{TxID: sh.TxID}, 500)
	a.Add(sh.TxID, rows)

	var mu sync.Mutex
	inflight := map[store.Outpoint]bool{}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				got := a.Pop(3)
				mu.Lock()
				for _, r := range got {
					assert.False(t, inflight[r.Outpoint], "outpoint popped while already in flight")
					inflight[r.Outpoint] = true
				}
				mu.Unlock()
				a.Add(sh.TxID, rows) // a concurrent "rescan" must not re-add in-flight rows
				mu.Lock()
				for _, r := range got {
					delete(inflight, r.Outpoint)
				}
				mu.Unlock()
				if i%2 == 0 {
					a.Forget(storetest.Ops(got)) // simulate CAS applied
				} else {
					a.Return(got) // simulate unflushed
				}
			}
		}()
	}
	wg.Wait()
	first := a.Pop(1000)
	again := a.Pop(1000)
	assert.Empty(t, again, "a second Pop yields nothing until Forget/Add")
	assertDistinctOutpoints(t, first)
	assert.Equal(t, 0, a.Available())
}

func TestAllocatorReturnAndForgetAreIdempotent(t *testing.T) {
	a := pool.NewAllocator()
	sh := owned(storetest.Start)
	require.True(t, a.AddShard(sh))
	rows := mkRows(store.Outpoint{TxID: sh.TxID}, 3)
	require.Equal(t, 3, a.Add(sh.TxID, rows))

	got := a.Pop(3)
	require.Len(t, got, 3)
	a.Return(got)
	a.Return(got) // duplicate Return must not double-queue
	assert.Equal(t, 3, a.Available())
	popped := a.Pop(10)
	require.Len(t, popped, 3)
	assertDistinctOutpoints(t, popped)

	// Return then Forget on the same (now in-flight again) rows: by the time
	// Forget runs the row is queued, not in flight, so Forget is a no-op and
	// the row stays queued (not lost).
	a.Return(popped)
	a.Forget(storetest.Ops(popped))
	assert.Equal(t, 3, a.Available(), "row stays queued; Forget on a queued row is a no-op")
	assert.Equal(t, 0, a.Add(sh.TxID, popped), "queued rows are not re-added")
	popped2 := a.Pop(10)
	require.Len(t, popped2, 3)
	assertDistinctOutpoints(t, popped2)
}

func TestAllocatorAddIgnoresForeignRows(t *testing.T) {
	a := pool.NewAllocator()
	shA := owned(storetest.Start)
	shB := owned(storetest.Start.Add(time.Minute))
	require.True(t, a.AddShard(shA))
	require.True(t, a.AddShard(shB))

	foreign := mkRows(store.Outpoint{TxID: shB.TxID}, 1) // belongs to shard B, never added anywhere
	assert.Equal(t, 0, a.Add(shA.TxID, foreign), "row of another shard is ignored")
	assert.Equal(t, 0, a.Available())
	assert.Empty(t, a.Pop(10), "foreign row must not appear anywhere")
	assert.Equal(t, 0, a.ShardAvailable(shA.TxID))
	assert.Equal(t, 0, a.ShardAvailable(shB.TxID))

	// the row was never actually tracked, so shard B can still add it for real.
	assert.Equal(t, 1, a.Add(shB.TxID, foreign))
}

func TestAllocatorForgetThenReturnDoesNotResurrect(t *testing.T) {
	a := pool.NewAllocator()
	sh := owned(storetest.Start)
	require.True(t, a.AddShard(sh))
	rows := mkRows(store.Outpoint{TxID: sh.TxID}, 1)
	require.Equal(t, 1, a.Add(sh.TxID, rows))

	got := a.Pop(1)
	require.Len(t, got, 1)
	a.Forget(storetest.Ops(got)) // simulate CAS applied: the row is gone for good
	a.Return(got)                // late/duplicate Return must not resurrect it
	assert.Equal(t, 0, a.Available())
	assert.Empty(t, a.Pop(1))
}

func BenchmarkAllocatorPopSteadyState100k(b *testing.B) {
	a := pool.NewAllocator()
	sh := owned(storetest.Start)
	a.AddShard(sh)
	all := mkRows(store.Outpoint{TxID: sh.TxID}, 100_000)
	a.Add(sh.TxID, all)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got := a.Pop(3)
		a.Forget(storetest.Ops(got))
		a.Add(sh.TxID, got)
	}
}

// TestAllocatorAddListedSkipsRowsForgottenDuringListing pins the listing
// guard at the allocator level: a row forgotten after a listing began is
// stale in that listing, a row forgotten before it began is not, and
// tombstones are only kept while some listing that needs them is open.
func TestAllocatorAddListedSkipsRowsForgottenDuringListing(t *testing.T) {
	a := pool.NewAllocator()
	sh := owned(storetest.Start)
	require.True(t, a.AddShard(sh))
	rows := mkRows(store.Outpoint{TxID: sh.TxID}, 4)
	require.Equal(t, 4, a.Add(sh.TxID, rows))

	before := a.Pop(1) // forgotten before any listing: no tombstone
	a.Forget(storetest.Ops(before))
	e, o := a.Tombstones()
	assert.Zero(t, e+o, "no tombstones while no listing is open")

	outer := a.BeginListing()
	during := a.Pop(1)
	a.Forget(storetest.Ops(during))
	inner := a.BeginListing() // begins after `during` was forgotten
	assert.Equal(t, 1, a.AddListed(sh.TxID, during, inner), "forgotten before the inner listing began: really back")
	all := a.Pop(10)
	require.Len(t, all, 3, "re-added at the back of the FIFO")
	require.Equal(t, during[0].Outpoint, all[2].Outpoint)
	a.Return(all[:2])
	a.Forget(storetest.Ops(all[2:]))
	assert.Zero(t, a.AddListed(sh.TxID, during, outer), "forgotten after the outer listing began: stale")
	assert.Zero(t, a.AddListed(sh.TxID, during, inner), "forgotten again after the inner listing began: stale")
	assert.Equal(t, 1, a.AddListed(sh.TxID, before, outer), "forgotten before the outer listing began: really back")

	a.EndListing(outer)
	e, _ = a.Tombstones()
	assert.Equal(t, 1, e, "the inner listing still needs its tombstone")
	a.EndListing(inner)
	e, o = a.Tombstones()
	assert.Zero(t, e+o, "all tombstones pruned once no listing is open")
	assert.Equal(t, 1, a.Add(sh.TxID, during), "plain Add ignores tombstones")
}
