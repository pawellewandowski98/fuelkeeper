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
	ops := map[store.Outpoint]bool{}
	for _, r := range first {
		assert.False(t, ops[r.Outpoint], "duplicate in one Pop")
		ops[r.Outpoint] = true
	}
	assert.Equal(t, 0, a.Available())
}
