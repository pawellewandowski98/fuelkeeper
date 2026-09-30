package pool

import (
	"bytes"
	"sort"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// OwnedShard is the allocator's view of a shard this instance leases.
type OwnedShard struct {
	TxID      chainhash.Hash
	Status    store.ShardStatus
	Version   int64
	CreatedAt time.Time
}

type shardQueue struct {
	head OwnedShard
	fifo []store.Row // front at index 0
}

// Allocator is the per-pool in-memory FIFO of available rows across owned
// shards. Rows are popped oldest-shard-first. A row is "known" from Add until
// Forget (or RemoveShard); known rows are never re-added, which makes rescans
// safe while rows are in flight to the coalescer.
type Allocator struct {
	mu    sync.Mutex
	order []*shardQueue // oldest first
	byTx  map[chainhash.Hash]*shardQueue
	known map[store.Outpoint]struct{}
	avail int
}

// NewAllocator returns an empty allocator.
func NewAllocator() *Allocator {
	return &Allocator{byTx: map[chainhash.Hash]*shardQueue{}, known: map[store.Outpoint]struct{}{}}
}

func lessOwned(a, b OwnedShard) bool {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.Before(b.CreatedAt)
	}
	return bytes.Compare(a.TxID[:], b.TxID[:]) < 0
}

// AddShard registers a shard; false if already present.
func (a *Allocator) AddShard(sh OwnedShard) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.byTx[sh.TxID]; ok {
		return false
	}
	q := &shardQueue{head: sh}
	a.byTx[sh.TxID] = q
	a.order = append(a.order, q)
	sort.SliceStable(a.order, func(i, j int) bool { return lessOwned(a.order[i].head, a.order[j].head) })
	return true
}

// RemoveShard drops a shard and its FIFO, forgetting those rows. Returns how
// many queued rows were dropped. Rows of that shard currently in flight stay
// known until Forget.
func (a *Allocator) RemoveShard(txid chainhash.Hash) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	q, ok := a.byTx[txid]
	if !ok {
		return 0
	}
	for _, r := range q.fifo {
		delete(a.known, r.Outpoint)
	}
	n := len(q.fifo)
	a.avail -= n
	delete(a.byTx, txid)
	for i, o := range a.order {
		if o == q {
			a.order = append(a.order[:i], a.order[i+1:]...)
			break
		}
	}
	return n
}

// Has reports whether the shard is registered.
func (a *Allocator) Has(txid chainhash.Hash) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.byTx[txid]
	return ok
}

// UpdateHead records a new status/version for an owned shard.
func (a *Allocator) UpdateHead(txid chainhash.Hash, status store.ShardStatus, version int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if q, ok := a.byTx[txid]; ok {
		q.head.Status, q.head.Version = status, version
	}
}

// Add appends rows to the shard's FIFO, skipping known outpoints and rows of
// other shards. Returns how many were added.
func (a *Allocator) Add(txid chainhash.Hash, rows []store.Row) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	q, ok := a.byTx[txid]
	if !ok {
		return 0
	}
	n := 0
	for _, r := range rows {
		if r.TxID != txid {
			continue
		}
		if _, dup := a.known[r.Outpoint]; dup {
			continue
		}
		a.known[r.Outpoint] = struct{}{}
		q.fifo = append(q.fifo, r)
		n++
	}
	a.avail += n
	return n
}

// Pop removes up to n rows, oldest shard first. Popped rows remain known.
//
// The fifo slice is drained from the front by re-slicing and copying the
// remainder down (via append to a fresh backing array), not by advancing a
// head index. That keeps the array Pop copies out of (out = append(out,
// q.fifo[:take]...)) fully detached from the array Return later prepends
// onto, so Return's `append([]store.Row{r}, q.fifo...)` can never alias or
// overwrite memory a caller is still holding from a prior Pop.
func (a *Allocator) Pop(n int) []store.Row {
	if n <= 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]store.Row, 0, n)
	for _, q := range a.order {
		if len(out) == n {
			break
		}
		take := n - len(out)
		if take > len(q.fifo) {
			take = len(q.fifo)
		}
		out = append(out, q.fifo[:take]...)
		q.fifo = append([]store.Row(nil), q.fifo[take:]...)
	}
	a.avail -= len(out)
	return out
}

// Return puts rows that never reached the store back at the FRONT of their
// shard's FIFO, preserving their relative order. Callers must only pass rows
// obtained from Pop that have not since been Forgotten (a row already back in
// a FIFO, or already re-added by a rescan, must never be passed to Return).
//
// If a row is no longer known -- because Forget already ran for it (the CAS
// applied) or RemoveShard dropped its shard -- it is dropped here silently
// rather than re-queued, even if the shard still exists. Re-queuing it would
// resurrect a row that is already reserved (or otherwise spoken for) under a
// Forget-then-Return race. If the shard is gone entirely the row is also
// forgotten so it does not leak in the known set.
func (a *Allocator) Return(rows []store.Row) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := len(rows) - 1; i >= 0; i-- { // reverse so original order is preserved at the front
		r := rows[i]
		q, ok := a.byTx[r.TxID]
		if !ok {
			delete(a.known, r.Outpoint)
			continue
		}
		if _, known := a.known[r.Outpoint]; !known {
			// Already Forgotten (CAS applied) or otherwise no longer ours: do not resurrect it.
			continue
		}
		q.fifo = append([]store.Row{r}, q.fifo...)
		a.avail++
	}
}

// Forget removes outpoints from the known set (after a CAS result, applied or
// missed). They are not in any FIFO at that point.
func (a *Allocator) Forget(ops []store.Outpoint) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, op := range ops {
		delete(a.known, op)
	}
}

// Available returns the number of queued rows across all shards.
func (a *Allocator) Available() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.avail
}

// ShardAvailable returns the queued rows of one shard.
func (a *Allocator) ShardAvailable(txid chainhash.Hash) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	if q, ok := a.byTx[txid]; ok {
		return len(q.fifo)
	}
	return 0
}

// Shards lists owned shards oldest first.
func (a *Allocator) Shards() []OwnedShard {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]OwnedShard, len(a.order))
	for i, q := range a.order {
		out[i] = q.head
	}
	return out
}
