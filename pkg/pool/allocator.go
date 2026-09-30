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

// rowState is where a tracked row currently sits.
type rowState uint8

const (
	// rowQueued is sitting in its shard's FIFO, available to Pop.
	rowQueued rowState = iota
	// rowInflight has been Popped and not yet Returned or Forgotten.
	rowInflight
)

// Allocator is the per-pool in-memory FIFO of available rows across owned
// shards. Rows are popped oldest-shard-first.
//
// Every row Add has accepted is tracked in rows as either queued (sitting in
// a shard's FIFO) or inflight (handed out by Pop, not yet resolved). A row is
// tracked from Add until it leaves via Forget or RemoveShard; a tracked row
// is never re-added, which makes rescans safe while rows are in flight to the
// coalescer. Tracking the state (not just membership) is what lets Return and
// Forget be idempotent: each only acts on a row in the state it expects and
// is a silent no-op otherwise, so calling either twice, or calling one after
// the other has already resolved the row, can never double-queue a row or
// double-hand it out from Pop.
type Allocator struct {
	mu    sync.Mutex
	order []*shardQueue // oldest first
	byTx  map[chainhash.Hash]*shardQueue
	rows  map[store.Outpoint]rowState
	avail int
}

// NewAllocator returns an empty allocator.
func NewAllocator() *Allocator {
	return &Allocator{byTx: map[chainhash.Hash]*shardQueue{}, rows: map[store.Outpoint]rowState{}}
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

// RemoveShard drops a shard and its FIFO. Returns how many queued rows were
// dropped; those rows are untracked immediately (a rescan may re-Add them
// under a new AddShard). Rows of that shard currently in flight are left
// tracked as inflight until Forget resolves them, or until Return is called
// for them -- at which point, since the shard is gone, Return drops them from
// tracking too instead of re-queuing them.
func (a *Allocator) RemoveShard(txid chainhash.Hash) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	q, ok := a.byTx[txid]
	if !ok {
		return 0
	}
	for _, r := range q.fifo {
		delete(a.rows, r.Outpoint) // only queued rows are in q.fifo; inflight rows stay tracked
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

// Add appends rows to the shard's FIFO as queued, skipping rows already
// tracked (queued or inflight) and rows belonging to another shard. Returns
// how many were added.
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
		if _, tracked := a.rows[r.Outpoint]; tracked {
			continue
		}
		a.rows[r.Outpoint] = rowQueued
		q.fifo = append(q.fifo, r)
		n++
	}
	a.avail += n
	return n
}

// Pop removes up to n rows, oldest shard first, marking each one inflight.
//
// The returned slice is built into its own fresh backing array (out starts
// from make, and every element is copied into it by append), so it never
// aliases any shard's fifo array. Each shard's fifo is then advanced past the
// popped prefix with a plain reslice (q.fifo = q.fifo[take:]), an O(1)
// operation that keeps sharing the same backing array rather than copying the
// remainder down. That sharing is safe precisely because out never aliases
// fifo's array (so nothing written through a later append to fifo can be
// observed through a row a caller is still holding from Pop) and because
// Return always allocates a brand new backing array when it prepends rather
// than mutating fifo's array in place. Once a shard's fifo is fully drained
// its backing array reference is dropped (set to nil) so it can be collected;
// if a large fifo has shrunk to a small fraction of its backing array's
// capacity, it is compacted into a right-sized array so that one big burst
// does not pin a large array in memory indefinitely.
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
		taken := q.fifo[:take]
		for _, r := range taken {
			a.rows[r.Outpoint] = rowInflight
		}
		out = append(out, taken...)
		q.fifo = q.fifo[take:]
		switch {
		case len(q.fifo) == 0:
			q.fifo = nil
		case cap(q.fifo) > 4096 && len(q.fifo) < cap(q.fifo)/8:
			compact := make([]store.Row, len(q.fifo))
			copy(compact, q.fifo)
			q.fifo = compact
		}
	}
	a.avail -= len(out)
	return out
}

// Return puts rows that never reached the store back at the FRONT of their
// shard's FIFO, preserving their relative order, transitioning each from
// inflight to queued.
//
// Return is idempotent: it only acts on a row it finds inflight. A row that
// is instead found queued (an earlier Return already requeued it, so this is
// a duplicate call), or not tracked at all (Forget already ran for it because
// the CAS applied, or it was never popped), is left exactly as it is -- so
// calling Return twice for the same rows, or calling it after Forget, can
// never create a duplicate in a FIFO or resurrect a row that is already
// spoken for. If the row's shard no longer exists (RemoveShard dropped it)
// the row is untracked entirely, since there is nowhere left to queue it.
//
// Return always allocates a fresh backing array when it prepends; see Pop's
// doc comment for why that, together with Pop never returning a slice that
// aliases a shard's fifo array, keeps the two methods safe to use together.
func (a *Allocator) Return(rows []store.Row) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := len(rows) - 1; i >= 0; i-- { // reverse so original order is preserved at the front
		r := rows[i]
		q, ok := a.byTx[r.TxID]
		if !ok {
			delete(a.rows, r.Outpoint)
			continue
		}
		if a.rows[r.Outpoint] != rowInflight {
			// Already queued (duplicate Return) or untracked (already Forgotten, or
			// never popped): leave it alone.
			continue
		}
		a.rows[r.Outpoint] = rowQueued
		q.fifo = append([]store.Row{r}, q.fifo...)
		a.avail++
	}
}

// Forget resolves outpoints that are inflight (a CAS result applied) by
// untracking them; they are not in any FIFO at that point.
//
// Forget is idempotent: it only deletes an outpoint it finds inflight. An
// outpoint that is instead queued (still sitting in a FIFO -- for instance
// because Return already ran for it) or already untracked is left alone, so
// Forget can never remove a row that a rescan or a Return has since put back
// into circulation.
func (a *Allocator) Forget(ops []store.Outpoint) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, op := range ops {
		if a.rows[op] == rowInflight {
			delete(a.rows, op)
		}
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
