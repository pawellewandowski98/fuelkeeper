package pool

import "github.com/bsv-blockchain/go-sdk/chainhash"

// Metric vars exposed to the external test package (see metrics.go for what
// each one measures).
var (
	CoalescerBatchSize    = coalescerBatchSize
	CoalescerFlushLatency = coalescerFlushLatency
	CoalescerInflightOps  = coalescerInflightOps
	CASUnknown            = casUnknown
	CompensatingRelease   = compensatingRelease
	ShardAdoptions        = shardAdoptions
	ShardDrops            = shardDrops
	LeaseRenewFailures    = leaseRenewFailures
	StoreOpLatency        = storeOpLatency
	SpendBatchSize        = spendBatchSize
	SpendFlushSeconds     = spendFlushSeconds
	SpendFallback         = spendFallback
	SpendFlushRetries     = spendFlushRetries
	SpendInflightOps      = spendInflightOps
	SpendBackpressure     = spendBackpressure
)

// Tombstones reports how many forgotten-row tombstones the allocator holds.
func (a *Allocator) Tombstones() (entries, order int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.gone), len(a.goneOrder)
}

// ReleasePending reports whether a background lease release of txid (see
// dropRotatedAsync) has not finished yet, i.e. txid is still kept out of AcquireShard.
func (m *ShardManager) ReleasePending(txid chainhash.Hash) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, pending := m.releasing[txid]
	return pending
}

// RecentlyEmpty reports whether txid is on the recently-drained exclusion list.
func (m *ShardManager) RecentlyEmpty(txid chainhash.Hash) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.recentlyEmpty[txid]
	return ok
}

// Allocator exposes the pool's allocator.
func (p *Pool) Allocator() *Allocator { return p.alloc }
