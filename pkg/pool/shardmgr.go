package pool

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

const (
	listPage        = 1000
	emptyExclusion  = 60 * time.Second // how long a drained shard is excluded from re-adoption
	emptyRescansMax = 2                // consecutive empty rescans before dropping a shard
	adoptAttempts   = 5                // per hunger event
)

type ownedState struct {
	head         store.ShardHead
	leaseUntil   time.Time
	emptyRescans int
	rescanNow    bool
}

// ShardManager leases shards for one pool on one instance and keeps the
// allocator fed. Correctness never depends on it: it only makes CAS conflicts
// rare (see the store contract's lease documentation in pkg/store/store.go).
type ShardManager struct {
	st     store.Store
	pc     Config
	cfg    InstanceConfig
	alloc  *Allocator
	clock  Clock
	log    *slog.Logger
	holder string
	serve  []store.ShardStatus

	mu            sync.Mutex
	owned         map[chainhash.Hash]*ownedState
	recentlyEmpty map[chainhash.Hash]time.Time
	hungry        chan struct{}
}

// NewShardManager builds a manager with a fresh holder id.
func NewShardManager(st store.Store, pc Config, cfg InstanceConfig, alloc *Allocator, clock Clock, log *slog.Logger) *ShardManager {
	return &ShardManager{
		st:            st,
		pc:            pc,
		cfg:           cfg,
		alloc:         alloc,
		clock:         clock,
		log:           log.With("component", "shardmgr", "pool", pc.Name),
		holder:        NewHolder(cfg.InstanceID),
		serve:         pc.ServeStatuses(),
		owned:         map[chainhash.Hash]*ownedState{},
		recentlyEmpty: map[chainhash.Hash]time.Time{},
		hungry:        make(chan struct{}, 1),
	}
}

// Holder returns this instance's lease holder id.
func (m *ShardManager) Holder() string { return m.holder }

// Hungry signals the run loop to try adopting (non-blocking).
func (m *ShardManager) Hungry() {
	select {
	case m.hungry <- struct{}{}:
	default:
	}
}

func (m *ShardManager) isHungry() bool {
	m.mu.Lock()
	n := len(m.owned)
	m.mu.Unlock()
	return n < m.cfg.MaxShards && m.alloc.Available() < m.cfg.LowWaterRows
}

// FeedOnce adopts shards while hungry, bounded by adoptAttempts per call.
// Hunger is re-checked after every adoption (owned < MaxShards and available
// rows below LowWaterRows): an instance stops pulling shards the moment it is
// no longer hungry, one shard at a time, so a single instance cannot hoard
// shards up to MaxShards when another instance may also need one.
func (m *ShardManager) FeedOnce(ctx context.Context) error {
	for i := 0; i < adoptAttempts && m.isHungry(); i++ {
		adopted, err := m.AdoptOnce(ctx)
		if err != nil {
			return err
		}
		if !adopted {
			return nil
		}
	}
	return nil
}

// EnsureRows is the bounded synchronous path Reserve uses when the FIFO is
// empty: it tries to adopt until rows are available, AdoptTimeout passes, or
// nothing is adoptable. Returns true if rows are now available.
func (m *ShardManager) EnsureRows(ctx context.Context) bool {
	if m.alloc.Available() > 0 {
		return true
	}
	ctx, cancel := context.WithTimeout(ctx, m.cfg.AdoptTimeout)
	defer cancel()
	for i := 0; i < adoptAttempts; i++ {
		m.mu.Lock()
		full := len(m.owned) >= m.cfg.MaxShards
		m.mu.Unlock()
		if full {
			return m.alloc.Available() > 0
		}
		adopted, err := m.AdoptOnce(ctx)
		if err != nil || !adopted {
			return m.alloc.Available() > 0
		}
		if m.alloc.Available() > 0 {
			return true
		}
	}
	return m.alloc.Available() > 0
}

func (m *ShardManager) excludeList(now time.Time) []chainhash.Hash {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ex []chainhash.Hash
	for txid, at := range m.recentlyEmpty {
		if now.Sub(at) > emptyExclusion {
			delete(m.recentlyEmpty, txid)
			continue
		}
		ex = append(ex, txid)
	}
	return ex
}

// AdoptOnce tries to acquire one adoptable shard and load its rows. It returns
// false when nothing was adoptable (or the adopted shard turned out empty and
// was released again).
func (m *ShardManager) AdoptOnce(ctx context.Context) (bool, error) {
	now := m.clock.Now()
	until := now.Add(m.cfg.LeaseTTL)
	sh, err := m.st.AcquireShard(ctx, m.pc.Name, m.holder, now, until, m.serve, m.excludeList(now))
	if err != nil {
		return false, err
	}
	if sh == nil {
		return false, nil
	}
	m.alloc.AddShard(OwnedShard{TxID: sh.TxID, Status: sh.Status, Version: sh.Version, CreatedAt: sh.CreatedAt})
	m.mu.Lock()
	m.owned[sh.TxID] = &ownedState{head: store.ShardHead{Status: sh.Status, Version: sh.Version}, leaseUntil: until}
	m.mu.Unlock()
	loaded, err := m.loadRows(ctx, sh.TxID)
	if err != nil {
		return true, err // keep the shard; rescan will retry
	}
	if loaded == 0 {
		m.drop(ctx, sh.TxID, true)
		return false, nil
	}
	m.log.Debug("adopted shard", "txid", sh.TxID.String(), "rows", loaded)
	return true, nil
}

// loadRows pages ListAvailable into the allocator.
func (m *ShardManager) loadRows(ctx context.Context, txid chainhash.Hash) (int, error) {
	total := 0
	var from uint32
	for {
		rows, err := m.st.ListAvailable(ctx, m.pc.Name, txid, from, listPage)
		if err != nil {
			return total, err
		}
		total += m.alloc.Add(txid, rows)
		if len(rows) < listPage {
			return total, nil
		}
		from = rows[len(rows)-1].Vout + 1
	}
}

// drop forgets a shard locally and, when release is true, releases its lease
// and excludes it from re-adoption for emptyExclusion.
func (m *ShardManager) drop(ctx context.Context, txid chainhash.Hash, release bool) {
	m.alloc.RemoveShard(txid)
	m.mu.Lock()
	delete(m.owned, txid)
	if release {
		m.recentlyEmpty[txid] = m.clock.Now()
	}
	m.mu.Unlock()
	if release {
		err := m.st.ReleaseShard(ctx, m.pc.Name, txid, m.holder)
		if err != nil && !errors.Is(err, store.ErrNotFound) && !errors.Is(err, store.ErrNotOwner) {
			m.log.Warn("release shard", "txid", txid.String(), "err", err)
		}
	}
}

func (m *ShardManager) ownedIDs() []chainhash.Hash {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]chainhash.Hash, 0, len(m.owned))
	for id := range m.owned {
		ids = append(ids, id)
	}
	return ids
}

// RenewOnce renews every owned lease and drops shards that were lost or are no
// longer servable.
func (m *ShardManager) RenewOnce(ctx context.Context) error {
	now := m.clock.Now()
	until := now.Add(m.cfg.LeaseTTL)
	var firstErr error
	for _, txid := range m.ownedIDs() {
		head, err := m.st.RenewShard(ctx, m.pc.Name, txid, m.holder, until)
		switch {
		case errors.Is(err, store.ErrNotOwner), errors.Is(err, store.ErrNotFound):
			m.log.Info("lost shard lease", "txid", txid.String())
			m.drop(ctx, txid, false)
			continue
		case err != nil:
			if firstErr == nil {
				firstErr = err
			}
			// Keep serving on a transient error, but drop if the lease has
			// been expired for more than one full TTL: something is wrong
			// and another holder may already be serving this shard.
			m.mu.Lock()
			st := m.owned[txid]
			stale := st != nil && now.Sub(st.leaseUntil) > m.cfg.LeaseTTL
			m.mu.Unlock()
			if stale {
				m.log.Warn("dropping shard with stale lease", "txid", txid.String())
				m.drop(ctx, txid, false)
			}
			continue
		}
		if !m.servable(head.Status) {
			m.log.Info("shard no longer servable", "txid", txid.String(), "status", head.Status.String())
			m.drop(ctx, txid, true)
			continue
		}
		m.mu.Lock()
		if st := m.owned[txid]; st != nil {
			st.leaseUntil = until
			st.head = head
		}
		m.mu.Unlock()
		m.alloc.UpdateHead(txid, head.Status, head.Version)
	}
	return firstErr
}

func (m *ShardManager) servable(s store.ShardStatus) bool {
	for _, want := range m.serve {
		if s == want {
			return true
		}
	}
	return false
}

// RescanOnce reloads available rows of owned shards and drops shards that stay
// empty for emptyRescansMax consecutive scans.
func (m *ShardManager) RescanOnce(ctx context.Context) error {
	var firstErr error
	for _, txid := range m.ownedIDs() {
		added, err := m.loadRows(ctx, txid)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		m.mu.Lock()
		st := m.owned[txid]
		if st == nil {
			m.mu.Unlock()
			continue
		}
		st.rescanNow = false
		if added == 0 && m.alloc.ShardAvailable(txid) == 0 {
			st.emptyRescans++
		} else {
			st.emptyRescans = 0
		}
		empty := st.emptyRescans >= emptyRescansMax
		m.mu.Unlock()
		if empty {
			m.log.Debug("dropping drained shard", "txid", txid.String())
			m.drop(ctx, txid, true)
		}
	}
	return firstErr
}

// rescanFlagged reloads rows only for shards flagged by NoteCASMiss, ahead of
// their regular RescanInterval turn.
func (m *ShardManager) rescanFlagged(ctx context.Context) error {
	m.mu.Lock()
	var flagged []chainhash.Hash
	for txid, st := range m.owned {
		if st.rescanNow {
			flagged = append(flagged, txid)
		}
	}
	m.mu.Unlock()
	var firstErr error
	for _, txid := range flagged {
		if _, err := m.loadRows(ctx, txid); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		m.mu.Lock()
		if st := m.owned[txid]; st != nil {
			st.rescanNow = false
		}
		m.mu.Unlock()
	}
	return firstErr
}

// NoteCASMiss flags a shard for an immediate rescan (another holder may be
// serving it) and wakes the run loop.
func (m *ShardManager) NoteCASMiss(txid chainhash.Hash) {
	m.mu.Lock()
	if st := m.owned[txid]; st != nil {
		st.rescanNow = true
	}
	m.mu.Unlock()
	m.Hungry()
}

// NoteReleased re-adds rows released through this instance when we own their shard.
func (m *ShardManager) NoteReleased(rows []store.Row) {
	byShard := map[chainhash.Hash][]store.Row{}
	for _, r := range rows {
		byShard[r.TxID] = append(byShard[r.TxID], r)
	}
	for txid, rs := range byShard {
		if m.alloc.Has(txid) {
			m.alloc.Add(txid, rs)
		}
	}
}

// ReleaseAll releases every owned shard (graceful shutdown).
func (m *ShardManager) ReleaseAll(ctx context.Context) error {
	var firstErr error
	for _, txid := range m.ownedIDs() {
		m.alloc.RemoveShard(txid)
		m.mu.Lock()
		delete(m.owned, txid)
		m.mu.Unlock()
		err := m.st.ReleaseShard(ctx, m.pc.Name, txid, m.holder)
		if err != nil && !errors.Is(err, store.ErrNotFound) && !errors.Is(err, store.ErrNotOwner) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Run drives FeedOnce on hunger, RenewOnce roughly every LeaseTTL/3 and
// RescanOnce every RescanInterval, until ctx is done; then it releases all
// owned shards before returning.
func (m *ShardManager) Run(ctx context.Context) error {
	renewEvery := m.cfg.LeaseTTL / 3
	if renewEvery <= 0 {
		renewEvery = time.Millisecond
	}
	renew := time.NewTicker(renewEvery)
	defer renew.Stop()
	rescan := time.NewTicker(m.cfg.RescanInterval)
	defer rescan.Stop()
	m.Hungry()
	for {
		select {
		case <-ctx.Done():
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			return m.ReleaseAll(rctx)
		case <-m.hungry:
			if err := m.rescanFlagged(ctx); err != nil && ctx.Err() == nil {
				m.log.Warn("rescan flagged", "err", err)
			}
			if err := m.FeedOnce(ctx); err != nil && ctx.Err() == nil {
				m.log.Warn("adopt", "err", err)
			}
		case <-renew.C:
			if err := m.RenewOnce(ctx); err != nil && ctx.Err() == nil {
				m.log.Warn("renew", "err", err)
			}
			if m.isHungry() {
				m.Hungry()
			}
		case <-rescan.C:
			if err := m.RescanOnce(ctx); err != nil && ctx.Err() == nil {
				m.log.Warn("rescan", "err", err)
			}
			if m.isHungry() {
				m.Hungry()
			}
		}
	}
}
