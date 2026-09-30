package pool

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// shardDropReasons are the reasons this manager ever passes to drop() or
// counts directly; used to pre-populate dropCounters in NewShardManager so
// drop's hot path never calls WithLabelValues.
var shardDropReasons = []string{"not_owner", "retired", "empty", "stale", "closed"}

const (
	listPage         = 1000
	emptyExclusion   = 60 * time.Second // how long a drained shard is excluded from re-adoption
	emptyRescansMax  = 2                // consecutive empty rescans before dropping a shard
	adoptAttempts    = 5                // per hunger event
	maxRecentlyEmpty = 256              // caps the exclusion set AcquireShard is called with

	// rotateMax bounds how many drained shards one rotation (see
	// rotateDrained) rescans and may release per EnsureRows or FeedOnce call,
	// so a single hungry request never does more than rotateMax extra
	// ListAvailable round trips plus the adoptions refilling the freed slots.
	rotateMax = 2
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
	recentOrder   []chainhash.Hash            // insertion order of recentlyEmpty, for eviction at maxRecentlyEmpty
	releasing     map[chainhash.Hash]struct{} // shards whose lease release runs in the background; never re-acquired meanwhile
	bg            sync.WaitGroup              // background lease releases; Run waits for them before returning
	closed        bool
	hungry        chan struct{}
	adoptSem      chan struct{} // capacity 1; serializes adopt+load across FeedOnce and EnsureRows

	// Metric children for this pool, fetched once here instead of calling
	// WithLabelValues on the hot path. See metrics.go for what each measures.
	adoptionsOK          prometheus.Counter
	adoptionsNone        prometheus.Counter
	adoptionsError       prometheus.Counter
	dropCounters         map[string]prometheus.Counter // reason -> counter, populated for shardDropReasons
	renewFailures        prometheus.Counter
	acquireShardLatency  prometheus.Observer
	renewShardLatency    prometheus.Observer
	releaseShardLatency  prometheus.Observer
	listAvailableLatency prometheus.Observer
}

// NewShardManager builds a manager with a fresh holder id.
func NewShardManager(st store.Store, pc Config, cfg InstanceConfig, alloc *Allocator, clock Clock, log *slog.Logger) *ShardManager {
	dropCounters := make(map[string]prometheus.Counter, len(shardDropReasons))
	for _, reason := range shardDropReasons {
		dropCounters[reason] = shardDrops.WithLabelValues(pc.Name, reason)
	}
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
		releasing:     map[chainhash.Hash]struct{}{},
		hungry:        make(chan struct{}, 1),
		adoptSem:      make(chan struct{}, 1),

		adoptionsOK:          shardAdoptions.WithLabelValues(pc.Name, "ok"),
		adoptionsNone:        shardAdoptions.WithLabelValues(pc.Name, "none"),
		adoptionsError:       shardAdoptions.WithLabelValues(pc.Name, "error"),
		dropCounters:         dropCounters,
		renewFailures:        leaseRenewFailures.WithLabelValues(pc.Name),
		acquireShardLatency:  storeOpLatency.WithLabelValues(pc.Name, "acquire_shard"),
		renewShardLatency:    storeOpLatency.WithLabelValues(pc.Name, "renew_shard"),
		releaseShardLatency:  storeOpLatency.WithLabelValues(pc.Name, "release_shard"),
		listAvailableLatency: storeOpLatency.WithLabelValues(pc.Name, "list_available"),
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

// isHungry reports whether the instance should pull another shard: in-memory
// rows are below LowWaterRows and either a slot is free (owned < MaxShards)
// or, at MaxShards, at least one owned shard's FIFO is empty so rotateDrained
// may be able to trade it for a fresh one. A drained shard whose store rows
// are all still in flight through this instance keeps the instance hungry
// without being rotated out, so each hunger pass repeats at most rotateMax
// ListAvailable calls (one pass at a time, serialized on adoptSem) until
// those flushes land and the shard reads empty.
func (m *ShardManager) isHungry() bool {
	if m.alloc.Available() >= m.cfg.LowWaterRows {
		return false
	}
	if m.ownedCount() < m.cfg.MaxShards {
		return true
	}
	for _, txid := range m.ownedIDs() {
		if m.alloc.ShardAvailable(txid) == 0 {
			return true
		}
	}
	return false
}

func (m *ShardManager) ownedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.owned)
}

// FeedOnce adopts shards while hungry, bounded by adoptAttempts per call.
// Hunger is re-checked after every adoption (owned < MaxShards and available
// rows below LowWaterRows): an instance stops pulling shards the moment it is
// no longer hungry, one shard at a time, so a single instance cannot hoard
// shards up to MaxShards when another instance may also need one. It shares
// adoptSem with EnsureRows so the two never adopt concurrently; it blocks on
// ctx (no separate timeout) to get its turn.
//
// When hungry at MaxShards it runs one rotateDrained pass first (at most once
// per call) and adopts only into the slots that pass freed.
func (m *ShardManager) FeedOnce(ctx context.Context) error {
	select {
	case m.adoptSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-m.adoptSem }()
	rotated := false
	for i := 0; i < adoptAttempts && m.isHungry(); i++ {
		if m.ownedCount() >= m.cfg.MaxShards {
			if rotated {
				return nil
			}
			rotated = true
			if released, refilled := m.rotateDrained(ctx, false); refilled || released == 0 {
				return nil
			}
		}
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
// empty. It serializes with FeedOnce (and other concurrent EnsureRows calls)
// on adoptSem, bounded by AdoptTimeout: a concurrent in-progress adoption
// (typically the background Run loop's own hunger-driven pull) is waited for
// instead of causing a spurious exhaustion, and concurrent callers cannot
// together push the owned shard count past MaxShards. Returns true if rows
// are available once it returns.
//
// At MaxShards with no rows in memory it does not give up: one rotateDrained
// pass rescans up to rotateMax drained shards, keeping any whose rows came
// back and releasing the ones the store shows empty, and the freed slots are
// then refilled by adoption while in-memory rows stay below LowWaterRows (so
// a rotation of two shards adopts up to two, not just the first with rows).
// It all runs under adoptSem; its ListAvailable and AcquireShard calls are
// bounded by AdoptTimeout. The rotated-out shards' ReleaseShard calls run in
// the background (each bounded separately at 1 s), so a Reserve is never
// charged for them.
func (m *ShardManager) EnsureRows(ctx context.Context) bool {
	if m.alloc.Available() > 0 {
		return true
	}
	ctx, cancel := context.WithTimeout(ctx, m.cfg.AdoptTimeout)
	defer cancel()
	select {
	case m.adoptSem <- struct{}{}:
	case <-ctx.Done():
		return m.alloc.Available() > 0
	}
	defer func() { <-m.adoptSem }()
	rotated := false // a rotation freed slots: keep filling them while below LowWaterRows
	for i := 0; i < adoptAttempts; i++ {
		avail := m.alloc.Available()
		if avail > 0 && (!rotated || avail >= m.cfg.LowWaterRows) {
			break
		}
		if m.ownedCount() >= m.cfg.MaxShards {
			if rotated || avail > 0 {
				break
			}
			released, refilled := m.rotateDrained(ctx, true)
			if refilled || released == 0 {
				break
			}
			rotated = true
		}
		if ok, err := m.AdoptOnce(ctx); err != nil || !ok {
			break
		}
	}
	return m.alloc.Available() > 0
}

// rotateDrained is the "hungry at MaxShards" path shared by EnsureRows and
// FeedOnce; callers hold adoptSem. It takes up to rotateMax owned shards whose
// allocator FIFO is empty, oldest first, and rescans each once via
// ListAvailable:
//   - rows the allocator does not already track came back (released by a
//     client or another instance): they are added, the shard is kept, and
//     rotation stops there (refilled is true);
//   - the store lists rows but all are already tracked (popped, in flight
//     through this instance's coalescer, not yet reserved in the store): the
//     shard is neither refilled nor released; rotation moves on to the next
//     candidate, and a later call sees it empty once those flushes land;
//   - the store lists zero available rows: the shard has nothing to give
//     now. It is dropped at once with reason "empty"
//     (lease released, excluded from re-adoption for emptyExclusion), without
//     waiting for RescanOnce's emptyRescansMax consecutive ticks, which exist
//     only to avoid churn when the instance is not starved.
//
// With async (the request path) the lease release itself runs in the
// background (see dropAsync); the shard is forgotten and excluded at once
// either way.
//
// Rows released back to the store after their shard was rotated out (by a
// client through this instance, see NoteReleased, or through another one)
// are not served by this instance until the shard is re-adopted after the
// emptyExclusion window; any other instance may adopt it in the meantime.
//
// Releasing is always safe: adoption never writes fuel rows and G1 rests on
// the store's row-level CAS, not on leases. Rows of a released shard can
// still be in flight through this instance's coalescer (their CAS applied
// but Reserve has not yet called Forget, or a concurrent Reserve popped rows
// NoteReleased re-added after the listing). They are handled exactly as on
// the not_owner drop path (drop -> Allocator.RemoveShard): they stay tracked
// as inflight until the pending CAS resolves them, Forget untracks them if it
// applied, and Return untracks rather than requeues them since the shard is
// gone; the store's CAS decides every outcome. Whoever adopts the shard later (us after the exclusion window,
// or another instance) reloads whatever the store then shows available.
//
// It returns how many shards it released (slots now free for AdoptOnce) and
// whether a rescan re-added rows.
func (m *ShardManager) rotateDrained(ctx context.Context, async bool) (released int, refilled bool) {
	var drained []chainhash.Hash
	for _, sh := range m.alloc.Shards() { // oldest first
		if len(drained) == rotateMax {
			break
		}
		m.mu.Lock()
		_, mine := m.owned[sh.TxID]
		m.mu.Unlock()
		if mine && m.alloc.ShardAvailable(sh.TxID) == 0 {
			drained = append(drained, sh.TxID)
		}
	}
	for _, txid := range drained {
		added, listed, err := m.loadRowsCounting(ctx, txid)
		if err != nil {
			if ctx.Err() != nil {
				return released, refilled
			}
			m.log.Warn("rotate: rescan drained shard", "txid", txid.String(), "err", err)
			continue
		}
		m.mu.Lock()
		st := m.owned[txid]
		if st == nil { // dropped concurrently (e.g. by RenewOnce)
			m.mu.Unlock()
			continue
		}
		if added > 0 || m.alloc.ShardAvailable(txid) > 0 {
			st.emptyRescans = 0
			m.mu.Unlock()
			return released, true
		}
		m.mu.Unlock()
		if listed > 0 { // only rows already in flight through us: not empty in the store
			continue
		}
		m.log.Debug("rotating out drained shard", "txid", txid.String())
		var dropped bool
		if async {
			dropped = m.dropAsync(ctx, txid, "empty")
		} else {
			dropped = m.drop(ctx, txid, true, "empty")
		}
		if dropped {
			released++
		}
	}
	return released, false
}

// excludeList returns the currently-excluded (recently drained) shard ids and
// lazily expires entries older than emptyExclusion, keeping recentOrder in
// sync with recentlyEmpty.
func (m *ShardManager) excludeList(now time.Time) []chainhash.Hash {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ex []chainhash.Hash
	kept := m.recentOrder[:0]
	for _, txid := range m.recentOrder {
		at, ok := m.recentlyEmpty[txid]
		if !ok {
			continue
		}
		if now.Sub(at) > emptyExclusion {
			delete(m.recentlyEmpty, txid)
			continue
		}
		kept = append(kept, txid)
		ex = append(ex, txid)
	}
	m.recentOrder = kept
	for txid := range m.releasing {
		if _, dup := m.recentlyEmpty[txid]; !dup {
			ex = append(ex, txid)
		}
	}
	return ex
}

// markRecentlyEmpty records txid as drained just now, evicting the oldest
// exclusion entry once the set would exceed maxRecentlyEmpty. Callers must
// hold m.mu.
func (m *ShardManager) markRecentlyEmpty(txid chainhash.Hash, at time.Time) {
	if _, exists := m.recentlyEmpty[txid]; !exists {
		m.recentOrder = append(m.recentOrder, txid)
	}
	m.recentlyEmpty[txid] = at
	for len(m.recentlyEmpty) > maxRecentlyEmpty && len(m.recentOrder) > 0 {
		oldest := m.recentOrder[0]
		m.recentOrder = m.recentOrder[1:]
		delete(m.recentlyEmpty, oldest)
	}
}

// AdoptOnce tries to acquire one adoptable shard and load its rows.
//
// AcquireShard may hand back a shard this instance already tracks in owned:
// our own lease lapsed (e.g. renewals failed for over a TTL but under two)
// and then became free again before any other holder took it. That case is
// detected under mu before touching the allocator: the shard's lease/head are
// refreshed in place and rows are neither reloaded nor dropped, since
// Allocator.AddShard would otherwise no-op on the duplicate txid and the
// zero-rows-loaded path below would wrongly treat a shard we already have
// rows for as empty and release it.
//
// It returns false when nothing was adoptable, the manager is shutting down
// (the shard is released immediately without ever being tracked), or the
// adopted shard turned out empty and was released again.
func (m *ShardManager) AdoptOnce(ctx context.Context) (bool, error) {
	now := m.clock.Now()
	until := now.Add(m.cfg.LeaseTTL)
	opStart := time.Now()
	sh, err := m.st.AcquireShard(ctx, m.pc.Name, m.holder, now, until, m.serve, m.excludeList(now))
	m.acquireShardLatency.Observe(time.Since(opStart).Seconds())
	if err != nil {
		m.adoptionsError.Inc()
		return false, err
	}
	if sh == nil {
		m.adoptionsNone.Inc()
		return false, nil
	}

	m.mu.Lock()
	switch {
	case m.closed:
		m.mu.Unlock()
		// Never tracked in owned, so this is not a "drop": nothing to count in
		// shard_drops_total, and shard_adoptions_total counts only a real
		// adoption (below, once the shard is confirmed kept), not this
		// declined-because-closing path.
		m.releaseLease(ctx, sh.TxID)
		return false, nil
	case m.owned[sh.TxID] != nil:
		// Re-acquiring a shard we already track (our lease lapsed and then
		// freed up again before anyone else took it): not a new adoption.
		st := m.owned[sh.TxID]
		st.leaseUntil = until
		st.head = store.ShardHead{Status: sh.Status, Version: sh.Version}
		m.mu.Unlock()
		m.alloc.UpdateHead(sh.TxID, sh.Status, sh.Version)
		return true, nil
	default:
		m.owned[sh.TxID] = &ownedState{head: store.ShardHead{Status: sh.Status, Version: sh.Version}, leaseUntil: until}
		m.mu.Unlock()
	}

	m.alloc.AddShard(OwnedShard{TxID: sh.TxID, Status: sh.Status, Version: sh.Version, CreatedAt: sh.CreatedAt})
	loaded, err := m.loadRows(ctx, sh.TxID)
	if err != nil {
		m.adoptionsOK.Inc() // kept the shard; rescan will retry loading
		return true, err
	}
	if loaded == 0 {
		// Adopted but immediately empty: not counted as a real adoption.
		m.drop(ctx, sh.TxID, true, "empty")
		return false, nil
	}
	m.adoptionsOK.Inc()
	m.log.Debug("adopted shard", "txid", sh.TxID.String(), "rows", loaded)
	return true, nil
}

// loadRows pages ListAvailable into the allocator and returns how many rows
// were newly added.
func (m *ShardManager) loadRows(ctx context.Context, txid chainhash.Hash) (int, error) {
	added, _, err := m.loadRowsCounting(ctx, txid)
	return added, err
}

// loadRowsCounting is loadRows that also reports how many rows the store
// listed as available, including ones the allocator already tracks (queued,
// or popped and still in flight through the coalescer) and so did not re-add.
func (m *ShardManager) loadRowsCounting(ctx context.Context, txid chainhash.Hash) (added, listed int, err error) {
	var from uint32
	for {
		opStart := time.Now()
		rows, err := m.st.ListAvailable(ctx, m.pc.Name, txid, from, listPage)
		m.listAvailableLatency.Observe(time.Since(opStart).Seconds())
		if err != nil {
			return added, listed, err
		}
		listed += len(rows)
		added += m.alloc.Add(txid, rows)
		if len(rows) < listPage {
			return added, listed, nil
		}
		from = rows[len(rows)-1].Vout + 1
	}
}

// releaseLease clears our lease on txid, bounded independently of ctx's own
// remaining budget so an expired Reserve/adopt context cannot leave a lease
// dangling past the point we decided to give it up.
func (m *ShardManager) releaseLease(ctx context.Context, txid chainhash.Hash) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	opStart := time.Now()
	err := m.st.ReleaseShard(rctx, m.pc.Name, txid, m.holder)
	m.releaseShardLatency.Observe(time.Since(opStart).Seconds())
	if err != nil && !errors.Is(err, store.ErrNotFound) && !errors.Is(err, store.ErrNotOwner) {
		m.log.Warn("release shard", "txid", txid.String(), "err", err)
	}
}

// drop forgets a shard locally and, when release is true, releases its lease
// and excludes it from re-adoption for emptyExclusion. reason feeds
// shard_drops_total (e.g. "empty", "not_owner", "stale", "retired"). It
// returns false, doing nothing, when the shard is no longer owned (another
// path dropped it first), so a shard is never dropped or counted twice.
func (m *ShardManager) drop(ctx context.Context, txid chainhash.Hash, release bool, reason string) bool {
	if ok, _ := m.forget(txid, release, false, reason); !ok {
		return false
	}
	if release {
		m.releaseLease(ctx, txid)
	}
	return true
}

// dropAsync is drop with release=true whose ReleaseShard call runs in a
// background goroutine (bounded at 1 s like releaseLease; Run waits for it
// before returning). Until that release completes the shard is kept out of
// AcquireShard via the releasing set, so a late release can never clear a
// lease this instance has meanwhile re-acquired. The background release is
// registered (bg.Add, releasing insert) inside forget's critical section and
// only while the manager is not closed: ReleaseAll sets closed under the same
// mutex before Run calls bg.Wait, so no Add can race that Wait. Once closed,
// the release runs synchronously here instead.
func (m *ShardManager) dropAsync(ctx context.Context, txid chainhash.Hash, reason string) bool {
	ok, background := m.forget(txid, true, true, reason)
	if !ok {
		return false
	}
	if !background {
		m.releaseLease(ctx, txid)
		return true
	}
	go func() {
		defer m.bg.Done()
		m.releaseLease(context.WithoutCancel(ctx), txid)
		m.mu.Lock()
		delete(m.releasing, txid)
		m.mu.Unlock()
	}()
	return true
}

// forget is drop's local half: check-and-delete txid from owned under mu
// (ok false if it was already gone), count the drop, record the exclusion
// when release is true, and remove the shard from the allocator. With
// backgroundRelease it also, in the same critical section and only if the
// manager is not closed, registers a background lease release (bg.Add and
// the releasing insert) and reports that the caller must start it.
func (m *ShardManager) forget(txid chainhash.Hash, release, backgroundRelease bool, reason string) (ok, background bool) {
	m.mu.Lock()
	if _, owned := m.owned[txid]; !owned {
		m.mu.Unlock()
		return false, false
	}
	delete(m.owned, txid)
	if release {
		m.markRecentlyEmpty(txid, m.clock.Now())
	}
	if release && backgroundRelease && !m.closed {
		m.releasing[txid] = struct{}{}
		m.bg.Add(1)
		background = true
	}
	m.mu.Unlock()
	if c, ok := m.dropCounters[reason]; ok {
		c.Inc()
	} else { // defensive: an unlisted reason still gets counted, just not cached
		shardDrops.WithLabelValues(m.pc.Name, reason).Inc()
	}
	m.alloc.RemoveShard(txid)
	return true, background
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
		opStart := time.Now()
		head, err := m.st.RenewShard(ctx, m.pc.Name, txid, m.holder, until)
		m.renewShardLatency.Observe(time.Since(opStart).Seconds())
		switch {
		case errors.Is(err, store.ErrNotOwner), errors.Is(err, store.ErrNotFound):
			m.log.Info("lost shard lease", "txid", txid.String())
			m.drop(ctx, txid, false, "not_owner")
			continue
		case err != nil:
			m.renewFailures.Inc()
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
				m.drop(ctx, txid, false, "stale")
			}
			continue
		}
		if !m.servable(head.Status) {
			m.log.Info("shard no longer servable", "txid", txid.String(), "status", head.Status.String())
			m.drop(ctx, txid, true, "retired")
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
// empty for emptyRescansMax consecutive scans. Requiring several consecutive
// empty scans avoids churn while the instance is not starved; a drained shard
// that blocks adoption at MaxShards is instead rotated out at once by
// rotateDrained.
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
			m.drop(ctx, txid, true, "empty")
		}
	}
	return firstErr
}

// rescanFlagged renews and reloads only shards flagged by NoteCASMiss, ahead
// of their regular RescanInterval turn. A renew failure with ErrNotOwner or
// ErrNotFound means another holder already has the shard: it is dropped at
// once instead of waiting for the next RenewOnce tick.
func (m *ShardManager) rescanFlagged(ctx context.Context) error {
	m.mu.Lock()
	var flagged []chainhash.Hash
	for txid, st := range m.owned {
		if st.rescanNow {
			flagged = append(flagged, txid)
		}
	}
	m.mu.Unlock()
	if len(flagged) == 0 {
		return nil
	}
	now := m.clock.Now()
	until := now.Add(m.cfg.LeaseTTL)
	var firstErr error
	for _, txid := range flagged {
		opStart := time.Now()
		head, err := m.st.RenewShard(ctx, m.pc.Name, txid, m.holder, until)
		m.renewShardLatency.Observe(time.Since(opStart).Seconds())
		switch {
		case errors.Is(err, store.ErrNotOwner), errors.Is(err, store.ErrNotFound):
			m.log.Info("lost shard lease", "txid", txid.String())
			m.drop(ctx, txid, false, "not_owner")
			continue
		case err != nil:
			m.renewFailures.Inc()
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !m.servable(head.Status) {
			m.log.Info("shard no longer servable", "txid", txid.String(), "status", head.Status.String())
			m.drop(ctx, txid, true, "retired")
			continue
		}
		m.mu.Lock()
		if st := m.owned[txid]; st != nil {
			st.leaseUntil = until
			st.head = head
		}
		m.mu.Unlock()
		m.alloc.UpdateHead(txid, head.Status, head.Version)

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

// NoteReleased re-adds rows released through this instance when we own their
// shard. Rows of a shard already rotated out (or otherwise dropped) are not
// re-added: they stay available in the store for whoever adopts the shard,
// which is not this instance until its emptyExclusion window has passed.
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

// ReleaseAll releases every owned shard (graceful shutdown) and marks the
// manager closed so any AdoptOnce racing this call releases whatever it
// acquires instead of leaking a lease past shutdown.
func (m *ShardManager) ReleaseAll(ctx context.Context) error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	var firstErr error
	for _, txid := range m.ownedIDs() {
		m.dropCounters["closed"].Inc() // these are genuinely owned, unlike AdoptOnce's declined-while-closing path
		m.alloc.RemoveShard(txid)
		m.mu.Lock()
		delete(m.owned, txid)
		m.mu.Unlock()
		opStart := time.Now()
		err := m.st.ReleaseShard(ctx, m.pc.Name, txid, m.holder)
		m.releaseShardLatency.Observe(time.Since(opStart).Seconds())
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
			err := m.ReleaseAll(rctx)
			m.bg.Wait() // background lease releases from request-path rotation
			return err
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
