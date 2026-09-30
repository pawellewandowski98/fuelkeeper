package pool_test

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func newMgr(t *testing.T, st store.Store, clk *storetest.Clock, cfg pool.InstanceConfig, pc pool.Config) (*pool.ShardManager, *pool.Allocator) {
	t.Helper()
	alloc := pool.NewAllocator()
	m := pool.NewShardManager(st, pc, cfg, alloc, pool.ClockFunc(clk.Now), slog.Default())
	return m, alloc
}

func testInstanceConfig() pool.InstanceConfig {
	c := pool.DefaultInstanceConfig()
	c.InstanceID = "test"
	c.MaxShards = 2
	c.LowWaterRows = 3
	return c
}

func TestShardManagerAdoptsOldestMinedAndLoadsRows(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	unmined, _ := storetest.MintShard(t, st, "main", 5, store.ShardUnmined)
	clk.Advance(time.Second)
	older, _ := storetest.MintShard(t, st, "main", 5, store.ShardMined)
	clk.Advance(time.Second)
	newer, _ := storetest.MintShard(t, st, "main", 5, store.ShardMined)
	m, alloc := newMgr(t, st, clk, testInstanceConfig(), pc)

	adopted, err := m.AdoptOnce(ctx)
	require.NoError(t, err)
	assert.True(t, adopted)
	assert.True(t, alloc.Has(older.TxID), "mined_only skips the unmined shard; oldest mined first")
	assert.False(t, alloc.Has(unmined.TxID))
	assert.False(t, alloc.Has(newer.TxID))
	assert.Equal(t, 5, alloc.Available())

	sh, err := st.GetShard(ctx, "main", older.TxID)
	require.NoError(t, err)
	assert.Equal(t, m.Holder(), sh.Owner)
	assert.Equal(t, clk.Now().Add(testInstanceConfig().LeaseTTL), sh.LeaseUntil.UTC())

	// Hungry: below LowWaterRows after popping, and under MaxShards → adopts the next.
	alloc.Pop(3)
	require.NoError(t, m.FeedOnce(ctx))
	assert.True(t, alloc.Has(newer.TxID))
	assert.Len(t, alloc.Shards(), 2)
	// At MaxShards nothing more is adopted even when hungry.
	alloc.Pop(7)
	require.NoError(t, m.FeedOnce(ctx))
	assert.Len(t, alloc.Shards(), 2)
}

// TestShardManagerFeedOnceStopsWhenNoLongerHungry pins the fairness semantics
// against a future regression: FeedOnce adopts one shard at a time and stops
// as soon as the instance is no longer hungry (available rows >=
// LowWaterRows), even when MaxShards has headroom and another adoptable
// shard exists. Filling up to MaxShards on a single hunger event would let
// one instance hoard shards that other instances need.
func TestShardManagerFeedOnceStopsWhenNoLongerHungry(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	first, _ := storetest.MintShard(t, st, "main", 5, store.ShardMined)
	clk.Advance(time.Second)
	second, _ := storetest.MintShard(t, st, "main", 5, store.ShardMined)
	cfg := testInstanceConfig() // MaxShards=2, LowWaterRows=3
	m, alloc := newMgr(t, st, clk, cfg, pc)

	require.NoError(t, m.FeedOnce(ctx))
	assert.True(t, alloc.Has(first.TxID), "oldest mined shard adopted first")
	assert.False(t, alloc.Has(second.TxID), "no longer hungry after the first 5-row shard clears LowWaterRows=3; MaxShards headroom is not used up-front")
	assert.Len(t, alloc.Shards(), 1)
	assert.Equal(t, 5, alloc.Available())
}

func TestShardManagerRenewDropsLostAndRetiredShards(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	a, _ := storetest.MintShard(t, st, "main", 3, store.ShardMined)
	clk.Advance(time.Second)
	b, _ := storetest.MintShard(t, st, "main", 3, store.ShardMined)
	cfg := testInstanceConfig()
	cfg.LowWaterRows = 10 // stay hungry past the first 3-row adopt so one FeedOnce fills both shards
	m, alloc := newMgr(t, st, clk, cfg, pc)
	require.NoError(t, m.FeedOnce(ctx))
	require.Len(t, alloc.Shards(), 2)

	// Renewal extends the lease while it is held.
	clk.Advance(cfg.LeaseTTL / 2)
	require.NoError(t, m.RenewOnce(ctx))
	sh, err := st.GetShard(ctx, "main", a.TxID)
	require.NoError(t, err)
	assert.Equal(t, clk.Now().Add(cfg.LeaseTTL), sh.LeaseUntil.UTC(), "renewed")

	// Another instance steals shard a after the lease expires.
	clk.Advance(cfg.LeaseTTL + time.Second)
	got, err := st.AcquireShard(ctx, "main", "other/1", clk.Now(), clk.Now().Add(time.Minute), []store.ShardStatus{store.ShardMined}, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, a.TxID, got.TxID)

	// Shard b gets retired by the janitor.
	_, err = st.RetireShard(ctx, "main", b.TxID)
	require.NoError(t, err)

	require.NoError(t, m.RenewOnce(ctx))
	assert.False(t, alloc.Has(a.TxID), "lost lease → dropped")
	assert.False(t, alloc.Has(b.TxID), "invalid status → dropped")
	assert.Equal(t, 0, alloc.Available())
}

func TestShardManagerRescanPicksUpReleasedRowsAndDropsEmptyShards(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	sh, rows := storetest.MintShard(t, st, "main", 4, store.ShardMined)
	m, alloc := newMgr(t, st, clk, testInstanceConfig(), pc)
	require.NoError(t, m.FeedOnce(ctx))

	popped := alloc.Pop(4)
	require.Len(t, popped, 4)
	// Reserve them "for real" under a client token, then forget them from the allocator.
	tok := store.NewToken()
	storetest.MustReserve(t, st, "main", popped, tok, clk.Now().Add(time.Minute))
	alloc.Forget(storetest.Ops(popped))

	// A different instance releases two rows: the owner only learns via rescan.
	n, err := st.Release(ctx, "main", tok, storetest.Ops(rows[:2]))
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.NoError(t, m.RescanOnce(ctx))
	assert.Equal(t, 2, alloc.Available())

	// Drain: pop the two rows, reserve them in the store under a second token
	// (as a real Reserve would), forget them locally; the shard then stays empty
	// for two rescans → dropped locally and released in the store.
	drained := alloc.Pop(2)
	require.Len(t, drained, 2)
	tok2 := store.NewToken()
	storetest.MustReserve(t, st, "main", drained, tok2, clk.Now().Add(time.Minute))
	alloc.Forget(storetest.Ops(drained))
	require.NoError(t, m.RescanOnce(ctx))
	assert.True(t, alloc.Has(sh.TxID), "one empty rescan is not enough")
	require.NoError(t, m.RescanOnce(ctx))
	assert.False(t, alloc.Has(sh.TxID))
	got, err := st.GetShard(ctx, "main", sh.TxID)
	require.NoError(t, err)
	assert.Empty(t, got.Owner, "empty shard released for others")

	// Rows come back (the other two are released) but a recently-drained shard is
	// excluded from immediate re-adoption...
	_, err = st.Release(ctx, "main", tok, storetest.Ops(rows[2:]))
	require.NoError(t, err)
	adopted, err := m.AdoptOnce(ctx)
	require.NoError(t, err)
	assert.False(t, adopted, "excluded for the exclusion window even though rows are available again")
	// ...until the exclusion window passes.
	clk.Advance(2 * time.Minute)
	adopted, err = m.AdoptOnce(ctx)
	require.NoError(t, err)
	assert.True(t, adopted)
	assert.Equal(t, 2, alloc.Available())
}

func TestShardManagerNoteReleasedAndCASMiss(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	sh, rows := storetest.MintShard(t, st, "main", 3, store.ShardMined)
	m, alloc := newMgr(t, st, clk, testInstanceConfig(), pc)
	require.NoError(t, m.FeedOnce(ctx))
	popped := alloc.Pop(3)
	require.Len(t, popped, 3)
	tok := store.NewToken()
	storetest.MustReserve(t, st, "main", popped, tok, clk.Now().Add(time.Minute))
	alloc.Forget(storetest.Ops(popped))
	assert.Equal(t, 0, alloc.Available())

	// Local fast path: a Release handled by this instance re-adds rows immediately.
	n, err := st.Release(ctx, "main", tok, storetest.Ops(rows[:1]))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	m.NoteReleased(rows[:1])
	assert.Equal(t, 1, alloc.Available())
	m.NoteReleased([]store.Row{{Outpoint: store.Outpoint{TxID: storetest.NewTxID()}}})
	assert.Equal(t, 1, alloc.Available(), "rows of shards we do not own are ignored")

	// A CAS miss flags the shard; the next RescanOnce refreshes it even off-interval.
	m.NoteCASMiss(sh.TxID)
	require.NoError(t, m.RescanOnce(ctx))
	assert.Equal(t, 1, alloc.Available(), "rows[0] is in memory (known); the other two are reserved in the store")

	require.NoError(t, m.ReleaseAll(ctx))
	got, err := st.GetShard(ctx, "main", sh.TxID)
	require.NoError(t, err)
	assert.Empty(t, got.Owner)
	assert.Empty(t, alloc.Shards())
}

func TestShardManagerEnsureRowsBounded(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	m, alloc := newMgr(t, st, clk, testInstanceConfig(), pc)
	start := time.Now()
	assert.False(t, m.EnsureRows(ctx), "nothing to adopt")
	assert.Less(t, time.Since(start), time.Second)
	storetest.MintShard(t, st, "main", 2, store.ShardMined)
	assert.True(t, m.EnsureRows(ctx))
	assert.Equal(t, 2, alloc.Available())
}

func TestShardManagerRunRenewsAndReleasesOnStop(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	sh, _ := storetest.MintShard(t, st, "main", 2, store.ShardMined)
	cfg := testInstanceConfig()
	cfg.LeaseTTL = 300 * time.Millisecond
	cfg.RescanInterval = 50 * time.Millisecond
	m, alloc := newMgr(t, st, clk, cfg, pc)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	require.Eventually(t, func() bool { return alloc.Has(sh.TxID) }, 2*time.Second, 5*time.Millisecond, "Run adopts on start")
	cancel()
	require.NoError(t, <-done)
	got, err := st.GetShard(context.Background(), "main", sh.TxID)
	require.NoError(t, err)
	assert.Empty(t, got.Owner, "shards released on shutdown")
}

// blockingListAvailable wraps a store.Store and blocks the first
// ListAvailable call on a channel, closing started right before blocking so a
// test can synchronize with "the call has been reached" before unblocking it.
type blockingListAvailable struct {
	store.Store
	once    sync.Once
	started chan struct{}
	block   chan struct{}
}

func (w *blockingListAvailable) ListAvailable(ctx context.Context, pool string, txid chainhash.Hash, fromVout uint32, limit int) ([]store.Row, error) {
	w.once.Do(func() {
		close(w.started)
		<-w.block
	})
	return w.Store.ListAvailable(ctx, pool, txid, fromVout, limit)
}

// TestShardManagerEnsureRowsWaitsForConcurrentLoad reproduces the production
// race behind a flaky TestPoolReserveSkipsRowsLostToCAS run: the background
// Run loop's FeedOnce can acquire the only adoptable shard and be mid-loadRows
// when a concurrent EnsureRows (from a Reserve call) finds Available()==0 and
// its own AdoptOnce sees nothing free (the shard is already ours). Without
// serialization EnsureRows returned false immediately, a spurious exhaustion.
// adoptSem fixes this: EnsureRows blocks for FeedOnce's turn to finish instead.
func TestShardManagerEnsureRowsWaitsForConcurrentLoad(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	storetest.MintShard(t, base, "main", 3, store.ShardMined)
	wrapped := &blockingListAvailable{Store: base, started: make(chan struct{}), block: make(chan struct{})}
	pc := pool.DefaultPoolConfig("main", 50)
	m, alloc := newMgr(t, wrapped, clk, testInstanceConfig(), pc)

	go func() { _ = m.FeedOnce(ctx) }()
	select {
	case <-wrapped.started:
	case <-time.After(2 * time.Second):
		t.Fatal("background FeedOnce never reached ListAvailable")
	}
	assert.Equal(t, 0, alloc.Available(), "shard owned but its rows are not loaded yet")

	go func() {
		time.Sleep(20 * time.Millisecond)
		close(wrapped.block)
	}()
	assert.True(t, m.EnsureRows(ctx), "EnsureRows should wait for the in-progress load instead of reporting exhaustion")
	assert.Equal(t, 3, alloc.Available())

	// With nothing adoptable at all, EnsureRows returns false promptly (well
	// under AdoptTimeout), not after spinning for the whole budget.
	emptySt := memory.New(memory.WithClock(clk.Now))
	m2, _ := newMgr(t, emptySt, clk, testInstanceConfig(), pc)
	start := time.Now()
	assert.False(t, m2.EnsureRows(ctx))
	assert.Less(t, time.Since(start), 50*time.Millisecond)
}

// latencyAcquireStore adds random latency to AcquireShard, widening the
// window in which concurrent adopters could race if they were not serialized.
type latencyAcquireStore struct {
	store.Store
	minDelay, maxDelay time.Duration
}

func (w *latencyAcquireStore) AcquireShard(ctx context.Context, pool, holder string, now, until time.Time, statuses []store.ShardStatus, exclude []chainhash.Hash) (*store.Shard, error) {
	d := w.minDelay
	if w.maxDelay > w.minDelay {
		d += time.Duration(rand.Int63n(int64(w.maxDelay - w.minDelay))) //nolint:gosec // test jitter, not a security context
	}
	select {
	case <-time.After(d):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return w.Store.AcquireShard(ctx, pool, holder, now, until, statuses, exclude)
}

// TestShardManagerEnsureRowsConcurrentBoundedByMaxShards drives 16 concurrent
// EnsureRows calls against a store with latency on AcquireShard (widening the
// check-then-act window a missing lock would race on) and asserts the
// instance never ends up owning more than MaxShards shards, in the allocator
// or in the store.
func TestShardManagerEnsureRowsConcurrentBoundedByMaxShards(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	for i := 0; i < 6; i++ {
		storetest.MintShard(t, base, "main", 3, store.ShardMined)
		clk.Advance(time.Millisecond)
	}
	wrapped := &latencyAcquireStore{Store: base, minDelay: 5 * time.Millisecond, maxDelay: 20 * time.Millisecond}
	cfg := testInstanceConfig() // MaxShards=2
	m, alloc := newMgr(t, wrapped, clk, cfg, pc)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.EnsureRows(ctx)
		}()
	}
	wg.Wait()

	assert.LessOrEqual(t, len(alloc.Shards()), cfg.MaxShards, "allocator must not hoard past MaxShards")
	holder := m.Holder()
	owned, err := base.ListShards(ctx, "main", store.ShardFilter{Owner: &holder})
	require.NoError(t, err)
	assert.LessOrEqual(t, len(owned), cfg.MaxShards, "store must not show more leased shards than MaxShards")
}

// releaseCounterStore counts ReleaseShard calls.
type releaseCounterStore struct {
	store.Store
	releases atomic.Int32
}

func (w *releaseCounterStore) ReleaseShard(ctx context.Context, pool string, txid chainhash.Hash, holder string) error {
	w.releases.Add(1)
	return w.Store.ReleaseShard(ctx, pool, txid, holder)
}

// TestShardManagerAdoptOnceReacquiresOwnExpiredLease covers re-acquiring a
// shard this instance already tracks: its own lease lapsed without being
// renewed, then AcquireShard hands it right back since nobody else took it.
// AddShard would otherwise silently no-op on the duplicate txid, loadRows
// would report 0 new rows, and the old code would wrongly drop-and-release a
// shard whose rows are still sitting in the allocator.
func TestShardManagerAdoptOnceReacquiresOwnExpiredLease(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	wrapped := &releaseCounterStore{Store: base}
	pc := pool.DefaultPoolConfig("main", 50)
	sh, _ := storetest.MintShard(t, base, "main", 3, store.ShardMined)
	cfg := testInstanceConfig()
	m, alloc := newMgr(t, wrapped, clk, cfg, pc)

	adopted, err := m.AdoptOnce(ctx)
	require.NoError(t, err)
	require.True(t, adopted)
	require.Equal(t, 3, alloc.Available())

	clk.Advance(cfg.LeaseTTL + time.Second) // lease lapses; nobody renews it

	adopted, err = m.AdoptOnce(ctx)
	require.NoError(t, err)
	assert.True(t, adopted, "re-acquiring our own shard keeps it")
	assert.True(t, alloc.Has(sh.TxID))
	assert.Equal(t, 3, alloc.Available(), "rows were neither reloaded nor dropped")
	assert.Equal(t, int32(0), wrapped.releases.Load(), "no ReleaseShard call for a shard we still hold")

	got, err := base.GetShard(ctx, "main", sh.TxID)
	require.NoError(t, err)
	assert.Equal(t, m.Holder(), got.Owner, "still ours")
	assert.True(t, got.LeaseUntil.UTC().Equal(clk.Now().Add(cfg.LeaseTTL)), "lease refreshed")
}

// TestShardManagerRunDropsStolenShardOnCASMissBeforeRenewTick pins that
// rescanFlagged (triggered by NoteCASMiss through Run's hunger channel) drops
// a stolen shard immediately via RenewShard, without waiting for the next
// renew tick: LeaseTTL and RescanInterval are both set to 1h so neither
// ticker can fire during the test.
func TestShardManagerRunDropsStolenShardOnCASMissBeforeRenewTick(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	sh, _ := storetest.MintShard(t, st, "main", 2, store.ShardMined)
	cfg := testInstanceConfig()
	cfg.LeaseTTL = time.Hour
	cfg.RescanInterval = time.Hour
	// One shard only: with MaxShards 2 and a 2-row shard the manager stays
	// hungry after adopting and could re-adopt the shard the test releases
	// before "other/1" takes it (seen under heavy CPU load).
	cfg.MaxShards = 1
	m, alloc := newMgr(t, st, clk, cfg, pc)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	require.Eventually(t, func() bool { return alloc.Has(sh.TxID) }, 2*time.Second, 5*time.Millisecond)

	// Force the shard free from under us and let another holder take it,
	// simulating a stolen lease (the renew/rescan tickers never fire here).
	require.NoError(t, st.ReleaseShard(context.Background(), "main", sh.TxID, m.Holder()))
	got, err := st.AcquireShard(context.Background(), "main", "other/1", clk.Now(), clk.Now().Add(time.Minute), []store.ShardStatus{store.ShardMined}, nil)
	require.NoError(t, err)
	require.NotNil(t, got)

	m.NoteCASMiss(sh.TxID)
	require.Eventually(t, func() bool { return !alloc.Has(sh.TxID) }, 2*time.Second, 5*time.Millisecond,
		"Run drops a stolen shard via rescanFlagged, not waiting for the renew tick")

	cancel()
	require.NoError(t, <-done)
}

// flakyRenewStore forces RenewShard to fail while armed.
type flakyRenewStore struct {
	store.Store
	failRenew bool
}

func (w *flakyRenewStore) RenewShard(ctx context.Context, pool string, txid chainhash.Hash, holder string, until time.Time) (store.ShardHead, error) {
	if w.failRenew {
		return store.ShardHead{}, store.ErrUnavailable
	}
	return w.Store.RenewShard(ctx, pool, txid, holder, until)
}

// TestShardManagerRenewOnceDropsShardWithStaleLeaseOnPersistentError pins the
// "expired for more than one full TTL" staleness guard: a single transient
// RenewShard failure is tolerated, but once the lease has been stale for over
// one TTL, RenewOnce drops the shard locally without releasing it (it is not
// ours to release; another holder may already have it).
func TestShardManagerRenewOnceDropsShardWithStaleLeaseOnPersistentError(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	sh, _ := storetest.MintShard(t, base, "main", 2, store.ShardMined)
	wrapped := &flakyRenewStore{Store: base}
	cfg := testInstanceConfig()
	m, alloc := newMgr(t, wrapped, clk, cfg, pc)
	require.NoError(t, m.FeedOnce(ctx))
	require.True(t, alloc.Has(sh.TxID))

	wrapped.failRenew = true
	clk.Advance(cfg.LeaseTTL) // now == leaseUntil: expired, but not yet stale by a full extra TTL
	require.Error(t, m.RenewOnce(ctx))
	assert.True(t, alloc.Has(sh.TxID), "kept across a single transient renew failure")

	clk.Advance(cfg.LeaseTTL + time.Second) // now exceeds leaseUntil by more than one full TTL
	require.Error(t, m.RenewOnce(ctx))
	assert.False(t, alloc.Has(sh.TxID), "dropped once the lease has been stale for over one TTL")

	got, err := base.GetShard(ctx, "main", sh.TxID)
	require.NoError(t, err)
	assert.Equal(t, m.Holder(), got.Owner, "not released: RenewOnce must not call ReleaseShard for a merely-lost shard")
}

// TestShardManagerAdoptOnceReleasesEmptyShard pins that a shard whose rows
// were all reserved before adoption is dropped and released rather than kept
// as a permanently empty owned shard.
func TestShardManagerAdoptOnceReleasesEmptyShard(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	sh, rows := storetest.MintShard(t, st, "main", 3, store.ShardMined)
	storetest.MustReserve(t, st, "main", rows, store.NewToken(), clk.Now().Add(time.Minute))
	m, alloc := newMgr(t, st, clk, testInstanceConfig(), pc)

	adopted, err := m.AdoptOnce(ctx)
	require.NoError(t, err)
	assert.False(t, adopted)
	assert.False(t, alloc.Has(sh.TxID))
	got, err := st.GetShard(ctx, "main", sh.TxID)
	require.NoError(t, err)
	assert.Empty(t, got.Owner)
}

// TestShardManagerRenewOnceDropsUnservableShardOnStatusChange pins the
// status-change drop path in RenewOnce through UpdateShard (not RetireShard,
// which clears the owner and would instead exercise the ErrNotOwner path).
func TestShardManagerRenewOnceDropsUnservableShardOnStatusChange(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50) // mined_only
	sh, _ := storetest.MintShard(t, st, "main", 2, store.ShardMined)
	m, alloc := newMgr(t, st, clk, testInstanceConfig(), pc)
	require.NoError(t, m.FeedOnce(ctx))
	require.True(t, alloc.Has(sh.TxID))

	require.NoError(t, st.UpdateShard(ctx, "main", sh.TxID, store.ShardUnmined, nil))
	require.NoError(t, m.RenewOnce(ctx))
	assert.False(t, alloc.Has(sh.TxID), "no longer mined; mined_only pool must drop it")
	got, err := st.GetShard(ctx, "main", sh.TxID)
	require.NoError(t, err)
	assert.Empty(t, got.Owner, "released back to the pool")
}

// mintN mints n shards of rows rows each, one clock millisecond apart so
// oldest-first adoption order is deterministic.
func mintN(t *testing.T, st store.Store, clk *storetest.Clock, poolName string, n, rows int) ([]store.Shard, [][]store.Row) {
	t.Helper()
	shards := make([]store.Shard, n)
	all := make([][]store.Row, n)
	for i := range n {
		shards[i], all[i] = storetest.MintShard(t, st, poolName, rows, store.ShardMined)
		clk.Advance(time.Millisecond)
	}
	return shards, all
}

// drainAll pops every queued row, reserves it store-side under one token (as
// a real Reserve would) and forgets it locally, leaving every owned shard's
// FIFO empty and the store with no available rows for those shards.
func drainAll(t *testing.T, st store.Store, clk *storetest.Clock, poolName string, alloc *pool.Allocator) (store.Token, []store.Row) {
	t.Helper()
	popped := alloc.Pop(alloc.Available())
	tok := store.NewToken()
	if len(popped) > 0 {
		storetest.MustReserve(t, st, poolName, popped, tok, clk.Now().Add(time.Hour))
		alloc.Forget(storetest.Ops(popped))
	}
	return tok, popped
}

// waitRotatedDrops waits until the background half of each request-path
// rotation drop of shards (retire check, drop count, lease release) is done.
func waitRotatedDrops(t *testing.T, m *pool.ShardManager, shards []store.Shard) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, sh := range shards {
			if m.ReleasePending(sh.TxID) {
				return false
			}
		}
		return true
	}, 2*time.Second, time.Millisecond, "background rotation drops finish")
}

func dropsEmpty(poolName string) float64 {
	return testutil.ToFloat64(pool.ShardDrops.WithLabelValues(poolName, "empty"))
}

func adoptionsOK(poolName string) float64 {
	return testutil.ToFloat64(pool.ShardAdoptions.WithLabelValues(poolName, "ok"))
}

// TestEnsureRowsRotatesDrainedShardsWithoutRescanTick pins the fix for the
// MaxShards throughput cliff: once every owned shard is drained, EnsureRows
// releases the drained shards and adopts fresh ones in the same call instead
// of reporting exhaustion until two RescanInterval ticks have passed.
func TestEnsureRowsRotatesDrainedShardsWithoutRescanTick(t *testing.T) {
	ctx := context.Background()
	const poolName = "rotate-ensure"
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	shards, rows := mintN(t, st, clk, poolName, 4, 10)
	cfg := testInstanceConfig() // MaxShards=2
	cfg.LowWaterRows = 100      // stay hungry so one FeedOnce owns both slots
	m, alloc := newMgr(t, st, clk, cfg, pc)
	require.NoError(t, m.FeedOnce(ctx))
	require.True(t, alloc.Has(shards[0].TxID))
	require.True(t, alloc.Has(shards[1].TxID))

	tok, _ := drainAll(t, st, clk, poolName, alloc)
	require.Equal(t, 0, alloc.Available())
	dropsBefore, adoptsBefore := dropsEmpty(poolName), adoptionsOK(poolName)

	// No clock advance, no RescanOnce: the request path alone must rotate.
	require.True(t, m.EnsureRows(ctx), "drained shards must be rotated out, not answered with exhaustion")
	waitRotatedDrops(t, m, shards[:2]) // the request path counts each drop once its retire check has run
	assert.InDelta(t, dropsBefore+2, dropsEmpty(poolName), 0, "both drained shards dropped as empty")
	assert.InDelta(t, adoptsBefore+2, adoptionsOK(poolName), 0, "both freed slots refilled while below LowWaterRows")
	assert.False(t, alloc.Has(shards[0].TxID))
	assert.False(t, alloc.Has(shards[1].TxID))
	assert.True(t, alloc.Has(shards[2].TxID))
	assert.True(t, alloc.Has(shards[3].TxID))
	require.Eventually(t, func() bool { // the request path releases leases in the background
		for _, sh := range shards[:2] {
			got, err := st.GetShard(ctx, poolName, sh.TxID)
			if err != nil || got.Owner != "" {
				return false
			}
		}
		return true
	}, 2*time.Second, 5*time.Millisecond, "released in the store")

	// Keep consuming: every time the FIFO runs dry EnsureRows must find rows
	// until all 40 are gone.
	consumed := 20
	for consumed < 40 {
		got := alloc.Pop(3)
		if len(got) == 0 {
			require.True(t, m.EnsureRows(ctx), "spurious exhaustion after %d of 40 rows", consumed)
			continue
		}
		storetest.MustReserve(t, st, poolName, got, store.NewToken(), clk.Now().Add(time.Hour))
		alloc.Forget(storetest.Ops(got))
		consumed += len(got)
	}
	assert.False(t, m.EnsureRows(ctx), "all 40 rows consumed: genuinely exhausted")

	// The rotated-out shards sit in the exclusion list: even with rows back in
	// the store they are not re-adopted inside the exclusion window.
	_, err := st.Release(ctx, poolName, tok, storetest.Ops(rows[0][:2]))
	require.NoError(t, err)
	adopted, err := m.AdoptOnce(ctx)
	require.NoError(t, err)
	assert.False(t, adopted, "rotated-out shard excluded from immediate re-adoption")
	assert.False(t, alloc.Has(shards[0].TxID))
}

// TestEnsureRowsKeepsShardWhoseRowsReturned pins that rotation rescans a
// drained shard before giving it up: rows released back to the store since
// the FIFO emptied are re-added and the shard is kept.
func TestEnsureRowsKeepsShardWhoseRowsReturned(t *testing.T) {
	ctx := context.Background()
	const poolName = "rotate-keep"
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	shards, _ := mintN(t, st, clk, poolName, 2, 5)
	cfg := testInstanceConfig()
	cfg.MaxShards = 1
	m, alloc := newMgr(t, st, clk, cfg, pc)
	require.NoError(t, m.FeedOnce(ctx))
	require.True(t, alloc.Has(shards[0].TxID))

	tok, popped := drainAll(t, st, clk, poolName, alloc)
	require.Len(t, popped, 5)
	n, err := st.Release(ctx, poolName, tok, storetest.Ops(popped[:3]))
	require.NoError(t, err)
	require.Equal(t, 3, n)
	dropsBefore := dropsEmpty(poolName)

	require.True(t, m.EnsureRows(ctx))
	assert.Equal(t, 3, alloc.Available(), "released rows picked up by the immediate rescan")
	assert.True(t, alloc.Has(shards[0].TxID), "shard with rows in the store is kept")
	assert.False(t, alloc.Has(shards[1].TxID), "no rotation needed")
	assert.InDelta(t, dropsBefore, dropsEmpty(poolName), 0)
	got, err := st.GetShard(ctx, poolName, shards[0].TxID)
	require.NoError(t, err)
	assert.Equal(t, m.Holder(), got.Owner)
}

// TestHungryAtMaxShardsTriggersBackgroundRotation pins that the background
// loop rotates too: with every owned shard drained at MaxShards, the next
// hunger signal releases and replaces them, with renew and rescan tickers
// set far beyond the test's duration.
func TestHungryAtMaxShardsTriggersBackgroundRotation(t *testing.T) {
	const poolName = "rotate-bg"
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	shards, _ := mintN(t, st, clk, poolName, 4, 10)
	cfg := testInstanceConfig() // MaxShards=2
	cfg.LowWaterRows = 100
	cfg.LeaseTTL = time.Hour
	cfg.RescanInterval = time.Hour
	m, alloc := newMgr(t, st, clk, cfg, pc)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	require.Eventually(t, func() bool {
		// Rows loaded too, not just both shards registered: AdoptOnce registers
		// a shard before loading it, and draining in between would leave the
		// second shard's rows to land afterwards, so it would never rotate.
		return alloc.Has(shards[0].TxID) && alloc.Has(shards[1].TxID) && alloc.Available() == 20
	}, 2*time.Second, 5*time.Millisecond, "Run fills both slots on start")

	drainAll(t, st, clk, poolName, alloc)
	m.Hungry() // what Reserve does when the allocator is below LowWaterRows

	require.Eventually(t, func() bool {
		return !alloc.Has(shards[0].TxID) && !alloc.Has(shards[1].TxID) &&
			alloc.Has(shards[2].TxID) && alloc.Has(shards[3].TxID)
	}, 2*time.Second, 5*time.Millisecond, "hunger at MaxShards with drained shards rotates without a rescan tick")
	assert.Equal(t, 20, alloc.Available())

	cancel()
	require.NoError(t, <-done)
}

// TestRotationBoundedPerCall pins the per-call rotation bound: one EnsureRows
// call releases and replaces at most two drained shards; the rest stay owned
// until a later call needs them.
func TestRotationBoundedPerCall(t *testing.T) {
	ctx := context.Background()
	const poolName = "rotate-bound"
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	shards, _ := mintN(t, st, clk, poolName, 10, 10)
	cfg := testInstanceConfig()
	cfg.MaxShards = 6
	cfg.LowWaterRows = 1000
	m, alloc := newMgr(t, st, clk, cfg, pc)
	require.NoError(t, m.FeedOnce(ctx))
	require.NoError(t, m.FeedOnce(ctx)) // adoptAttempts is per call; make sure all six slots are filled
	require.Len(t, alloc.Shards(), 6)

	drainAll(t, st, clk, poolName, alloc)
	dropsBefore, adoptsBefore := dropsEmpty(poolName), adoptionsOK(poolName)

	require.True(t, m.EnsureRows(ctx))
	waitRotatedDrops(t, m, shards[:2])
	assert.InDelta(t, dropsBefore+2, dropsEmpty(poolName), 0, "at most two drained shards released per call")
	assert.InDelta(t, adoptsBefore+2, adoptionsOK(poolName), 0, "at most two fresh shards adopted per call")
	assert.Len(t, alloc.Shards(), 6)
	assert.False(t, alloc.Has(shards[0].TxID), "oldest drained shards rotate first")
	assert.False(t, alloc.Has(shards[1].TxID))
	for _, sh := range shards[2:6] {
		assert.True(t, alloc.Has(sh.TxID), "remaining drained shards stay owned")
	}
	assert.True(t, alloc.Has(shards[6].TxID))
	assert.True(t, alloc.Has(shards[7].TxID))

	// Rows are available now, so a further call does not rotate anything.
	require.True(t, m.EnsureRows(ctx))
	assert.InDelta(t, dropsBefore+2, dropsEmpty(poolName), 0)
}

// TestRotationStopsAtFirstRefilledShard pins the keep branch with two
// candidates: both owned shards are drained, only the older one gets rows
// back in the store, so rotation re-adds them, keeps it and stops there: the
// second drained shard is neither rescanned into a drop nor released.
func TestRotationStopsAtFirstRefilledShard(t *testing.T) {
	ctx := context.Background()
	const poolName = "rotate-stop"
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	shards, _ := mintN(t, st, clk, poolName, 3, 5)
	cfg := testInstanceConfig() // MaxShards=2
	cfg.LowWaterRows = 100
	m, alloc := newMgr(t, st, clk, cfg, pc)
	require.NoError(t, m.FeedOnce(ctx))
	require.True(t, alloc.Has(shards[0].TxID))
	require.True(t, alloc.Has(shards[1].TxID))

	tok, popped := drainAll(t, st, clk, poolName, alloc)
	var back []store.Row
	for _, r := range popped {
		if r.TxID == shards[0].TxID {
			back = append(back, r)
		}
	}
	require.Len(t, back, 5)
	_, err := st.Release(ctx, poolName, tok, storetest.Ops(back[:2]))
	require.NoError(t, err)
	dropsBefore := dropsEmpty(poolName)

	require.True(t, m.EnsureRows(ctx))
	assert.Equal(t, 2, alloc.Available())
	assert.True(t, alloc.Has(shards[0].TxID), "refilled shard kept")
	assert.True(t, alloc.Has(shards[1].TxID), "rotation stops at the first refill: the other drained shard is not dropped")
	assert.False(t, alloc.Has(shards[2].TxID))
	assert.InDelta(t, dropsBefore, dropsEmpty(poolName), 0)
	got, err := st.GetShard(ctx, poolName, shards[1].TxID)
	require.NoError(t, err)
	assert.Equal(t, m.Holder(), got.Owner)
}

// slowReleaseStore delays ReleaseShard calls (only for txids in only, when set).
type slowReleaseStore struct {
	store.Store
	delay time.Duration
	only  map[chainhash.Hash]bool
}

func (w *slowReleaseStore) ReleaseShard(ctx context.Context, pool string, txid chainhash.Hash, holder string) error {
	if w.only == nil || w.only[txid] {
		time.Sleep(w.delay)
	}
	return w.Store.ReleaseShard(ctx, pool, txid, holder)
}

// TestEnsureRowsRotationDoesNotWaitForLeaseRelease pins that the request
// path does not charge a Reserve for ReleaseShard latency: rotated-out
// shards are forgotten and excluded synchronously, their leases released in
// the background.
func TestEnsureRowsRotationDoesNotWaitForLeaseRelease(t *testing.T) {
	ctx := context.Background()
	const poolName = "rotate-slowrel"
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	shards, _ := mintN(t, base, clk, poolName, 4, 10)
	cfg := testInstanceConfig() // MaxShards=2
	cfg.LowWaterRows = 100
	cfg.AdoptTimeout = 200 * time.Millisecond
	wrapped := &slowReleaseStore{Store: base, delay: 400 * time.Millisecond}
	m, alloc := newMgr(t, wrapped, clk, cfg, pc)
	require.NoError(t, m.FeedOnce(ctx))
	drainAll(t, base, clk, poolName, alloc)

	start := time.Now()
	require.True(t, m.EnsureRows(ctx))
	assert.Less(t, time.Since(start), cfg.AdoptTimeout, "EnsureRows must not block on ReleaseShard")
	assert.False(t, alloc.Has(shards[0].TxID))
	assert.False(t, alloc.Has(shards[1].TxID))
	assert.True(t, alloc.Has(shards[2].TxID))

	require.Eventually(t, func() bool {
		for _, sh := range shards[:2] {
			got, err := base.GetShard(ctx, poolName, sh.TxID)
			if err != nil || got.Owner != "" {
				return false
			}
		}
		return true
	}, 3*time.Second, 10*time.Millisecond, "leases still released, asynchronously")
}

// blockingReleaseStore blocks every ReleaseShard call until unblock is closed
// or the call's own context (releaseLease's 1 s budget) ends.
type blockingReleaseStore struct {
	store.Store
	unblock chan struct{}
}

func (w *blockingReleaseStore) ReleaseShard(ctx context.Context, pool string, txid chainhash.Hash, holder string) error {
	select {
	case <-w.unblock:
	case <-ctx.Done():
		return ctx.Err()
	}
	return w.Store.ReleaseShard(ctx, pool, txid, holder)
}

// TestRotatedShardNotReacquiredWhileReleasePending pins the releasing guard:
// while a request-path rotation's background ReleaseShard is still pending,
// the shard stays out of AcquireShard even after emptyExclusion (and our own
// lease) have lapsed and its rows are back, so a late release can never clear
// a lease this instance has re-acquired.
func TestRotatedShardNotReacquiredWhileReleasePending(t *testing.T) {
	ctx := context.Background()
	const poolName = "rotate-pending"
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	shards, _ := mintN(t, base, clk, poolName, 4, 10)
	cfg := testInstanceConfig() // MaxShards=2
	cfg.LowWaterRows = 100
	wrapped := &blockingReleaseStore{Store: base, unblock: make(chan struct{})}
	m, alloc := newMgr(t, wrapped, clk, cfg, pc)
	require.NoError(t, m.FeedOnce(ctx))
	tok, popped := drainAll(t, base, clk, poolName, alloc)

	require.True(t, m.EnsureRows(ctx)) // rotates shards 0 and 1 out; their releases block
	require.False(t, alloc.Has(shards[0].TxID))
	var back []store.Row
	for _, r := range popped {
		if r.TxID == shards[0].TxID {
			back = append(back, r)
		}
	}
	_, err := base.Release(ctx, poolName, tok, storetest.Ops(back))
	require.NoError(t, err)
	clk.Advance(2 * time.Minute) // past emptyExclusion and LeaseTTL

	for range 4 {
		_, err := m.AdoptOnce(ctx)
		require.NoError(t, err)
	}
	assert.False(t, alloc.Has(shards[0].TxID), "shard with a pending background release must not be re-acquired")
	assert.False(t, alloc.Has(shards[1].TxID))

	close(wrapped.unblock)
	// Wait for the manager's own completion signal (the shard leaving the
	// releasing set), not the store's owner field: the background goroutine
	// clears the lease first and only then un-excludes the shard.
	require.Eventually(t, func() bool { return !m.ReleasePending(shards[0].TxID) },
		2*time.Second, time.Millisecond)
	got, err := base.GetShard(ctx, poolName, shards[0].TxID)
	require.NoError(t, err)
	assert.Empty(t, got.Owner, "lease cleared before the shard is un-excluded")
	adopted, err := m.AdoptOnce(ctx)
	require.NoError(t, err)
	assert.True(t, adopted)
	assert.True(t, alloc.Has(shards[0].TxID), "adoptable again once its release completed")
}

// reentrantRenewStore reports every renewal as lost and, on the first call,
// runs a nested RenewOnce on the same manager from inside RenewShard: both
// the inner and the outer RenewOnce then try to drop the same shard.
type reentrantRenewStore struct {
	store.Store
	m      *pool.ShardManager
	nested atomic.Bool
}

func (w *reentrantRenewStore) RenewShard(ctx context.Context, _ string, _ chainhash.Hash, _ string, _ time.Time) (store.ShardHead, error) {
	if w.nested.CompareAndSwap(false, true) {
		_ = w.m.RenewOnce(ctx)
	}
	return store.ShardHead{}, store.ErrNotOwner
}

// TestDropCountsOnceWhenTwoPathsDropSameShard pins the double-drop guard: two
// paths dropping the same shard count it in shard_drops_total once.
func TestDropCountsOnceWhenTwoPathsDropSameShard(t *testing.T) {
	ctx := context.Background()
	const poolName = "drop-twice"
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	sh, _ := storetest.MintShard(t, base, poolName, 3, store.ShardMined)
	cfg := testInstanceConfig()
	cfg.MaxShards = 1
	wrapped := &reentrantRenewStore{Store: base}
	m, alloc := newMgr(t, wrapped, clk, cfg, pc)
	wrapped.m = m
	require.NoError(t, m.FeedOnce(ctx))
	require.True(t, alloc.Has(sh.TxID))
	before := testutil.ToFloat64(pool.ShardDrops.WithLabelValues(poolName, "not_owner"))

	_ = m.RenewOnce(ctx)
	assert.False(t, alloc.Has(sh.TxID))
	assert.InDelta(t, before+1, testutil.ToFloat64(pool.ShardDrops.WithLabelValues(poolName, "not_owner")), 0,
		"dropped by both the nested and the outer RenewOnce, counted once")
}

// TestRunWaitsForBackgroundLeaseReleases pins that Run returns only after the
// request-path rotation's background lease releases have completed.
func TestRunWaitsForBackgroundLeaseReleases(t *testing.T) {
	const poolName = "rotate-runwait"
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	shards, _ := mintN(t, base, clk, poolName, 4, 10)
	cfg := testInstanceConfig() // MaxShards=2
	cfg.LowWaterRows = 100
	cfg.LeaseTTL = time.Hour
	cfg.RescanInterval = time.Hour
	// Only the shards rotation will release are slow, so a Run that skipped
	// waiting would return while their leases are still held.
	wrapped := &slowReleaseStore{Store: base, delay: 300 * time.Millisecond, only: map[chainhash.Hash]bool{shards[0].TxID: true, shards[1].TxID: true}}
	m, alloc := newMgr(t, wrapped, clk, cfg, pc)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	require.Eventually(t, func() bool { return alloc.Has(shards[0].TxID) && alloc.Has(shards[1].TxID) },
		2*time.Second, 5*time.Millisecond)
	drainAll(t, base, clk, poolName, alloc)

	require.True(t, m.EnsureRows(context.Background())) // background releases of shards 0 and 1 now in flight
	cancel()
	require.NoError(t, <-done)
	for _, sh := range shards {
		got, err := base.GetShard(context.Background(), poolName, sh.TxID)
		require.NoError(t, err)
		assert.Empty(t, got.Owner, "every lease cleared by the time Run returns")
	}
}

// spendOut reserves and terminally spends (spend_policy=trust) every row of
// each shard, leaving the shards mined with no row that can ever become
// available again.
func spendOut(t *testing.T, st store.Store, clk *storetest.Clock, poolName string, rows [][]store.Row) {
	t.Helper()
	for _, rs := range rows {
		tok := store.NewToken()
		storetest.MustReserve(t, st, poolName, rs, tok, clk.Now().Add(time.Hour))
		n, err := st.Spend(context.Background(), poolName, tok, storetest.Ops(rs), storetest.NewTxID(), true)
		require.NoError(t, err)
		require.Equal(t, len(rs), n)
	}
}

// TestAdoptionReachesLiveShardBehindSpentOutShards pins the fix for the
// permanent adoption stall seen under sustained load: fully spent shards stay
// mined, so AcquireShard (oldest first) keeps handing them out ahead of every
// shard that still has rows. The only thing that kept them out was the
// bounded (256-entry) recently-empty exclusion list; once more than that many
// spent-out shards preceded the live ones, each adoption of an evicted one
// evicted the next, and the instance cycled through dead shards forever
// while the store held plenty of available rows.
func TestAdoptionReachesLiveShardBehindSpentOutShards(t *testing.T) {
	ctx := context.Background()
	const poolName = "spent-prefix"
	const dead = 300 // more than the exclusion list can hold
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	shards, rows := mintN(t, st, clk, poolName, dead, 2)
	spendOut(t, st, clk, poolName, rows[1:])
	// Shard 0 is drained but not spent out: one row is still reserved and can
	// come back, so it must be dropped as before, never retired.
	storetest.MustReserve(t, st, poolName, rows[0], store.NewToken(), clk.Now().Add(time.Hour))
	live, _ := storetest.MintShard(t, st, poolName, 5, store.ShardMined)
	m, alloc := newMgr(t, st, clk, testInstanceConfig(), pc)

	found := false
	for i := 0; i < 3*dead && !found; i++ {
		found = m.EnsureRows(ctx)
	}
	require.True(t, found, "adoption must get past spent-out shards to the live one")
	assert.True(t, alloc.Has(live.TxID))
	assert.Equal(t, 5, alloc.Available())

	got, err := st.GetShard(ctx, poolName, shards[0].TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardMined, got.Status, "a shard with a non-terminal row is not retired")
	for _, sh := range shards[1:] {
		got, err := st.GetShard(ctx, poolName, sh.TxID)
		require.NoError(t, err)
		require.Equal(t, store.ShardRetired, got.Status, "spent-out shard %s retired", sh.TxID)
		assert.Empty(t, got.Owner)
	}
}

// TestPoolServesEveryRowPastSpentOutShardsUnderConcurrentLoad is the
// request-path shape of the same stall: concurrent reserve+spend workers
// drain more shards than the exclusion list holds while the background
// FeedOnce and every Reserve's EnsureRows race to adopt. Every row of the
// pool must still be served.
func TestPoolServesEveryRowPastSpentOutShardsUnderConcurrentLoad(t *testing.T) {
	const shardsN, perShard, workers = 320, 2, 8
	f := newFixture(t, func(c *pool.Config) { c.SpendPolicy = pool.SpendTrust })
	mintN(t, f.st, f.clk, "main", shardsN, perShard)

	var served, attempts atomic.Int64
	const total = shardsN * perShard
	const maxAttempts = 50 * total
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for served.Load() < total && attempts.Add(1) <= maxAttempts {
				res, err := f.p.Reserve(ctx, pool.ReserveRequest{Count: 1})
				if err != nil {
					continue
				}
				_, err = f.p.Spend(ctx, res.Token, outpointsOf(res.Outputs), storetest.NewTxID())
				assert.NoError(t, err)
				served.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int64(total), served.Load(), "every row served (attempts: %d)", attempts.Load())
}

// listHookStore runs hook after the wrapped ListAvailable has read its
// snapshot and before the snapshot is returned to the caller: the window in
// which a concurrent Reserve can hand out, reserve and forget a listed row.
type listHookStore struct {
	store.Store
	hook func(txid chainhash.Hash)
}

func (w *listHookStore) ListAvailable(ctx context.Context, pool string, txid chainhash.Hash, fromVout uint32, limit int) ([]store.Row, error) {
	rows, err := w.Store.ListAvailable(ctx, pool, txid, fromVout, limit)
	if err == nil && w.hook != nil {
		w.hook(txid)
	}
	return rows, err
}

// TestRescanDoesNotReAddRowsHandedOutDuringListing pins the stale re-add
// race: a rescan lists a shard's available rows, a concurrent Reserve then
// pops, reserves and forgets some of those rows, and only afterwards does the
// rescan add its (now stale) snapshot to the allocator. The rows handed out
// in between must not be queued again: they are reserved in the store, so
// handing them out a second time can only miss its CAS (inflating
// cas_missed_total, the double-hand-out audit signal, and returning
// exhausted to a request while fuel exists).
func TestRescanDoesNotReAddRowsHandedOutDuringListing(t *testing.T) {
	ctx := context.Background()
	const poolName = "stale-readd"
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	sh, _ := storetest.MintShard(t, base, poolName, 5, store.ShardMined)
	wrapped := &listHookStore{Store: base}
	cfg := testInstanceConfig()
	cfg.LowWaterRows = 1
	m, alloc := newMgr(t, wrapped, clk, cfg, pc)
	require.NoError(t, m.FeedOnce(ctx))
	require.Equal(t, 5, alloc.Available())

	var handedOut []store.Row
	tok := store.NewToken()
	wrapped.hook = func(txid chainhash.Hash) {
		if txid != sh.TxID || handedOut != nil {
			return
		}
		handedOut = alloc.Pop(2) // a concurrent Reserve, exactly as Pool.Reserve does it
		storetest.MustReserve(t, base, poolName, handedOut, tok, clk.Now().Add(time.Hour))
		alloc.Forget(storetest.Ops(handedOut))
	}
	require.NoError(t, m.RescanOnce(ctx))
	require.Len(t, handedOut, 2)

	assert.Equal(t, 3, alloc.Available(), "only the 3 rows still available in the store are queued")
	rest := alloc.Pop(10)
	for _, r := range rest {
		for _, h := range handedOut {
			assert.NotEqual(t, h.Outpoint, r.Outpoint, "row handed out during the listing re-queued")
		}
	}

	alloc.Return(rest)

	// Rows genuinely released back later are picked up by the next rescan,
	// whose listing starts after they were handed out.
	wrapped.hook = nil
	n, err := base.Release(ctx, poolName, tok, storetest.Ops(handedOut))
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.NoError(t, m.RescanOnce(ctx))
	assert.Equal(t, 5, alloc.Available())
}

// hideListStore lists no available rows for the shards in hide, as if the
// listing had raced a release made through another instance.
type hideListStore struct {
	store.Store
	hide map[chainhash.Hash]bool
}

func (w *hideListStore) ListAvailable(ctx context.Context, pool string, txid chainhash.Hash, fromVout uint32, limit int) ([]store.Row, error) {
	if w.hide[txid] {
		return nil, nil
	}
	return w.Store.ListAvailable(ctx, pool, txid, fromVout, limit)
}

func dropsRetired(poolName string) float64 {
	return testutil.ToFloat64(pool.ShardDrops.WithLabelValues(poolName, "retired"))
}

// assertDroppedAsEmpty checks the pre-retirement handling of an adoption that
// found sh empty: dropped as "empty", lease released, excluded from
// re-adoption, and still mined in the store.
func assertDroppedAsEmpty(t *testing.T, st store.Store, m *pool.ShardManager, poolName string, sh store.Shard, emptyBefore, retiredBefore float64) {
	t.Helper()
	got, err := st.GetShard(context.Background(), poolName, sh.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardMined, got.Status, "not retired")
	assert.Empty(t, got.Owner, "lease released")
	assert.True(t, m.RecentlyEmpty(sh.TxID), "excluded from re-adoption as before")
	assert.InDelta(t, emptyBefore+1, dropsEmpty(poolName), 0)
	assert.InDelta(t, retiredBefore, dropsRetired(poolName), 0)
}

// TestAdoptOnceDoesNotRetireShardWithAvailableRow pins that retirement needs
// every row terminal: a shard whose adoption listing came back empty while one
// row is in fact available (e.g. released through another instance meanwhile)
// is handled as empty, never retired, so that row is not stranded.
func TestAdoptOnceDoesNotRetireShardWithAvailableRow(t *testing.T) {
	ctx := context.Background()
	const poolName = "retire-avail"
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	sh, rows := storetest.MintShard(t, base, poolName, 3, store.ShardMined)
	spendOut(t, base, clk, poolName, [][]store.Row{rows[1:]}) // rows[0] stays available
	wrapped := &hideListStore{Store: base, hide: map[chainhash.Hash]bool{sh.TxID: true}}
	m, alloc := newMgr(t, wrapped, clk, testInstanceConfig(), pc)
	emptyBefore, retiredBefore := dropsEmpty(poolName), dropsRetired(poolName)

	adopted, err := m.AdoptOnce(ctx)
	require.NoError(t, err)
	assert.False(t, adopted)
	assert.False(t, alloc.Has(sh.TxID))
	assertDroppedAsEmpty(t, base, m, poolName, sh, emptyBefore, retiredBefore)
}

// TestAdoptOnceDoesNotRetireShardWithUnexpectedVouts pins the found == Size
// check: rows are only looked up at vouts 0..Size-1, so a shard laid out
// differently (here vouts 1..Size) cannot be proven spent out and is handled
// as empty, never retired, even though every row it has is terminal.
func TestAdoptOnceDoesNotRetireShardWithUnexpectedVouts(t *testing.T) {
	ctx := context.Background()
	const poolName = "retire-gap"
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	sh, rows := storetest.BuildShard(poolName, 3, store.ShardMined)
	for i := range rows {
		rows[i].Vout = uint32(i + 1) //nolint:gosec // tiny test count
	}
	require.NoError(t, st.PutShard(ctx, sh, rows))
	spendOut(t, st, clk, poolName, [][]store.Row{rows})
	m, _ := newMgr(t, st, clk, testInstanceConfig(), pc)
	emptyBefore, retiredBefore := dropsEmpty(poolName), dropsRetired(poolName)

	adopted, err := m.AdoptOnce(ctx)
	require.NoError(t, err)
	assert.False(t, adopted)
	assertDroppedAsEmpty(t, st, m, poolName, sh, emptyBefore, retiredBefore)
}

// TestAdoptOnceRetiredShardNotOnRecentlyEmptyList pins that a retired shard
// takes no slot on the bounded recently-empty list (AcquireShard never
// returns it again, so an entry would only evict a useful one), while its
// lease is still released and the drop is counted as "retired".
func TestAdoptOnceRetiredShardNotOnRecentlyEmptyList(t *testing.T) {
	ctx := context.Background()
	const poolName = "retire-noexclude"
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	sh, rows := storetest.MintShard(t, st, poolName, 3, store.ShardMined)
	spendOut(t, st, clk, poolName, [][]store.Row{rows})
	m, _ := newMgr(t, st, clk, testInstanceConfig(), pc)
	emptyBefore, retiredBefore := dropsEmpty(poolName), dropsRetired(poolName)

	adopted, err := m.AdoptOnce(ctx)
	require.NoError(t, err)
	assert.False(t, adopted)
	got, err := st.GetShard(ctx, poolName, sh.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardRetired, got.Status)
	assert.Empty(t, got.Owner, "lease released")
	assert.False(t, m.RecentlyEmpty(sh.TxID), "retired shard not on the recently-empty list")
	assert.InDelta(t, retiredBefore+1, dropsRetired(poolName), 0)
	assert.InDelta(t, emptyBefore, dropsEmpty(poolName), 0)
}

// getRowsHookStore runs hook after the wrapped GetRows has read its rows and
// before they are returned: the window in noteReleased between reading the
// released rows and adding them back.
type getRowsHookStore struct {
	store.Store
	mu   sync.Mutex
	hook func() // runs once, then cleared
}

func (w *getRowsHookStore) setHook(h func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.hook = h
}

func (w *getRowsHookStore) GetRows(ctx context.Context, pool string, ops []store.Outpoint) ([]store.Row, error) {
	rows, err := w.Store.GetRows(ctx, pool, ops)
	w.mu.Lock()
	h := w.hook
	w.hook = nil
	w.mu.Unlock()
	if err == nil && h != nil {
		h()
	}
	return rows, err
}

// TestReleaseDoesNotReAddRowsHandedOutDuringItsRead is the release-path twin
// of TestRescanDoesNotReAddRowsHandedOutDuringListing: Release (trust) reads
// the released rows back with GetRows and re-adds the available ones. If one
// of them is meanwhile picked up (here by a rescan), handed out, reserved and
// forgotten, the stale copy from that read must not be queued again.
func TestReleaseDoesNotReAddRowsHandedOutDuringItsRead(t *testing.T) {
	ctx := context.Background()
	const poolName = "stale-release"
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	wrapped := &getRowsHookStore{Store: base}
	pc := pool.DefaultPoolConfig(poolName, 50)
	pc.MinTTL = time.Second
	pc.ReleasePolicy = pool.ReleaseTrust
	require.NoError(t, pc.Validate())
	p := pool.NewPool(wrapped, pc, testInstanceConfig(), pool.ClockFunc(clk.Now), slog.Default())
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(runCtx) }()
	t.Cleanup(func() { cancel(); <-done })
	sh, _ := storetest.MintShard(t, base, poolName, 2, store.ShardMined)

	res, err := p.Reserve(ctx, pool.ReserveRequest{Count: 2})
	require.NoError(t, err)
	alloc := p.Allocator()
	require.Zero(t, alloc.Available())
	ops := outpointsOf(res.Outputs)

	var handedOut []store.Row
	wrapped.setHook(func() {
		// A rescan picks the first released row up, and a concurrent Reserve
		// hands it out, reserves it and forgets it.
		rows := storetest.RowsByOutpoint(t, base, poolName, ops[:1])
		alloc.Add(sh.TxID, []store.Row{rows[ops[0]]})
		handedOut = alloc.Pop(1)
		storetest.MustReserve(t, base, poolName, handedOut, store.NewToken(), clk.Now().Add(time.Hour))
		alloc.Forget(storetest.Ops(handedOut))
	})
	n, err := p.Release(ctx, res.Token, ops)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Len(t, handedOut, 1)
	require.Equal(t, ops[0], handedOut[0].Outpoint)

	assert.Equal(t, 1, alloc.Available(), "only the row still available in the store is queued")
	queued := alloc.Pop(10)
	require.Len(t, queued, 1)
	assert.Equal(t, ops[1], queued[0].Outpoint)
}

// gatedRescanStore blocks every ListAvailable call for one shard after the
// first (the adoption load), until gate is closed or the call's ctx ends:
// the first rescan of that shard then stands in for a slow store read (such
// as a Pebble read waiting for the WAL fsync of a batch it observed).
type gatedRescanStore struct {
	store.Store
	txid    chainhash.Hash
	calls   atomic.Int32
	blocked chan struct{} // closed when the first gated call starts waiting
	once    sync.Once
	gate    chan struct{}
}

func (w *gatedRescanStore) ListAvailable(ctx context.Context, pool string, txid chainhash.Hash, fromVout uint32, limit int) ([]store.Row, error) {
	if txid == w.txid && w.calls.Add(1) > 1 {
		w.once.Do(func() { close(w.blocked) })
		select {
		case <-w.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return w.Store.ListAvailable(ctx, pool, txid, fromVout, limit)
}

// TestRunAdoptsWhileRescanIsSlow pins that hunger-driven adoption does not
// wait behind a slow RescanOnce. RescanOnce reads every owned shard in turn,
// so its duration is owned shards times the store's read latency; a Run
// loop that also served hunger could not adopt at all while it ran. Under
// sustained multi-output demand on Pebble (reads wait for the WAL fsync
// chain, ~30-60 ms each) the loop spent most of its time rescanning, and the
// allocator drained to zero between adoptions: ~5-20% "exhausted" reserves
// with the store holding 150k+ available rows.
func TestRunAdoptsWhileRescanIsSlow(t *testing.T) {
	const poolName = "slow-rescan"
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	first, _ := storetest.MintShard(t, base, poolName, 10, store.ShardMined)
	wrapped := &gatedRescanStore{Store: base, txid: first.TxID, blocked: make(chan struct{}), gate: make(chan struct{})}
	cfg := testInstanceConfig()
	cfg.MaxShards = 4
	cfg.LowWaterRows = 1000 // always hungry
	cfg.LeaseTTL = time.Hour
	cfg.RescanInterval = 20 * time.Millisecond
	m, alloc := newMgr(t, wrapped, clk, cfg, pc)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	defer func() {
		cancel()
		require.NoError(t, <-done)
	}()

	require.Eventually(t, func() bool { return alloc.Has(first.TxID) }, 2*time.Second, 5*time.Millisecond, "Run adopts the first shard")
	select {
	case <-wrapped.blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("RescanOnce never reached the gated ListAvailable")
	}

	second, _ := storetest.MintShard(t, base, poolName, 10, store.ShardMined)
	m.Hungry() // what Reserve does below LowWaterRows
	assert.Eventually(t, func() bool { return alloc.Has(second.TxID) && alloc.ShardAvailable(second.TxID) == 10 },
		time.Second, 5*time.Millisecond, "adoption must proceed while a rescan is blocked on a slow read")
	close(wrapped.gate)
}

// TestRescanRetiresSpentOutShardWhenDroppingIt pins that a shard consumed
// while owned is retired when RescanOnce drops it, not left mined for a later
// adoption to retire. Left mined, every consumed shard came back at the head
// of AcquireShard's oldest-first order once its emptyExclusion window ran
// out, so each live adoption paid for a dead one first (acquire, listing,
// GetRows, retire, release); after an idle minute the whole backlog came due
// at once and adoption stalled behind it long enough to drain the allocator.
// A drained shard with a row still reserved is dropped as "empty" as before.
func TestRescanRetiresSpentOutShardWhenDroppingIt(t *testing.T) {
	ctx := context.Background()
	const poolName = "rescan-retire"
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	shards, _ := mintN(t, st, clk, poolName, 2, 3)
	cfg := testInstanceConfig() // MaxShards=2
	cfg.LowWaterRows = 100
	m, alloc := newMgr(t, st, clk, cfg, pc)
	require.NoError(t, m.FeedOnce(ctx))
	require.True(t, alloc.Has(shards[0].TxID) && alloc.Has(shards[1].TxID))

	tok, popped := drainAll(t, st, clk, poolName, alloc)
	var spend, keep []store.Outpoint
	for _, r := range popped {
		if r.TxID == shards[1].TxID && len(keep) == 0 {
			keep = append(keep, r.Outpoint) // shard 1 keeps one reserved row
			continue
		}
		spend = append(spend, r.Outpoint)
	}
	n, err := st.Spend(ctx, poolName, tok, spend, storetest.NewTxID(), true)
	require.NoError(t, err)
	require.Equal(t, len(spend), n)

	emptyBefore, retiredBefore := dropsEmpty(poolName), dropsRetired(poolName)
	for range 2 { // emptyRescansMax consecutive empty rescans
		require.NoError(t, m.RescanOnce(ctx))
	}

	got, err := st.GetShard(ctx, poolName, shards[0].TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardRetired, got.Status, "spent-out shard retired on drop")
	assert.Empty(t, got.Owner, "lease released")
	assert.False(t, m.RecentlyEmpty(shards[0].TxID), "a retired shard needs no exclusion entry")
	assert.False(t, alloc.Has(shards[0].TxID))

	got, err = st.GetShard(ctx, poolName, shards[1].TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardMined, got.Status, "a shard with a reserved row is not retired")
	assert.Empty(t, got.Owner, "lease released")
	assert.True(t, m.RecentlyEmpty(shards[1].TxID))

	assert.InDelta(t, retiredBefore+1, dropsRetired(poolName), 0)
	assert.InDelta(t, emptyBefore+1, dropsEmpty(poolName), 0)
}

// slowGetRowsStore delays every GetRows call (the retire check's read).
type slowGetRowsStore struct {
	store.Store
	delay time.Duration
}

func (w *slowGetRowsStore) GetRows(ctx context.Context, pool string, ops []store.Outpoint) ([]store.Row, error) {
	select {
	case <-time.After(w.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return w.Store.GetRows(ctx, pool, ops)
}

// rotationFixture owns shards 0 and 1 of four (MaxShards=2) and leaves both
// drained in the store: shard 0 spent out (every row terminal), shard 1 with
// one row still reserved, so rotation must retire 0 and drop 1 as empty.
func rotationFixture(t *testing.T, st store.Store, clk *storetest.Clock, poolName string, m *pool.ShardManager, alloc *pool.Allocator, shards []store.Shard) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, m.FeedOnce(ctx))
	require.True(t, alloc.Has(shards[0].TxID) && alloc.Has(shards[1].TxID))
	tok, popped := drainAll(t, st, clk, poolName, alloc)
	var spend []store.Outpoint
	kept := false
	for _, r := range popped {
		if r.TxID == shards[1].TxID && !kept {
			kept = true // shard 1 keeps one reserved row
			continue
		}
		spend = append(spend, r.Outpoint)
	}
	n, err := st.Spend(ctx, poolName, tok, spend, storetest.NewTxID(), true)
	require.NoError(t, err)
	require.Equal(t, len(spend), n)
}

// assertRotatedOut checks rotation's outcome for rotationFixture's shards:
// shard 0 retired with no exclusion entry, shard 1 still mined and excluded,
// both released, the freed slots refilled by shards 2 and 3.
func assertRotatedOut(t *testing.T, st store.Store, m *pool.ShardManager, alloc *pool.Allocator, poolName string, shards []store.Shard, emptyBefore, retiredBefore float64) {
	t.Helper()
	ctx := context.Background()
	got, err := st.GetShard(ctx, poolName, shards[0].TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardRetired, got.Status, "spent-out shard retired by rotation")
	assert.Empty(t, got.Owner)
	assert.False(t, m.RecentlyEmpty(shards[0].TxID), "a retired shard needs no exclusion entry")
	got, err = st.GetShard(ctx, poolName, shards[1].TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardMined, got.Status, "a shard with a reserved row is not retired")
	assert.Empty(t, got.Owner)
	assert.True(t, m.RecentlyEmpty(shards[1].TxID))
	assert.InDelta(t, retiredBefore+1, dropsRetired(poolName), 0)
	assert.InDelta(t, emptyBefore+1, dropsEmpty(poolName), 0)
	assert.True(t, alloc.Has(shards[2].TxID) && alloc.Has(shards[3].TxID), "freed slots refilled")
}

// TestFeedOnceRotationRetiresSpentOutShard pins that the background rotation
// (FeedOnce at MaxShards) retires a spent-out shard instead of excluding it,
// so it never comes back as a dead adoption after emptyExclusion.
func TestFeedOnceRotationRetiresSpentOutShard(t *testing.T) {
	ctx := context.Background()
	const poolName = "rotate-retire-feed"
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	shards, _ := mintN(t, st, clk, poolName, 4, 3)
	cfg := testInstanceConfig() // MaxShards=2
	cfg.LowWaterRows = 100
	m, alloc := newMgr(t, st, clk, cfg, pc)
	rotationFixture(t, st, clk, poolName, m, alloc, shards)
	emptyBefore, retiredBefore := dropsEmpty(poolName), dropsRetired(poolName)

	require.NoError(t, m.FeedOnce(ctx))
	assertRotatedOut(t, st, m, alloc, poolName, shards, emptyBefore, retiredBefore)
}

// TestEnsureRowsRotationRetiresInBackground pins the request-path variant:
// EnsureRows rotation retires a spent-out shard too, but does the retire
// check in the background with the lease release, so a slow GetRows (here
// 400 ms, twice AdoptTimeout) is never charged to the Reserve.
func TestEnsureRowsRotationRetiresInBackground(t *testing.T) {
	ctx := context.Background()
	const poolName = "rotate-retire-ensure"
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	st := &slowGetRowsStore{Store: base}
	pc := pool.DefaultPoolConfig(poolName, 50)
	shards, _ := mintN(t, base, clk, poolName, 4, 3)
	cfg := testInstanceConfig() // MaxShards=2, AdoptTimeout 200 ms
	cfg.LowWaterRows = 100
	m, alloc := newMgr(t, st, clk, cfg, pc)
	rotationFixture(t, base, clk, poolName, m, alloc, shards)
	emptyBefore, retiredBefore := dropsEmpty(poolName), dropsRetired(poolName)
	st.delay = 400 * time.Millisecond

	start := time.Now()
	require.True(t, m.EnsureRows(ctx), "rotation refills the freed slots")
	assert.Less(t, time.Since(start), cfg.AdoptTimeout, "the retire check's GetRows is not charged to the request")
	assert.True(t, m.ReleasePending(shards[0].TxID), "retire check still running in the background")
	waitRotatedDrops(t, m, shards[:2])
	assertRotatedOut(t, base, m, alloc, poolName, shards, emptyBefore, retiredBefore)
}

// TestRescanDoesNotRetireShardWithUnexpectedVouts is the rescan-path variant
// of TestAdoptOnceDoesNotRetireShardWithUnexpectedVouts: a shard consumed
// while owned whose rows sit at vouts 1..Size cannot be proven spent out
// (vout 0 is missing), so RescanOnce drops it as empty, never retires it.
func TestRescanDoesNotRetireShardWithUnexpectedVouts(t *testing.T) {
	ctx := context.Background()
	const poolName = "rescan-retire-gap"
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	sh, rows := storetest.BuildShard(poolName, 3, store.ShardMined)
	for i := range rows {
		rows[i].Vout = uint32(i + 1) //nolint:gosec // tiny test count
	}
	require.NoError(t, st.PutShard(ctx, sh, rows))
	m, alloc := newMgr(t, st, clk, testInstanceConfig(), pc)
	adopted, err := m.AdoptOnce(ctx)
	require.NoError(t, err)
	require.True(t, adopted)
	require.Equal(t, 3, alloc.Available())
	tok, popped := drainAll(t, st, clk, poolName, alloc)
	n, err := st.Spend(ctx, poolName, tok, storetest.Ops(popped), storetest.NewTxID(), true)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	emptyBefore, retiredBefore := dropsEmpty(poolName), dropsRetired(poolName)

	for range 2 { // emptyRescansMax consecutive empty rescans
		require.NoError(t, m.RescanOnce(ctx))
	}
	assert.False(t, alloc.Has(sh.TxID))
	assertDroppedAsEmpty(t, st, m, poolName, sh, emptyBefore, retiredBefore)
}

// slowNotOwnerRenewStore makes RenewShard of one shard wait briefly and then
// fail with ErrNotOwner, so the hunger signal NoteCASMiss sends is consumed
// by the feed goroutine (finding nothing to do at MaxShards) before
// rescanFlagged drops the shard.
type slowNotOwnerRenewStore struct {
	store.Store
	txid  chainhash.Hash
	armed atomic.Bool
}

func (w *slowNotOwnerRenewStore) RenewShard(ctx context.Context, pool string, txid chainhash.Hash, holder string, until time.Time) (store.ShardHead, error) {
	if txid == w.txid && w.armed.Load() {
		time.Sleep(50 * time.Millisecond)
		return store.ShardHead{}, store.ErrNotOwner
	}
	return w.Store.RenewShard(ctx, pool, txid, holder, until)
}

// TestFlaggedNotOwnerDropReadoptsWithoutTick pins that a not_owner drop on the
// flagged-rescan path re-signals hunger, as the renew and rescan ticks do:
// the freed slot is refilled without waiting for a Reserve or a tick (both
// tickers are 1 h here).
func TestFlaggedNotOwnerDropReadoptsWithoutTick(t *testing.T) {
	const poolName = "flagged-readopt"
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig(poolName, 50)
	shards, _ := mintN(t, base, clk, poolName, 2, 5)
	st := &slowNotOwnerRenewStore{Store: base, txid: shards[0].TxID}
	cfg := testInstanceConfig()
	cfg.MaxShards = 1
	cfg.LowWaterRows = 100 // hungry whenever a slot is free; at MaxShards only if the FIFO is empty
	cfg.LeaseTTL = time.Hour
	cfg.RescanInterval = time.Hour
	m, alloc := newMgr(t, st, clk, cfg, pc)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	defer func() {
		cancel()
		require.NoError(t, <-done)
	}()
	require.Eventually(t, func() bool { return alloc.ShardAvailable(shards[0].TxID) == 5 }, 2*time.Second, 5*time.Millisecond)

	st.armed.Store(true)
	m.NoteCASMiss(shards[0].TxID)
	require.Eventually(t, func() bool { return !alloc.Has(shards[0].TxID) }, 2*time.Second, 5*time.Millisecond, "flagged rescan drops the lost shard")
	assert.Eventually(t, func() bool { return alloc.ShardAvailable(shards[1].TxID) == 5 }, time.Second, 5*time.Millisecond,
		"the freed slot is refilled without a Reserve or a tick")
}
