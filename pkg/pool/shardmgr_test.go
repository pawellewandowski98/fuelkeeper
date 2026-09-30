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
	assert.InDelta(t, dropsBefore+2, dropsEmpty(poolName), 0, "both drained shards dropped as empty")
	assert.InDelta(t, adoptsBefore+2, adoptionsOK(poolName), 0, "both freed slots refilled while below LowWaterRows")
	assert.False(t, alloc.Has(shards[0].TxID))
	assert.False(t, alloc.Has(shards[1].TxID))
	assert.True(t, alloc.Has(shards[2].TxID))
	assert.True(t, alloc.Has(shards[3].TxID))
	for _, sh := range shards[:2] {
		got, err := st.GetShard(ctx, poolName, sh.TxID)
		require.NoError(t, err)
		assert.Empty(t, got.Owner, "released in the store")
	}

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
	require.Eventually(t, func() bool { return alloc.Has(shards[0].TxID) && alloc.Has(shards[1].TxID) },
		2*time.Second, 5*time.Millisecond, "Run fills both slots on start")

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
