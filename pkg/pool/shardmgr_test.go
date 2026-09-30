package pool_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

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
