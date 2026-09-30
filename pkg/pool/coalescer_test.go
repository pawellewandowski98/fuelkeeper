package pool_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func startCoalescer(t *testing.T, st store.Store, cfg pool.CoalescerConfig) *pool.Coalescer {
	t.Helper()
	c := pool.NewCoalescer(st, "main", cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return c
}

func TestCoalescerAppliesAndReportsMissed(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	_, rows := storetest.MintShard(t, st, "main", 10, store.ShardMined)
	c := startCoalescer(t, st, pool.CoalescerConfig{Slots: 2, MaxBatch: 64})

	other := store.NewToken()
	storetest.MustReserve(t, st, "main", rows[3:4], other, clk.Now().Add(time.Minute))

	tok := store.NewToken()
	res := c.Submit(context.Background(), storetest.ReserveOps(rows[:5], tok, clk.Now().Add(time.Minute)))
	require.NoError(t, res.Err)
	assert.False(t, res.Unflushed)
	assert.Len(t, res.Applied, 4)
	assert.Equal(t, []store.Outpoint{rows[3].Outpoint}, res.Missed)
	for _, a := range res.Applied {
		assert.Equal(t, tok, a.Token)
	}
	got := storetest.RowsByOutpoint(t, st, "main", storetest.Ops(rows[:5]))
	for _, r := range rows[:5] {
		if r.Outpoint == rows[3].Outpoint {
			assert.Equal(t, other, got[r.Outpoint].ReservedBy)
		} else {
			assert.Equal(t, tok, got[r.Outpoint].ReservedBy)
		}
	}
}

func TestCoalescerBatchesConcurrentRequests(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	_, rows := storetest.MintShard(t, st, "main", 200, store.ShardMined)
	f := storetest.NewFaulty(st)
	release := make(chan struct{})
	f.BeforeReserve = func([]store.ReserveOp) { <-release } // hold every flush until released
	c := startCoalescer(t, f, pool.CoalescerConfig{Slots: 1, MaxBatch: 64})

	var wg sync.WaitGroup
	results := make([]pool.Result, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tok := store.NewToken()
			results[i] = c.Submit(context.Background(), storetest.ReserveOps(rows[i*2:i*2+2], tok, clk.Now().Add(time.Minute)))
		}(i)
	}
	time.Sleep(100 * time.Millisecond) // let requests queue behind the held slot
	close(release)
	wg.Wait()

	for i, r := range results {
		require.NoError(t, r.Err, "request %d", i)
		assert.Len(t, r.Applied, 2)
		for _, op := range r.Applied {
			assert.Equal(t, rows[i*2].TxID, op.TxID)
		}
	}
	assert.Less(t, f.ReserveCalls(), 50, "requests were coalesced into fewer flushes")
	assert.GreaterOrEqual(t, f.ReserveCalls(), 2, "100 ops exceed MaxBatch 64, so at least two flushes")
}

func TestCoalescerInDoubtRetriesIdempotently(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	_, rows := storetest.MintShard(t, st, "main", 10, store.ShardMined)
	f := storetest.NewFaulty(st)
	c := startCoalescer(t, f, pool.CoalescerConfig{Slots: 1, MaxBatch: 64, RetryBackoff: time.Millisecond})

	f.InDoubtNext(1) // first attempt commits but reports ErrUnavailable; the retry replays cleanly
	tok := store.NewToken()
	res := c.Submit(context.Background(), storetest.ReserveOps(rows[:3], tok, clk.Now().Add(time.Minute)))
	require.NoError(t, res.Err, "an in-doubt flush is retried and the replay reports applied")
	assert.Len(t, res.Applied, 3)
	assert.Equal(t, 2, f.ReserveCalls())
	got := storetest.RowsByOutpoint(t, st, "main", storetest.Ops(rows[:3]))
	for _, r := range rows[:3] {
		assert.Equal(t, tok, got[r.Outpoint].ReservedBy)
	}
}

func TestCoalescerCompensatesAfterRetriesExhausted(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	_, rows := storetest.MintShard(t, st, "main", 10, store.ShardMined)
	f := storetest.NewFaulty(st)
	f.InDoubtEvery(1) // every attempt in doubt
	c := startCoalescer(t, f, pool.CoalescerConfig{Slots: 1, MaxBatch: 64, Retries: 2, RetryBackoff: time.Millisecond})

	tok := store.NewToken()
	res := c.Submit(context.Background(), storetest.ReserveOps(rows[:3], tok, clk.Now().Add(time.Minute)))
	require.ErrorIs(t, res.Err, pool.ErrUnavailable)
	assert.Empty(t, res.Applied)
	assert.False(t, res.Unflushed)
	assert.Equal(t, 3, f.ReserveCalls(), "1 attempt + 2 retries")
	st2 := storetest.RowsByOutpoint(t, st, "main", storetest.Ops(rows[:3]))
	for _, r := range rows[:3] {
		assert.Equal(t, store.StateAvailable, st2[r.Outpoint].State, "compensating release undid the hidden commit")
	}
}

func TestCoalescerPartialApplyIsNeverReturned(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	_, rows := storetest.MintShard(t, st, "main", 10, store.ShardMined)
	f := storetest.NewFaulty(st)
	f.FailAfterApplying(2) // first attempt: 2 of 4 applied, then error
	c := startCoalescer(t, f, pool.CoalescerConfig{Slots: 1, MaxBatch: 64, Retries: 1, RetryBackoff: time.Millisecond})

	tok := store.NewToken()
	res := c.Submit(context.Background(), storetest.ReserveOps(rows[:4], tok, clk.Now().Add(time.Minute)))
	require.NoError(t, res.Err, "retry replays idempotently: the 2 hidden applies plus the other 2")
	assert.Len(t, res.Applied, 4)
	assert.Equal(t, 2, f.ReserveCalls())
}

func TestCoalescerBackpressureAndCancel(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	_, rows := storetest.MintShard(t, st, "main", 40, store.ShardMined)
	f := storetest.NewFaulty(st)
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	f.BeforeReserve = func([]store.ReserveOp) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	}
	c := startCoalescer(t, f, pool.CoalescerConfig{Slots: 1, MaxBatch: 4, MaxInflightOps: 10})

	tok := store.NewToken()
	// r1 (4 ops) occupies the slot (held inside the flush). r2 (4 ops) queues → 8 in flight.
	// r3 (4 ops) would make 12 > 10 → backpressure. r4 (2 ops) fits (10) and is then cancelled.
	var r1, r2 pool.Result
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		r1 = c.Submit(context.Background(), storetest.ReserveOps(rows[0:4], tok, clk.Now().Add(time.Minute)))
	}()
	<-entered // r1's flush has started and is held
	go func() {
		defer wg.Done()
		r2 = c.Submit(context.Background(), storetest.ReserveOps(rows[4:8], tok, clk.Now().Add(time.Minute)))
	}()
	require.Eventually(t, func() bool { return c.PendingOps() == 4 }, time.Second, time.Millisecond, "r2 queued")
	r3 := c.Submit(context.Background(), storetest.ReserveOps(rows[8:12], tok, clk.Now().Add(time.Minute)))
	require.ErrorIs(t, r3.Err, pool.ErrBackpressure)
	assert.True(t, r3.Unflushed, "rejected requests never touched the store")

	// A queued (not yet flushed) request that is cancelled is withdrawn.
	ctx, cancel := context.WithCancel(context.Background())
	var r4 pool.Result
	done := make(chan struct{})
	go func() {
		defer close(done)
		r4 = c.Submit(ctx, storetest.ReserveOps(rows[12:14], store.NewToken(), clk.Now().Add(time.Minute)))
	}()
	require.Eventually(t, func() bool { return c.PendingOps() == 6 }, time.Second, time.Millisecond, "r4 queued")
	cancel()
	<-done
	require.ErrorIs(t, r4.Err, context.Canceled)
	assert.True(t, r4.Unflushed)
	assert.Equal(t, 4, c.PendingOps(), "withdrawn request left the queue")

	close(release)
	wg.Wait()
	require.NoError(t, r1.Err)
	require.NoError(t, r2.Err)
	assert.Len(t, r1.Applied, 4)
	assert.Len(t, r2.Applied, 4)
	got := storetest.RowsByOutpoint(t, st, "main", storetest.Ops(rows[12:14]))
	for _, r := range rows[12:14] {
		assert.Equal(t, store.StateAvailable, got[r.Outpoint].State, "withdrawn request never reserved")
	}
}

func TestCoalescerAbandonedAfterFlushIsReleased(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	_, rows := storetest.MintShard(t, st, "main", 10, store.ShardMined)
	f := storetest.NewFaulty(st)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	f.BeforeReserve = func([]store.ReserveOp) { entered <- struct{}{}; <-release }
	c := startCoalescer(t, f, pool.CoalescerConfig{Slots: 1, MaxBatch: 64})

	ctx, cancel := context.WithCancel(context.Background())
	var res pool.Result
	done := make(chan struct{})
	tok := store.NewToken()
	go func() {
		defer close(done)
		res = c.Submit(ctx, storetest.ReserveOps(rows[:3], tok, clk.Now().Add(time.Minute)))
	}()
	<-entered // the flush has started (request cannot be withdrawn any more)
	cancel()
	<-done
	require.ErrorIs(t, res.Err, context.Canceled)
	assert.False(t, res.Unflushed, "the request reached the store")
	assert.Empty(t, res.Applied)
	close(release)

	require.Eventually(t, func() bool {
		got := storetest.RowsByOutpoint(t, st, "main", storetest.Ops(rows[:3]))
		for _, r := range rows[:3] {
			if got[r.Outpoint].State != store.StateAvailable {
				return false
			}
		}
		return true
	}, 2*time.Second, 5*time.Millisecond, "rows applied for an abandoned request are released")
}

func TestCoalescerRejectsBadBatches(t *testing.T) {
	st := memory.New()
	c := startCoalescer(t, st, pool.CoalescerConfig{Slots: 1, MaxBatch: 4})
	res := c.Submit(context.Background(), nil)
	require.ErrorIs(t, res.Err, pool.ErrBadRequest)
	assert.True(t, res.Unflushed)
	tok := store.NewToken()
	ops := make([]store.ReserveOp, 5)
	for i := range ops {
		ops[i] = store.ReserveOp{Outpoint: store.Outpoint{TxID: storetest.NewTxID(), Vout: 0}, Token: tok, ExpiresAt: time.Now()}
	}
	res = c.Submit(context.Background(), ops)
	require.ErrorIs(t, res.Err, pool.ErrBadRequest, "a request larger than MaxBatch is rejected")
	assert.ErrorIs(t, res.Err, pool.ErrBadRequest)
}

// TestCoalescerDuplicateAcrossRequests covers the defensive case where two
// different requests in the same flush batch name the same outpoint (the
// allocator's known set is supposed to make this impossible, but the
// coalescer must not hand the store a batch with a duplicate outpoint, and
// must not let both requests believe they won it).
func TestCoalescerDuplicateAcrossRequests(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	_, rows := storetest.MintShard(t, st, "main", 10, store.ShardMined)
	c := startCoalescer(t, st, pool.CoalescerConfig{Slots: 1, MaxBatch: 64, Window: 200 * time.Millisecond})

	shared := rows[0].Outpoint
	tokA := store.NewToken()
	tokB := store.NewToken()

	var wg sync.WaitGroup
	var resA, resB pool.Result
	wg.Add(1)
	go func() {
		defer wg.Done()
		resA = c.Submit(context.Background(), storetest.ReserveOps(rows[0:1], tokA, clk.Now().Add(time.Minute)))
	}()
	require.Eventually(t, func() bool { return c.PendingOps() == 1 }, time.Second, time.Millisecond, "request A queued")

	wg.Add(1)
	go func() {
		defer wg.Done()
		resB = c.Submit(context.Background(), storetest.ReserveOps(rows[0:1], tokB, clk.Now().Add(time.Minute)))
	}()
	require.Eventually(t, func() bool { return c.PendingOps() == 2 }, time.Second, time.Millisecond, "request B queued before the flush window elapses")
	wg.Wait()

	require.NoError(t, resA.Err)
	require.NoError(t, resB.Err)
	require.Len(t, resA.Applied, 1)
	assert.Equal(t, tokA, resA.Applied[0].Token)
	assert.Empty(t, resA.Missed)
	assert.Empty(t, resB.Applied)
	assert.Equal(t, []store.Outpoint{shared}, resB.Missed)

	got := storetest.RowsByOutpoint(t, st, "main", []store.Outpoint{shared})
	assert.Equal(t, tokA, got[shared].ReservedBy)
}

func TestCoalescerStoppedFailsQueuedAsUnflushed(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	_, rows := storetest.MintShard(t, st, "main", 10, store.ShardMined)
	f := storetest.NewFaulty(st)
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	f.BeforeReserve = func([]store.ReserveOp) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	}
	c := pool.NewCoalescer(f, "main", pool.CoalescerConfig{Slots: 1, MaxBatch: 64})
	runCtx, stop := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = c.Run(runCtx) }()

	var r1, r2 pool.Result
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		r1 = c.Submit(context.Background(), storetest.ReserveOps(rows[0:2], store.NewToken(), clk.Now().Add(time.Minute)))
	}()
	<-entered
	go func() {
		defer wg.Done()
		r2 = c.Submit(context.Background(), storetest.ReserveOps(rows[2:4], store.NewToken(), clk.Now().Add(time.Minute)))
	}()
	require.Eventually(t, func() bool { return c.PendingOps() == 2 }, time.Second, time.Millisecond)
	stop() // shut the dispatcher down while r1 is in flight and r2 is queued
	<-runDone
	close(release)
	wg.Wait()
	require.NoError(t, r1.Err, "in-flight flush settles on its own detached context")
	assert.Len(t, r1.Applied, 2)
	require.ErrorIs(t, r2.Err, context.Canceled)
	assert.True(t, r2.Unflushed, "queued requests fail as Unflushed on shutdown")
	r3 := c.Submit(context.Background(), storetest.ReserveOps(rows[4:6], store.NewToken(), clk.Now().Add(time.Minute)))
	require.ErrorIs(t, r3.Err, pool.ErrUnavailable)
	assert.True(t, r3.Unflushed)
}
