package pool_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
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

// countingBatcher wraps a memory store's SpendBatch, recording every call. A
// non-nil hold makes each call announce itself on entered and then wait for a
// receive on hold; fail, when set, decides a call's injected error (nil =
// delegate to the memory store).
type countingBatcher struct {
	*memory.Store
	mu      sync.Mutex
	calls   [][]store.SpendRequest
	entered chan struct{}
	hold    chan struct{}
	fail    func(call int, reqs []store.SpendRequest) error
	spends  int // direct Spend calls
}

func (c *countingBatcher) Spend(ctx context.Context, pool string, token store.Token, ops []store.Outpoint, txid chainhash.Hash, terminal bool) (int, error) {
	c.mu.Lock()
	c.spends++
	c.mu.Unlock()
	return c.Store.Spend(ctx, pool, token, ops, txid, terminal)
}

func (c *countingBatcher) Spends() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.spends
}

func newCountingBatcher(st *memory.Store) *countingBatcher {
	return &countingBatcher{Store: st, entered: make(chan struct{}, 64)}
}

func (c *countingBatcher) SpendBatch(ctx context.Context, pool string, reqs []store.SpendRequest) ([]int, error) {
	c.mu.Lock()
	c.calls = append(c.calls, append([]store.SpendRequest(nil), reqs...))
	call := len(c.calls)
	hold, fail := c.hold, c.fail
	c.mu.Unlock()
	if hold != nil {
		c.entered <- struct{}{}
		<-hold
	}
	if fail != nil {
		if err := fail(call, reqs); err != nil {
			return nil, err
		}
	}
	return c.Store.SpendBatch(ctx, pool, reqs)
}

func (c *countingBatcher) Calls() [][]store.SpendRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]store.SpendRequest(nil), c.calls...)
}

func startSpendCoalescer(t *testing.T, st store.SpendBatcher, cfg pool.SpendCoalescerConfig) *pool.SpendCoalescer {
	t.Helper()
	c := pool.NewSpendCoalescer(st, "main", cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return c
}

// reservedFixture mints a shard of n rows and reserves them two at a time
// under fresh tokens, returning one spend request per token.
func reservedFixture(t *testing.T, n int) (*storetest.Clock, *memory.Store, []store.SpendRequest) {
	t.Helper()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	_, rows := storetest.MintShard(t, st, "main", n, store.ShardMined)
	var reqs []store.SpendRequest
	for i := 0; i+1 < n; i += 2 {
		tok := store.NewToken()
		storetest.MustReserve(t, st, "main", rows[i:i+2], tok, clk.Now().Add(time.Minute))
		reqs = append(reqs, store.SpendRequest{Token: tok, Ops: storetest.Ops(rows[i : i+2]), TxID: storetest.NewTxID(), Terminal: true})
	}
	return clk, st, reqs
}

// waitPending waits until the coalescer holds n queued requests.
func waitPending(t *testing.T, c *pool.SpendCoalescer, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return c.PendingRequests() == n }, 5*time.Second, time.Millisecond)
}

func TestSpendCoalescerBatchesConcurrentRequests(t *testing.T) {
	_, st, reqs := reservedFixture(t, 100)
	cb := newCountingBatcher(st)
	cb.hold = make(chan struct{})
	c := startSpendCoalescer(t, cb, pool.SpendCoalescerConfig{Slots: 1, MaxBatch: 1000})

	counts := make([]int, len(reqs))
	errs := make([]error, len(reqs))
	var wg sync.WaitGroup
	submit := func(i int) {
		defer wg.Done()
		counts[i], errs[i] = c.Submit(context.Background(), reqs[i])
	}
	wg.Add(1)
	go submit(0)
	<-cb.entered // the first request is in flight, holding the only slot
	for i := 1; i < len(reqs); i++ {
		wg.Add(1)
		go submit(i)
	}
	waitPending(t, c, len(reqs)-1) // everyone else queued behind it
	close(cb.hold)
	wg.Wait()

	for i := range reqs {
		require.NoError(t, errs[i], "request %d", i)
		assert.Equal(t, 2, counts[i], "request %d", i)
	}
	calls := cb.Calls()
	assert.Len(t, calls, 2, "the requests that queued behind the first flush share one flush")
	assert.Len(t, calls[1], len(reqs)-1)
	stats, err := st.Stats(context.Background(), "main")
	require.NoError(t, err)
	assert.Equal(t, int64(100), stats.Spent)
}

func TestSpendCoalescerRoutesPerRequestResults(t *testing.T) {
	_, st, reqs := reservedFixture(t, 8)
	cb := newCountingBatcher(st)
	cb.hold = make(chan struct{})
	c := startSpendCoalescer(t, cb, pool.SpendCoalescerConfig{Slots: 1})

	wrong := reqs[1]
	wrong.Token = store.NewToken() // holds nothing
	partial := reqs[2]
	partial.Ops = append(append([]store.Outpoint(nil), partial.Ops...), reqs[3].Ops[0]) // one op held by another token
	dupOfFirst := reqs[0]                                                               // the same rows again: the earlier request wins
	batch := []store.SpendRequest{reqs[0], wrong, partial, dupOfFirst}
	want := []int{2, 0, 2, 0}

	// Park a throwaway flush so the four requests below all queue into one batch.
	go func() { _, _ = c.Submit(context.Background(), reqs[3]) }()
	<-cb.entered
	got := make([]int, len(batch))
	var wg sync.WaitGroup
	for i := range batch {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := c.Submit(context.Background(), batch[i])
			assert.NoError(t, err)
			got[i] = n
		}()
		waitPending(t, c, i+1) // keep slice order inside the batch
	}
	close(cb.hold)
	wg.Wait()
	assert.Equal(t, want, got)
	require.Len(t, cb.Calls(), 2)
}

func TestSpendCoalescerFailingFlushFailsOnlyItsRequests(t *testing.T) {
	_, st, reqs := reservedFixture(t, 10)
	cb := newCountingBatcher(st)
	cb.hold = make(chan struct{})
	boom := errors.New("boom")
	cb.fail = func(call int, _ []store.SpendRequest) error {
		if call == 1 {
			return boom // not ErrUnavailable: not retried
		}
		return nil
	}
	c := startSpendCoalescer(t, cb, pool.SpendCoalescerConfig{Slots: 1})

	firstErr := make(chan error, 1)
	go func() { _, err := c.Submit(context.Background(), reqs[0]); firstErr <- err }()
	<-cb.entered
	var wg sync.WaitGroup
	errs := make([]error, len(reqs))
	for i := 1; i < len(reqs); i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, errs[i] = c.Submit(context.Background(), reqs[i]) }()
	}
	waitPending(t, c, len(reqs)-1)
	close(cb.hold)
	wg.Wait()
	require.ErrorIs(t, <-firstErr, boom)
	for i := 1; i < len(reqs); i++ {
		require.NoError(t, errs[i], "request %d was not in the failed flush", i)
	}
	assert.Len(t, cb.Calls(), 2)
}

func TestSpendCoalescerRetriesUnavailable(t *testing.T) {
	_, st, reqs := reservedFixture(t, 4)
	cb := newCountingBatcher(st)
	cb.fail = func(call int, _ []store.SpendRequest) error {
		if call == 1 {
			return fmt.Errorf("%w: flaky", store.ErrUnavailable)
		}
		return nil
	}
	c := startSpendCoalescer(t, cb, pool.SpendCoalescerConfig{Slots: 1, RetryBackoff: time.Millisecond})
	n, err := c.Submit(context.Background(), reqs[0])
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Len(t, cb.Calls(), 2, "one failed attempt, one retry")
}

func TestSpendCoalescerGivesUpAfterRetries(t *testing.T) {
	_, st, reqs := reservedFixture(t, 4)
	cb := newCountingBatcher(st)
	cb.fail = func(int, []store.SpendRequest) error { return fmt.Errorf("%w: down", store.ErrUnavailable) }
	c := startSpendCoalescer(t, cb, pool.SpendCoalescerConfig{Slots: 1, Retries: 2, RetryBackoff: time.Millisecond})
	_, err := c.Submit(context.Background(), reqs[0])
	require.ErrorIs(t, err, store.ErrUnavailable)
	assert.Len(t, cb.Calls(), 3)
}

func TestSpendCoalescerWithdrawsCancelledRequests(t *testing.T) {
	_, st, reqs := reservedFixture(t, 6)
	cb := newCountingBatcher(st)
	cb.hold = make(chan struct{})
	c := startSpendCoalescer(t, cb, pool.SpendCoalescerConfig{Slots: 1})

	first := make(chan error, 1)
	go func() { _, err := c.Submit(context.Background(), reqs[0]); first <- err }()
	<-cb.entered

	ctx, cancel := context.WithCancel(context.Background())
	withdrawn := make(chan error, 1)
	go func() { _, err := c.Submit(ctx, reqs[1]); withdrawn <- err }()
	waitPending(t, c, 1)
	cancel()
	require.ErrorIs(t, <-withdrawn, context.Canceled)
	assert.Equal(t, 0, c.PendingRequests(), "the cancelled request left the queue")

	close(cb.hold)
	require.NoError(t, <-first)
	n, err := c.Submit(context.Background(), reqs[2])
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	for _, call := range cb.Calls() {
		for _, r := range call {
			assert.NotEqual(t, reqs[1].Token, r.Token, "a withdrawn request never reaches the store")
		}
	}
	got := storetest.RowsByOutpoint(t, st, "main", reqs[1].Ops)
	for _, r := range got {
		assert.Equal(t, store.StateReserved, r.State)
	}
}

func TestSpendCoalescerCancelledInFlightGetsFlushResult(t *testing.T) {
	_, st, reqs := reservedFixture(t, 2)
	cb := newCountingBatcher(st)
	cb.hold = make(chan struct{})
	c := startSpendCoalescer(t, cb, pool.SpendCoalescerConfig{Slots: 1})
	ctx, cancel := context.WithCancel(context.Background())
	type res struct {
		n   int
		err error
	}
	out := make(chan res, 1)
	go func() { n, err := c.Submit(ctx, reqs[0]); out <- res{n, err} }()
	<-cb.entered
	cancel()
	close(cb.hold)
	r := <-out
	require.NoError(t, r.err, "once in a flush the caller gets the flush's result")
	assert.Equal(t, 2, r.n)
}

func TestSpendCoalescerBackpressure(t *testing.T) {
	_, st, reqs := reservedFixture(t, 20)
	cb := newCountingBatcher(st)
	cb.hold = make(chan struct{})
	c := startSpendCoalescer(t, cb, pool.SpendCoalescerConfig{Slots: 1, MaxBatch: 4, MaxInflightOps: 6})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _, _ = c.Submit(context.Background(), reqs[0]) }() // 2 ops in flight
	<-cb.entered
	for i := 1; i <= 2; i++ { // 4 more queued: 6 in flight + queued
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = c.Submit(context.Background(), reqs[i]) }()
		waitPending(t, c, i)
	}
	_, err := c.Submit(context.Background(), reqs[3])
	require.ErrorIs(t, err, pool.ErrBackpressure)
	close(cb.hold)
	wg.Wait()
}

func TestSpendCoalescerRejectsInvalidRequests(t *testing.T) {
	_, st, reqs := reservedFixture(t, 4)
	cb := newCountingBatcher(st)
	c := startSpendCoalescer(t, cb, pool.SpendCoalescerConfig{Slots: 1})
	dup := reqs[0]
	dup.Ops = []store.Outpoint{reqs[0].Ops[0], reqs[0].Ops[0]}
	for name, r := range map[string]store.SpendRequest{
		"duplicate outpoint":      dup,
		"empty ops":               {Token: reqs[0].Token, TxID: reqs[0].TxID},
		"zero token":              {Ops: reqs[0].Ops, TxID: reqs[0].TxID},
		"terminal with zero txid": {Token: reqs[0].Token, Ops: reqs[0].Ops, Terminal: true, TxID: chainhash.Hash{}},
	} {
		_, err := c.Submit(context.Background(), r)
		require.ErrorIs(t, err, pool.ErrBadRequest, name)
	}
	assert.Empty(t, cb.Calls(), "an invalid request never reaches the store")
}

func TestSpendCoalescerStoppedIsUnavailable(t *testing.T) {
	_, st, reqs := reservedFixture(t, 4)
	cb := newCountingBatcher(st)
	c := pool.NewSpendCoalescer(cb, "main", pool.SpendCoalescerConfig{Slots: 1})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, c.Run(ctx))
	_, err := c.Submit(context.Background(), reqs[0])
	require.ErrorIs(t, err, pool.ErrUnavailable)
}

// noBatchStore hides a store's SpendBatch, like a backend without one.
type noBatchStore struct{ store.Store }

// startPool runs a pool named poolName over st, with 4 mined 2-row shards'
// worth of rows minted in the memory store under it.
func startPool(t *testing.T, st store.Store, mem *memory.Store, clk *storetest.Clock, poolName string, mutate func(*pool.Config)) *pool.Pool {
	t.Helper()
	storetest.MintShard(t, mem, poolName, 8, store.ShardMined)
	pc := pool.DefaultPoolConfig(poolName, 50)
	pc.MinTTL = time.Second
	if mutate != nil {
		mutate(&pc)
	}
	require.NoError(t, pc.Validate())
	p := pool.NewPool(st, pc, testInstanceConfig(), pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return p
}

func TestPoolSpendGoesThroughSpendBatch(t *testing.T) {
	const poolName = "spend-batch-pool"
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	mem := memory.New(memory.WithClock(clk.Now))
	cb := newCountingBatcher(mem)
	p := startPool(t, cb, mem, clk, poolName, func(c *pool.Config) { c.SpendPolicy = pool.SpendTrust })

	sizeBefore, sizeSumBefore := histObserved(t, pool.SpendBatchSize, poolName)
	flushBefore, _ := histObserved(t, pool.SpendFlushSeconds, poolName)
	opBefore, _ := histObserved(t, pool.StoreOpLatency, poolName, "spend_batch")

	res, err := p.Reserve(ctx, pool.ReserveRequest{Count: 2})
	require.NoError(t, err)
	n, err := p.Spend(ctx, res.Token, outpointsOf(res.Outputs), storetest.NewTxID())
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	require.Len(t, cb.Calls(), 1)
	assert.Equal(t, 0, cb.Spends(), "no direct Spend when the store batches")
	got := storetest.RowsByOutpoint(t, mem, poolName, outpointsOf(res.Outputs))
	for _, o := range res.Outputs {
		assert.Equal(t, store.StateSpent, got[o.Outpoint].State)
	}

	size, sizeSum := histObserved(t, pool.SpendBatchSize, poolName)
	assert.Equal(t, sizeBefore+1, size)
	assert.InDelta(t, sizeSumBefore+1, sizeSum, 0, "one request in the flush")
	flush, _ := histObserved(t, pool.SpendFlushSeconds, poolName)
	assert.Equal(t, flushBefore+1, flush)
	op, _ := histObserved(t, pool.StoreOpLatency, poolName, "spend_batch")
	assert.Equal(t, opBefore+1, op, "store_op_latency_seconds{op=spend_batch} is observed per flush attempt")

	// A request Spend itself would reject is a bad request and never reaches the store.
	dup := []store.Outpoint{res.Outputs[0].Outpoint, res.Outputs[0].Outpoint}
	_, err = p.Spend(ctx, res.Token, dup, storetest.NewTxID())
	require.ErrorIs(t, err, pool.ErrBadRequest)
	assert.Len(t, cb.Calls(), 1)
}

func TestPoolVerifyReleaseGoesThroughSpendBatch(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	mem := memory.New(memory.WithClock(clk.Now))
	cb := newCountingBatcher(mem)
	p := startPool(t, cb, mem, clk, "spend-batch-release", func(c *pool.Config) { c.ReleasePolicy = pool.ReleaseVerify })

	res, err := p.Reserve(ctx, pool.ReserveRequest{Count: 2})
	require.NoError(t, err)
	n, err := p.Release(ctx, res.Token, outpointsOf(res.Outputs))
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	calls := cb.Calls()
	require.Len(t, calls, 1)
	require.Len(t, calls[0], 1)
	assert.Equal(t, chainhash.Hash{}, calls[0][0].TxID, "verify release is a non-terminal spend with no hint")
	assert.False(t, calls[0][0].Terminal)
	got := storetest.RowsByOutpoint(t, mem, "spend-batch-release", outpointsOf(res.Outputs))
	for _, o := range res.Outputs {
		assert.Equal(t, store.StateQuarantined, got[o.Outpoint].State)
		assert.Nil(t, got[o.Outpoint].SpentHint)
	}
}

func TestPoolSpendFallsBackWithoutSpendBatcher(t *testing.T) {
	const poolName = "spend-batch-unsupported"
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	mem := memory.New(memory.WithClock(clk.Now))
	p := startPool(t, noBatchStore{mem}, mem, clk, poolName, nil)
	before := testutil.ToFloat64(pool.SpendFallback.WithLabelValues(poolName, "unsupported"))

	res, err := p.Reserve(ctx, pool.ReserveRequest{Count: 2})
	require.NoError(t, err)
	n, err := p.Spend(ctx, res.Token, outpointsOf(res.Outputs), storetest.NewTxID())
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.InDelta(t, before+1, testutil.ToFloat64(pool.SpendFallback.WithLabelValues(poolName, "unsupported")), 0)
}

func TestPoolSpendBatchingDisabled(t *testing.T) {
	const poolName = "spend-batch-disabled"
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	mem := memory.New(memory.WithClock(clk.Now))
	cb := newCountingBatcher(mem)
	p := startPool(t, cb, mem, clk, poolName, func(c *pool.Config) { c.SpendBatching.Enabled = false })
	before := testutil.ToFloat64(pool.SpendFallback.WithLabelValues(poolName, "disabled"))

	res, err := p.Reserve(ctx, pool.ReserveRequest{Count: 2})
	require.NoError(t, err)
	n, err := p.Spend(ctx, res.Token, outpointsOf(res.Outputs), storetest.NewTxID())
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Empty(t, cb.Calls())
	assert.Equal(t, 1, cb.Spends())
	assert.InDelta(t, before+1, testutil.ToFloat64(pool.SpendFallback.WithLabelValues(poolName, "disabled")), 0)
}
