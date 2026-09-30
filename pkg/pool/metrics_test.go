package pool_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// startCoalescerNamed is startCoalescer (coalescer_test.go) parameterized on
// pool name, so each metrics test can use a label value unique to it and
// avoid interference with other tests sharing the default registry.
func startCoalescerNamed(t *testing.T, st store.Store, poolName string, cfg pool.CoalescerConfig) *pool.Coalescer {
	t.Helper()
	c := pool.NewCoalescer(st, poolName, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return c
}

// histObserved returns a HistogramVec series' sample count and sum via the
// standard Metric.Write path (testutil.ToFloat64 does not support
// histograms).
func histObserved(t *testing.T, hv *prometheus.HistogramVec, lbls ...string) (count uint64, sum float64) {
	t.Helper()
	var m dto.Metric
	metric, ok := hv.WithLabelValues(lbls...).(prometheus.Metric)
	require.True(t, ok, "HistogramVec.WithLabelValues must return a prometheus.Metric")
	require.NoError(t, metric.Write(&m))
	h := m.GetHistogram()
	return h.GetSampleCount(), h.GetSampleSum()
}

func newShardManager(st store.Store, poolName string, clk *storetest.Clock, mutate func(*pool.InstanceConfig)) (*pool.ShardManager, *pool.Allocator) {
	pc := pool.DefaultPoolConfig(poolName, 50)
	cfg := pool.DefaultInstanceConfig()
	cfg.InstanceID = "metrics-" + poolName
	if mutate != nil {
		mutate(&cfg)
	}
	alloc := pool.NewAllocator()
	return pool.NewShardManager(st, pc, cfg, alloc, pool.ClockFunc(clk.Now), slog.Default()), alloc
}

// TestMetricsCoalescerFlushObservesBatchLatencyAndInflight covers the
// coalescer's group-commit metrics: a single flush of N ops observes batch
// size N, one flush-latency sample, one store_op_latency_seconds{op=reserve}
// sample, and the inflight gauge returns to 0 once the flush settles.
func TestMetricsCoalescerFlushObservesBatchLatencyAndInflight(t *testing.T) {
	const poolName = "metrics-flush-batch"
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	_, rows := storetest.MintShard(t, st, poolName, 5, store.ShardMined)
	c := startCoalescerNamed(t, st, poolName, pool.CoalescerConfig{Slots: 1, MaxBatch: 64})

	batchBefore, batchSumBefore := histObserved(t, pool.CoalescerBatchSize, poolName)
	flushBefore, _ := histObserved(t, pool.CoalescerFlushLatency, poolName)
	reserveBefore, _ := histObserved(t, pool.StoreOpLatency, poolName, "reserve")

	tok := store.NewToken()
	res := c.Submit(context.Background(), storetest.ReserveOps(rows, tok, clk.Now().Add(time.Minute)))
	require.NoError(t, res.Err)
	assert.Len(t, res.Applied, 5)

	// Compare against the running total captured just above, not a literal:
	// under -count>1 this test function runs repeatedly in the same process,
	// and poolName's histogram series (like any metric on the shared default
	// registry) keeps accumulating across those runs.
	batchCount, batchSum := histObserved(t, pool.CoalescerBatchSize, poolName)
	assert.Equal(t, batchBefore+1, batchCount, "one flush carried the whole request")
	assert.InDelta(t, batchSumBefore+5, batchSum, 0, "the flush's batch size was 5 ops")

	flushCount, _ := histObserved(t, pool.CoalescerFlushLatency, poolName)
	assert.Equal(t, flushBefore+1, flushCount)

	reserveCount, _ := histObserved(t, pool.StoreOpLatency, poolName, "reserve")
	assert.Equal(t, reserveBefore+1, reserveCount, "one store.Reserve call for the one attempt")

	// Submit returns as soon as deliver() closes req.done, which happens
	// inside flush() before flush's own deferred inflight decrement runs (LIFO
	// defers): there is a real, if narrow, window where Submit has returned
	// but the gauge has not yet settled back to 0. Poll instead of asserting
	// immediately.
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(pool.CoalescerInflightOps.WithLabelValues(poolName)) == 0
	}, time.Second, time.Millisecond,
		"inflight drops back to 0 once the flush that carried every op has settled")
}

// TestMetricsCASUnknownAndCompensatingRelease covers the in-doubt path: once
// retries are exhausted the coalescer must report every sent op as
// cas-unknown and issue (and count) a compensating Release.
func TestMetricsCASUnknownAndCompensatingRelease(t *testing.T) {
	const poolName = "metrics-compensate"
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	_, rows := storetest.MintShard(t, st, poolName, 3, store.ShardMined)
	f := storetest.NewFaulty(st)
	f.InDoubtEvery(1) // every attempt commits but reports ErrUnavailable
	c := startCoalescerNamed(t, f, poolName, pool.CoalescerConfig{Slots: 1, MaxBatch: 64, Retries: 1, RetryBackoff: time.Millisecond})

	unknownBefore := testutil.ToFloat64(pool.CASUnknown.WithLabelValues(poolName))
	compBefore := testutil.ToFloat64(pool.CompensatingRelease.WithLabelValues(poolName))

	tok := store.NewToken()
	res := c.Submit(context.Background(), storetest.ReserveOps(rows, tok, clk.Now().Add(time.Minute)))
	require.ErrorIs(t, res.Err, pool.ErrUnavailable)
	assert.Empty(t, res.Applied)

	assert.InDelta(t, unknownBefore+3, testutil.ToFloat64(pool.CASUnknown.WithLabelValues(poolName)), 0,
		"all 3 ops in the flush were in doubt after retries were exhausted")
	assert.InDelta(t, compBefore+1, testutil.ToFloat64(pool.CompensatingRelease.WithLabelValues(poolName)), 0,
		"one compensating Release call for the batch's single token")

	got := storetest.RowsByOutpoint(t, st, poolName, storetest.Ops(rows))
	for _, r := range rows {
		assert.Equal(t, store.StateAvailable, got[r.Outpoint].State, "the compensating release undid the hidden commit")
	}
}

// TestMetricsCompensatingReleaseNotCountedOnAbandonedSuccess covers a caller
// that cancels after its request was already taken by a flush that goes on to
// succeed: the coalescer releases the now-unwanted applied rows via
// releaseApplied, a courtesy release for an abandoned caller, not a
// compensating one (the store's outcome was never in doubt), so
// compensating_release_total must not move.
func TestMetricsCompensatingReleaseNotCountedOnAbandonedSuccess(t *testing.T) {
	const poolName = "metrics-abandoned-success"
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	_, rows := storetest.MintShard(t, st, poolName, 3, store.ShardMined)
	f := storetest.NewFaulty(st)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	f.BeforeReserve = func([]store.ReserveOp) { entered <- struct{}{}; <-release }
	c := startCoalescerNamed(t, f, poolName, pool.CoalescerConfig{Slots: 1, MaxBatch: 64})

	compBefore := testutil.ToFloat64(pool.CompensatingRelease.WithLabelValues(poolName))

	ctx, cancel := context.WithCancel(context.Background())
	var res pool.Result
	done := make(chan struct{})
	tok := store.NewToken()
	go func() {
		defer close(done)
		res = c.Submit(ctx, storetest.ReserveOps(rows, tok, clk.Now().Add(time.Minute)))
	}()
	<-entered // the flush has started; the request can no longer be withdrawn
	cancel()
	<-done
	require.ErrorIs(t, res.Err, context.Canceled)
	assert.False(t, res.Unflushed, "the request reached the store")
	close(release) // let the flush finish; it succeeds (no fault armed)

	require.Eventually(t, func() bool {
		got := storetest.RowsByOutpoint(t, st, poolName, storetest.Ops(rows))
		for _, r := range rows {
			if got[r.Outpoint].State != store.StateAvailable {
				return false
			}
		}
		return true
	}, 2*time.Second, 5*time.Millisecond, "rows applied for an abandoned request are released back")

	assert.InDelta(t, compBefore, testutil.ToFloat64(pool.CompensatingRelease.WithLabelValues(poolName)), 0,
		"a courtesy release after abandonment is not a compensating release")
}

// TestMetricsShardAdoptions covers shard_adoptions_total{result}: "none" when
// nothing is adoptable, "ok" once a mined shard is claimed.
func TestMetricsShardAdoptions(t *testing.T) {
	const poolName = "metrics-adopt"
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	m, _ := newShardManager(st, poolName, clk, nil)

	noneBefore := testutil.ToFloat64(pool.ShardAdoptions.WithLabelValues(poolName, "none"))
	adopted, err := m.AdoptOnce(ctx)
	require.NoError(t, err)
	assert.False(t, adopted)
	assert.InDelta(t, noneBefore+1, testutil.ToFloat64(pool.ShardAdoptions.WithLabelValues(poolName, "none")), 0)

	storetest.MintShard(t, st, poolName, 3, store.ShardMined)
	okBefore := testutil.ToFloat64(pool.ShardAdoptions.WithLabelValues(poolName, "ok"))
	adopted, err = m.AdoptOnce(ctx)
	require.NoError(t, err)
	assert.True(t, adopted)
	assert.InDelta(t, okBefore+1, testutil.ToFloat64(pool.ShardAdoptions.WithLabelValues(poolName, "ok")), 0)
}

// acquireErrStore forces AcquireShard to fail, to exercise
// shard_adoptions_total{result="error"}.
type acquireErrStore struct {
	store.Store
	err error
}

func (s *acquireErrStore) AcquireShard(context.Context, string, string, time.Time, time.Time, []store.ShardStatus, []chainhash.Hash) (*store.Shard, error) {
	return nil, s.err
}

func TestMetricsShardAdoptionError(t *testing.T) {
	const poolName = "metrics-adopt-error"
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	wrapped := &acquireErrStore{Store: base, err: store.ErrUnavailable}
	m, _ := newShardManager(wrapped, poolName, clk, nil)

	errBefore := testutil.ToFloat64(pool.ShardAdoptions.WithLabelValues(poolName, "error"))
	_, err := m.AdoptOnce(ctx)
	require.ErrorIs(t, err, store.ErrUnavailable)
	assert.InDelta(t, errBefore+1, testutil.ToFloat64(pool.ShardAdoptions.WithLabelValues(poolName, "error")), 0)
}

// TestMetricsShardDropReasons drives the shard manager through lease loss
// (not_owner) and a status no longer served (retired) and checks each lands
// under the right shard_drops_total reason.
func TestMetricsShardDropReasons(t *testing.T) {
	const poolName = "metrics-drop"
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	a, _ := storetest.MintShard(t, st, poolName, 3, store.ShardMined)
	clk.Advance(time.Second)
	b, _ := storetest.MintShard(t, st, poolName, 3, store.ShardMined)
	m, alloc := newShardManager(st, poolName, clk, func(c *pool.InstanceConfig) {
		c.MaxShards = 2
		c.LowWaterRows = 10 // stay hungry so one FeedOnce adopts both shards
	})
	require.NoError(t, m.FeedOnce(ctx))
	require.Len(t, alloc.Shards(), 2)

	notOwnerBefore := testutil.ToFloat64(pool.ShardDrops.WithLabelValues(poolName, "not_owner"))
	retiredBefore := testutil.ToFloat64(pool.ShardDrops.WithLabelValues(poolName, "retired"))

	// Another instance steals shard a once its lease has expired (never renewed here).
	clk.Advance(10 * time.Minute)
	got, err := st.AcquireShard(ctx, poolName, "other/1", clk.Now(), clk.Now().Add(time.Minute), []store.ShardStatus{store.ShardMined}, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, a.TxID, got.TxID)

	// UpdateShard (unlike RetireShard) leaves the lease untouched, so RenewOnce
	// succeeds and only then finds the new status unservable.
	require.NoError(t, st.UpdateShard(ctx, poolName, b.TxID, store.ShardRetired, nil))

	require.NoError(t, m.RenewOnce(ctx))
	assert.InDelta(t, notOwnerBefore+1, testutil.ToFloat64(pool.ShardDrops.WithLabelValues(poolName, "not_owner")), 0)
	assert.InDelta(t, retiredBefore+1, testutil.ToFloat64(pool.ShardDrops.WithLabelValues(poolName, "retired")), 0)
	assert.False(t, alloc.Has(a.TxID))
	assert.False(t, alloc.Has(b.TxID))
}

// TestMetricsShardDropEmptyOnAdopt covers the "empty" reason: a shard adopted
// with nothing left available (raced out from under it) is dropped at once.
func TestMetricsShardDropEmptyOnAdopt(t *testing.T) {
	const poolName = "metrics-drop-empty"
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	_, rows := storetest.MintShard(t, st, poolName, 3, store.ShardMined)
	storetest.MustReserve(t, st, poolName, rows, store.NewToken(), clk.Now().Add(time.Minute))
	m, _ := newShardManager(st, poolName, clk, nil)

	before := testutil.ToFloat64(pool.ShardDrops.WithLabelValues(poolName, "empty"))
	adopted, err := m.AdoptOnce(ctx)
	require.NoError(t, err)
	assert.False(t, adopted, "the adopted shard turned out to have nothing available")
	assert.InDelta(t, before+1, testutil.ToFloat64(pool.ShardDrops.WithLabelValues(poolName, "empty")), 0)
}

// TestMetricsShardDropClosedOnReleaseAll covers the "closed" reason: graceful
// shutdown releases every owned shard.
func TestMetricsShardDropClosedOnReleaseAll(t *testing.T) {
	const poolName = "metrics-drop-closed"
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	storetest.MintShard(t, st, poolName, 3, store.ShardMined)
	m, alloc := newShardManager(st, poolName, clk, nil)
	require.NoError(t, m.FeedOnce(ctx))
	require.Len(t, alloc.Shards(), 1)

	before := testutil.ToFloat64(pool.ShardDrops.WithLabelValues(poolName, "closed"))
	require.NoError(t, m.ReleaseAll(ctx))
	assert.InDelta(t, before+1, testutil.ToFloat64(pool.ShardDrops.WithLabelValues(poolName, "closed")), 0)
}

// renewErrStore forces RenewShard to fail with a plain (non-lease-loss)
// error, to exercise lease_renew_failures_total without dropping the shard.
type renewErrStore struct {
	store.Store
	err error
}

func (s *renewErrStore) RenewShard(context.Context, string, chainhash.Hash, string, time.Time) (store.ShardHead, error) {
	return store.ShardHead{}, s.err
}

func TestMetricsLeaseRenewFailures(t *testing.T) {
	const poolName = "metrics-renew-fail"
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	sh, _ := storetest.MintShard(t, base, poolName, 3, store.ShardMined)
	wrapped := &renewErrStore{Store: base, err: errors.New("transient renew failure")}
	m, alloc := newShardManager(wrapped, poolName, clk, nil)
	require.NoError(t, m.FeedOnce(ctx))
	require.True(t, alloc.Has(sh.TxID))

	before := testutil.ToFloat64(pool.LeaseRenewFailures.WithLabelValues(poolName))
	require.Error(t, m.RenewOnce(ctx))
	assert.InDelta(t, before+1, testutil.ToFloat64(pool.LeaseRenewFailures.WithLabelValues(poolName)), 0)
	assert.True(t, alloc.Has(sh.TxID), "a freshly-renewed lease is not yet stale, so the shard is kept")
}

// TestMetricsShardDropStale drives RenewOnce's stale-lease branch: a shard
// whose renews keep failing transiently (never ErrNotOwner/ErrNotFound, which
// would be lease loss, not failure) is dropped once its lease has been
// expired for more than one full TTL, and that drop must land under
// shard_drops_total{reason="stale"}, not some other reason.
func TestMetricsShardDropStale(t *testing.T) {
	const poolName = "metrics-drop-stale"
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	sh, _ := storetest.MintShard(t, base, poolName, 3, store.ShardMined)
	wrapped := &renewErrStore{Store: base, err: errors.New("renew store down")}
	m, alloc := newShardManager(wrapped, poolName, clk, nil)
	require.NoError(t, m.FeedOnce(ctx))
	require.True(t, alloc.Has(sh.TxID))

	// leaseUntil = adoptNow + LeaseTTL; stale requires now - leaseUntil >
	// LeaseTTL, i.e. now > adoptNow + 2*LeaseTTL. 3*LeaseTTL clears that with
	// margin.
	clk.Advance(3 * pool.DefaultInstanceConfig().LeaseTTL)

	staleBefore := testutil.ToFloat64(pool.ShardDrops.WithLabelValues(poolName, "stale"))
	notOwnerBefore := testutil.ToFloat64(pool.ShardDrops.WithLabelValues(poolName, "not_owner"))
	require.Error(t, m.RenewOnce(ctx))
	assert.InDelta(t, staleBefore+1, testutil.ToFloat64(pool.ShardDrops.WithLabelValues(poolName, "stale")), 0)
	assert.InDelta(t, notOwnerBefore, testutil.ToFloat64(pool.ShardDrops.WithLabelValues(poolName, "not_owner")), 0,
		"a transient renew failure is not lease loss; must not be mislabelled not_owner")
	assert.False(t, alloc.Has(sh.TxID), "a lease stale under a permanently failing renew is dropped")
}
