package jobs_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/jobs"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury/mock"
)

func newMinter(t *testing.T, st store.Store, tr treasury.Treasury, pcs []pool.Config, cfg jobs.MinterConfig, clk *storetest.Clock) *jobs.Minter {
	t.Helper()
	m, err := jobs.NewMinter(st, tr, pcs, cfg, pool.ClockFunc(clk.Now), slog.Default())
	require.NoError(t, err)
	return m
}

// mustListAvailable lists every shard of pool and pages ListAvailable for
// each, returning every available row.
func mustListAvailable(t *testing.T, st store.Store, poolName string) []store.Row {
	t.Helper()
	ctx := context.Background()
	shards, err := st.ListShards(ctx, poolName, store.ShardFilter{})
	require.NoError(t, err)
	var rows []store.Row
	for _, sh := range shards {
		var fromVout uint32
		for {
			page, err := st.ListAvailable(ctx, poolName, sh.TxID, fromVout, 100)
			require.NoError(t, err)
			rows = append(rows, page...)
			if len(page) < 100 {
				break
			}
			fromVout = page[len(page)-1].Vout + 1
		}
	}
	return rows
}

func TestMinterFillsToTargetAndStops(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	tr := mock.New()
	pc := pool.DefaultPoolConfig("main", 50)
	pc.SplitSize = 10
	pc.TargetAvailable = 45
	pc.LowWater = 35
	cfg := jobs.DefaultMinterConfig()
	cfg.MaxPendingShards = 3
	m := newMinter(t, st, tr, []pool.Config{pc}, cfg, clk)

	require.NoError(t, m.RunOnce(ctx))
	stats, err := st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, int64(30), stats.Available, "capped by MaxPendingShards (3 × 10)")
	assert.Equal(t, int64(3), stats.ShardsUnmined)
	assert.Equal(t, 3, tr.Built())

	require.NoError(t, m.RunOnce(ctx))
	assert.Equal(t, 3, tr.Built(), "pending cap holds while shards are unmined")

	shards, err := st.ListShards(ctx, "main", store.ShardFilter{})
	require.NoError(t, err)
	for _, sh := range shards {
		require.NoError(t, st.UpdateShard(ctx, "main", sh.TxID, store.ShardMined, sh.BEEF))
	}
	require.NoError(t, m.RunOnce(ctx))
	stats, err = st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, int64(50), stats.Available, "30 < low_water 35 triggers → deficit 15 → two more splits of 10")
	assert.Equal(t, 5, tr.Built())

	require.NoError(t, m.RunOnce(ctx))
	assert.Equal(t, 5, tr.Built(), "at or above low_water: nothing minted")

	all, err := st.ListShards(ctx, "main", store.ShardFilter{})
	require.NoError(t, err)
	require.Len(t, all, 5)
	for _, sh := range all {
		assert.True(t, tr.Sent(sh.TxID), "every recorded shard was sent")
		assert.Nil(t, tr.LastSendBEEF(sh.TxID), "a first send passes no BEEF: the treasury's wallet posts it")
	}
}

// TestMinterLowWaterHysteresis checks that LowWater is a hysteresis threshold,
// not a second target: while available sits anywhere in [LowWater,
// TargetAvailable) the minter must not split at all, even though a plain
// available < target check would see a deficit.
func TestMinterLowWaterHysteresis(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	tr := mock.New()
	pc := pool.DefaultPoolConfig("main", 50)
	pc.SplitSize = 10
	pc.TargetAvailable = 45
	pc.LowWater = 35
	cfg := jobs.DefaultMinterConfig()
	m := newMinter(t, st, tr, []pool.Config{pc}, cfg, clk)

	// Seed 40 available rows directly: between LowWater (35) and
	// TargetAvailable (45), so there is a deficit (5) but no trigger.
	_, rows := storetest.MintShard(t, st, "main", 40, store.ShardMined)
	require.Len(t, rows, 40)

	require.NoError(t, m.RunOnce(ctx))
	stats, err := st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, int64(40), stats.Available, "available in [low_water, target): nothing minted")
	assert.Equal(t, 0, tr.Built())
	assert.Equal(t, 0, tr.BuildAttempts())
}

func TestMinterOrderingAndFailures(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	tr := mock.New()
	pc := pool.DefaultPoolConfig("main", 50)
	pc.SplitSize = 5
	pc.TargetAvailable = 5
	cfg := jobs.DefaultMinterConfig()
	m := newMinter(t, st, tr, []pool.Config{pc}, cfg, clk)

	tr.FailBuild(treasury.ErrInsufficientFunds)
	require.NoError(t, m.RunOnce(ctx), "insufficient funds is logged, not fatal")
	stats, err := st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats.Available)
	tr.FailBuild(nil)

	tr.FailSend(treasury.ErrRejected)
	require.NoError(t, m.RunOnce(ctx))
	stats, err = st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.ShardsInvalid, "a rejected split is recorded then retired")
	assert.Equal(t, int64(5), stats.Invalid)
	tr.FailSend(nil)

	tr.FailSend(errors.New("network flake"))
	require.NoError(t, m.RunOnce(ctx))
	stats, err = st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.ShardsUnmined, "transient send failure leaves the shard unmined for the watcher")
	assert.Equal(t, int64(5), stats.Available)
	tr.FailSend(nil)
}

func TestMinterMaxPerRoundAndMultiplePools(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	tr := mock.New()
	a := pool.DefaultPoolConfig("a", 50)
	a.SplitSize, a.TargetAvailable, a.LowWater = 10, 100, 10
	b := pool.DefaultPoolConfig("b", 100)
	b.SplitSize, b.TargetAvailable, b.LowWater = 10, 20, 10
	cfg := jobs.DefaultMinterConfig()
	cfg.MaxPerRound = 2
	m := newMinter(t, st, tr, []pool.Config{a, b}, cfg, clk)
	require.NoError(t, m.RunOnce(ctx))
	sa, err := st.Stats(ctx, "a")
	require.NoError(t, err)
	sb, err := st.Stats(ctx, "b")
	require.NoError(t, err)
	assert.Equal(t, int64(20), sa.Available, "MaxPerRound bounds work per pool per tick")
	assert.Equal(t, int64(20), sb.Available)
	rows := storetest.RowsByOutpoint(t, st, "b", storetest.Ops(mustListAvailable(t, st, "b")))
	for _, r := range rows {
		assert.Equal(t, uint64(100), r.Satoshis, "pool b mints its own denomination")
	}
}

func TestMinterConfigValidate(t *testing.T) {
	cfg := jobs.DefaultMinterConfig()
	require.NoError(t, cfg.Validate())
	bad := cfg
	bad.Interval = 0
	require.Error(t, bad.Validate())
	bad = cfg
	bad.MaxPendingShards = 0
	require.Error(t, bad.Validate())
	bad = cfg
	bad.MaxPerRound = 0
	require.Error(t, bad.Validate())
	_, err := jobs.NewMinter(memory.New(), mock.New(), nil, bad, pool.RealClock{}, slog.Default())
	require.Error(t, err)
}

// TestMinterInsufficientFundsStopsPoolForTick checks that ErrInsufficientFunds
// stops the whole pool for this tick after exactly one Build attempt, rather
// than retrying up to MaxPerRound times within the same RunOnce call.
func TestMinterInsufficientFundsStopsPoolForTick(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	tr := mock.New()
	pc := pool.DefaultPoolConfig("main", 50)
	pc.SplitSize = 5
	pc.TargetAvailable = 1000 // a large deficit: several attempts would occur if the stop leaked
	cfg := jobs.DefaultMinterConfig()
	m := newMinter(t, st, tr, []pool.Config{pc}, cfg, clk)

	tr.FailBuild(treasury.ErrInsufficientFunds)
	require.NoError(t, m.RunOnce(ctx), "insufficient funds is logged, not fatal")
	assert.Equal(t, 1, tr.BuildAttempts(), "exactly one attempt this tick")
	assert.Equal(t, 0, tr.Built())
	tr.FailBuild(nil)
}

// TestMinterInvalidShardsDoNotCountAsPending checks that a shard retired by a
// rejected send does not count toward MaxPendingShards on the next tick:
// pending is stats.ShardsUnmined, which excludes invalid shards.
func TestMinterInvalidShardsDoNotCountAsPending(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	tr := mock.New()
	pc := pool.DefaultPoolConfig("main", 50)
	pc.SplitSize = 5
	pc.TargetAvailable = 10
	cfg := jobs.DefaultMinterConfig()
	cfg.MaxPendingShards = 1
	m := newMinter(t, st, tr, []pool.Config{pc}, cfg, clk)

	tr.FailSend(treasury.ErrRejected)
	require.NoError(t, m.RunOnce(ctx))
	stats, err := st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.ShardsInvalid, "the rejected split was recorded then retired")
	assert.Equal(t, int64(0), stats.ShardsUnmined)
	tr.FailSend(nil)

	require.NoError(t, m.RunOnce(ctx), "MaxPendingShards=1 must not be exhausted by the invalid shard")
	stats, err = st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.ShardsUnmined, "a fresh split was minted despite MaxPendingShards=1")
	assert.Equal(t, int64(5), stats.Available)
}

// putShardFailStore wraps a store.Store, failing PutShard for a chosen pool
// (or every pool, when failPool is "") and recording the last shard it was
// asked to put, so a test can check what would have been sent had recording
// succeeded.
type putShardFailStore struct {
	*memory.Store
	failPool string
	err      error
	last     *store.Shard
}

func (s *putShardFailStore) PutShard(ctx context.Context, shard store.Shard, rows []store.Row) error {
	if s.failPool != "" && shard.Pool != s.failPool {
		return s.Store.PutShard(ctx, shard, rows)
	}
	sh := shard
	s.last = &sh
	return s.err
}

// TestMinterPutShardFailurePreventsSend checks the record-before-send
// ordering from the other side: when PutShard fails, RunOnce returns the
// error, Build was attempted exactly once, and the built split is never sent
// (a signed-but-unrecorded split must not reach the network).
func TestMinterPutShardFailurePreventsSend(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	wantErr := errors.New("put shard boom")
	st := &putShardFailStore{Store: base, err: wantErr}
	tr := mock.New()
	pc := pool.DefaultPoolConfig("main", 50)
	pc.SplitSize = 5
	pc.TargetAvailable = 5
	cfg := jobs.DefaultMinterConfig()
	m := newMinter(t, st, tr, []pool.Config{pc}, cfg, clk)

	err := m.RunOnce(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, wantErr)
	assert.Equal(t, 1, tr.BuildAttempts(), "Build was attempted exactly once")
	require.NotNil(t, st.last, "PutShard was called with the built shard")
	assert.Equal(t, 0, tr.Sends(st.last.TxID), "a split never recorded must never be sent")

	shards, lerr := base.ListShards(ctx, "main", store.ShardFilter{})
	require.NoError(t, lerr)
	assert.Empty(t, shards, "no shard was ever recorded")
}

// TestMinterPutShardFailureIsolatedPerPool checks that one pool's PutShard
// failure does not stop another pool from minting, and that the joined error
// names the failing pool.
func TestMinterPutShardFailureIsolatedPerPool(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	wantErr := errors.New("put shard boom")
	st := &putShardFailStore{Store: base, failPool: "bad", err: wantErr}
	tr := mock.New()
	bad := pool.DefaultPoolConfig("bad", 50)
	bad.SplitSize, bad.TargetAvailable, bad.LowWater = 5, 5, 0
	good := pool.DefaultPoolConfig("good", 50)
	good.SplitSize, good.TargetAvailable, good.LowWater = 5, 5, 0
	cfg := jobs.DefaultMinterConfig()
	m := newMinter(t, st, tr, []pool.Config{bad, good}, cfg, clk)

	err := m.RunOnce(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, wantErr)
	assert.Contains(t, err.Error(), "pool bad:", "the joined error names the failing pool")

	sbad, berr := base.Stats(ctx, "bad")
	require.NoError(t, berr)
	assert.Equal(t, int64(0), sbad.Available, "the failing pool minted nothing")

	sgood, gerr := base.Stats(ctx, "good")
	require.NoError(t, gerr)
	assert.Equal(t, int64(5), sgood.Available, "the other pool still minted")
}

// reconcilingTreasury is a mock treasury with the optional Reconciler and
// ChunkEnsurer capabilities.
type reconcilingTreasury struct {
	*mock.Treasury
	reconciles   int
	reconcileErr error
	report       treasury.ReconcileReport
	known        func(context.Context, chainhash.Hash) (bool, error)
	ensures      int
	ensureErr    error
	buildDL      time.Duration // remaining time on Build's ctx deadline
}

func (r *reconcilingTreasury) Reconcile(_ context.Context, known func(context.Context, chainhash.Hash) (bool, error)) (treasury.ReconcileReport, error) {
	r.reconciles++
	r.known = known
	return r.report, r.reconcileErr
}

func (r *reconcilingTreasury) EnsureChunks(context.Context) error {
	r.ensures++
	return r.ensureErr
}

func (r *reconcilingTreasury) Build(ctx context.Context, req treasury.MintRequest) (*treasury.BuiltShard, error) {
	if dl, ok := ctx.Deadline(); ok {
		r.buildDL = time.Until(dl)
	}
	return r.Treasury.Build(ctx, req)
}

var (
	_ treasury.Reconciler   = (*reconcilingTreasury)(nil)
	_ treasury.ChunkEnsurer = (*reconcilingTreasury)(nil)
)

func reconcileSetup(t *testing.T) (*jobs.Minter, *reconcilingTreasury, *storetest.Clock, jobs.MinterConfig) {
	t.Helper()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	tr := &reconcilingTreasury{Treasury: mock.New()}
	pc := pool.DefaultPoolConfig("main", 50)
	pc.SplitSize = 10
	pc.TargetAvailable = 10
	pc.LowWater = 0
	cfg := jobs.DefaultMinterConfig()
	return newMinter(t, st, tr, []pool.Config{pc}, cfg, clk), tr, clk, cfg
}

func TestMinterReconcilesOnFirstRoundOfEachLease(t *testing.T) {
	ctx := context.Background()
	m, tr, clk, cfg := reconcileSetup(t)

	require.NoError(t, m.RunOnce(ctx))
	assert.Equal(t, 1, tr.reconciles, "first round of the process reconciles")
	assert.Equal(t, 1, tr.ensures)
	assert.Equal(t, 1, tr.Built(), "the round still mints")

	clk.Advance(cfg.Interval)
	require.NoError(t, m.RunOnce(ctx))
	assert.Equal(t, 1, tr.reconciles, "consecutive rounds of one lease do not reconcile again")
	assert.Equal(t, 2, tr.ensures, "EnsureChunks runs every round")

	clk.Advance(3 * cfg.Interval) // ticks missed: the lease was held elsewhere
	require.NoError(t, m.RunOnce(ctx))
	assert.Equal(t, 2, tr.reconciles, "a re-acquired lease reconciles again")
	assert.Equal(t, 3, tr.ensures)
}

func TestMinterRetriesFailedReconcileAndKeepsMinting(t *testing.T) {
	ctx := context.Background()
	m, tr, clk, cfg := reconcileSetup(t)
	tr.reconcileErr = errors.New("wallet down")
	tr.ensureErr = errors.New("no reserve")

	require.NoError(t, m.RunOnce(ctx))
	assert.Equal(t, 1, tr.reconciles)
	assert.Equal(t, 1, tr.Built(), "neither failure stops minting")

	clk.Advance(cfg.Interval)
	tr.reconcileErr = nil
	tr.report = treasury.ReconcileReport{
		Aborted: []chainhash.Hash{{1}},
		Errors:  map[chainhash.Hash]error{{2}: errors.New("x")},
	}
	require.NoError(t, m.RunOnce(ctx))
	assert.Equal(t, 2, tr.reconciles, "a failed reconcile is retried next round")

	clk.Advance(cfg.Interval)
	require.NoError(t, m.RunOnce(ctx))
	assert.Equal(t, 2, tr.reconciles, "a completed reconcile (even with per-split errors) is not repeated")
}

// getShardFailStore fails GetShard for one pool.
type getShardFailStore struct {
	store.Store
	failPool string
}

var errGetShard = errors.New("store unavailable")

func (s getShardFailStore) GetShard(ctx context.Context, p string, txid chainhash.Hash) (*store.Shard, error) {
	if p == s.failPool {
		return nil, errGetShard
	}
	return s.Store.GetShard(ctx, p, txid)
}

func TestMinterReconcileKnownConsultsEveryPool(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	mem := memory.New(memory.WithClock(clk.Now))
	a := pool.DefaultPoolConfig("a", 50)
	a.SplitSize, a.TargetAvailable, a.LowWater = 10, 10, 0
	b := pool.DefaultPoolConfig("b", 50)
	b.SplitSize, b.TargetAvailable, b.LowWater = 10, 10, 0
	tr := &reconcilingTreasury{Treasury: mock.New()}
	m := newMinter(t, mem, tr, []pool.Config{a, b}, jobs.DefaultMinterConfig(), clk)
	require.NoError(t, m.RunOnce(ctx))
	require.NotNil(t, tr.known)

	shards, err := mem.ListShards(ctx, "b", store.ShardFilter{})
	require.NoError(t, err)
	require.Len(t, shards, 1)
	ok, err := tr.known(ctx, shards[0].TxID)
	require.NoError(t, err)
	assert.True(t, ok, "a shard of the second pool is known")

	ok, err = tr.known(ctx, chainhash.Hash{9})
	require.NoError(t, err)
	assert.False(t, ok, "ErrNotFound in every pool is unknown, not an error")

	tr2 := &reconcilingTreasury{Treasury: mock.New()}
	m2 := newMinter(t, getShardFailStore{Store: mem, failPool: "b"}, tr2, []pool.Config{a, b}, jobs.DefaultMinterConfig(), clk)
	require.NoError(t, m2.RunOnce(ctx))
	ok, err = tr2.known(ctx, chainhash.Hash{9})
	require.ErrorIs(t, err, errGetShard, "a store failure is an error, never unknown")
	assert.False(t, ok)
}

// deadlineStore records the deadline PutShard saw.
type deadlineStore struct {
	store.Store
	putDL time.Duration
}

func (s *deadlineStore) PutShard(ctx context.Context, sh store.Shard, rows []store.Row) error {
	if dl, ok := ctx.Deadline(); ok {
		s.putDL = time.Until(dl)
	}
	return s.Store.PutShard(ctx, sh, rows)
}

func TestMinterBoundsBuildAndPutShardByBuildTimeout(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := &deadlineStore{Store: memory.New(memory.WithClock(clk.Now))}
	tr := &reconcilingTreasury{Treasury: mock.New()}
	pc := pool.DefaultPoolConfig("main", 50)
	pc.SplitSize, pc.TargetAvailable, pc.LowWater = 10, 10, 0
	cfg := jobs.DefaultMinterConfig()
	cfg.BuildTimeout = 5 * time.Second
	m := newMinter(t, st, tr, []pool.Config{pc}, cfg, clk)
	require.NoError(t, m.RunOnce(ctx))
	assert.Positive(t, tr.buildDL)
	assert.LessOrEqual(t, tr.buildDL, cfg.BuildTimeout)
	assert.Positive(t, st.putDL)
	assert.LessOrEqual(t, st.putDL, tr.buildDL, "PutShard shares Build's deadline")

	bad := jobs.DefaultMinterConfig()
	bad.BuildTimeout = 0
	require.ErrorContains(t, bad.Validate(), "build_timeout")
	assert.Equal(t, time.Minute, jobs.DefaultMinterConfig().BuildTimeout)
}
