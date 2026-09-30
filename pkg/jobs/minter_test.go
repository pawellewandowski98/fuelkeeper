package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
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

// spendAvailable removes n available rows from pool by reserving and
// terminally spending them, so a test can drop a pool's available count
// without going through pkg/pool.
func spendAvailable(t *testing.T, st store.Store, clk *storetest.Clock, poolName string, n int) {
	t.Helper()
	ctx := context.Background()
	rows := mustListAvailable(t, st, poolName)
	require.GreaterOrEqual(t, len(rows), n, "not enough available rows to spend down")
	rows = rows[:n]
	tok := store.NewToken()
	storetest.MustReserve(t, st, poolName, rows, tok, clk.Now().Add(time.Minute))
	spent, err := st.Spend(ctx, poolName, tok, storetest.Ops(rows), storetest.NewTxID(), true)
	require.NoError(t, err)
	require.Equal(t, n, spent)
}

// TestMinterRefillsFromLowWaterToTarget reproduces the hysteresis bug
// observed on the bench cluster: with target_available 300, low_water 200
// and split_size 10 (max_per_round 20), filling from 0 took exactly one
// round to land available precisely on low_water (200), and every round
// after that saw available >= low_water and skipped minting forever,
// instead of continuing on to target_available. A pool must keep minting,
// once triggered, until it actually reaches target_available -- regardless
// of where available sits relative to low_water in the meantime -- and only
// stop refilling there; it must not refill again until available truly
// drops below low_water.
func TestMinterRefillsFromLowWaterToTarget(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	tr := mock.New()
	pc := pool.DefaultPoolConfig("main", 50)
	pc.SplitSize = 10
	pc.TargetAvailable = 300
	pc.LowWater = 200
	cfg := jobs.DefaultMinterConfig()
	cfg.MaxPerRound = 20
	cfg.MaxPendingShards = 100 // large enough that the pending cap never interferes here
	m := newMinter(t, st, tr, []pool.Config{pc}, cfg, clk)

	// Round 1: 0 < low_water (200) triggers refilling; max_per_round (20)
	// caps this round at exactly low_water.
	require.NoError(t, m.RunOnce(ctx))
	stats, err := st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, int64(200), stats.Available, "round 1 mints max_per_round (20) splits of 10")
	assert.Equal(t, 20, tr.Built())

	// Round 2: available (200) is no longer < low_water, but refilling was
	// already triggered and must continue -- this is the bug: the old code
	// re-checked "available < low_water" at the top of every round and
	// skipped here forever.
	require.NoError(t, m.RunOnce(ctx))
	stats, err = st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, int64(300), stats.Available, "refilling continues past low_water up to target_available")
	assert.Equal(t, 30, tr.Built())

	// Round 3: supply is at target_available; refilling has turned off.
	require.NoError(t, m.RunOnce(ctx))
	assert.Equal(t, 30, tr.Built(), "at target_available: nothing minted")

	// Drop supply to 250: still >= low_water (200), so no refill starts.
	spendAvailable(t, st, clk, "main", 50)
	stats, err = st.Stats(ctx, "main")
	require.NoError(t, err)
	require.Equal(t, int64(250), stats.Available)
	require.NoError(t, m.RunOnce(ctx))
	assert.Equal(t, 30, tr.Built(), "250 >= low_water 200: no minting")

	// Drop supply to 199: now < low_water (200), so minting resumes and
	// fills back up past target_available (deficit 101 rounds up to 11
	// splits of 10).
	spendAvailable(t, st, clk, "main", 51)
	stats, err = st.Stats(ctx, "main")
	require.NoError(t, err)
	require.Equal(t, int64(199), stats.Available)
	require.NoError(t, m.RunOnce(ctx))
	stats, err = st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, 41, tr.Built(), "199 < low_water 200: minting resumes")
	assert.Equal(t, int64(309), stats.Available, "mints past target_available by one split_size, same as any round")
}

// TestMinterRefillSurvivesRestart pins the chosen restart behavior: the
// refilling flag lives only in process memory (see the Minter doc comment),
// so a freshly created Minter that finds a pool sitting strictly between
// low_water and target_available does NOT assume a refill was in progress.
// It waits for a real dip below low_water instead of minting on every
// restart -- the conservative choice, since the alternative (treating
// "below target at startup" as refilling) would mint on essentially every
// process restart whenever a pool is not already sitting exactly at target.
func TestMinterRefillSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	tr := mock.New()
	pc := pool.DefaultPoolConfig("main", 50)
	pc.SplitSize = 10
	pc.TargetAvailable = 300
	pc.LowWater = 200

	// Seed 250 available rows directly: between low_water and
	// target_available, as if a previous process's refill had gotten partway
	// before the process restarted (or had simply never been triggered).
	_, rows := storetest.MintShard(t, st, "main", 250, store.ShardMined)
	require.Len(t, rows, 250)

	m := newMinter(t, st, tr, []pool.Config{pc}, jobs.DefaultMinterConfig(), clk)
	require.NoError(t, m.RunOnce(ctx))
	stats, err := st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, int64(250), stats.Available, "restart between low_water and target_available: no surprise minting")
	assert.Equal(t, 0, tr.Built())

	require.NoError(t, m.RunOnce(ctx))
	assert.Equal(t, 0, tr.Built(), "stays quiet across rounds until available truly dips below low_water")

	// Confirm it is not simply broken: a real dip below low_water still
	// triggers a refill on this same, already-running Minter.
	spendAvailable(t, st, clk, "main", 51) // 250 - 51 = 199 < low_water (200)
	require.NoError(t, m.RunOnce(ctx))
	stats, err = st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Positive(t, tr.Built(), "a genuine dip below low_water still triggers a refill")
	assert.GreaterOrEqual(t, stats.Available, pc.TargetAvailable)
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
	mu           sync.Mutex
	grace        time.Duration
	snapshot     []chainhash.Hash // what ReconcileStart returns
	starts       int
	startErr     error
	finishes     [][]chainhash.Hash // the snapshots ReconcileFinish got
	finishErr    error
	finishReport treasury.ReconcileReport
	known        func(context.Context, chainhash.Hash) (bool, error)
	ensures      int
	ensureErr    error
	buildErr     error
	buildDL      time.Duration             // remaining time on Build's ctx deadline
	onStart      func(ctx context.Context) // called at ReconcileStart, if set
	onFinish     func(ctx context.Context) // called at ReconcileFinish, if set
}

func (r *reconcilingTreasury) ReconcileGrace() time.Duration { return r.grace }

func (r *reconcilingTreasury) ReconcileStart(ctx context.Context, known func(context.Context, chainhash.Hash) (bool, error)) ([]chainhash.Hash, treasury.ReconcileReport, error) {
	if r.onStart != nil {
		r.onStart(ctx)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.starts++
	r.known = known
	if r.startErr != nil {
		return nil, treasury.ReconcileReport{}, r.startErr
	}
	return r.snapshot, treasury.ReconcileReport{}, nil
}

func (r *reconcilingTreasury) ReconcileFinish(ctx context.Context, snap []chainhash.Hash, _ func(context.Context, chainhash.Hash) (bool, error)) (treasury.ReconcileReport, error) {
	if r.onFinish != nil {
		r.onFinish(ctx)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finishes = append(r.finishes, snap)
	return r.finishReport, r.finishErr
}

func (r *reconcilingTreasury) EnsureChunks(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensures++
	return r.ensureErr
}

func (r *reconcilingTreasury) Build(ctx context.Context, req treasury.MintRequest) (*treasury.BuiltShard, error) {
	r.mu.Lock()
	if dl, ok := ctx.Deadline(); ok {
		r.buildDL = time.Until(dl)
	}
	err := r.buildErr
	r.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return r.Treasury.Build(ctx, req)
}

func (r *reconcilingTreasury) counts() (starts, finishes, ensures int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.starts, len(r.finishes), r.ensures
}

var (
	_ treasury.Reconciler   = (*reconcilingTreasury)(nil)
	_ treasury.ChunkEnsurer = (*reconcilingTreasury)(nil)
)

// putFailStore fails PutShard while fail is set.
type putFailStore struct {
	store.Store
	fail bool
}

func (s *putFailStore) PutShard(ctx context.Context, sh store.Shard, rows []store.Row) error {
	if s.fail {
		return errors.New("disk full")
	}
	return s.Store.PutShard(ctx, sh, rows)
}

// reconcileSetup returns a minter over a pool with a large deficit (one
// split per round) and a reconciling treasury with a one-minute grace.
func reconcileSetup(t *testing.T) (*jobs.Minter, *reconcilingTreasury, *putFailStore, *storetest.Clock, jobs.MinterConfig) {
	t.Helper()
	clk := storetest.NewClock(storetest.Start)
	st := &putFailStore{Store: memory.New(memory.WithClock(clk.Now))}
	tr := &reconcilingTreasury{Treasury: mock.New(), grace: time.Minute}
	pc := pool.DefaultPoolConfig("main", 50)
	pc.SplitSize = 10
	pc.TargetAvailable = 1000
	pc.LowWater = 0
	cfg := jobs.DefaultMinterConfig()
	cfg.MaxPerRound = 1
	return newMinter(t, st, tr, []pool.Config{pc}, cfg, clk), tr, st, clk, cfg
}

func TestMinterReconcileIsTwoPhaseAndNeverWaits(t *testing.T) {
	ctx := context.Background()
	m, tr, _, clk, cfg := reconcileSetup(t)
	tr.snapshot = []chainhash.Hash{{7}}

	require.NoError(t, m.RunOnce(ctx))
	starts, finishes, ensures := tr.counts()
	assert.Equal(t, []int{1, 0, 1}, []int{starts, finishes, ensures}, "first round of the process takes a snapshot")
	assert.Equal(t, 1, tr.Built(), "the round still mints")

	clk.Advance(cfg.Interval) // 30s after the snapshot: not due
	require.NoError(t, m.RunOnce(ctx))
	starts, finishes, ensures = tr.counts()
	assert.Equal(t, []int{1, 0, 2}, []int{starts, finishes, ensures}, "nothing waits for the grace")

	clk.Advance(cfg.Interval) // 60s: due
	require.NoError(t, m.RunOnce(ctx))
	starts, finishes, _ = tr.counts()
	assert.Equal(t, []int{1, 1}, []int{starts, finishes})
	assert.Equal(t, [][]chainhash.Hash{{{7}}}, tr.finishes, "the finish gets the snapshot")

	clk.Advance(cfg.Interval)
	require.NoError(t, m.RunOnce(ctx))
	starts, finishes, _ = tr.counts()
	assert.Equal(t, []int{1, 1}, []int{starts, finishes}, "a finished reconcile is not repeated within the lease")

	clk.Advance(3 * cfg.Interval) // ticks missed: the lease was held elsewhere
	require.NoError(t, m.RunOnce(ctx))
	starts, _, _ = tr.counts()
	assert.Equal(t, 2, starts, "a re-acquired lease takes a new snapshot")
}

func TestMinterReconcileEmptySnapshotNeedsNoFinish(t *testing.T) {
	ctx := context.Background()
	m, tr, _, clk, cfg := reconcileSetup(t)
	for range 4 {
		require.NoError(t, m.RunOnce(ctx))
		clk.Advance(cfg.Interval)
	}
	starts, finishes, _ := tr.counts()
	assert.Equal(t, []int{1, 0}, []int{starts, finishes})
}

func TestMinterReconcileRetriesAndKeepsMinting(t *testing.T) {
	ctx := context.Background()
	m, tr, _, clk, cfg := reconcileSetup(t)
	tr.startErr = errors.New("wallet down")
	tr.ensureErr = errors.New("no reserve")
	tr.snapshot = []chainhash.Hash{{1}}

	require.NoError(t, m.RunOnce(ctx))
	assert.Equal(t, 1, tr.Built(), "neither failure stops minting")

	clk.Advance(cfg.Interval)
	tr.startErr = nil
	require.NoError(t, m.RunOnce(ctx))
	starts, _, _ := tr.counts()
	assert.Equal(t, 2, starts, "a failed snapshot is retried next round")

	clk.Advance(2 * cfg.Interval) // due
	tr.finishErr = errors.New("wallet down")
	require.NoError(t, m.RunOnce(ctx))
	clk.Advance(cfg.Interval)
	tr.finishErr = nil
	tr.finishReport = treasury.ReconcileReport{Deferred: []chainhash.Hash{{2}}}
	require.NoError(t, m.RunOnce(ctx))
	starts, _, _ = tr.counts()
	assert.Equal(t, [][]chainhash.Hash{{{1}}, {{1}}}, tr.finishes, "a failed finish keeps its snapshot")
	assert.Equal(t, 3, starts, "a finish that deferred a split takes a new snapshot in the same round")
}

func TestMinterReconcilesAfterRecordFailure(t *testing.T) {
	ctx := context.Background()
	m, tr, st, clk, cfg := reconcileSetup(t)
	require.NoError(t, m.RunOnce(ctx))
	clk.Advance(cfg.Interval)
	require.NoError(t, m.RunOnce(ctx))
	starts, _, _ := tr.counts()
	require.Equal(t, 1, starts)

	st.fail = true
	clk.Advance(cfg.Interval)
	require.Error(t, m.RunOnce(ctx), "PutShard failed after a successful Build")
	st.fail = false
	clk.Advance(cfg.Interval)
	require.NoError(t, m.RunOnce(ctx))
	starts, _, _ = tr.counts()
	assert.Equal(t, 2, starts, "a split left unrecorded is reconciled without waiting for a new lease")
}

func TestMinterReconcilesAfterBuildDeadline(t *testing.T) {
	ctx := context.Background()
	m, tr, _, clk, cfg := reconcileSetup(t)
	require.NoError(t, m.RunOnce(ctx))

	tr.buildErr = errors.New("treasury: create split: boom") // not a deadline
	clk.Advance(cfg.Interval)
	require.Error(t, m.RunOnce(ctx))
	clk.Advance(cfg.Interval)
	require.Error(t, m.RunOnce(ctx))
	starts, _, _ := tr.counts()
	assert.Equal(t, 1, starts, "an ordinary build failure leaves nothing unrecorded")

	tr.buildErr = fmt.Errorf("treasury: create split: %w", context.DeadlineExceeded)
	clk.Advance(cfg.Interval)
	require.Error(t, m.RunOnce(ctx))
	tr.buildErr = nil
	clk.Advance(cfg.Interval)
	require.NoError(t, m.RunOnce(ctx))
	starts, _, _ = tr.counts()
	assert.Equal(t, 2, starts, "a timed-out Build may have left a split behind: reconcile")
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
	tr := &reconcilingTreasury{Treasury: mock.New(), grace: time.Minute}
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

	tr2 := &reconcilingTreasury{Treasury: mock.New(), grace: time.Minute}
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
	tr := &reconcilingTreasury{Treasury: mock.New(), grace: time.Minute}
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

// roundCounter wraps a job and tracks how many rounds, across every
// instance sharing it, run at the same moment.
type roundCounter struct {
	jobs.Job
	active, maxActive *atomic.Int32
	rounds            *atomic.Int32
}

func (r roundCounter) RunOnce(ctx context.Context) error {
	n := r.active.Add(1)
	defer r.active.Add(-1)
	for {
		m := r.maxActive.Load()
		if n <= m || r.maxActive.CompareAndSwap(m, n) {
			break
		}
	}
	r.rounds.Add(1)
	return r.Job.RunOnce(ctx)
}

// TestTwoRunnersNeverOverlapDuringReconcile runs two instances' runners on
// one lease store. The reconcile grace (400ms) exceeds the lease TTL
// (300ms): a minter that waited out the grace inside a round would lose the
// lease mid-round and let the other instance mint concurrently. With the
// two-phase reconcile no round ever overlaps another, and the snapshot is
// still finished after its grace.
func TestTwoRunnersNeverOverlapDuringReconcile(t *testing.T) {
	st := memory.New()
	var active, maxActive, rounds atomic.Int32
	cfg := jobs.DefaultMinterConfig()
	cfg.Interval = 50 * time.Millisecond
	pc := pool.DefaultPoolConfig("main", 50)
	pc.SplitSize, pc.TargetAvailable, pc.LowWater = 10, 100, 0

	type instance struct {
		tr     *reconcilingTreasury
		cancel context.CancelFunc
		done   chan struct{}
	}
	start := func(holder string) instance {
		tr := &reconcilingTreasury{Treasury: mock.New(), grace: 400 * time.Millisecond, snapshot: []chainhash.Hash{{1}}}
		m, err := jobs.NewMinter(st, tr, []pool.Config{pc}, cfg, pool.RealClock{}, slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		r := jobs.NewRunner(st, holder, 300*time.Millisecond, slog.New(slog.DiscardHandler))
		require.NoError(t, r.Add(roundCounter{Job: m, active: &active, maxActive: &maxActive, rounds: &rounds}))
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); _ = r.Run(ctx) }()
		return instance{tr: tr, cancel: cancel, done: done}
	}
	a := start("a")
	time.Sleep(100 * time.Millisecond) // a takes the lease first
	b := start("b")
	time.Sleep(1100 * time.Millisecond)
	a.cancel() // hand the lease over: b's first round takes a snapshot
	<-a.done
	time.Sleep(1200 * time.Millisecond)
	b.cancel()
	<-b.done

	assert.Equal(t, int32(1), maxActive.Load(), "no two minter rounds ever ran at once")
	assert.Greater(t, rounds.Load(), int32(10))
	aStarts, aFinishes, _ := a.tr.counts()
	bStarts, bFinishes, _ := b.tr.counts()
	assert.Equal(t, []int{1, 1}, []int{aStarts, bStarts}, "one snapshot per lease: a's, then b's after the hand-over")
	assert.Equal(t, []int{1, 1}, []int{aFinishes, bFinishes}, "each finished its snapshot a grace later, without blocking a round")
}

// TestMinterReconcileGraceRunsFromTheFirstListing reproduces a finish that
// takes 9 s and defers a split: the new snapshot taken in the same round must
// be due a full grace after its own listing, not after the round started.
func TestMinterReconcileGraceRunsFromTheFirstListing(t *testing.T) {
	ctx := context.Background()
	m, tr, _, clk, _ := reconcileSetup(t)
	tr.grace = 61 * time.Second
	tr.snapshot = []chainhash.Hash{{1}}
	var starts, finishes []time.Time
	tr.onStart = func(context.Context) { starts = append(starts, clk.Now()) }
	tr.onFinish = func(context.Context) {
		finishes = append(finishes, clk.Now())
		clk.Advance(9 * time.Second) // a slow second listing
		tr.finishReport = treasury.ReconcileReport{Deferred: []chainhash.Hash{{2}}}
	}

	require.NoError(t, m.RunOnce(ctx)) // snapshot 1 at t0
	for clk.Now().Sub(starts[0]) < 61*time.Second {
		clk.Advance(time.Second)
		require.NoError(t, m.RunOnce(ctx))
	}
	require.Len(t, finishes, 1)
	require.Len(t, starts, 2, "the deferred finish takes a new snapshot in the same round")
	assert.Equal(t, 9*time.Second, starts[1].Sub(finishes[0]))

	for len(finishes) < 2 {
		clk.Advance(time.Second)
		require.NoError(t, m.RunOnce(ctx))
	}
	assert.GreaterOrEqual(t, finishes[1].Sub(starts[1]), 61*time.Second,
		"the two listings of a reconcile are at least a grace apart")
}

// TestMinterDeferredOrFailedFinishTakesNewSnapshot steps by exactly one
// interval (no lease gap), so only the finish's outcome can ask for a new
// snapshot.
func TestMinterDeferredOrFailedFinishTakesNewSnapshot(t *testing.T) {
	for name, rep := range map[string]treasury.ReconcileReport{
		"deferred": {Deferred: []chainhash.Hash{{2}}},
		"errors":   {Errors: map[chainhash.Hash]error{{2}: errors.New("x")}},
		"clean":    {Aborted: []chainhash.Hash{{1}}},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			m, tr, _, clk, cfg := reconcileSetup(t)
			tr.grace = cfg.Interval
			tr.snapshot = []chainhash.Hash{{1}}
			tr.finishReport = rep
			require.NoError(t, m.RunOnce(ctx))
			clk.Advance(cfg.Interval)
			require.NoError(t, m.RunOnce(ctx))
			starts, finishes, _ := tr.counts()
			require.Equal(t, 1, finishes)
			if name == "clean" {
				assert.Equal(t, 1, starts, "a clean finish needs no new snapshot")
			} else {
				assert.Equal(t, 2, starts, "the next snapshot is taken right after the finish")
			}
		})
	}
}

func TestMinterBoundsReconcilePhasesByBudget(t *testing.T) {
	ctx := context.Background()
	m, tr, _, clk, _ := reconcileSetup(t)
	tr.grace = time.Second
	tr.snapshot = []chainhash.Hash{{1}}
	var dls []time.Duration
	record := func(c context.Context) {
		dl, ok := c.Deadline()
		require.True(t, ok, "each phase runs under a deadline")
		dls = append(dls, time.Until(dl))
	}
	tr.onStart, tr.onFinish = record, record
	require.NoError(t, m.RunOnce(ctx))
	clk.Advance(time.Minute)
	require.NoError(t, m.RunOnce(ctx))
	require.Len(t, dls, 3, "start; then finish and, the minute-long gap being a new lease, a new start")
	for _, d := range dls {
		assert.Positive(t, d)
		assert.LessOrEqual(t, d, jobs.ReconcileBudget)
	}
}
