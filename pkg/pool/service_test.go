package pool_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

type fixture struct {
	clk *storetest.Clock
	st  *memory.Store
	pc  pool.Config
	p   *pool.Pool
}

func newFixture(t *testing.T, mutate func(*pool.Config)) *fixture {
	t.Helper()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	pc.MinTTL = time.Second
	if mutate != nil {
		mutate(&pc)
	}
	require.NoError(t, pc.Validate())
	ic := testInstanceConfig()
	p := pool.NewPool(st, pc, ic, pool.ClockFunc(clk.Now), slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return &fixture{clk: clk, st: st, pc: pc, p: p}
}

func TestPoolReserveHappyPath(t *testing.T) {
	f := newFixture(t, nil)
	ctx := context.Background()
	sh, _ := storetest.MintShard(t, f.st, "main", 5, store.ShardMined)

	res, err := f.p.Reserve(ctx, pool.ReserveRequest{Count: 3})
	require.NoError(t, err)
	assert.False(t, res.Token.IsZero())
	assert.Equal(t, f.pc.DefaultTTL, res.TTL)
	assert.Equal(t, f.clk.Now().Add(f.pc.DefaultTTL), res.ExpiresAt)
	require.Len(t, res.Outputs, 3)
	require.Len(t, res.Shards, 1)
	assert.Equal(t, sh.TxID, res.Shards[0].TxID)
	assert.Equal(t, int64(1), res.Shards[0].Version)
	assert.Equal(t, store.LockPuzzle, res.LockMode)
	for _, o := range res.Outputs {
		assert.Equal(t, uint64(50), o.Satoshis)
		assert.NotEmpty(t, o.LockingScript)
		assert.NotEmpty(t, o.UnlockingScript)
	}
	got := storetest.RowsByOutpoint(t, f.st, "main", outpointsOf(res.Outputs))
	for _, o := range res.Outputs {
		assert.Equal(t, store.StateReserved, got[o.Outpoint].State)
		assert.Equal(t, res.Token, got[o.Outpoint].ReservedBy)
	}

	// Partial fill with min_count: 2 rows left, ask for 5, accept ≥1.
	res2, err := f.p.Reserve(ctx, pool.ReserveRequest{Count: 5, MinCount: 1, TTL: 5 * time.Second})
	require.NoError(t, err)
	assert.Len(t, res2.Outputs, 2)
	assert.Equal(t, 5*time.Second, res2.TTL)

	_, err = f.p.Reserve(ctx, pool.ReserveRequest{Count: 1})
	require.ErrorIs(t, err, pool.ErrExhausted)
}

func TestPoolReserveInsufficientReleasesPartial(t *testing.T) {
	f := newFixture(t, nil)
	ctx := context.Background()
	_, rows := storetest.MintShard(t, f.st, "main", 2, store.ShardMined)
	_, err := f.p.Reserve(ctx, pool.ReserveRequest{Count: 5, MinCount: 3})
	require.ErrorIs(t, err, pool.ErrInsufficient)
	got := storetest.RowsByOutpoint(t, f.st, "main", storetest.Ops(rows))
	for _, r := range rows {
		assert.Equal(t, store.StateAvailable, got[r.Outpoint].State, "partial reservation released")
	}
	// And the released rows are served again immediately.
	res, err := f.p.Reserve(ctx, pool.ReserveRequest{Count: 2})
	require.NoError(t, err)
	assert.Len(t, res.Outputs, 2)
}

func TestPoolReserveValidation(t *testing.T) {
	f := newFixture(t, nil)
	ctx := context.Background()
	for _, req := range []pool.ReserveRequest{
		{Count: 0},
		{Count: f.pc.MaxCount + 1},
		{Count: 2, MinCount: 3},
		{Count: 1, TTL: f.pc.MinTTL - time.Millisecond},
		{Count: 1, TTL: f.pc.MaxTTL + time.Second},
	} {
		_, err := f.p.Reserve(ctx, req)
		require.ErrorIs(t, err, pool.ErrBadRequest, "%+v", req)
	}
}

func TestPoolReserveSkipsRowsLostToCAS(t *testing.T) {
	f := newFixture(t, nil)
	ctx := context.Background()
	_, rows := storetest.MintShard(t, f.st, "main", 6, store.ShardMined)
	// Warm the allocator, then steal row 0 behind its back.
	require.True(t, f.p.EnsureRows(ctx))
	storetest.MustReserve(t, f.st, "main", rows[:1], store.NewToken(), f.clk.Now().Add(time.Minute))

	res, err := f.p.Reserve(ctx, pool.ReserveRequest{Count: 3})
	require.NoError(t, err)
	require.Len(t, res.Outputs, 3)
	for _, o := range res.Outputs {
		assert.NotEqual(t, rows[0].Outpoint, o.Outpoint, "the stolen row is never handed out")
	}
}

func TestPoolReserveMaxCountAboveDefaultBatch(t *testing.T) {
	f := newFixture(t, func(c *pool.Config) { c.MaxCount = 400 })
	ctx := context.Background()
	storetest.MintShard(t, f.st, "main", 500, store.ShardMined)
	res, err := f.p.Reserve(ctx, pool.ReserveRequest{Count: 400})
	require.NoError(t, err, "a MaxCount above the 256 default batch must still be servable")
	assert.Len(t, res.Outputs, 400)
}

func TestPoolReleasePolicies(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, nil) // trust
	storetest.MintShard(t, f.st, "main", 4, store.ShardMined)
	res, err := f.p.Reserve(ctx, pool.ReserveRequest{Count: 2})
	require.NoError(t, err)
	n, err := f.p.Release(ctx, res.Token, outpointsOf(res.Outputs))
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	got := storetest.RowsByOutpoint(t, f.st, "main", outpointsOf(res.Outputs))
	for _, o := range res.Outputs {
		assert.Equal(t, store.StateAvailable, got[o.Outpoint].State)
	}
	// Released rows are re-served immediately by the same instance (local fast path).
	res2, err := f.p.Reserve(ctx, pool.ReserveRequest{Count: 4})
	require.NoError(t, err)
	assert.Len(t, res2.Outputs, 4)

	n, err = f.p.Release(ctx, store.NewToken(), outpointsOf(res2.Outputs))
	require.NoError(t, err)
	assert.Equal(t, 0, n, "wrong token releases nothing")
	_, err = f.p.Release(ctx, res2.Token, nil)
	require.ErrorIs(t, err, pool.ErrBadRequest)
	_, err = f.p.Release(ctx, store.Token{}, outpointsOf(res2.Outputs))
	require.ErrorIs(t, err, pool.ErrBadRequest)

	fv := newFixture(t, func(c *pool.Config) { c.ReleasePolicy = pool.ReleaseVerify })
	storetest.MintShard(t, fv.st, "main", 2, store.ShardMined)
	rv, err := fv.p.Reserve(ctx, pool.ReserveRequest{Count: 2})
	require.NoError(t, err)
	n, err = fv.p.Release(ctx, rv.Token, outpointsOf(rv.Outputs))
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	gotv := storetest.RowsByOutpoint(t, fv.st, "main", outpointsOf(rv.Outputs))
	for _, o := range rv.Outputs {
		assert.Equal(t, store.StateQuarantined, gotv[o.Outpoint].State, "verify policy quarantines")
		assert.Nil(t, gotv[o.Outpoint].SpentHint, "no hint")
	}
}

func TestPoolSpendPolicies(t *testing.T) {
	ctx := context.Background()
	txid := storetest.NewTxID()

	f := newFixture(t, nil) // verify
	storetest.MintShard(t, f.st, "main", 2, store.ShardMined)
	res, err := f.p.Reserve(ctx, pool.ReserveRequest{Count: 2})
	require.NoError(t, err)
	n, err := f.p.Spend(ctx, res.Token, outpointsOf(res.Outputs), txid)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	got := storetest.RowsByOutpoint(t, f.st, "main", outpointsOf(res.Outputs))
	for _, o := range res.Outputs {
		assert.Equal(t, store.StateQuarantined, got[o.Outpoint].State)
		require.NotNil(t, got[o.Outpoint].SpentHint)
		assert.Equal(t, txid, *got[o.Outpoint].SpentHint)
	}
	n, err = f.p.Spend(ctx, res.Token, outpointsOf(res.Outputs), txid)
	require.NoError(t, err)
	assert.Equal(t, 0, n, "duplicate spend is a no-op")
	_, err = f.p.Spend(ctx, res.Token, outpointsOf(res.Outputs), chainhash.Hash{})
	require.ErrorIs(t, err, pool.ErrBadRequest, "spend needs a txid")

	ft := newFixture(t, func(c *pool.Config) { c.SpendPolicy = pool.SpendTrust })
	storetest.MintShard(t, ft.st, "main", 1, store.ShardMined)
	rt, err := ft.p.Reserve(ctx, pool.ReserveRequest{Count: 1})
	require.NoError(t, err)
	n, err = ft.p.Spend(ctx, rt.Token, outpointsOf(rt.Outputs), txid)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	gt := storetest.RowsByOutpoint(t, ft.st, "main", outpointsOf(rt.Outputs))
	assert.Equal(t, store.StateSpent, gt[rt.Outputs[0].Outpoint].State)
}

func TestPoolReserveAdoptsWhenPartiallyFilled(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	pc.MinTTL = time.Second
	require.NoError(t, pc.Validate())
	ic := testInstanceConfig()
	ic.LowWaterRows = 0 // keep the background hunger loop from adopting shard B on its own
	p := pool.NewPool(st, pc, ic, pool.ClockFunc(clk.Now), slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	storetest.MintShard(t, st, "main", 1, store.ShardMined) // shard A: 1 row
	require.True(t, p.EnsureRows(ctx), "adopts shard A and loads its single row")
	clk.Advance(time.Second)
	storetest.MintShard(t, st, "main", 5, store.ShardMined) // shard B: 5 rows, not yet adopted

	res, err := p.Reserve(ctx, pool.ReserveRequest{Count: 3, MinCount: 3})
	require.NoError(t, err, "a partially filled request must still get one bounded adoption attempt")
	assert.Len(t, res.Outputs, 3)
	assert.Len(t, res.Shards, 2, "rows drawn from both the already-owned shard and the newly adopted one")
}

func TestPoolReserveFlushErrorForgetsRows(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	faulty := storetest.NewFaulty(base)
	var lastFlushed []store.Outpoint
	faulty.BeforeReserve = func(ops []store.ReserveOp) {
		outs := make([]store.Outpoint, len(ops))
		for i, op := range ops {
			outs[i] = op.Outpoint
		}
		lastFlushed = outs
	}
	pc := pool.DefaultPoolConfig("main", 50)
	pc.MinTTL = time.Second
	require.NoError(t, pc.Validate())
	ic := testInstanceConfig()
	p := pool.NewPool(faulty, pc, ic, pool.ClockFunc(clk.Now), slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	storetest.MintShard(t, base, "main", 3, store.ShardMined)
	require.True(t, p.EnsureRows(ctx))

	faulty.InDoubtNext(10) // every retry attempt commits but reports ErrUnavailable, so the flush fails
	_, err := p.Reserve(ctx, pool.ReserveRequest{Count: 2})
	require.ErrorIs(t, err, pool.ErrUnavailable)
	require.Len(t, lastFlushed, 2, "the failed flush carried the 2 rows it popped")
	failedOps := append([]store.Outpoint(nil), lastFlushed...)

	faulty.InDoubtNext(0)
	res, err := p.Reserve(ctx, pool.ReserveRequest{Count: 1})
	require.NoError(t, err, "the failed rows were forgotten, not requeued, so the third row is still in memory")
	require.Len(t, res.Outputs, 1)
	for _, fo := range failedOps {
		assert.NotEqual(t, fo, res.Outputs[0].Outpoint, "a row from the failed flush must never be handed out from memory again")
	}
}

func TestPoolStatsAndBEEF(t *testing.T) {
	f := newFixture(t, nil)
	ctx := context.Background()
	sh, _ := storetest.MintShard(t, f.st, "main", 3, store.ShardMined)
	st, err := f.p.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(3), st.Available)

	beef, ver, status, err := f.p.BEEF(ctx, sh.TxID)
	require.NoError(t, err)
	assert.Equal(t, sh.BEEF, beef)
	assert.Equal(t, int64(1), ver)
	assert.Equal(t, store.ShardMined, status)
	_, _, _, err = f.p.BEEF(ctx, storetest.NewTxID())
	require.ErrorIs(t, err, store.ErrNotFound)
	assert.Equal(t, "main", f.p.Config().Name)
	assert.NotEmpty(t, f.p.Holder())
}

// getShardErrStore wraps a store.Store and forces GetShard to return err.
type getShardErrStore struct {
	store.Store
	err error
}

func (s *getShardErrStore) GetShard(_ context.Context, _ string, _ chainhash.Hash) (*store.Shard, error) {
	return nil, s.err
}

func TestPoolBEEFMapsStoreErrors(t *testing.T) {
	base := memory.New(memory.WithClock(storetest.NewClock(storetest.Start).Now))
	wrapped := &getShardErrStore{Store: base, err: store.ErrUnavailable}
	pc := pool.DefaultPoolConfig("main", 50)
	pc.MinTTL = time.Second
	require.NoError(t, pc.Validate())
	p := pool.NewPool(wrapped, pc, testInstanceConfig(), pool.RealClock{}, slog.Default())
	ctx := context.Background()

	_, _, _, err := p.BEEF(ctx, storetest.NewTxID())
	require.ErrorIs(t, err, pool.ErrUnavailable, "a store outage must map to pool.ErrUnavailable")

	wrapped.err = store.ErrNotFound
	_, _, _, err = p.BEEF(ctx, storetest.NewTxID())
	require.ErrorIs(t, err, store.ErrNotFound, "not-found must pass through unchanged")
	assert.NotErrorIs(t, err, pool.ErrUnavailable)
}

func TestPoolReady(t *testing.T) {
	f := newFixture(t, nil)
	ctx := context.Background()
	assert.False(t, f.p.Ready(ctx), "nothing owned and nothing queued")

	storetest.MintShard(t, f.st, "main", 3, store.ShardMined)
	require.True(t, f.p.EnsureRows(ctx))
	assert.True(t, f.p.Ready(ctx), "rows queued in the allocator")

	_, err := f.p.Reserve(ctx, pool.ReserveRequest{Count: 3})
	require.NoError(t, err)
	assert.True(t, f.p.Ready(ctx), "the shard is still owned even though its rows are all reserved")
}

func outpointsOf(outs []pool.Output) []store.Outpoint {
	ops := make([]store.Outpoint, len(outs))
	for i, o := range outs {
		ops[i] = o.Outpoint
	}
	return ops
}
