package jobs_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

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
	pc.LowWater = 20
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
	assert.Equal(t, int64(50), stats.Available, "deficit 15 → two more splits of 10")
	assert.Equal(t, 5, tr.Built())

	require.NoError(t, m.RunOnce(ctx))
	assert.Equal(t, 5, tr.Built(), "at or above target: nothing minted")

	all, err := st.ListShards(ctx, "main", store.ShardFilter{})
	require.NoError(t, err)
	require.Len(t, all, 5)
	for _, sh := range all {
		assert.True(t, tr.Sent(sh.TxID), "every recorded shard was sent")
	}
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
