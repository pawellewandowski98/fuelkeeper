package jobs_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/jobs"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury/mock"
)

func mintViaTreasury(ctx context.Context, t *testing.T, st store.Store, tr *mock.Treasury, n int) *treasury.BuiltShard {
	t.Helper()
	b, err := tr.Build(ctx, treasury.MintRequest{Pool: "main", Denomination: 50, Count: n, LockMode: store.LockPuzzle})
	require.NoError(t, err)
	require.NoError(t, st.PutShard(ctx, b.Shard, b.Rows))
	require.NoError(t, tr.Send(ctx, b.Shard.TxID))
	return b
}

func TestWatcherMinesRetiresAndResends(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	orc := oracle.NewMock()
	tr := mock.New()
	cfg := jobs.DefaultWatcherConfig()
	w := jobs.NewWatcher(st, orc, tr, []string{"main"}, cfg, pool.ClockFunc(clk.Now), slog.Default())

	mined := mintViaTreasury(ctx, t, st, tr, 3)
	rejected := mintViaTreasury(ctx, t, st, tr, 3)
	lost := mintViaTreasury(ctx, t, st, tr, 3)
	pending := mintViaTreasury(ctx, t, st, tr, 3)
	tok := store.NewToken()
	storetest.MustReserve(t, st, "main", rejected.Rows[:1], tok, clk.Now().Add(time.Minute))

	orc.SetTx(mined.Shard.TxID, oracle.TxStatus{Known: true, Mined: true, BlockHeight: 7, MerklePath: []byte{9, 9}})
	orc.SetTx(rejected.Shard.TxID, oracle.TxStatus{Known: true, Rejected: true, Status: "REJECTED"})
	orc.SetTx(pending.Shard.TxID, oracle.TxStatus{Known: true, Status: "SEEN_ON_NETWORK"})
	// lost: oracle has never seen it

	require.NoError(t, w.RunOnce(ctx))

	sh, err := st.GetShard(ctx, "main", mined.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardMined, sh.Status)
	assert.Equal(t, int64(2), sh.Version)
	assert.Equal(t, append(append([]byte("mock-mined-beef-"), mined.Shard.TxID[:]...), 9, 9), sh.BEEF)

	sh, err = st.GetShard(ctx, "main", rejected.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardInvalid, sh.Status)
	got := storetest.RowsByOutpoint(t, st, "main", storetest.Ops(rejected.Rows))
	for _, r := range rejected.Rows {
		assert.Equal(t, store.StateInvalid, got[r.Outpoint].State, "rows of a rejected split are invalid, reserved or not")
	}

	sh, err = st.GetShard(ctx, "main", pending.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardUnmined, sh.Status, "known but unmined: wait")
	assert.Equal(t, 1, tr.Sends(pending.Shard.TxID), "a known tx is never re-sent")

	sh, err = st.GetShard(ctx, "main", lost.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardUnmined, sh.Status)
	assert.Equal(t, 1, tr.Sends(lost.Shard.TxID), "not re-sent before RebroadcastAfter (only the original send)")

	clk.Advance(cfg.RebroadcastAfter + time.Second)
	require.NoError(t, w.RunOnce(ctx))
	assert.Equal(t, 2, tr.Sends(lost.Shard.TxID), "re-sent after RebroadcastAfter")
	require.NoError(t, w.RunOnce(ctx))
	assert.Equal(t, 2, tr.Sends(lost.Shard.TxID), "not re-sent again within the same interval")
	clk.Advance(cfg.RebroadcastAfter + time.Second)
	require.NoError(t, w.RunOnce(ctx))
	assert.Equal(t, 3, tr.Sends(lost.Shard.TxID), "re-sent once per interval")
	assert.Equal(t, 1, tr.Sends(pending.Shard.TxID), "still never re-sent")

	orc.SetDown(true)
	require.NoError(t, w.RunOnce(ctx), "oracle outage is not a job failure")
	orc.SetDown(false)

	// A mined verdict whose BEEF cannot be built leaves the shard unmined for the next tick.
	tr.FailMinedBEEF(errors.New("bad bump"))
	orc.SetTx(lost.Shard.TxID, oracle.TxStatus{Known: true, Mined: true, BlockHeight: 8, MerklePath: []byte{1}})
	require.NoError(t, w.RunOnce(ctx))
	sh, err = st.GetShard(ctx, "main", lost.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardUnmined, sh.Status)
	tr.FailMinedBEEF(nil)
	require.NoError(t, w.RunOnce(ctx))
	sh, err = st.GetShard(ctx, "main", lost.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardMined, sh.Status)
}

func TestWatcherRebroadcastRejectionRetires(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	orc := oracle.NewMock()
	tr := mock.New()
	cfg := jobs.DefaultWatcherConfig()
	w := jobs.NewWatcher(st, orc, tr, []string{"main"}, cfg, pool.ClockFunc(clk.Now), slog.Default())

	lost := mintViaTreasury(ctx, t, st, tr, 3)
	// oracle never sees it, so RunOnce past RebroadcastAfter re-sends via the treasury.

	clk.Advance(cfg.RebroadcastAfter + time.Second)
	tr.FailSend(treasury.ErrRejected)
	require.NoError(t, w.RunOnce(ctx), "a rejection discovered on rebroadcast is not a job failure")

	sh, err := st.GetShard(ctx, "main", lost.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardInvalid, sh.Status, "Send's ErrRejected retires the shard like an oracle Rejected verdict")

	got := storetest.RowsByOutpoint(t, st, "main", storetest.Ops(lost.Rows))
	for _, r := range lost.Rows {
		assert.Equal(t, store.StateInvalid, got[r.Outpoint].State)
	}

	tr.FailSend(nil)
	clk.Advance(cfg.RebroadcastAfter + time.Second)
	require.NoError(t, w.RunOnce(ctx), "a retired shard is no longer listed as unmined, so no further rebroadcast happens")
	assert.Equal(t, 1, tr.Sends(lost.Shard.TxID), "only the original successful Send counts; the failed rebroadcast attempt does not")
}

func TestWatcherMinedWithoutMerklePathWaits(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	orc := oracle.NewMock()
	tr := mock.New()
	w := jobs.NewWatcher(st, orc, tr, []string{"main"}, jobs.DefaultWatcherConfig(), pool.ClockFunc(clk.Now), slog.Default())
	b := mintViaTreasury(ctx, t, st, tr, 2)
	orc.SetTx(b.Shard.TxID, oracle.TxStatus{Known: true, Mined: true, BlockHeight: 7}) // no BUMP yet
	require.NoError(t, w.RunOnce(ctx))
	sh, err := st.GetShard(ctx, "main", b.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardUnmined, sh.Status, "mined without a merkle path: wait for the proof")
	assert.Equal(t, 1, tr.Sends(b.Shard.TxID), "a known tx is not re-sent")
}
