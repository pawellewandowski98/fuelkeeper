package jobs_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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

func mintViaTreasury(ctx context.Context, t *testing.T, st store.Store, tr *mock.Treasury, poolName string, n int) *treasury.BuiltShard {
	t.Helper()
	b, err := tr.Build(ctx, treasury.MintRequest{Pool: poolName, Denomination: 50, Count: n, LockMode: store.LockPuzzle})
	require.NoError(t, err)
	require.NoError(t, st.PutShard(ctx, b.Shard, b.Rows))
	require.NoError(t, tr.Send(ctx, b.Shard.TxID, b.Shard.BEEF))
	return b
}

// newWatcher builds a watcher and fails the test immediately if cfg is invalid.
func newWatcher(t *testing.T, st store.Store, orc oracle.Oracle, tr treasury.Treasury, pools []string, cfg jobs.WatcherConfig, now func() time.Time) *jobs.Watcher {
	t.Helper()
	w, err := jobs.NewWatcher(st, orc, tr, pools, cfg, pool.ClockFunc(now), slog.Default())
	require.NoError(t, err)
	return w
}

// counterValue reads one label combination of a registered counter vec from
// the default Prometheus registry, so a test can assert a metric without the
// package exporting its otherwise-private collector.
func counterValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if len(m.GetLabel()) != len(labels) {
				continue
			}
			match := true
			for _, l := range m.GetLabel() {
				if labels[l.GetName()] != l.GetValue() {
					match = false
					break
				}
			}
			if match {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func TestWatcherMinesRetiresAndResends(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	orc := oracle.NewMock()
	tr := mock.New()
	cfg := jobs.DefaultWatcherConfig()
	w := newWatcher(t, st, orc, tr, []string{"main"}, cfg, clk.Now)

	mined := mintViaTreasury(ctx, t, st, tr, "main", 3)
	rejected := mintViaTreasury(ctx, t, st, tr, "main", 3)
	lost := mintViaTreasury(ctx, t, st, tr, "main", 3)
	pending := mintViaTreasury(ctx, t, st, tr, "main", 3)
	tok := store.NewToken()
	storetest.MustReserve(t, st, "main", rejected.Rows[:1], tok, clk.Now().Add(time.Minute))

	storedBEEF, err := st.GetShard(ctx, "main", mined.Shard.TxID)
	require.NoError(t, err)
	require.NotEmpty(t, storedBEEF.BEEF, "the minted shard must have stored BEEF for MinedBEEF to thread through")

	orc.SetTx(mined.Shard.TxID, oracle.TxStatus{Known: true, Mined: true, BlockHeight: 7, MerklePath: []byte{9, 9}})
	orc.SetTx(rejected.Shard.TxID, oracle.TxStatus{Known: true, Rejected: true, Status: "REJECTED"})
	orc.SetTx(pending.Shard.TxID, oracle.TxStatus{Known: true, Status: "SEEN_ON_NETWORK"})
	// lost: oracle has never seen it

	require.NoError(t, w.RunOnce(ctx))

	sh, err := st.GetShard(ctx, "main", mined.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardMined, sh.Status)
	assert.Equal(t, int64(2), sh.Version)
	wantBEEF := append([]byte("mock-mined-beef-"), mined.Shard.TxID[:]...)
	wantBEEF = append(wantBEEF, storedBEEF.BEEF...)
	wantBEEF = append(wantBEEF, 9, 9)
	assert.Equal(t, wantBEEF, sh.BEEF, "the watcher threads the shard's stored unmined BEEF (from GetShard) into MinedBEEF, not just the txid and merkle path")

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
	assert.Equal(t, lost.Shard.BEEF, tr.LastSendBEEF(lost.Shard.TxID), "the rebroadcast carries the shard's stored BEEF (ListShards omits it)")
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
	w := newWatcher(t, st, orc, tr, []string{"main"}, cfg, clk.Now)

	lost := mintViaTreasury(ctx, t, st, tr, "main", 3)
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

func TestWatcherRebroadcastTransientFailureLeavesUnmined(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	orc := oracle.NewMock()
	tr := mock.New()
	cfg := jobs.DefaultWatcherConfig()
	w := newWatcher(t, st, orc, tr, []string{"main"}, cfg, clk.Now)

	lost := mintViaTreasury(ctx, t, st, tr, "main", 3)
	clk.Advance(cfg.RebroadcastAfter + time.Second)

	before := counterValue(t, "fuelkeeper_watcher_transitions_total", map[string]string{"pool": "main", "outcome": "rebroadcast_failed"})
	tr.FailSend(errors.New("flake"))
	require.NoError(t, w.RunOnce(ctx), "a transient Send failure is not a job failure")

	sh, err := st.GetShard(ctx, "main", lost.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardUnmined, sh.Status, "a transient rebroadcast failure leaves the shard unmined")
	assert.Equal(t, 1, tr.Sends(lost.Shard.TxID), "the failed attempt is not counted as a send")

	after := counterValue(t, "fuelkeeper_watcher_transitions_total", map[string]string{"pool": "main", "outcome": "rebroadcast_failed"})
	assert.InDelta(t, before+1, after, 0.001, "the rebroadcast_failed outcome is counted")
}

func TestWatcherMinedWithoutMerklePathWaits(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	orc := oracle.NewMock()
	tr := mock.New()
	w := newWatcher(t, st, orc, tr, []string{"main"}, jobs.DefaultWatcherConfig(), clk.Now)
	b := mintViaTreasury(ctx, t, st, tr, "main", 2)
	orc.SetTx(b.Shard.TxID, oracle.TxStatus{Known: true, Mined: true, BlockHeight: 7}) // no BUMP yet
	require.NoError(t, w.RunOnce(ctx))
	sh, err := st.GetShard(ctx, "main", b.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardUnmined, sh.Status, "mined without a merkle path: wait for the proof")
	assert.Equal(t, 1, tr.Sends(b.Shard.TxID), "a known tx is not re-sent")
}

// TestWatcherRotationChecksWholeBacklogWithinBound proves that a fetched
// batch larger than MaxChecksPerTick does not starve the shards past the
// first window: the rotation must reach shards created later even though
// they are behind more shards than fit in a single tick's checks, and even
// though the permanently-unknown shards ahead of them never leave the
// unmined set (so the fetched batch never shrinks out from under them).
func TestWatcherRotationChecksWholeBacklogWithinBound(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	orc := oracle.NewMock()
	tr := mock.New()
	cfg := jobs.DefaultWatcherConfig()
	cfg.Limit = 10
	cfg.MaxChecksPerTick = 3
	w := newWatcher(t, st, orc, tr, []string{"main"}, cfg, clk.Now)

	// 8 shards the oracle never resolves (permanently unknown), oldest first.
	for range 8 {
		mintViaTreasury(ctx, t, st, tr, "main", 1)
		clk.Advance(time.Millisecond)
	}
	// 2 more, created later (so they sort last), that the oracle reports mined.
	m1 := mintViaTreasury(ctx, t, st, tr, "main", 1)
	clk.Advance(time.Millisecond)
	m2 := mintViaTreasury(ctx, t, st, tr, "main", 1)
	orc.SetTx(m1.Shard.TxID, oracle.TxStatus{Known: true, Mined: true, BlockHeight: 7, MerklePath: []byte{1}})
	orc.SetTx(m2.Shard.TxID, oracle.TxStatus{Known: true, Mined: true, BlockHeight: 7, MerklePath: []byte{2}})

	for range 4 {
		require.NoError(t, w.RunOnce(ctx))
	}

	sh1, err := st.GetShard(ctx, "main", m1.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardMined, sh1.Status, "promoted within 4 = ceil(10/3) ticks despite 8 unresolved shards ahead of it")
	sh2, err := st.GetShard(ctx, "main", m2.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardMined, sh2.Status, "promoted within 4 ticks: the rotation must wrap around rather than re-checking the same leading shards forever")
}

// TestWatcherTwoPoolsRebroadcastOncePerInterval guards against lastTx
// bookkeeping for one pool being wiped by another pool's prune pass in the
// same tick: each pool has one permanently-unknown shard older than
// RebroadcastAfter, and three consecutive ticks must each resend at most
// once in total, not once per tick per pool.
func TestWatcherTwoPoolsRebroadcastOncePerInterval(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	orc := oracle.NewMock()
	tr := mock.New()
	cfg := jobs.DefaultWatcherConfig()
	w := newWatcher(t, st, orc, tr, []string{"main", "alt"}, cfg, clk.Now)

	a := mintViaTreasury(ctx, t, st, tr, "main", 1)
	b := mintViaTreasury(ctx, t, st, tr, "alt", 1)
	// Neither txid is ever configured on the oracle: both stay permanently unknown.

	clk.Advance(cfg.RebroadcastAfter + time.Second)
	for range 3 {
		require.NoError(t, w.RunOnce(ctx))
	}

	assert.Equal(t, 2, tr.Sends(a.Shard.TxID), "exactly one rebroadcast beyond the original send over three ticks")
	assert.Equal(t, 2, tr.Sends(b.Shard.TxID), "exactly one rebroadcast beyond the original send over three ticks; a per-pool prune would have wiped this pool's lastTx entry and caused a resend every tick")
}

// failingListShards wraps a store.Store and fails ListShards for one pool,
// so a test can simulate one pool being unavailable while others are fine.
type failingListShards struct {
	store.Store
	badPool string
	err     error
}

func (f *failingListShards) ListShards(ctx context.Context, poolName string, filter store.ShardFilter) ([]store.Shard, error) {
	if poolName == f.badPool {
		return nil, f.err
	}
	return f.Store.ListShards(ctx, poolName, filter)
}

// TestWatcherRunOnceContinuesPastPoolError proves that one pool's ListShards
// failure does not block the rest: RunOnce must still service every other
// pool and report the failure by joining it into its returned error.
func TestWatcherRunOnceContinuesPastPoolError(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	injected := errors.New("boom")
	st := &failingListShards{Store: memory.New(memory.WithClock(clk.Now)), badPool: "bad", err: injected}
	orc := oracle.NewMock()
	tr := mock.New()
	cfg := jobs.DefaultWatcherConfig()
	w := newWatcher(t, st, orc, tr, []string{"bad", "main"}, cfg, clk.Now)

	lost := mintViaTreasury(ctx, t, st, tr, "main", 1)
	clk.Advance(cfg.RebroadcastAfter + time.Second)

	err := w.RunOnce(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, injected, "the failing pool's error is joined into RunOnce's result")
	assert.Equal(t, 2, tr.Sends(lost.Shard.TxID), "the good pool ('main') is still serviced despite 'bad' failing")
}

func TestWatcherConfigValidate(t *testing.T) {
	valid := jobs.DefaultWatcherConfig()
	require.NoError(t, valid.Validate())

	cases := []struct {
		name string
		cfg  jobs.WatcherConfig
	}{
		{"interval", jobs.WatcherConfig{RebroadcastAfter: time.Minute, AlertAfter: time.Hour, Limit: 1, MaxChecksPerTick: 1}},
		{"rebroadcast_after", jobs.WatcherConfig{Interval: time.Second, AlertAfter: time.Hour, Limit: 1, MaxChecksPerTick: 1}},
		{"alert_after", jobs.WatcherConfig{Interval: time.Second, RebroadcastAfter: time.Hour, AlertAfter: time.Minute, Limit: 1, MaxChecksPerTick: 1}},
		{"limit", jobs.WatcherConfig{Interval: time.Second, RebroadcastAfter: time.Minute, AlertAfter: time.Hour, MaxChecksPerTick: 1}},
		{"max_checks_per_tick", jobs.WatcherConfig{Interval: time.Second, RebroadcastAfter: time.Minute, AlertAfter: time.Hour, Limit: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, tc.cfg.Validate())
			_, err := jobs.NewWatcher(memory.New(), oracle.NewMock(), mock.New(), []string{"main"}, tc.cfg, pool.RealClock{}, slog.Default())
			require.Error(t, err, "NewWatcher rejects an invalid config")
		})
	}
}
