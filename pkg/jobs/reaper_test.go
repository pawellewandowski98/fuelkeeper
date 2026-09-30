package jobs_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/jobs"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func reaperFixture(t *testing.T) (*storetest.Clock, *memory.Store, *oracle.Mock, *jobs.Reaper, jobs.ReaperConfig) {
	t.Helper()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	orc := oracle.NewMock()
	cfg := jobs.DefaultReaperConfig()
	r := jobs.NewReaper(st, orc, []string{"main"}, cfg, pool.ClockFunc(clk.Now), slog.Default())
	return clk, st, orc, r, cfg
}

func states(t *testing.T, st store.Store, rows []store.Row) []store.State {
	t.Helper()
	m := storetest.RowsByOutpoint(t, st, "main", storetest.Ops(rows))
	out := make([]store.State, len(rows))
	for i, r := range rows {
		out[i] = m[r.Outpoint].State
	}
	return out
}

func TestReaperExpiryThenUnspentRequeue(t *testing.T) {
	ctx := context.Background()
	clk, st, orc, r, cfg := reaperFixture(t)
	_, rows := storetest.MintShard(t, st, "main", 3, store.ShardMined)
	tok := store.NewToken()
	storetest.MustReserve(t, st, "main", rows, tok, clk.Now().Add(10*time.Second))
	for _, row := range rows {
		orc.SetUnspent(row.Outpoint)
	}

	clk.Advance(10*time.Second + cfg.Grace/2)
	require.NoError(t, r.RunOnce(ctx))
	assert.Equal(t, []store.State{store.StateReserved, store.StateReserved, store.StateReserved}, states(t, st, rows), "inside the grace window nothing is quarantined")

	clk.Advance(cfg.Grace)
	require.NoError(t, r.RunOnce(ctx))
	assert.Equal(t, []store.State{store.StateQuarantined, store.StateQuarantined, store.StateQuarantined}, states(t, st, rows))
	assert.Equal(t, 0, orc.Calls(), "rows younger than QuarantineMinAge are not checked yet")

	clk.Advance(cfg.QuarantineMinAge + time.Second)
	require.NoError(t, r.RunOnce(ctx))
	assert.Equal(t, []store.State{store.StateAvailable, store.StateAvailable, store.StateAvailable}, states(t, st, rows), "unspent after MinUnspentAge → available")
	assert.Positive(t, orc.Calls())
	got := storetest.RowsByOutpoint(t, st, "main", storetest.Ops(rows))
	for _, row := range rows {
		assert.True(t, got[row.Outpoint].ReservedBy.IsZero(), "provenance cleared on requeue")
		assert.Nil(t, got[row.Outpoint].SpentHint)
	}
}

func TestReaperSpentHintResolvesSpent(t *testing.T) {
	ctx := context.Background()
	clk, st, orc, r, cfg := reaperFixture(t)
	_, rows := storetest.MintShard(t, st, "main", 2, store.ShardMined)
	tok := store.NewToken()
	spender := storetest.NewTxID()
	storetest.MustReserve(t, st, "main", rows, tok, clk.Now().Add(time.Minute))
	_, err := st.Spend(ctx, "main", tok, storetest.Ops(rows), spender, false)
	require.NoError(t, err)

	orc.SetSpent(rows[0].Outpoint, spender)                       // outpoint oracle knows row 0
	orc.SetTx(spender, oracle.TxStatus{Known: true, Mined: true}) // row 1 is Unknown by outpoint; Tx(hint) says seen
	clk.Advance(cfg.QuarantineMinAge + time.Second)
	require.NoError(t, r.RunOnce(ctx))
	got := storetest.RowsByOutpoint(t, st, "main", storetest.Ops(rows))
	for _, row := range rows {
		assert.Equal(t, store.StateSpent, got[row.Outpoint].State)
		require.NotNil(t, got[row.Outpoint].SpentBy)
		assert.Equal(t, spender, *got[row.Outpoint].SpentBy)
	}
}

func TestReaperHintedUnspentWaitsForPatience(t *testing.T) {
	ctx := context.Background()
	clk, st, orc, r, cfg := reaperFixture(t)
	_, rows := storetest.MintShard(t, st, "main", 1, store.ShardMined)
	tok := store.NewToken()
	spender := storetest.NewTxID()
	storetest.MustReserve(t, st, "main", rows, tok, clk.Now().Add(time.Minute))
	_, err := st.Spend(ctx, "main", tok, storetest.Ops(rows), spender, false)
	require.NoError(t, err)
	orc.SetUnspent(rows[0].Outpoint) // the client's tx never propagated

	clk.Advance(cfg.MinUnspentAge + time.Second)
	require.NoError(t, r.RunOnce(ctx))
	assert.Equal(t, store.StateQuarantined, states(t, st, rows)[0], "a hinted row waits HintPatience, not MinUnspentAge")

	clk.Advance(cfg.HintPatience)
	require.NoError(t, r.RunOnce(ctx))
	assert.Equal(t, store.StateAvailable, states(t, st, rows)[0])
}

// TestReaperLeavesRowsOnUnknownConflictOrOracleDown deviates from the plan's
// listing by adding one clk.Advance between the tick that quarantines the rows
// and the ticks that inspect them: ListQuarantined's before argument is
// evaluated against the current clock each tick (now - QuarantineMinAge), and
// a row's quarantined_at is stamped from that same clock at the instant it is
// quarantined. Without letting time pass after the quarantining tick, the rows
// never clear the QuarantineMinAge gate and the oracle is never consulted, so
// the final tick could not resolve row 2. See reaper_test.go's paging test for
// the same underlying timing property, called out explicitly in the plan.
func TestReaperLeavesRowsOnUnknownConflictOrOracleDown(t *testing.T) {
	ctx := context.Background()
	clk, st, orc, r, cfg := reaperFixture(t)
	_, rows := storetest.MintShard(t, st, "main", 3, store.ShardMined)
	tok := store.NewToken()
	storetest.MustReserve(t, st, "main", rows, tok, clk.Now().Add(time.Second))
	clk.Advance(time.Second + cfg.Grace + cfg.QuarantineMinAge + time.Hour)
	require.NoError(t, r.RunOnce(ctx)) // quarantines; too fresh for the oracle to be consulted yet
	assert.Equal(t, []store.State{store.StateQuarantined, store.StateQuarantined, store.StateQuarantined}, states(t, st, rows))

	// Let the freshly quarantined rows clear QuarantineMinAge before the next ticks.
	clk.Advance(cfg.QuarantineMinAge + time.Second)

	orc.SetConflicting(rows[1].Outpoint)
	orc.SetUnspent(rows[2].Outpoint)
	orc.SetDown(true)
	require.NoError(t, r.RunOnce(ctx), "oracle outage is not a job failure")
	assert.Equal(t, []store.State{store.StateQuarantined, store.StateQuarantined, store.StateQuarantined}, states(t, st, rows))

	orc.SetDown(false)
	require.NoError(t, r.RunOnce(ctx))
	assert.Equal(t, []store.State{store.StateQuarantined, store.StateQuarantined, store.StateAvailable}, states(t, st, rows), "unknown and conflicting stay; unspent returns")
}

func TestReaperNilHintIsUnhinted(t *testing.T) {
	ctx := context.Background()
	clk, st, orc, r, cfg := reaperFixture(t)
	_, rows := storetest.MintShard(t, st, "main", 1, store.ShardMined)
	tok := store.NewToken()
	storetest.MustReserve(t, st, "main", rows, tok, clk.Now().Add(time.Minute))
	_, err := st.Spend(ctx, "main", tok, storetest.Ops(rows), chainhash.Hash{}, false) // release_policy=verify path: zero txid in, nil SpentHint stored
	require.NoError(t, err)
	orc.SetUnspent(rows[0].Outpoint)
	clk.Advance(cfg.MinUnspentAge + time.Second)
	require.NoError(t, r.RunOnce(ctx))
	assert.Equal(t, store.StateAvailable, states(t, st, rows)[0], "nil hint → MinUnspentAge applies")
}

// TestReaperPagesAndChunksLargeQuarantine restructures the plan's single-tick
// version. The 1000 rows are quarantined in the same tick that would try to
// list them: ListQuarantined's before argument is now - QuarantineMinAge, and
// a row's quarantined_at is stamped at that same now, so a just-quarantined
// row never clears the QuarantineMinAge gate in the tick that quarantined it
// (see reaper.go's reapPool: the age check is against the clock, not wall
// time elapsed before the tick started). So: tick 1 only quarantines (all
// 1000, since QuarantineBatch is 1000); we then advance the clock so those
// rows clear QuarantineMinAge and MinUnspentAge; tick 2 then resolves at most
// Batch×MaxPages = 500 of them (the paging cap this test exists to prove);
// tick 3 finishes the remaining 500.
func TestReaperPagesAndChunksLargeQuarantine(t *testing.T) {
	ctx := context.Background()
	clk, st, orc, _, cfg := reaperFixture(t)
	cfg.Batch = 50
	cfg.MaxPages = 10
	cfg.OracleBatch = 30
	r := jobs.NewReaper(st, orc, []string{"main"}, cfg, pool.ClockFunc(clk.Now), slog.Default())
	_, rows := storetest.MintShard(t, st, "main", 1000, store.ShardMined)
	storetest.MustReserve(t, st, "main", rows, store.NewToken(), clk.Now().Add(time.Second))
	for _, row := range rows {
		orc.SetUnspent(row.Outpoint)
	}

	clk.Advance(time.Second + cfg.Grace + time.Second)
	require.NoError(t, r.RunOnce(ctx)) // quarantines all 1000 (QuarantineBatch 1000); too fresh to list yet
	stats, err := st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, int64(1000), stats.Quarantined, "all rows quarantined in one tick")
	assert.Equal(t, int64(0), stats.Available)

	clk.Advance(cfg.QuarantineMinAge + cfg.MinUnspentAge + time.Second)
	require.NoError(t, r.RunOnce(ctx)) // resolves up to Batch×MaxPages = 500 this tick
	stats, err = st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, int64(500), stats.Available, "one tick resolves at most Batch×MaxPages rows")
	assert.Equal(t, int64(500), stats.Quarantined)

	require.NoError(t, r.RunOnce(ctx))
	stats, err = st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, int64(1000), stats.Available)
	assert.Equal(t, int64(0), stats.Quarantined)
}
