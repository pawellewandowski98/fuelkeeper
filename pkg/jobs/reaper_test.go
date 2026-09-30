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
	r, err := jobs.NewReaper(st, orc, []string{"main"}, cfg, pool.ClockFunc(clk.Now), slog.Default())
	require.NoError(t, err)
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

func allStates(t *testing.T, st store.Store, rows []store.Row, want store.State) bool {
	t.Helper()
	for _, s := range states(t, st, rows) {
		if s != want {
			return false
		}
	}
	return true
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
	orc.SetTx(spender, oracle.TxStatus{Known: true, Mined: true}) // row 1 is Unknown by outpoint; Tx(hint) says mined
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

// TestReaperLeavesRowsOnUnknownConflictOrOracleDown adds one clk.Advance after
// the quarantining tick: a row's quarantined_at is stamped from the same
// clock as the QuarantineMinAge gate, so without letting time pass first the
// gate never opens and the last tick could not resolve row 2.
func TestReaperLeavesRowsOnUnknownConflictOrOracleDown(t *testing.T) {
	ctx := context.Background()
	clk, st, orc, r, cfg := reaperFixture(t)
	_, rows := storetest.MintShard(t, st, "main", 3, store.ShardMined)
	tok := store.NewToken()
	storetest.MustReserve(t, st, "main", rows, tok, clk.Now().Add(time.Second))
	clk.Advance(time.Second + cfg.Grace + cfg.QuarantineMinAge + time.Hour)
	require.NoError(t, r.RunOnce(ctx)) // quarantines; too fresh for the oracle to be consulted yet
	assert.Equal(t, []store.State{store.StateQuarantined, store.StateQuarantined, store.StateQuarantined}, states(t, st, rows))

	clk.Advance(cfg.QuarantineMinAge + time.Second) // let the freshly quarantined rows clear QuarantineMinAge

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

// TestReaperPagesAndChunksLargeQuarantine splits the plan's single tick into
// three: rows quarantined this tick are too fresh to be listed in the same
// tick (see the comment above), so tick 1 only quarantines; a clock advance
// then lets tick 2 resolve the paging cap (Batch×MaxPages = 500) and tick 3
// finish the rest.
func TestReaperPagesAndChunksLargeQuarantine(t *testing.T) {
	ctx := context.Background()
	clk, st, orc, _, cfg := reaperFixture(t)
	cfg.Batch = 50
	cfg.MaxPages = 10
	cfg.OracleBatch = 30
	r, err := jobs.NewReaper(st, orc, []string{"main"}, cfg, pool.ClockFunc(clk.Now), slog.Default())
	require.NoError(t, err)
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

// TestReaperCursorAdvancesPastStuckRows reproduces C1: 20 rows that never
// resolve (Unknown, no hint) sit in front of 5 newer Unspent rows. With
// Batch=10, MaxPages=2 the window is exactly the 20 stuck rows, so a reaper
// that always restarts paging from the head would starve the 5 forever. A
// resumed cursor must walk past the stuck rows and resolve the 5 within a
// few ticks.
func TestReaperCursorAdvancesPastStuckRows(t *testing.T) {
	ctx := context.Background()
	clk, st, orc, _, cfg := reaperFixture(t)
	cfg.Batch = 10
	cfg.MaxPages = 2
	r, err := jobs.NewReaper(st, orc, []string{"main"}, cfg, pool.ClockFunc(clk.Now), slog.Default())
	require.NoError(t, err)

	_, rows := storetest.MintShard(t, st, "main", 25, store.ShardMined)
	stuck, fresh := rows[:20], rows[20:]
	storetest.MustReserve(t, st, "main", rows, store.NewToken(), clk.Now().Add(time.Second))
	for _, row := range fresh {
		orc.SetUnspent(row.Outpoint) // stuck rows are left Unknown (never configured)
	}

	clk.Advance(time.Second + cfg.Grace + time.Second)
	require.NoError(t, r.RunOnce(ctx)) // quarantines all 25; too fresh to list yet

	clk.Advance(cfg.QuarantineMinAge + time.Second)
	const maxTicks = 6
	resolved := false
	for i := 0; i < maxTicks && !resolved; i++ {
		require.NoError(t, r.RunOnce(ctx))
		resolved = allStates(t, st, fresh, store.StateAvailable)
	}
	assert.True(t, resolved, "the 5 newer rows must become available within a few ticks")
	assert.True(t, allStates(t, st, stuck, store.StateQuarantined), "the 20 stuck rows are never resolved")
}

// TestReaperResolvesPartialClassificationOnOracleFailure covers I1: a failure
// on a later Outpoints batch must not discard verdicts already classified
// from earlier batches in the same tick.
func TestReaperResolvesPartialClassificationOnOracleFailure(t *testing.T) {
	ctx := context.Background()
	clk, st, orc, _, cfg := reaperFixture(t)
	cfg.OracleBatch = 10
	fo := &failAfterNOracle{Oracle: orc, failOn: 3}
	r, err := jobs.NewReaper(st, fo, []string{"main"}, cfg, pool.ClockFunc(clk.Now), slog.Default())
	require.NoError(t, err)

	_, rows := storetest.MintShard(t, st, "main", 30, store.ShardMined)
	storetest.MustReserve(t, st, "main", rows, store.NewToken(), clk.Now().Add(time.Second))
	for _, row := range rows {
		orc.SetUnspent(row.Outpoint)
	}
	clk.Advance(time.Second + cfg.Grace + time.Second)
	require.NoError(t, r.RunOnce(ctx)) // quarantines all 30

	clk.Advance(cfg.QuarantineMinAge + cfg.MinUnspentAge + time.Second)
	require.NoError(t, r.RunOnce(ctx)) // 3rd Outpoints batch (rows 20-29) fails
	stats, err := st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, int64(20), stats.Available, "batches classified before the failure are still resolved")
	assert.Equal(t, int64(10), stats.Quarantined)
}

// failAfterNOracle wraps an Oracle and fails the Nth Outpoints call.
type failAfterNOracle struct {
	oracle.Oracle
	failOn int
	calls  int
}

func (o *failAfterNOracle) Outpoints(ctx context.Context, ops []store.Outpoint) ([]oracle.Result, error) {
	o.calls++
	if o.calls == o.failOn {
		return nil, oracle.ErrUnavailable
	}
	return o.Oracle.Outpoints(ctx, ops)
}

// TestReaperTxDownIsCheckedOncePerTick covers I2: many rows sharing one hint
// must cost at most one Tx lookup (and one warning) per tick when Tx is down,
// not one per row.
func TestReaperTxDownIsCheckedOncePerTick(t *testing.T) {
	ctx := context.Background()
	clk, st, orc, _, cfg := reaperFixture(t)
	txo := &txDownOracle{Oracle: orc}
	r, err := jobs.NewReaper(st, txo, []string{"main"}, cfg, pool.ClockFunc(clk.Now), slog.Default())
	require.NoError(t, err)

	_, rows := storetest.MintShard(t, st, "main", 50, store.ShardMined)
	tok := store.NewToken()
	hint := storetest.NewTxID()
	storetest.MustReserve(t, st, "main", rows, tok, clk.Now().Add(time.Minute))
	_, err = st.Spend(ctx, "main", tok, storetest.Ops(rows), hint, false)
	require.NoError(t, err)
	// Outpoints verdict stays Unknown for all 50 (never configured on orc).

	clk.Advance(cfg.QuarantineMinAge + time.Second)
	require.NoError(t, r.RunOnce(ctx))
	assert.LessOrEqual(t, txo.txCalls, 1, "one Tx lookup per tick, not one per row")
	assert.True(t, allStates(t, st, rows, store.StateQuarantined))
}

// txDownOracle wraps an Oracle and always fails Tx, counting calls.
type txDownOracle struct {
	oracle.Oracle
	txCalls int
}

func (o *txDownOracle) Tx(_ context.Context, _ chainhash.Hash) (oracle.TxStatus, error) {
	o.txCalls++
	return oracle.TxStatus{}, oracle.ErrUnavailable
}

// TestReaperHintedUnknownStaysQuarantinedUnlessMined covers I4/M6: a hinted
// row whose Outpoints verdict is Unknown only becomes spent once Tx(hint)
// reports Known && Mined && !Rejected.
func TestReaperHintedUnknownStaysQuarantinedUnlessMined(t *testing.T) {
	cases := []struct {
		name string
		set  bool
		ts   oracle.TxStatus
	}{
		{"tx_unset", false, oracle.TxStatus{}},
		{"known_but_unmined", true, oracle.TxStatus{Known: true}},
		{"known_but_rejected", true, oracle.TxStatus{Known: true, Rejected: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			clk, st, orc, r, cfg := reaperFixture(t)
			_, rows := storetest.MintShard(t, st, "main", 1, store.ShardMined)
			tok := store.NewToken()
			hint := storetest.NewTxID()
			storetest.MustReserve(t, st, "main", rows, tok, clk.Now().Add(time.Minute))
			_, err := st.Spend(ctx, "main", tok, storetest.Ops(rows), hint, false)
			require.NoError(t, err)
			if tc.set {
				orc.SetTx(hint, tc.ts)
			}
			// Outpoints verdict stays Unknown (never configured).

			clk.Advance(cfg.QuarantineMinAge + time.Second)
			require.NoError(t, r.RunOnce(ctx))
			assert.Equal(t, store.StateQuarantined, states(t, st, rows)[0])
		})
	}
}

// TestReaperOutpointOracleOverridesHint covers the mixed-evidence test: the
// outpoint oracle's own SpendingTxID is trusted over a conflicting client hint.
func TestReaperOutpointOracleOverridesHint(t *testing.T) {
	ctx := context.Background()
	clk, st, orc, r, cfg := reaperFixture(t)
	_, rows := storetest.MintShard(t, st, "main", 1, store.ShardMined)
	tok := store.NewToken()
	hinted := storetest.NewTxID()
	actual := storetest.NewTxID()
	storetest.MustReserve(t, st, "main", rows, tok, clk.Now().Add(time.Minute))
	_, err := st.Spend(ctx, "main", tok, storetest.Ops(rows), hinted, false)
	require.NoError(t, err)
	orc.SetSpent(rows[0].Outpoint, actual)
	orc.SetTx(actual, oracle.TxStatus{Known: true, Mined: true}) // the oracle's own SpendingTxID must also be confirmed mined

	clk.Advance(cfg.QuarantineMinAge + time.Second)
	require.NoError(t, r.RunOnce(ctx))
	got := storetest.RowsByOutpoint(t, st, "main", storetest.Ops(rows))
	require.Equal(t, store.StateSpent, got[rows[0].Outpoint].State)
	require.NotNil(t, got[rows[0].Outpoint].SpentBy)
	assert.Equal(t, actual, *got[rows[0].Outpoint].SpentBy)
}

// TestReaperSpentBySRequiresMinedTx covers point 1 of the final review: the
// outpoint oracle's own SpendingTxID is only mempool evidence until Tx
// confirms it is mined.
func TestReaperSpentBySRequiresMinedTx(t *testing.T) {
	ctx := context.Background()
	clk, st, orc, r, cfg := reaperFixture(t)
	_, rows := storetest.MintShard(t, st, "main", 1, store.ShardMined)
	tok := store.NewToken()
	spender := storetest.NewTxID()
	storetest.MustReserve(t, st, "main", rows, tok, clk.Now().Add(time.Second))
	orc.SetSpent(rows[0].Outpoint, spender)
	orc.SetTx(spender, oracle.TxStatus{Known: true}) // seen, but not yet mined

	clk.Advance(time.Second + cfg.Grace + time.Second)
	require.NoError(t, r.RunOnce(ctx)) // quarantines; too fresh to consult the oracle yet
	assert.Equal(t, store.StateQuarantined, states(t, st, rows)[0])

	clk.Advance(cfg.QuarantineMinAge + time.Second)
	require.NoError(t, r.RunOnce(ctx))
	assert.Equal(t, store.StateQuarantined, states(t, st, rows)[0], "an unmined spender is not terminal yet")

	orc.SetTx(spender, oracle.TxStatus{Known: true, Mined: true})
	require.NoError(t, r.RunOnce(ctx))
	got := storetest.RowsByOutpoint(t, st, "main", storetest.Ops(rows))
	assert.Equal(t, store.StateSpent, got[rows[0].Outpoint].State)
	require.NotNil(t, got[rows[0].Outpoint].SpentBy)
	assert.Equal(t, spender, *got[rows[0].Outpoint].SpentBy)
}

// TestReaperHintPathDoesNotVerifyInputs documents a known gap recorded in the
// Reaper doc comment: TxStatus carries no input list, so nothing checks that
// a mined txid named by the outpoint oracle (with no SpendingTxID) or by a
// client's hint actually spends THIS outpoint. Under spend_policy=verify, a
// hostile client could name any unrelated mined txid and have the row
// incorrectly resolved to spent. Written out (and skipped) so the gap is
// executable once Plan 5 adds inputs to TxStatus.
func TestReaperHintPathDoesNotVerifyInputs(t *testing.T) {
	t.Skip("Plan 5: TxStatus must carry inputs so the hint path can verify the outpoint is spent by the hinted tx")
	ctx := context.Background()
	clk, st, orc, r, cfg := reaperFixture(t)
	_, rows := storetest.MintShard(t, st, "main", 1, store.ShardMined)
	tok := store.NewToken()
	unrelated := storetest.NewTxID() // mined, but does not actually spend rows[0]
	storetest.MustReserve(t, st, "main", rows, tok, clk.Now().Add(time.Minute))
	_, err := st.Spend(ctx, "main", tok, storetest.Ops(rows), unrelated, false) // hostile client names an unrelated txid
	require.NoError(t, err)
	orc.SetTx(unrelated, oracle.TxStatus{Known: true, Mined: true}) // mined, but for a wholly different outpoint
	// The outpoint oracle's Outpoints verdict for rows[0] stays Unknown, so
	// the reaper falls back to Tx(hint) — which has no way to check whether
	// `unrelated`'s inputs actually include rows[0].

	clk.Advance(cfg.QuarantineMinAge + time.Second)
	require.NoError(t, r.RunOnce(ctx))
	assert.Equal(t, store.StateQuarantined, states(t, st, rows)[0], "must not resolve spent without verifying the hinted tx actually spends this outpoint")
}

// TestReaperResolveChunksToStoreMaxBatch covers the chunking test: Resolve is
// never called with more ops than the store's Capabilities().MaxBatch, even
// when a tick's window holds more rows than that.
func TestReaperResolveChunksToStoreMaxBatch(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	st := &assertingResolveStore{Store: base, t: t}
	orc := oracle.NewMock()
	cfg := jobs.DefaultReaperConfig()
	cfg.Batch = 500
	cfg.MaxPages = 10
	cfg.QuarantineBatch = 1500
	r, err := jobs.NewReaper(st, orc, []string{"main"}, cfg, pool.ClockFunc(clk.Now), slog.Default())
	require.NoError(t, err)

	_, rows := storetest.MintShard(t, st, "main", 1500, store.ShardMined)
	tok := store.NewToken()
	exp := clk.Now().Add(time.Second)
	// Reserve.MaxBatch (1000) is smaller than 1500, so reserve in two batches.
	storetest.MustReserve(t, st, "main", rows[:1000], tok, exp)
	storetest.MustReserve(t, st, "main", rows[1000:], tok, exp)
	for _, row := range rows {
		orc.SetUnspent(row.Outpoint)
	}
	clk.Advance(time.Second + cfg.Grace + time.Second)
	require.NoError(t, r.RunOnce(ctx)) // quarantines all 1500

	clk.Advance(cfg.QuarantineMinAge + cfg.MinUnspentAge + time.Second)
	require.NoError(t, r.RunOnce(ctx)) // window (5000) covers all 1500; Resolve must chunk to MaxBatch (1000)
	stats, err := st.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, int64(1500), stats.Available)
}

// assertingResolveStore wraps a *memory.Store and fails the test if Resolve
// is ever called with more ops than Capabilities().MaxBatch allows.
type assertingResolveStore struct {
	*memory.Store
	t *testing.T
}

func (s *assertingResolveStore) Resolve(ctx context.Context, pool string, ops []store.Outpoint, to store.State, spentBy *chainhash.Hash) (int, error) {
	assert.LessOrEqual(s.t, len(ops), s.Store.Capabilities().MaxBatch, "Resolve must respect Capabilities().MaxBatch")
	return s.Store.Resolve(ctx, pool, ops, to, spentBy)
}

func TestReaperConfigValidate(t *testing.T) {
	valid := jobs.DefaultReaperConfig()
	require.NoError(t, valid.Validate())

	cases := []struct {
		name string
		mod  func(c jobs.ReaperConfig) jobs.ReaperConfig
	}{
		{"interval", func(c jobs.ReaperConfig) jobs.ReaperConfig { c.Interval = 0; return c }},
		{"grace", func(c jobs.ReaperConfig) jobs.ReaperConfig { c.Grace = -1; return c }},
		{"quarantine_min_age", func(c jobs.ReaperConfig) jobs.ReaperConfig { c.QuarantineMinAge = -1; return c }},
		{"min_unspent_age", func(c jobs.ReaperConfig) jobs.ReaperConfig { c.MinUnspentAge = -1; return c }},
		{"hint_patience", func(c jobs.ReaperConfig) jobs.ReaperConfig { c.HintPatience = c.MinUnspentAge - time.Second; return c }},
		{"batch", func(c jobs.ReaperConfig) jobs.ReaperConfig { c.Batch = 0; return c }},
		{"max_pages", func(c jobs.ReaperConfig) jobs.ReaperConfig { c.MaxPages = 0; return c }},
		{"quarantine_batch", func(c jobs.ReaperConfig) jobs.ReaperConfig { c.QuarantineBatch = 0; return c }},
		{"oracle_batch", func(c jobs.ReaperConfig) jobs.ReaperConfig { c.OracleBatch = 0; return c }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, tc.mod(valid).Validate())
		})
	}
}

func TestReaperNewRejectsInvalidConfig(t *testing.T) {
	st := memory.New()
	orc := oracle.NewMock()
	cfg := jobs.DefaultReaperConfig()
	cfg.Batch = 0
	_, err := jobs.NewReaper(st, orc, []string{"main"}, cfg, pool.RealClock{}, slog.Default())
	require.Error(t, err)
}
