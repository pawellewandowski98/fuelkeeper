package jobs_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/jobs"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// countingOracle wraps an Oracle, counts Tx calls per txid, and can fail Tx
// for every txid (down) or for one txid (bad).
type countingOracle struct {
	oracle.Oracle
	mu    sync.Mutex
	calls map[chainhash.Hash]int
	total int
	down  bool
	bad   *chainhash.Hash
}

func newCountingOracle(inner oracle.Oracle) *countingOracle {
	return &countingOracle{Oracle: inner, calls: map[chainhash.Hash]int{}}
}

func (o *countingOracle) Tx(ctx context.Context, txid chainhash.Hash) (oracle.TxStatus, error) {
	o.mu.Lock()
	o.calls[txid]++
	o.total++
	fail := o.down || (o.bad != nil && *o.bad == txid)
	o.mu.Unlock()
	if fail {
		return oracle.TxStatus{}, oracle.ErrUnavailable
	}
	return o.Oracle.Tx(ctx, txid)
}

func (o *countingOracle) setDown(v bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.down = v
}

func (o *countingOracle) txCalls() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.total
}

type hintEnv struct {
	clk *storetest.Clock
	st  *memory.Store
	orc *oracle.Mock
	r   *jobs.Reaper
	cfg jobs.ReaperConfig
	p   string
	log *bytes.Buffer
}

// newHintEnv builds a reaper over one pool named p. wrap, when non-nil,
// wraps the mock oracle the reaper sees.
func newHintEnv(t *testing.T, p string, cfg jobs.ReaperConfig, wrap func(*oracle.Mock) oracle.Oracle) *hintEnv {
	t.Helper()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	mock := oracle.NewMock()
	var orc oracle.Oracle = mock
	if wrap != nil {
		orc = wrap(mock)
	}
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	r, err := jobs.NewReaper(st, orc, testPools(p), cfg, pool.ClockFunc(clk.Now), log)
	require.NoError(t, err)
	return &hintEnv{clk: clk, st: st, orc: mock, r: r, cfg: cfg, p: p, log: &buf}
}

// hinted mints n rows and spends them with hint (quarantined now, hinted).
func (e *hintEnv) hinted(t *testing.T, n int, hint chainhash.Hash) []store.Row {
	t.Helper()
	_, rows := storetest.MintShard(t, e.st, e.p, n, store.ShardMined)
	tok := store.NewToken()
	storetest.MustReserve(t, e.st, e.p, rows, tok, e.clk.Now().Add(time.Minute))
	_, err := e.st.Spend(context.Background(), e.p, tok, storetest.Ops(rows), hint, false)
	require.NoError(t, err)
	return rows
}

func (e *hintEnv) tick(t *testing.T) {
	t.Helper()
	require.NoError(t, e.r.RunOnce(context.Background()))
}

func (e *hintEnv) held(reason string) float64 {
	return testutil.ToFloat64(jobs.ReaperHeld.WithLabelValues(e.p, reason))
}

func (e *hintEnv) requeued(reason string) float64 {
	return testutil.ToFloat64(jobs.ReaperRequeued.WithLabelValues(e.p, reason))
}

func (e *hintEnv) spent(source string) float64 {
	return testutil.ToFloat64(jobs.ReaperSpent.WithLabelValues(e.p, source))
}

func (e *hintEnv) spentBy(t *testing.T, rows []store.Row) []chainhash.Hash {
	t.Helper()
	got := storetest.RowsByOutpoint(t, e.st, e.p, storetest.Ops(rows))
	out := make([]chainhash.Hash, len(rows))
	for i, row := range rows {
		require.Equal(t, store.StateSpent, got[row.Outpoint].State)
		require.NotNil(t, got[row.Outpoint].SpentBy)
		out[i] = *got[row.Outpoint].SpentBy
	}
	return out
}

func rejected(reason oracle.RejectReason, competitors ...chainhash.Hash) oracle.TxStatus {
	ts := oracle.TxStatus{Known: true, Rejected: true, RejectReason: reason}
	for _, c := range competitors {
		ts.CompetingTxs = append(ts.CompetingTxs, c.String())
	}
	return ts
}

func TestReaperHintedRejectedInvalidRequeuesAfterHintPatience(t *testing.T) {
	e := newHintEnv(t, "hint-invalid", jobs.DefaultReaperConfig(), nil)
	hint := storetest.NewTxID()
	rows := e.hinted(t, 2, hint)
	e.orc.SetTx(hint, rejected(oracle.RejectReasonTxInvalid))
	before := e.requeued("rejected")

	e.clk.Advance(e.cfg.QuarantineMinAge + time.Second)
	e.tick(t)
	assert.True(t, allIn(t, e.st, e.p, rows, store.StateQuarantined), "an invalid hint waits HintPatience")
	assert.InDelta(t, 2, e.held("rejected_fresh"), 0)

	e.clk.Advance(e.cfg.HintPatience - e.cfg.QuarantineMinAge - 2*time.Second)
	e.tick(t)
	assert.True(t, allIn(t, e.st, e.p, rows, store.StateQuarantined), "just under HintPatience")

	e.clk.Advance(time.Second)
	e.tick(t)
	assert.True(t, allIn(t, e.st, e.p, rows, store.StateQuarantined), "HintPatience reached, but the hint was answered under a second ago: throttled")
	assert.InDelta(t, 2, e.held("rejected_fresh"), 0, "throttled rows keep their last reason")

	e.clk.Advance(e.cfg.TxRecheckInterval)
	e.tick(t)
	assert.True(t, allIn(t, e.st, e.p, rows, store.StateAvailable), "an invalid hint's rows are requeued after HintPatience, on the next recheck")
	got := storetest.RowsByOutpoint(t, e.st, e.p, storetest.Ops(rows))
	for _, row := range rows {
		assert.Nil(t, got[row.Outpoint].SpentHint)
		assert.True(t, got[row.Outpoint].ReservedBy.IsZero())
	}
	assert.InDelta(t, before+2, e.requeued("rejected"), 0)
}

func TestReaperHintedRejectedInputsSpentRetiresRow(t *testing.T) {
	e := newHintEnv(t, "hint-inputs-spent", jobs.DefaultReaperConfig(), nil)
	hint := storetest.NewTxID()
	rows := e.hinted(t, 2, hint)
	e.orc.SetTx(hint, rejected(oracle.RejectReasonInputsSpent))
	before := e.spent("inputs_spent")

	e.clk.Advance(e.cfg.QuarantineMinAge + time.Second)
	e.tick(t)
	assert.Equal(t, []chainhash.Hash{hint, hint}, e.spentBy(t, rows), "SpentBy is the attempt (the hint), not the unknown spender")
	assert.InDelta(t, before+2, e.spent("inputs_spent"), 0)
}

func TestReaperHintedWithMinedCompetitorResolvesSpentByCompetitor(t *testing.T) {
	e := newHintEnv(t, "hint-competitor", jobs.DefaultReaperConfig(), nil)
	hint, comp := storetest.NewTxID(), storetest.NewTxID()
	rows := e.hinted(t, 3, hint)
	e.orc.SetTx(hint, rejected(oracle.RejectReasonInputsSpent, comp))
	e.orc.SetTx(comp, oracle.TxStatus{Known: true, Mined: true}) // Inputs unknown, no resolver: trusted
	before := e.spent("competitor")

	e.clk.Advance(e.cfg.QuarantineMinAge + time.Second)
	e.tick(t)
	assert.Equal(t, []chainhash.Hash{comp, comp, comp}, e.spentBy(t, rows))
	assert.InDelta(t, before+3, e.spent("competitor"), 0)
	assert.Equal(t, 1, strings.Count(e.log.String(), "trusting a mined competitor without an input check"),
		"Warn once per hint txid per tick when the competitor's inputs are unknown")
}

func TestReaperCompetitorInputsCheckedWhenResolverExists(t *testing.T) {
	for _, tc := range []struct {
		name     string
		includes bool
	}{{"includes_outpoint", true}, {"excludes_outpoint", false}} {
		t.Run(tc.name, func(t *testing.T) {
			var io *inputsOracle
			e := newHintEnv(t, "hint-comp-inputs-"+tc.name, jobs.DefaultReaperConfig(), func(m *oracle.Mock) oracle.Oracle {
				io = &inputsOracle{Mock: m, inputs: map[chainhash.Hash][]store.Outpoint{}, calls: map[chainhash.Hash]int{}}
				return io
			})
			hint, comp := storetest.NewTxID(), storetest.NewTxID()
			rows := e.hinted(t, 1, hint)
			e.orc.SetTx(hint, rejected(oracle.RejectReasonInputsSpent, comp))
			e.orc.SetTx(comp, oracle.TxStatus{Known: true, Mined: true})
			other := store.Outpoint{TxID: storetest.NewTxID(), Vout: 0}
			if tc.includes {
				io.inputs[comp] = []store.Outpoint{other, rows[0].Outpoint}
			} else {
				io.inputs[comp] = []store.Outpoint{other}
			}

			e.clk.Advance(e.cfg.QuarantineMinAge + time.Second)
			e.tick(t)
			assert.Equal(t, 1, io.calls[comp], "the resolver is asked for the competitor's inputs")
			if tc.includes {
				assert.Equal(t, []chainhash.Hash{comp}, e.spentBy(t, rows), "a mined competitor spending the outpoint is the spender")
			} else {
				assert.Equal(t, store.StateQuarantined, statesIn(t, e.st, e.p, rows)[0],
					"the only mined competitor provably spends other inputs: the fuel output is not assumed gone")
				assert.InDelta(t, 1, e.held("hint_pending"), 0)
			}
		})
	}
}

func TestReaperUnminedCompetitorHolds(t *testing.T) {
	cfg := jobs.DefaultReaperConfig()
	cfg.HintMaxWait = 0
	e := newHintEnv(t, "hint-comp-unmined", cfg, nil)
	hint, comp := storetest.NewTxID(), storetest.NewTxID()
	rows := e.hinted(t, 1, hint)
	e.orc.SetTx(hint, rejected(oracle.RejectReasonInputsSpent, comp))
	e.orc.SetTx(comp, oracle.TxStatus{Known: true}) // seen, not mined

	e.clk.Advance(e.cfg.QuarantineMinAge + time.Second)
	e.tick(t)
	assert.Equal(t, store.StateQuarantined, statesIn(t, e.st, e.p, rows)[0], "an unmined competitor is not proof; the inputs-spent rule must not pre-empt it")
	assert.InDelta(t, 1, e.held("competitor_pending"), 0)

	e.clk.Advance(30 * 24 * time.Hour)
	e.tick(t)
	assert.Equal(t, store.StateQuarantined, statesIn(t, e.st, e.p, rows)[0], "hint_max_wait 0: held indefinitely")

	e.orc.SetTx(comp, oracle.TxStatus{Known: true, Mined: true})
	e.clk.Advance(e.cfg.TxRecheckInterval)
	e.tick(t)
	assert.Equal(t, []chainhash.Hash{comp}, e.spentBy(t, rows), "once the competitor is mined it is the spender")
}

// holdsUntilHintMaxWait drives a hinted row whose Tx(hint) answer never
// changes (set by setup) and checks it is held with reason until
// HintMaxWait, then requeued (hint_expired); with HintMaxWait 0, never.
func holdsUntilHintMaxWait(t *testing.T, name, reason string, setup func(*oracle.Mock, chainhash.Hash)) {
	t.Run("requeued_at_hint_max_wait", func(t *testing.T) {
		e := newHintEnv(t, name, jobs.DefaultReaperConfig(), nil)
		hint := storetest.NewTxID()
		rows := e.hinted(t, 2, hint)
		setup(e.orc, hint)
		before := e.requeued("hint_expired")

		e.clk.Advance(e.cfg.QuarantineMinAge + time.Second)
		e.tick(t)
		assert.True(t, allIn(t, e.st, e.p, rows, store.StateQuarantined))
		assert.InDelta(t, 2, e.held(reason), 0)

		e.clk.Advance(e.cfg.HintMaxWait - e.cfg.QuarantineMinAge - 2*time.Second)
		e.tick(t)
		assert.True(t, allIn(t, e.st, e.p, rows, store.StateQuarantined), "just under hint_max_wait")
		assert.InDelta(t, 2, e.held(reason), 0)

		e.clk.Advance(time.Second)
		e.tick(t)
		assert.True(t, allIn(t, e.st, e.p, rows, store.StateQuarantined), "hint_max_wait reached, but inside the recheck interval: throttled")

		e.clk.Advance(e.cfg.TxRecheckInterval)
		e.tick(t)
		assert.True(t, allIn(t, e.st, e.p, rows, store.StateAvailable), "requeued on the first recheck at or after hint_max_wait")
		assert.InDelta(t, before+2, e.requeued("hint_expired"), 0)
		assert.Equal(t, 1, strings.Count(e.log.String(), "requeuing hinted rows after hint_max_wait"), "Warn once per txid per tick")
	})
	t.Run("never_with_zero_hint_max_wait", func(t *testing.T) {
		cfg := jobs.DefaultReaperConfig()
		cfg.HintMaxWait = 0
		e := newHintEnv(t, name+"-zero", cfg, nil)
		hint := storetest.NewTxID()
		rows := e.hinted(t, 1, hint)
		setup(e.orc, hint)
		e.clk.Advance(e.cfg.QuarantineMinAge + time.Second)
		e.tick(t)
		e.clk.Advance(30 * 24 * time.Hour)
		e.tick(t)
		assert.True(t, allIn(t, e.st, e.p, rows, store.StateQuarantined))
		assert.InDelta(t, 1, e.held(reason), 0)
	})
}

func TestReaperHintedUnminedHoldsUntilHintMaxWait(t *testing.T) {
	holdsUntilHintMaxWait(t, "hint-unmined", "hint_pending", func(m *oracle.Mock, hint chainhash.Hash) {
		m.SetTx(hint, oracle.TxStatus{Known: true})
	})
}

func TestReaperHintUnseenHoldsUntilHintMaxWait(t *testing.T) {
	holdsUntilHintMaxWait(t, "hint-unseen", "hint_unseen", func(*oracle.Mock, chainhash.Hash) {})
}

func TestReaperHintedRejectedUnknownHoldsUntilHintMaxWait(t *testing.T) {
	holdsUntilHintMaxWait(t, "hint-rej-unknown", "rejected_unknown", func(m *oracle.Mock, hint chainhash.Hash) {
		m.SetTx(hint, rejected(oracle.RejectReasonUnknown))
	})
}

func TestReaperTxOutageNeverRequeuesHintedRows(t *testing.T) {
	comp := storetest.NewTxID()
	cases := []struct {
		name       string
		ts         *oracle.TxStatus
		competitor bool // only the competitor's lookup fails
	}{
		{"unseen", nil, false},
		{"unmined", &oracle.TxStatus{Known: true}, false},
		{"rejected_invalid", ptr(rejected(oracle.RejectReasonTxInvalid)), false},
		{"rejected_unknown", ptr(rejected(oracle.RejectReasonUnknown)), false},
		{"competitor_down", ptr(rejected(oracle.RejectReasonInputsSpent, comp)), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var co *countingOracle
			e := newHintEnv(t, "hint-txdown-"+tc.name, jobs.DefaultReaperConfig(), func(m *oracle.Mock) oracle.Oracle {
				co = newCountingOracle(m)
				return co
			})
			hint := storetest.NewTxID()
			rows := e.hinted(t, 1, hint)
			if tc.ts != nil {
				e.orc.SetTx(hint, *tc.ts)
			}
			if tc.competitor {
				c := comp
				co.bad = &c
			} else {
				co.setDown(true)
			}
			e.clk.Advance(e.cfg.HintMaxWait + e.cfg.HintPatience + e.cfg.UnknownRequeueAge + time.Hour)
			for range 3 {
				e.tick(t)
				e.clk.Advance(e.cfg.TxRecheckInterval)
			}
			assert.Equal(t, store.StateQuarantined, statesIn(t, e.st, e.p, rows)[0], "no hinted transition without a Tx answer")
			assert.InDelta(t, 1, e.held("tx_down"), 0)
		})
	}
}

func ptr[T any](v T) *T { return &v }

func TestReaperCompetitorLookupsCappedAndCached(t *testing.T) {
	for _, m := range []int{3, 12} {
		var co *countingOracle
		e := newHintEnv(t, "hint-comp-cap", jobs.DefaultReaperConfig(), func(mk *oracle.Mock) oracle.Oracle {
			co = newCountingOracle(mk)
			return co
		})
		hint := storetest.NewTxID()
		comps := make([]chainhash.Hash, m)
		for i := range comps {
			comps[i] = storetest.NewTxID()
			e.orc.SetTx(comps[i], oracle.TxStatus{Known: true}) // none mined
		}
		e.hinted(t, 5, hint)
		e.orc.SetTx(hint, rejected(oracle.RejectReasonInputsSpent, comps...))

		e.clk.Advance(e.cfg.QuarantineMinAge + time.Second)
		e.tick(t)
		assert.LessOrEqual(t, co.txCalls(), 1+min(m, 8), "M=%d: one hint lookup plus at most maxCompetitorsPerHint competitor lookups, shared by every row", m)
		assert.InDelta(t, 5, e.held("competitor_pending"), 0)
	}
}

func TestReaperRechecksHintAtMostOncePerInterval(t *testing.T) {
	var co *countingOracle
	e := newHintEnv(t, "hint-recheck", jobs.DefaultReaperConfig(), func(m *oracle.Mock) oracle.Oracle {
		co = newCountingOracle(m)
		return co
	})
	hint := storetest.NewTxID()
	e.hinted(t, 3, hint)

	e.clk.Advance(e.cfg.QuarantineMinAge + time.Second)
	e.tick(t)
	require.Equal(t, 1, co.calls[hint])
	for elapsed := time.Second; elapsed < e.cfg.TxRecheckInterval; elapsed += time.Second {
		e.clk.Advance(time.Second)
		e.tick(t)
	}
	assert.Equal(t, 1, co.calls[hint], "no recheck within tx_recheck_interval")
	assert.InDelta(t, 3, e.held("hint_unseen"), 0, "throttled rows are held with their last reason")

	e.clk.Advance(time.Second)
	e.tick(t)
	assert.Equal(t, 2, co.calls[hint], "rechecked once the interval has passed")
}

func TestReaperHintThrottleIsBounded(t *testing.T) {
	e := newHintEnv(t, "hint-throttle-cap", jobs.DefaultReaperConfig(), nil)
	jobs.SetHintCheckCap(e.r, 3)
	for range 5 {
		e.hinted(t, 1, storetest.NewTxID())
	}
	e.clk.Advance(e.cfg.QuarantineMinAge + time.Second)
	e.tick(t)
	assert.LessOrEqual(t, jobs.HintChecks(e.r), 3)
	assert.InDelta(t, 5, e.held("hint_unseen"), 0)

	e.clk.Advance(e.cfg.TxRecheckInterval)
	e.tick(t)
	e.clk.Advance(e.cfg.TxRecheckInterval)
	jobs.PruneHintChecks(e.r, e.clk.Now().Add(e.cfg.TxRecheckInterval))
	assert.Zero(t, jobs.HintChecks(e.r), "entries past their next check are dropped")
}

func TestReaperConflictingOutpointVerdictHoldsPastEveryAge(t *testing.T) {
	e := newHintEnv(t, "hint-conflicting", jobs.DefaultReaperConfig(), nil)
	hint := storetest.NewTxID()
	hintedRows := e.hinted(t, 1, hint)
	e.orc.SetTx(hint, rejected(oracle.RejectReasonTxInvalid))
	_, plain := storetest.MintShard(t, e.st, e.p, 1, store.ShardMined)
	reserveToExpire(t, e.st, e.clk, e.p, plain)
	e.orc.SetConflicting(hintedRows[0].Outpoint)
	e.orc.SetConflicting(plain[0].Outpoint)
	quarantineTick(t, e.r, e.clk, e.cfg)

	e.clk.Advance(30 * 24 * time.Hour)
	e.tick(t)
	e.clk.Advance(e.cfg.TxRecheckInterval)
	e.tick(t)
	assert.Equal(t, store.StateQuarantined, statesIn(t, e.st, e.p, hintedRows)[0])
	assert.Equal(t, store.StateQuarantined, statesIn(t, e.st, e.p, plain)[0])
	assert.InDelta(t, 2, e.held("conflicting"), 0)
}

// verdictOracle overrides the mock's Outpoints verdict for chosen outpoints.
type verdictOracle struct {
	*oracle.Mock
	over map[store.Outpoint]oracle.Result
}

func (o *verdictOracle) Outpoints(ctx context.Context, ops []store.Outpoint) ([]oracle.Result, error) {
	res, err := o.Mock.Outpoints(ctx, ops)
	if err != nil {
		return nil, err
	}
	for i, op := range ops {
		if r, ok := o.over[op]; ok {
			res[i] = r
		}
	}
	return res, nil
}

func TestReaperHeldGaugeCoversEveryExaminedRow(t *testing.T) {
	var vo *verdictOracle
	cfg := jobs.DefaultReaperConfig()
	cfg.MinUnspentAge = 2 * time.Minute
	e := newHintEnv(t, "held-cover", cfg, func(m *oracle.Mock) oracle.Oracle {
		vo = &verdictOracle{Mock: m, over: map[store.Outpoint]oracle.Result{}}
		return vo
	})
	_, rows := storetest.MintShard(t, e.st, e.p, 3, store.ShardMined)
	reserveToExpire(t, e.st, e.clk, e.p, rows)
	e.orc.SetUnspent(rows[0].Outpoint)                                                                // unspent_fresh
	vo.over[rows[1].Outpoint] = oracle.Result{Outpoint: rows[1].Outpoint, Status: oracle.StatusSpent} // spent_no_txid
	e.orc.SetSpent(rows[2].Outpoint, storetest.NewTxID())                                             // spender_pending: the named txid is unknown
	quarantineTick(t, e.r, e.clk, cfg)

	e.clk.Advance(cfg.QuarantineMinAge + time.Second)
	e.tick(t)
	assert.InDelta(t, 1, e.held("unspent_fresh"), 0)
	assert.InDelta(t, 1, e.held("spent_no_txid"), 0)
	assert.InDelta(t, 1, e.held("spender_pending"), 0)
	assert.InDelta(t, 0, e.held("hint_unseen"), 0, "an unhinted row waiting on the oracle's own spender is not a hint_* row")

	e.orc.SetDown(true)
	e.clk.Advance(time.Second)
	e.tick(t)
	assert.InDelta(t, 3, e.held("oracle_down"), 0, "rows skipped by an outpoint-oracle outage are counted")
	assert.InDelta(t, 0, e.held("spent_no_txid"), 0)
}

// TestReaperInputsSpentHintWhoseInputsExcludeOutputHolds pins rule 3's own
// input check: an inputs-spent rejection of a hint whose known inputs do not
// include the fuel output says nothing about that output.
func TestReaperInputsSpentHintWhoseInputsExcludeOutputHolds(t *testing.T) {
	e := newHintEnv(t, "hint-inputs-spent-excl", jobs.DefaultReaperConfig(), nil)
	hint := storetest.NewTxID()
	rows := e.hinted(t, 1, hint)
	ts := rejected(oracle.RejectReasonInputsSpent)
	ts.Inputs = []store.Outpoint{{TxID: storetest.NewTxID(), Vout: 0}}
	e.orc.SetTx(hint, ts)
	before := e.spent("inputs_spent")

	e.clk.Advance(e.cfg.QuarantineMinAge + time.Second)
	e.tick(t)
	assert.Equal(t, store.StateQuarantined, statesIn(t, e.st, e.p, rows)[0])
	assert.InDelta(t, 1, e.held("hint_pending"), 0)
	assert.InDelta(t, before, e.spent("inputs_spent"), 0)
}

// TestReaperEmptyExpiryPolicyHolds pins that a pool whose ExpiryPolicy was
// never set (a Go caller skipping pool.Config.Validate) is treated as hold,
// never as requeue.
func TestReaperEmptyExpiryPolicyHolds(t *testing.T) {
	const p = "expiry-empty"
	cfg := jobs.DefaultReaperConfig()
	pools := testPools(p)
	pools[0].ExpiryPolicy = ""
	clk, st, _, r := unhintedFixture(t, cfg, pools)
	_, rows := storetest.MintShard(t, st, p, 1, store.ShardMined)
	reserveToExpire(t, st, clk, p, rows)
	quarantineTick(t, r, clk, cfg)

	clk.Advance(cfg.UnknownRequeueAge + time.Hour)
	require.NoError(t, r.RunOnce(context.Background()))
	assert.Equal(t, store.StateQuarantined, statesIn(t, st, p, rows)[0])
	assert.InDelta(t, 1, testutil.ToFloat64(jobs.ReaperHeld.WithLabelValues(p, "expiry_hold")), 0)
}

// TestReaperInputsSpentUsesGatewayNamedSpentInputs covers rule 3b: when the
// gateway names the spent inputs, the fuel output is retired only if it is
// among them; otherwise the transaction died on another input and the row
// is requeued like an invalid transaction, after HintPatience. nil keeps
// the assumed-gone retirement (rule 3d).
func TestReaperInputsSpentUsesGatewayNamedSpentInputs(t *testing.T) {
	other := store.Outpoint{TxID: storetest.NewTxID(), Vout: 1}
	cases := []struct {
		name    string
		spent   func(op store.Outpoint) []store.Outpoint
		retired bool
	}{
		{"n_includes_outpoint", func(op store.Outpoint) []store.Outpoint { return []store.Outpoint{other, op} }, true},
		{"o_excludes_outpoint", func(store.Outpoint) []store.Outpoint { return []store.Outpoint{other} }, false},
		{"p_empty_non_nil", func(store.Outpoint) []store.Outpoint { return []store.Outpoint{} }, false},
		{"q_nil", func(store.Outpoint) []store.Outpoint { return nil }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newHintEnv(t, "hint-spent-inputs-"+tc.name, jobs.DefaultReaperConfig(), nil)
			hint := storetest.NewTxID()
			rows := e.hinted(t, 1, hint)
			ts := rejected(oracle.RejectReasonInputsSpent)
			ts.SpentInputs = tc.spent(rows[0].Outpoint)
			e.orc.SetTx(hint, ts)
			beforeSpent, beforeRequeued := e.spent("inputs_spent"), e.requeued("rejected")

			e.clk.Advance(e.cfg.QuarantineMinAge + time.Second)
			e.tick(t)
			if tc.retired {
				assert.Equal(t, []chainhash.Hash{hint}, e.spentBy(t, rows), "SpentBy is the attempt (the hint)")
				assert.InDelta(t, beforeSpent+1, e.spent("inputs_spent"), 0)
				return
			}
			assert.Equal(t, store.StateQuarantined, statesIn(t, e.st, e.p, rows)[0], "before HintPatience the row is held")
			assert.InDelta(t, 1, e.held("rejected_fresh"), 0)
			assert.InDelta(t, beforeSpent, e.spent("inputs_spent"), 0)

			e.clk.Advance(e.cfg.HintPatience)
			e.tick(t)
			assert.Equal(t, store.StateAvailable, statesIn(t, e.st, e.p, rows)[0], "a conflict on another input requeues the fuel output")
			assert.InDelta(t, beforeRequeued+1, e.requeued("rejected"), 0)
		})
	}
}

// TestReaperGatewayNamedSpentInputsOutrankMinedCompetitors pins rule 3's
// order: SpentInputs excluding the outpoint (3b) wins over mined competitors
// that spend only other inputs (3c, which would hold hint_pending).
func TestReaperGatewayNamedSpentInputsOutrankMinedCompetitors(t *testing.T) {
	var io *inputsOracle
	e := newHintEnv(t, "hint-spent-inputs-vs-comp", jobs.DefaultReaperConfig(), func(m *oracle.Mock) oracle.Oracle {
		io = &inputsOracle{Mock: m, inputs: map[chainhash.Hash][]store.Outpoint{}, calls: map[chainhash.Hash]int{}}
		return io
	})
	hint, comp := storetest.NewTxID(), storetest.NewTxID()
	other := store.Outpoint{TxID: storetest.NewTxID(), Vout: 0}
	rows := e.hinted(t, 1, hint)
	ts := rejected(oracle.RejectReasonInputsSpent, comp)
	ts.SpentInputs = []store.Outpoint{other}
	e.orc.SetTx(hint, ts)
	e.orc.SetTx(comp, oracle.TxStatus{Known: true, Mined: true})
	io.inputs[comp] = []store.Outpoint{other}

	e.clk.Advance(e.cfg.QuarantineMinAge + time.Second)
	e.tick(t)
	assert.InDelta(t, 1, e.held("rejected_fresh"), 0)
	e.clk.Advance(e.cfg.HintPatience)
	e.tick(t)
	assert.Equal(t, store.StateAvailable, statesIn(t, e.st, e.p, rows)[0])
}

// TestReaperArcadeUTXOSpentShapeResolvesInOneTick feeds the reaper the exact
// TxStatus arcade.Oracle.Tx returns for a UTXO_SPENT line: Rejected,
// RejectReasonInputsSpent, SpentInputs naming one outpoint, the named
// spender appended to CompetingTxs, and Inputs nil (arcade never serves
// them; no InputsResolver). The gateway's list decides the row (rule 3b)
// whatever arcade knows about the spender: a spender that is mined must not
// be trusted as the fuel output's spender when the list excludes it, and an
// unknown or unmined one must not hold the row as competitor_pending.
func TestReaperArcadeUTXOSpentShapeResolvesInOneTick(t *testing.T) {
	spenders := []struct {
		name string
		set  func(m *oracle.Mock, spender chainhash.Hash)
	}{
		{"spender_mined", func(m *oracle.Mock, s chainhash.Hash) { m.SetTx(s, oracle.TxStatus{Known: true, Mined: true}) }},
		{"spender_unmined", func(m *oracle.Mock, s chainhash.Hash) { m.SetTx(s, oracle.TxStatus{Known: true}) }},
		{"spender_unknown", func(*oracle.Mock, chainhash.Hash) {}}, // 404 at arcade
	}
	for _, includes := range []bool{true, false} {
		for _, sp := range spenders {
			name := "excludes_" + sp.name
			if includes {
				name = "includes_" + sp.name
			}
			t.Run(name, func(t *testing.T) {
				e := newHintEnv(t, "hint-arcade-utxo-spent-"+name, jobs.DefaultReaperConfig(), nil)
				hint, spender := storetest.NewTxID(), storetest.NewTxID()
				rows := e.hinted(t, 1, hint)
				named := store.Outpoint{TxID: storetest.NewTxID(), Vout: 3}
				if includes {
					named = rows[0].Outpoint
				}
				e.orc.SetTx(hint, oracle.TxStatus{
					Known: true, Rejected: true, RejectReason: oracle.RejectReasonInputsSpent,
					SpentInputs: []store.Outpoint{named}, CompetingTxs: []string{spender.String()},
				})
				sp.set(e.orc, spender)
				beforeInputsSpent, beforeCompetitor := e.spent("inputs_spent"), e.spent("competitor")
				beforeRejected := e.requeued("rejected")

				e.clk.Advance(e.cfg.QuarantineMinAge + time.Second)
				e.tick(t)
				assert.InDelta(t, beforeCompetitor, e.spent("competitor"), 0, "the named spender is never trusted over the gateway's list")
				assert.InDelta(t, 0, e.held("competitor_pending"), 0)
				if includes {
					assert.Equal(t, []chainhash.Hash{hint}, e.spentBy(t, rows), "retired in one tick; SpentBy is the attempt")
					assert.InDelta(t, beforeInputsSpent+1, e.spent("inputs_spent"), 0)
					return
				}
				assert.Equal(t, store.StateQuarantined, statesIn(t, e.st, e.p, rows)[0])
				assert.InDelta(t, 1, e.held("rejected_fresh"), 0)

				e.clk.Advance(e.cfg.HintPatience)
				e.tick(t)
				assert.Equal(t, store.StateAvailable, statesIn(t, e.st, e.p, rows)[0], "the conflict was on another input: requeued after hint_patience")
				assert.InDelta(t, beforeRejected+1, e.requeued("rejected"), 0)
				assert.InDelta(t, beforeInputsSpent, e.spent("inputs_spent"), 0)
			})
		}
	}
}
