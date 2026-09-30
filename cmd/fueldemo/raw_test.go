package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api"
	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/arcadeclient"
	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// mintPuzzleShard mints (and inserts) a shard of n real hash-puzzle rows: an
// actual fuel.PuzzleLock/fuel.PuzzleUnlock pair per row, unlike
// storetest.MintShard's placeholder scripts, so that fuel.VerifyPuzzle
// genuinely validates every row raw mode spends in these tests.
func mintPuzzleShard(t *testing.T, s store.Store, poolName string, n int) []store.Row {
	t.Helper()
	seed := fuel.NewSeed()
	txid := storetest.NewTxID()
	rows := make([]store.Row, n)
	for i := range rows {
		pre := fuel.Preimage(seed, uint32(i)) //nolint:gosec // n is always small in tests
		rows[i] = store.Row{
			Outpoint:        store.Outpoint{TxID: txid, Vout: uint32(i)}, //nolint:gosec // n is always small in tests
			Pool:            poolName,
			Satoshis:        50,
			LockingScript:   fuel.PuzzleLock(fuel.PuzzleHash(pre)),
			UnlockingScript: fuel.PuzzleUnlock(pre),
		}
	}
	shard := store.Shard{
		Pool:         poolName,
		TxID:         txid,
		Size:         n,
		Denomination: 50,
		LockMode:     store.LockPuzzle,
		Status:       store.ShardMined,
		BEEF:         append([]byte("beef-"), txid[:4]...),
	}
	require.NoError(t, s.PutShard(context.Background(), shard, rows))
	return rows
}

// newRawTestEnv builds a running FuelKeeper API server over a real pool.Pool
// and memory store, seeded with shards shards of rowsPerShard real
// hash-puzzle rows each (see mintPuzzleShard), for raw mode's tests to drive
// with rawRunner.worker. It mirrors cmd/fuelbench/run_test.go's
// newBenchEnv, including its ic.MaxShards fixture adjustment (the shard
// manager only rotates in a fresh shard on its background rescan, long past
// a short test's duration, so a "well-stocked" pool needs every minted shard
// ownable up front). The manual clock never advances, so reservations never
// expire mid-test. It returns the server and every minted row, for reading
// state back through the store after a run.
func newRawTestEnv(t *testing.T, shards, rowsPerShard int) (*httptest.Server, store.Store, []store.Row) {
	t.Helper()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	var allRows []store.Row
	for i := 0; i < shards; i++ {
		allRows = append(allRows, mintPuzzleShard(t, st, "main", rowsPerShard)...)
	}
	pc := pool.DefaultPoolConfig("main", 50)
	pc.SpendPolicy = pool.SpendTrust
	pc.MinTTL = time.Second
	require.NoError(t, pc.Validate())
	ic := pool.DefaultInstanceConfig()
	ic.InstanceID = "fueldemo-raw-test"
	if shards > ic.MaxShards {
		ic.MaxShards = shards
	}
	p := pool.NewPool(st, pc, ic, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	srv := httptest.NewServer(api.NewHandler(api.Deps{Pools: []api.PoolService{p}, Store: st}))
	t.Cleanup(func() { srv.Close(); cancel(); <-done })
	return srv, st, allRows
}

// efMarker is the 6 bytes that follow a transaction's 4-byte version in
// extended format: a zero-length input count, a zero-length output count,
// and the 4-byte 0x00000000EF marker (see go-sdk's Transaction.ReadFrom).
var efMarker = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0xEF}

// fakeSubmitter is a fake Submitter for raw mode's tests. Every PostTx call
// asserts the extended-format marker, parses the transaction (go-sdk's
// NewTransactionFromBytes transparently understands EF: see
// Transaction.ReadFrom), and verifies every input's hash-puzzle unlock
// against its attached source output -- so a bug that fed arcade a
// malformed or non-spendable transaction fails the test regardless of which
// scenario (success, reject, backpressure) is under test. err, when set, is
// returned instead of a success response.
type fakeSubmitter struct {
	mu    sync.Mutex
	posts int
	err   error
}

func (f *fakeSubmitter) PostTx(_ context.Context, tx []byte) (*arcadeclient.SubmitResponse, error) {
	if len(tx) < 10 || !bytes.Equal(tx[4:10], efMarker) {
		return nil, fmt.Errorf("fake: expected EF marker %x after the 4-byte version, got %x", efMarker, safeSlice(tx, 4, 10))
	}
	parsed, err := transaction.NewTransactionFromBytes(tx)
	if err != nil {
		return nil, fmt.Errorf("fake: parse EF transaction: %w", err)
	}
	for i, in := range parsed.Inputs {
		src := in.SourceTxOutput()
		if src == nil {
			return nil, fmt.Errorf("fake: input %d carries no source output", i)
		}
		if in.UnlockingScript == nil || !fuel.VerifyPuzzle(*src.LockingScript, *in.UnlockingScript) {
			return nil, fmt.Errorf("fake: input %d does not unlock its source output", i)
		}
	}

	f.mu.Lock()
	f.posts++
	f.mu.Unlock()

	if f.err != nil {
		return nil, f.err
	}
	return &arcadeclient.SubmitResponse{TxID: parsed.TxID().String(), Status: 200, TxStatus: "SEEN_ON_NETWORK"}, nil
}

func (f *fakeSubmitter) postsCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.posts
}

// safeSlice returns tx[from:to], or the whole slice if it is too short, so a
// failing assertion's error message never panics on a short/malformed
// payload.
func safeSlice(tx []byte, from, to int) []byte {
	if len(tx) < to {
		return tx
	}
	return tx[from:to]
}

// spentCount reports how many of rows are now StateSpent, read back through
// st.
// getRowsPageSize pages spentCount's GetRows calls under the store's
// 1..1000-outpoints-per-call limit (see store.ValidateOutpoints), so a test
// env minted with many more than 1000 rows can still be read back.
const getRowsPageSize = 1000

func spentCount(t *testing.T, st store.Store, poolName string, rows []store.Row) int64 {
	t.Helper()
	ops := storetest.Ops(rows)
	var n int64
	for i := 0; i < len(ops); i += getRowsPageSize {
		end := min(i+getRowsPageSize, len(ops))
		got, err := st.GetRows(context.Background(), poolName, ops[i:end])
		require.NoError(t, err)
		for _, r := range got {
			if r.State == store.StateSpent {
				n++
			}
		}
	}
	return n
}

// TestWorkerSpendsFuelIntoOpReturn is raw mode's happy-path end-to-end test:
// several workers reserve, build, submit and report spends against a
// well-stocked pool for one second. It also doubles as the mutation guard
// for "worker forgets to call Spend after a successful submit": if that call
// were dropped, rep.Sent would still be positive but spentCount would stay
// at 0, so the final assertion would fail.
func TestWorkerSpendsFuelIntoOpReturn(t *testing.T) {
	srv, st, rows := newRawTestEnv(t, 400, 200) // 80,000 rows: comfortably outlasts a 1s run
	fk, err := client.New(srv.URL)
	require.NoError(t, err)
	fake := &fakeSubmitter{}
	rep := newReport("raw", "test", 4)
	r := &rawRunner{fk: fk, arc: fake, pool: "main", batch: 5, rep: rep, memo: []byte("fueldemo-test"), satPerKB: 100}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(4)
	for i := 0; i < 4; i++ {
		go func(id int) { defer wg.Done(); r.worker(ctx, id) }(i)
	}
	wg.Wait()

	assert.Positive(t, rep.Sent)
	assert.Empty(t, rep.Failed, "unexpected failures against a well-stocked pool: %v", rep.Failed)
	assert.Equal(t, int(rep.Sent), fake.postsCount())
	assert.Equal(t, rep.Sent, spentCount(t, st, "main", rows), "every reported send must show spent in the store")
}

// TestWorkerRejectStillReportsSpend checks that arcade rejecting a
// submission is still tallied and still reported to FuelKeeper as spent (a
// rejected fuel spend is handed to the oracle, not released).
func TestWorkerRejectStillReportsSpend(t *testing.T) {
	srv, st, rows := newRawTestEnv(t, 1, 5)
	fk, err := client.New(srv.URL)
	require.NoError(t, err)
	fake := &fakeSubmitter{err: &arcadeclient.RejectError{Status: 461, Title: "INVALID_OUTPUTS", Detail: "test"}}
	rep := newReport("raw", "test", 1)
	r := &rawRunner{fk: fk, arc: fake, pool: "main", batch: 5, rep: rep, memo: []byte("x"), satPerKB: 100}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	r.worker(ctx, 0)

	assert.Zero(t, rep.Sent)
	assert.Equal(t, int64(5), rep.Failed["arcade:461"])
	assert.Equal(t, int64(5), spentCount(t, st, "main", rows), "a rejected spend must still be reported to the oracle as spent")
}

// TestWorkerBackpressureReleasesOutputs checks that arcade backpressure is
// tallied and that the affected outputs are released back to the pool
// rather than left reserved or reported spent.
func TestWorkerBackpressureReleasesOutputs(t *testing.T) {
	srv, st, rows := newRawTestEnv(t, 1, 5)
	fk, err := client.New(srv.URL)
	require.NoError(t, err)
	fake := &fakeSubmitter{err: &arcadeclient.BackpressureError{RetryAfter: 0}}
	rep := newReport("raw", "test", 1)
	r := &rawRunner{fk: fk, arc: fake, pool: "main", batch: 5, rep: rep, memo: []byte("x"), satPerKB: 100}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	r.worker(ctx, 0)

	assert.Zero(t, rep.Sent)
	assert.Positive(t, rep.Failed["arcade:backpressure"])
	assert.Zero(t, spentCount(t, st, "main", rows), "backpressure must never be reported as spent")

	got, err := st.GetRows(context.Background(), "main", storetest.Ops(rows))
	require.NoError(t, err)
	for _, row := range got {
		assert.Equal(t, store.StateAvailable, row.State, "unused output must be released back to available")
	}
}

// p2pkhLockingScript returns a syntactically plausible (not cryptographically
// meaningful) 25-byte P2PKH locking script, for tests that only need a
// locking script of the right shape and size.
func p2pkhLockingScript() []byte {
	s := make([]byte, 25)
	s[0], s[1], s[2] = 0x76, 0xa9, 0x14
	s[23], s[24] = 0x88, 0xac
	return s
}

// TestBuildFuelTxP2PKHConstraints checks that buildFuelTx pins the
// transaction's version/lockTime and the input's sequence number to a
// non-nil constraints value, as a presigned P2PKH output's signature
// requires.
func TestBuildFuelTxP2PKHConstraints(t *testing.T) {
	txid := storetest.NewTxID()
	o := wire.Output{
		TxID:          txid.String(),
		Vout:          3,
		Outpoint:      txid.String() + ":3",
		Satoshis:      1000,
		LockingScript: hex.EncodeToString(p2pkhLockingScript()),
		Unlock:        wire.Unlock{Mode: "p2pkh", Script: hex.EncodeToString(bytes.Repeat([]byte{0x01}, 107)), Length: 107},
	}
	constraints := &wire.Constraints{TxVersion: 1, LockTime: 0, Sequence: 0xffffffff}

	tx, err := buildFuelTx(o, constraints, []byte("memo"), nil, 100)
	require.NoError(t, err)

	assert.Equal(t, constraints.TxVersion, tx.Version)
	assert.Equal(t, constraints.LockTime, tx.LockTime)
	require.Len(t, tx.Inputs, 1)
	assert.Equal(t, constraints.Sequence, tx.Inputs[0].SequenceNumber)
	assert.Equal(t, o.Vout, tx.Inputs[0].SourceTxOutIndex)
	assert.Equal(t, txid, *tx.Inputs[0].SourceTXID)
}

// TestBuildFuelTxPuzzleDefaults checks buildFuelTx's defaults when
// constraints is nil (puzzle mode): version 1, lockTime 0, sequence
// 0xffffffff.
func TestBuildFuelTxPuzzleDefaults(t *testing.T) {
	seed := fuel.NewSeed()
	pre := fuel.Preimage(seed, 0)
	txid := storetest.NewTxID()
	o := wire.Output{
		TxID:          txid.String(),
		Vout:          0,
		Outpoint:      txid.String() + ":0",
		Satoshis:      50,
		LockingScript: hex.EncodeToString(fuel.PuzzleLock(fuel.PuzzleHash(pre))),
		Unlock:        wire.Unlock{Mode: "puzzle", Script: hex.EncodeToString(fuel.PuzzleUnlock(pre)), Length: fuel.PreimageSize + 1},
	}

	tx, err := buildFuelTx(o, nil, []byte("memo"), nil, 100)
	require.NoError(t, err)

	assert.Equal(t, uint32(1), tx.Version)
	assert.Equal(t, uint32(0), tx.LockTime)
	require.Len(t, tx.Inputs, 1)
	assert.Equal(t, uint32(0xffffffff), tx.Inputs[0].SequenceNumber)
	require.Len(t, tx.Outputs, 1, "no --change-to: a single OP_RETURN output")
	assert.True(t, tx.Outputs[0].LockingScript.IsData())
}

// TestBuildFuelTxChangeToAddsChangeOutput checks that a non-empty changeTo
// adds a change output sized at the denomination minus the estimated fee.
func TestBuildFuelTxChangeToAddsChangeOutput(t *testing.T) {
	seed := fuel.NewSeed()
	pre := fuel.Preimage(seed, 0)
	txid := storetest.NewTxID()
	o := wire.Output{
		TxID:          txid.String(),
		Vout:          0,
		Outpoint:      txid.String() + ":0",
		Satoshis:      1000,
		LockingScript: hex.EncodeToString(fuel.PuzzleLock(fuel.PuzzleHash(pre))),
		Unlock:        wire.Unlock{Mode: "puzzle", Script: hex.EncodeToString(fuel.PuzzleUnlock(pre)), Length: fuel.PreimageSize + 1},
	}
	changeScript := p2pkhLockingScript()

	tx, err := buildFuelTx(o, nil, []byte("m"), changeScript, 100)
	require.NoError(t, err)

	require.Len(t, tx.Outputs, 2, "OP_RETURN + change")
	change := tx.Outputs[1]
	wantFee := fuel.FeeSats(tx.Size(), 100)
	assert.Equal(t, o.Satoshis-wantFee, change.Satoshis)
	assert.Equal(t, changeScript, []byte(*change.LockingScript))
}

// TestBuildFuelTxChangeToOmittedWhenFeeExceedsDenomination checks that
// buildFuelTx drops the change output entirely (rather than emit a
// zero/negative-value one) when the estimated fee would leave less than 1
// satoshi of the denomination.
func TestBuildFuelTxChangeToOmittedWhenFeeExceedsDenomination(t *testing.T) {
	seed := fuel.NewSeed()
	pre := fuel.Preimage(seed, 0)
	txid := storetest.NewTxID()
	o := wire.Output{
		TxID:          txid.String(),
		Vout:          0,
		Outpoint:      txid.String() + ":0",
		Satoshis:      50,
		LockingScript: hex.EncodeToString(fuel.PuzzleLock(fuel.PuzzleHash(pre))),
		Unlock:        wire.Unlock{Mode: "puzzle", Script: hex.EncodeToString(fuel.PuzzleUnlock(pre)), Length: fuel.PreimageSize + 1},
	}

	tx, err := buildFuelTx(o, nil, []byte("m"), p2pkhLockingScript(), 100_000) // absurd fee rate forces removal
	require.NoError(t, err)

	require.Len(t, tx.Outputs, 1, "fee-only tx: no change output when it would leave < 1 satoshi")
}

// TestApiCode checks the error-to-code mapping raw mode uses to tally
// failures.
func TestApiCode(t *testing.T) {
	assert.Equal(t, "exhausted", apiCode(&client.APIError{Code: "exhausted"}))
	assert.Equal(t, "461", apiCode(&arcadeclient.RejectError{Status: 461}))
	assert.Equal(t, "backpressure", apiCode(&arcadeclient.BackpressureError{RetryAfter: 2}))
	assert.Equal(t, "transport", apiCode(errors.New("boom")))
	assert.Equal(t, "transport", apiCode(context.DeadlineExceeded))
}

// TestReportFinishPercentiles checks report.finish's percentiles against a
// known, hand-computed sample.
func TestReportFinishPercentiles(t *testing.T) {
	r := newReport("raw", "test", 1)
	for _, ms := range []int64{10, 20, 30, 40, 50} {
		r.ok(time.Duration(ms) * time.Millisecond)
	}
	r.finish()

	sorted := []int64{10000, 20000, 30000, 40000, 50000} // µs, ascending
	want := func(p float64) float64 {
		return float64(sorted[int(float64(len(sorted)-1)*p)]) / 1000
	}

	assert.Equal(t, int64(5), r.Sent)
	assert.InDelta(t, want(0.50), r.P50Ms, 0.0001)
	assert.InDelta(t, want(0.95), r.P95Ms, 0.0001)
	assert.InDelta(t, want(0.99), r.P99Ms, 0.0001)
	assert.Positive(t, r.Seconds)
	assert.InDelta(t, float64(r.Sent)/r.Seconds, r.TPS, 1e-6)
	assert.Nil(t, r.Failed, "no failures recorded: Failed must be omitted, not an empty map")
}

// TestReportFinishNoSamplesLeavesPercentilesZero checks that finish on a
// report with no successful sends does not panic and leaves the percentiles
// at their zero value.
func TestReportFinishNoSamplesLeavesPercentilesZero(t *testing.T) {
	r := newReport("raw", "test", 1)
	r.fail("reserve:exhausted")
	r.finish()

	assert.Zero(t, r.Sent)
	assert.Zero(t, r.P50Ms)
	assert.Zero(t, r.P95Ms)
	assert.Zero(t, r.P99Ms)
	assert.Equal(t, map[string]int64{"reserve:exhausted": 1}, r.Failed)
}

// TestParseFlagsBadFlagIsUsageError checks that an unknown flag is reported
// as a usageError, so main would exit 2.
func TestParseFlagsBadFlagIsUsageError(t *testing.T) {
	_, err := parseFlags([]string{"--not-a-real-flag"})
	require.Error(t, err)
	var uerr *usageError
	require.ErrorAs(t, err, &uerr)
	assert.Equal(t, 2, exitCode(err))
}

// TestValidateRejectsMissingRequiredFlags checks options.validate's usage
// errors for each required flag and for raw mode's --arcade requirement.
func TestValidateRejectsMissingRequiredFlags(t *testing.T) {
	tests := []struct {
		name string
		o    options
	}{
		{"missing url", options{mode: "raw", pool: "main", arcadeURL: "http://x", workers: 1, batch: 1, duration: time.Second}},
		{"missing pool", options{mode: "raw", url: "http://x", arcadeURL: "http://x", workers: 1, batch: 1, duration: time.Second}},
		{"raw without arcade", options{mode: "raw", url: "http://x", pool: "main", workers: 1, batch: 1, duration: time.Second}},
		{"bad mode", options{mode: "bogus", url: "http://x", pool: "main", workers: 1, batch: 1, duration: time.Second}},
		{"zero workers", options{mode: "raw", url: "http://x", pool: "main", arcadeURL: "http://x", workers: 0, batch: 1, duration: time.Second}},
		{"zero batch", options{mode: "raw", url: "http://x", pool: "main", arcadeURL: "http://x", workers: 1, batch: 0, duration: time.Second}},
		{"zero duration", options{mode: "raw", url: "http://x", pool: "main", arcadeURL: "http://x", workers: 1, batch: 1}},
		{"negative rate", options{mode: "raw", url: "http://x", pool: "main", arcadeURL: "http://x", workers: 1, batch: 1, duration: time.Second, rateLim: -1}},
		{"bad change-to hex", options{mode: "raw", url: "http://x", pool: "main", arcadeURL: "http://x", workers: 1, batch: 1, duration: time.Second, changeTo: "zz"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.o.validate()
			require.Error(t, err)
			var uerr *usageError
			require.ErrorAs(t, err, &uerr)
			assert.Equal(t, 2, exitCode(err))
		})
	}

	// toolbox mode does not require --arcade.
	require.NoError(t, (options{mode: "toolbox", url: "http://x", pool: "main", workers: 1, batch: 1, duration: time.Second}).validate())
}

// TestParseFlagsHelpExitsZero checks that -h/--help maps to exit code 0.
func TestParseFlagsHelpExitsZero(t *testing.T) {
	for _, h := range []string{"-h", "--help"} {
		_, err := parseFlags([]string{h})
		require.Error(t, err)
		require.ErrorIs(t, err, flag.ErrHelp)
		assert.Equal(t, 0, exitCode(err), h)
	}
}

// TestExitCode checks exitCode's mapping directly.
func TestExitCode(t *testing.T) {
	assert.Equal(t, 0, exitCode(nil))
	assert.Equal(t, 1, exitCode(errors.New("boom")))
	assert.Equal(t, 2, exitCode(usageErrorf("bad")))
}

// TestRunRequiresArcadeInRawMode checks run's top-level wiring: a raw-mode
// run without --arcade never dials FuelKeeper.
func TestRunRequiresArcadeInRawMode(t *testing.T) {
	o := options{mode: "raw", url: "http://127.0.0.1:1", pool: "main", workers: 1, batch: 1, duration: time.Second}
	rep, err := run(context.Background(), o)
	require.Error(t, err)
	assert.Nil(t, rep)
	var uerr *usageError
	require.ErrorAs(t, err, &uerr)
}
