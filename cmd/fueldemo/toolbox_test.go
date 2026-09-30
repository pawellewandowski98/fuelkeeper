package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"github.com/bsv-blockchain/go-sdk/block"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/defs"
	pkgerrors "github.com/bsv-blockchain/go-wallet-toolbox/pkg/errors"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/wdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api"
	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

const (
	toolboxTestKey    = "0000000000000000000000000000000000000000000000000000000000000007"
	toolboxTestHeight = 100
)

func toolboxTestOptions(t *testing.T) options {
	t.Helper()
	return options{
		mode:      "toolbox",
		pool:      "main",
		network:   "ttn",
		walletKey: toolboxTestKey,
		arcadeURL: "http://127.0.0.1:1",
		storage:   filepath.Join(t.TempDir(), "fueldemo.sqlite"),
		feeKB:     100,
		workers:   1,
		batch:     3,
		duration:  time.Second,
	}
}

// TestToolboxRunnerConstructionFailsCleanly checks that toolbox mode refuses
// a missing or unreachable chaintracks (and other bad flags) before creating
// storage, says what is wrong, never echoes a supplied wallet key, and
// leaves no goroutine running.
func TestToolboxRunnerConstructionFailsCleanly(t *testing.T) {
	fk, err := client.New("http://127.0.0.1:1")
	require.NoError(t, err)

	cases := map[string]struct {
		mut  func(*options)
		want string
	}{
		"no chaintracks":          {func(o *options) { o.chaintracks = "" }, "--chaintracks"},
		"unreachable chaintracks": {func(o *options) { o.chaintracks = "http://127.0.0.1:1" }, "chaintracks at http://127.0.0.1:1 unreachable"},
		"no arcade":               {func(o *options) { o.arcadeURL = ""; o.chaintracks = "http://127.0.0.1:1" }, "--arcade"},
		"bad network":             {func(o *options) { o.network = "moon"; o.chaintracks = "http://127.0.0.1:1" }, "--network"},
		"bad wallet key":          {func(o *options) { o.walletKey = "zz" + toolboxTestKey; o.chaintracks = "http://127.0.0.1:1" }, "--wallet-key"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			before := runtime.NumGoroutine()
			o := toolboxTestOptions(t)
			tc.mut(&o)

			err := newToolboxRunner(context.Background(), o, fk, newReport("toolbox", "ttn", 1))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.NotContains(t, err.Error(), toolboxTestKey)

			_, statErr := os.Stat(o.storage)
			require.ErrorIs(t, statErr, os.ErrNotExist, "no sqlite file may be left behind")
			assertNoGoroutineLeak(t, before)
		})
	}
}

// assertNoGoroutineLeak waits up to 5 s for the goroutine count to drop
// back to before (polling by hand: assert.Eventually's own goroutines would
// skew the count).
func assertNoGoroutineLeak(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	assert.LessOrEqual(t, runtime.NumGoroutine(), before, "goroutines leaked")
}

// TestRunToolboxModeRequiresArcade checks run's toolbox path end to end at
// the flag level: a missing --arcade is a usage error (exit code 2), and a
// report shell is still returned.
func TestRunToolboxModeRequiresArcade(t *testing.T) {
	o := options{mode: "toolbox", url: "http://127.0.0.1:1", pool: "main", workers: 1, batch: 1, duration: time.Second, feeKB: 100, network: "ttn"}
	rep, err := run(context.Background(), o)
	require.Error(t, err)
	require.NotNil(t, rep)
	assert.Contains(t, err.Error(), "--arcade")
	assert.Equal(t, 2, exitCode(err))
}

// TestToolboxClassifiesCreateErrors pins classifyCreateError's mapping.
func TestToolboxClassifiesCreateErrors(t *testing.T) {
	someTxID := "ab" + strings.Repeat("0", 62)
	cases := map[string]struct {
		err  error
		code string
		then outcome
		txid string
	}{
		"reject with txid": {
			fmt.Errorf("create action failed: %w", pkgerrors.NewProcessActionError(nil, nil).WithTx(someTxID, nil).Wrap(errors.New("arcade said no"))),
			"reject", outcomeSpend, someTxID,
		},
		"reject without txid": {
			// As storage raises it after a broadcast it then failed to record.
			fmt.Errorf("cannot update single tx after broadcast: %w", pkgerrors.NewProcessActionError(nil, nil).Wrap(pkgerrors.NewTransactionErrorFromTxIDHex(someTxID))),
			"reject:unknown", outcomeHold, "",
		},
		"reject wrapping not-enough-funds": {
			// ProcessActionError.Is also matches its cause; it is still a reject.
			pkgerrors.NewProcessActionError(nil, nil).WithTx(someTxID, nil).Wrap(wdk.ErrNotEnoughFunds),
			"reject", outcomeSpend, someTxID,
		},
		"action deadline": {
			fmt.Errorf("create action failed: failed to process created action: %w", context.DeadlineExceeded),
			"timeout", outcomeHold, "",
		},
		"underfunded": {
			fmt.Errorf("create action failed: failed to create transaction: %w", wdk.ErrNotEnoughFunds),
			"underfunded", outcomeRelease, "",
		},
		"other": {errors.New("failed to verify beef"), "create", outcomeRelease, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			code, then, txid := classifyCreateError(tc.err)
			assert.Equal(t, tc.code, code)
			assert.Equal(t, tc.then, then)
			assert.Equal(t, tc.txid, txid)
		})
	}
}

// TestToolboxServicesConfigIsolated checks that only the command line's
// arcade and chaintracks are enabled: no failover broadcaster or header
// source could move a run off the system under test.
func TestToolboxServicesConfigIsolated(t *testing.T) {
	o := toolboxTestOptions(t)
	o.arcadeURL = "https://arcade.invalid"
	o.chaintracks = "https://chaintracks.invalid"
	id := "02" + strings.Repeat("11", 32)
	for _, network := range []defs.BSVNetwork{defs.NetworkTTN, defs.NetworkMainnet, defs.NetworkTestnet} {
		s, err := servicesConfig(o, network, id)
		require.NoError(t, err, network)
		assert.True(t, s.Arcade.Enabled, network)
		assert.Equal(t, o.arcadeURL, s.Arcade.URL, network)
		assert.Equal(t, wdk.DeriveArcadeCallbackToken(id), s.Arcade.CallbackToken, network)
		assert.True(t, s.ChaintracksClient.Enabled, network)
		assert.Equal(t, defs.ChaintracksClientModeRemote, s.ChaintracksClient.Mode, network)
		assert.Equal(t, o.chaintracks, s.ChaintracksClient.RemoteURL, network)
		assert.False(t, s.ArcConfig.Enabled, network)
		assert.False(t, s.ArcGorillaPoolConfig.Enabled, network)
		assert.False(t, s.WhatsOnChain.Enabled, network)
		assert.False(t, s.Bitails.Enabled, network)
	}
}

// TestToolboxActionArgsP2PKHConstraints checks that a P2PKH reservation's
// constraints reach the createAction call: tx version and lockTime on the
// args, sequence on the input. The values differ from every default so a
// dropped constraint cannot pass.
func TestToolboxActionArgsP2PKHConstraints(t *testing.T) {
	txid := storetest.NewTxID()
	res := &wire.ReserveResponse{
		Token: "tok",
		Outputs: []wire.Output{{
			TxID: txid.String(), Vout: 3, Outpoint: txid.String() + ":3", Satoshis: 50,
			Unlock: wire.Unlock{Mode: "p2pkh", Script: strings.Repeat("01", 107), Length: 107},
		}},
		Constraints: &wire.Constraints{TxVersion: 2, LockTime: 600000, Sequence: 0xfffffffe},
	}
	inputs, err := client.CreateActionInputs(res, toolboxInputDescription)
	require.NoError(t, err)
	tr := &toolboxRunner{memo: []byte{0x00, 0x6a}}
	args := tr.actionArgs(res, []byte("beef"), inputs[0])

	require.NotNil(t, args.Version)
	require.NotNil(t, args.LockTime)
	assert.Equal(t, uint32(2), *args.Version)
	assert.Equal(t, uint32(600000), *args.LockTime)
	require.Len(t, args.Inputs, 1)
	require.NotNil(t, args.Inputs[0].SequenceNumber)
	assert.Equal(t, uint32(0xfffffffe), *args.Inputs[0].SequenceNumber)
	assert.Equal(t, uint32(3), args.Inputs[0].Outpoint.Index)

	// Puzzle mode: no constraints, wallet defaults.
	res.Constraints = nil
	inputs, err = client.CreateActionInputs(res, toolboxInputDescription)
	require.NoError(t, err)
	args = tr.actionArgs(res, []byte("beef"), inputs[0])
	assert.Nil(t, args.Version)
	assert.Nil(t, args.LockTime)
	assert.Nil(t, args.Inputs[0].SequenceNumber)
}

// fakeCreator is an actionCreator that always fails with err.
type fakeCreator struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (f *fakeCreator) CreateAction(context.Context, sdk.CreateActionArgs, string) (*sdk.CreateActionResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return nil, f.err
}

// TestToolboxHoldsOutputsOfUnknownFate checks that an output whose
// broadcast fate is unknown (a txid-less process error, or the action's own
// deadline) is neither released nor reported spent: it stays reserved for
// the reaper's quarantine + oracle path, so no other worker can reserve an
// output that may already be on the network.
func TestToolboxHoldsOutputsOfUnknownFate(t *testing.T) {
	cases := map[string]struct {
		err  error
		code string
	}{
		"reject without txid": {pkgerrors.NewProcessActionError(nil, nil).Wrap(errors.New("cannot update single tx after broadcast")), "reject:unknown"},
		"timeout":             {fmt.Errorf("create action failed: %w", context.DeadlineExceeded), "timeout"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv, st, rows, _ := newToolboxFKEnv(t, 6)
			fk, err := client.New(srv.URL)
			require.NoError(t, err)
			rep := newReport("toolbox", "test", 1)
			tr := &toolboxRunner{fk: fk, w: &fakeCreator{err: tc.err}, pool: "main", batch: 3, rep: rep, memo: []byte{0x00, 0x6a}}

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			tr.worker(ctx, 0)

			assert.Zero(t, rep.Sent)
			assert.Equal(t, int64(6), rep.Failed[tc.code], "every output tried exactly once: %v", rep.Failed)
			got, err := st.GetRows(context.Background(), "main", storetest.Ops(rows))
			require.NoError(t, err)
			for _, r := range got {
				assert.Equal(t, store.StateReserved, r.State, "an output of unknown fate must stay reserved")
			}
		})
	}
}

// TestToolboxPausesAfterFailedBatch checks that a batch with no success
// pauses before the next reserve, so a persistent create failure does not
// hot-loop reserve/release.
func TestToolboxPausesAfterFailedBatch(t *testing.T) {
	srv, st, rows, _ := newToolboxFKEnv(t, 30)
	fk, err := client.New(srv.URL)
	require.NoError(t, err)
	rep := newReport("toolbox", "test", 1)
	tr := &toolboxRunner{fk: fk, w: &fakeCreator{err: errors.New("boom")}, pool: "main", batch: 3, rep: rep, memo: []byte{0x00, 0x6a}}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	tr.worker(ctx, 0)

	assert.Positive(t, rep.Reserves)
	assert.LessOrEqual(t, rep.Reserves, int64(8), "300 ms at one reserve per >= 50 ms pause")
	got, err := st.GetRows(context.Background(), "main", storetest.Ops(rows))
	require.NoError(t, err)
	for _, r := range got {
		assert.Equal(t, store.StateAvailable, r.State, "a pre-broadcast failure releases its output")
	}
}

// mintMinedPuzzleShard mints (and inserts) one mined shard of n real
// hash-puzzle rows whose BEEF is a real BEEF v2: the split transaction plus
// a one-leaf BUMP at toolboxTestHeight (the block's merkle root is the
// split's txid). It returns the rows and that merkle root, which the
// chaintracks stand-in must serve.
func mintMinedPuzzleShard(t *testing.T, s store.Store, poolName string, n int) ([]store.Row, chainhash.Hash) {
	t.Helper()
	seed := fuel.NewSeed()
	split := transaction.NewTransaction()
	// A dummy funding input: the BUMP proves the split, so its ancestry is
	// never needed.
	fundTxID := storetest.NewTxID()
	split.AddInput(&transaction.TransactionInput{SourceTXID: &fundTxID, UnlockingScript: script.NewFromBytes([]byte{script.OpTRUE}), SequenceNumber: 0xffffffff})
	pres := make([][fuel.PreimageSize]byte, n)
	for i := range pres {
		pres[i] = fuel.Preimage(seed, uint32(i)) //nolint:gosec // n is small in tests
		split.AddOutput(&transaction.TransactionOutput{Satoshis: 50, LockingScript: script.NewFromBytes(fuel.PuzzleLock(fuel.PuzzleHash(pres[i])))})
	}
	txid := *split.TxID()

	unmined := transaction.NewBeefV2()
	_, err := unmined.MergeRawTx(split.Bytes(), nil)
	require.NoError(t, err)
	unminedBytes, err := unmined.Bytes()
	require.NoError(t, err)
	isTxid := true
	mp := transaction.NewMerklePath(toolboxTestHeight, [][]*transaction.PathElement{{{Offset: 0, Hash: &txid, Txid: &isTxid}}})
	mined, err := fuel.MinedBEEF(unminedBytes, txid, mp.Bytes())
	require.NoError(t, err)

	rows := make([]store.Row, n)
	for i := range rows {
		rows[i] = store.Row{
			Outpoint:        store.Outpoint{TxID: txid, Vout: uint32(i)}, //nolint:gosec // n is small in tests
			Pool:            poolName,
			Satoshis:        50,
			LockingScript:   fuel.PuzzleLock(fuel.PuzzleHash(pres[i])),
			UnlockingScript: fuel.PuzzleUnlock(pres[i]),
		}
	}
	require.NoError(t, s.PutShard(context.Background(), store.Shard{
		Pool: poolName, TxID: txid, Size: n, Denomination: 50,
		LockMode: store.LockPuzzle, Status: store.ShardMined, BEEF: mined,
	}, rows))
	return rows, txid
}

// fakeChaintracks serves the go-chaintracks remote API the toolbox uses:
// the tip and the header at toolboxTestHeight (whose merkle root is root);
// everything else, including the SSE streams, 404s.
func fakeChaintracks(t *testing.T, root chainhash.Hash) *httptest.Server {
	t.Helper()
	h := &block.Header{Version: 1, MerkleRoot: root, Timestamp: uint32(time.Now().Unix()), Bits: 0x207fffff} //nolint:gosec // test clock
	// A pointer, so chainhash.Hash's pointer-receiver MarshalJSON applies
	// (the client decodes hashes as hex strings).
	body, err := json.Marshal(&chaintracks.BlockHeader{Header: h, Height: toolboxTestHeight, Hash: h.Hash()})
	require.NoError(t, err)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/tip", "/v2/header/height/" + strconv.Itoa(toolboxTestHeight):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// fakeArcade accepts POST /tx (binary extended format, as the toolbox's
// arcade service sends it), verifies every input's hash-puzzle unlock
// against its attached source output, records the transaction and answers
// SEEN_ON_NETWORK; GET /health answers 200.
type fakeArcade struct {
	mu  sync.Mutex
	txs []*transaction.Transaction
}

func (f *fakeArcade) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/health":
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPost && r.URL.Path == "/tx":
		b, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, `{"error":"read"}`, http.StatusBadRequest)
			return
		}
		tx, err := transaction.NewTransactionFromBytes(b)
		if err != nil {
			http.Error(w, `{"error":"parse"}`, http.StatusBadRequest)
			return
		}
		for _, in := range tx.Inputs {
			src := in.SourceTxOutput()
			if src == nil || in.UnlockingScript == nil || !fuel.VerifyPuzzle(*src.LockingScript, *in.UnlockingScript) {
				http.Error(w, `{"error":"bad unlock"}`, http.StatusBadRequest)
				return
			}
		}
		f.mu.Lock()
		f.txs = append(f.txs, tx)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"txid":%q,"txStatus":"SEEN_ON_NETWORK"}`, tx.TxID().String())
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeArcade) received() []*transaction.Transaction {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*transaction.Transaction(nil), f.txs...)
}

// newToolboxFKEnv runs an in-process FuelKeeper (memory store, trust spend
// policy, a clock that never advances so reservations never expire) over one
// mined shard of n real hash-puzzle rows (see mintMinedPuzzleShard). It
// returns the server, the store, the rows and the shard's merkle root.
func newToolboxFKEnv(t *testing.T, n int) (*httptest.Server, store.Store, []store.Row, chainhash.Hash) {
	t.Helper()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	rows, root := mintMinedPuzzleShard(t, st, "main", n)

	pc := pool.DefaultPoolConfig("main", 50)
	pc.SpendPolicy = pool.SpendTrust
	pc.MinTTL = time.Second
	require.NoError(t, pc.Validate())
	ic := pool.DefaultInstanceConfig()
	ic.InstanceID = "fueldemo-toolbox-test"
	p := pool.NewPool(st, pc, ic, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	srv := httptest.NewServer(api.NewHandler(api.Deps{Pools: []api.PoolService{p}, Store: st}))
	t.Cleanup(func() { srv.Close(); cancel(); <-done })
	return srv, st, rows, root
}

// TestToolboxEndToEndOffline proves the whole BRC-100 path offline: a real
// go-wallet-toolbox wallet on a temp sqlite, holding no funds of its own,
// reserves fuel from an in-process FuelKeeper (memory store, one mined shard
// whose BEEF carries a BUMP), SPV-verifies that BEEF against a chaintracks
// stand-in, builds a createAction whose only input is the fuel output and
// whose only output is an OP_RETURN (no change: the surplus goes to fee),
// broadcasts it to an arcade stand-in, and reports the spend back -- so every
// arcade transaction is a single-fuel-input OP_RETURN spend and every
// reported send shows spent in the store.
func TestToolboxEndToEndOffline(t *testing.T) {
	if testing.Short() {
		t.Skip("opens a full toolbox stack")
	}
	fkSrv, st, rows, root := newToolboxFKEnv(t, 1000) // GetRows caps at 1000; the rate limit below keeps a run well inside it
	fuelOutpoints := make(map[string]bool, len(rows))
	for _, r := range rows {
		op := r.Outpoint
		fuelOutpoints[op.String()] = true
	}

	arc := &fakeArcade{}
	arcSrv := httptest.NewServer(arc)
	t.Cleanup(arcSrv.Close)
	ct := fakeChaintracks(t, root)

	fk, err := client.New(fkSrv.URL)
	require.NoError(t, err)
	o := toolboxTestOptions(t)
	o.arcadeURL = arcSrv.URL
	o.chaintracks = ct.URL
	o.duration = 2 * time.Second
	o.rateLim = 200 // at most ~400 sends: a fast machine cannot exhaust the fixture
	rep := newReport("toolbox", "ttn", o.workers)

	require.NoError(t, newToolboxRunner(context.Background(), o, fk, rep))
	t.Logf("sent %d in %s over %d reservations", rep.Sent, o.duration, rep.Reserves)

	assert.Positive(t, rep.Sent)
	assert.Empty(t, rep.Failed, "unexpected failures: %v", rep.Failed)
	txs := arc.received()
	assert.Len(t, txs, int(rep.Sent))
	for _, tx := range txs {
		require.Len(t, tx.Inputs, 1, "the fuel output must be the action's only input")
		in := tx.Inputs[0]
		assert.True(t, fuelOutpoints[in.SourceTXID.String()+":"+strconv.Itoa(int(in.SourceTxOutIndex))], "input %s:%d is not a fuel output", in.SourceTXID, in.SourceTxOutIndex)
		require.Len(t, tx.Outputs, 1, "no change output: the surplus goes to fee")
		assert.True(t, tx.Outputs[0].LockingScript.IsData(), "the only output must be the OP_RETURN")
		assert.Zero(t, tx.Outputs[0].Satoshis)
	}

	got, err := st.GetRows(context.Background(), "main", storetest.Ops(rows))
	require.NoError(t, err)
	var spent int64
	for _, r := range got {
		if r.State == store.StateSpent {
			spent++
		}
	}
	assert.Equal(t, rep.Sent, spent, "every reported send must show spent in the store")
}
