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
	pkgerrors "github.com/bsv-blockchain/go-wallet-toolbox/pkg/errors"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/wdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api"
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
	reject := pkgerrors.NewProcessActionError(nil, nil).WithTx("ab"+strings.Repeat("0", 62), nil).Wrap(errors.New("arcade said no"))
	code, txid := classifyCreateError(fmt.Errorf("create action failed: %w", reject))
	assert.Equal(t, "reject", code)
	assert.Equal(t, "ab"+strings.Repeat("0", 62), txid)

	code, txid = classifyCreateError(fmt.Errorf("create action failed: %w", pkgerrors.NewProcessActionError(nil, nil)))
	assert.Equal(t, "reject", code)
	assert.Empty(t, txid, "a process error without a txid carries none")

	// ProcessActionError.Is also matches its cause; a reject wrapping
	// not-enough-funds is still a reject.
	code, _ = classifyCreateError(pkgerrors.NewProcessActionError(nil, nil).Wrap(wdk.ErrNotEnoughFunds))
	assert.Equal(t, "reject", code)

	code, txid = classifyCreateError(fmt.Errorf("create action failed: failed to create transaction: %w", wdk.ErrNotEnoughFunds))
	assert.Equal(t, "underfunded", code)
	assert.Empty(t, txid)

	code, txid = classifyCreateError(errors.New("failed to verify beef"))
	assert.Equal(t, "create", code)
	assert.Empty(t, txid)
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
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	rows, root := mintMinedPuzzleShard(t, st, "main", 1000) // outlasts a 2 s run (~550 sends without -race); GetRows caps at 1000
	fuelOutpoints := make(map[string]bool, len(rows))
	for _, r := range rows {
		op := r.Outpoint
		fuelOutpoints[op.String()] = true
	}

	pc := pool.DefaultPoolConfig("main", 50)
	pc.SpendPolicy = pool.SpendTrust
	pc.MinTTL = time.Second
	require.NoError(t, pc.Validate())
	ic := pool.DefaultInstanceConfig()
	ic.InstanceID = "fueldemo-toolbox-test"
	p := pool.NewPool(st, pc, ic, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))
	pctx, pcancel := context.WithCancel(context.Background())
	pdone := make(chan struct{})
	go func() { defer close(pdone); _ = p.Run(pctx) }()
	fkSrv := httptest.NewServer(api.NewHandler(api.Deps{Pools: []api.PoolService{p}, Store: st}))
	t.Cleanup(func() { fkSrv.Close(); pcancel(); <-pdone })

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
