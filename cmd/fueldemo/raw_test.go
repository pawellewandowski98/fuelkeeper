package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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

	// txStatus overrides the default success verdict ("SEEN_ON_NETWORK")
	// when non-empty, e.g. "DOUBLE_SPEND_ATTEMPTED"; competingTxs is copied
	// into the response's CompetingTxs. Both are ignored when err is set.
	txStatus     string
	competingTxs []string
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
	status := f.txStatus
	if status == "" {
		status = "SEEN_ON_NETWORK"
	}
	return &arcadeclient.SubmitResponse{TxID: parsed.TxID().String(), Status: 200, TxStatus: status, CompetingTxs: f.competingTxs}, nil
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

func spentCount(t *testing.T, st store.Store, poolName string, rows []store.Row) int64 { //nolint:unparam // every current caller uses "main"; kept general-purpose like newRawTestEnv
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

// TestWorkerInvalidRejectReleases checks that arcade rejecting a submission
// as invalid, with no competing transaction, is tallied and its output
// released back to the pool -- not reported spent (that would burn it under
// spend_policy: trust for a submission that never even reached the network).
//
// It stops the worker after exactly one reserve/process/release cycle via
// afterCycle (see TestWorkerBackpressureReleasesOutputs's doc comment for
// why): released rows are immediately reservable again, so a wall-clock
// cutoff would let the worker reserve, reject and release the same five
// rows many times over instead of once.
func TestWorkerInvalidRejectReleases(t *testing.T) {
	srv, st, rows := newRawTestEnv(t, 1, 5)
	fk, err := client.New(srv.URL)
	require.NoError(t, err)
	fake := &fakeSubmitter{err: &arcadeclient.RejectError{Status: 461, Title: "INVALID_OUTPUTS", Detail: "test"}}
	rep := newReport("raw", "test", 1)
	r := &rawRunner{fk: fk, arc: fake, pool: "main", batch: 5, rep: rep, memo: []byte("x"), satPerKB: 100}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var cycles atomic.Int32
	r.afterCycle = func() {
		if cycles.Add(1) == 1 {
			cancel()
		}
	}

	done := make(chan struct{})
	go func() { defer close(done); r.worker(ctx, 0) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop within 5s of its first cycle completing")
	}

	assert.Zero(t, rep.Sent)
	assert.Equal(t, int64(5), rep.Failed["arcade:461"])
	assert.Zero(t, spentCount(t, st, "main", rows), "an invalid rejection with no competing tx must never be reported spent")

	require.Eventually(t, func() bool {
		got, gerr := st.GetRows(context.Background(), "main", storetest.Ops(rows))
		if gerr != nil {
			return false
		}
		for _, row := range got {
			if row.State != store.StateAvailable {
				return false
			}
		}
		return true
	}, time.Second, 5*time.Millisecond, "an invalid rejection must release its output")
}

// TestWorkerRejectDoubleSpendReportsSpend checks that a 4xx RejectError
// naming a competing transaction is reported spent by that competitor, not
// released (RejectError.DoubleSpend() branch of worker). Real arcade never
// populates a 4xx body's competingTxs synchronously (see worker's doc
// comment); this fake exercises the forward/other-gateway-compatibility
// branch, not live arcade behaviour.
func TestWorkerRejectDoubleSpendReportsSpend(t *testing.T) {
	srv, st, rows := newRawTestEnv(t, 1, 5)
	fk, err := client.New(srv.URL)
	require.NoError(t, err)
	competitor := storetest.NewTxID().String()
	fake := &fakeSubmitter{err: &arcadeclient.RejectError{Status: 462, Title: "DOUBLE_SPEND", Detail: "test", CompetingTxs: []string{competitor}}}
	rep := newReport("raw", "test", 1)
	r := &rawRunner{fk: fk, arc: fake, pool: "main", batch: 5, rep: rep, memo: []byte("x"), satPerKB: 100}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	r.worker(ctx, 0)

	assert.Zero(t, rep.Sent)
	assert.Equal(t, int64(5), rep.Failed["arcade:462"])
	got, err := st.GetRows(context.Background(), "main", storetest.Ops(rows))
	require.NoError(t, err)
	for _, row := range got {
		assert.Equal(t, store.StateSpent, row.State, "a double-spent output must be reported spent, not released")
		require.NotNil(t, row.SpentBy)
		assert.Equal(t, competitor, row.SpentBy.String())
	}
}

// TestWorkerDoubleSpendReportsSpend checks that a submission answered with a
// 2xx DOUBLE_SPEND_ATTEMPTED verdict (naming a competitor) is reported spent
// by that competitor under trust, never released -- a spent output released
// back to the pool would be handed to every later consumer, each failing the
// same way in turn. Real arcade has no synchronous DOUBLE_SPEND_ATTEMPTED
// producer (see worker's doc comment); this fake exercises the
// forward/other-gateway-compatibility branch, not live arcade behaviour.
func TestWorkerDoubleSpendReportsSpend(t *testing.T) {
	srv, st, rows := newRawTestEnv(t, 1, 5)
	fk, err := client.New(srv.URL)
	require.NoError(t, err)
	competitor := storetest.NewTxID().String()
	fake := &fakeSubmitter{txStatus: "DOUBLE_SPEND_ATTEMPTED", competingTxs: []string{competitor}}
	rep := newReport("raw", "test", 1)
	r := &rawRunner{fk: fk, arc: fake, pool: "main", batch: 5, rep: rep, memo: []byte("x"), satPerKB: 100}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	r.worker(ctx, 0)

	assert.Zero(t, rep.Sent)
	assert.Equal(t, int64(5), rep.Failed["arcade:double_spend"])
	got, err := st.GetRows(context.Background(), "main", storetest.Ops(rows))
	require.NoError(t, err)
	for _, row := range got {
		assert.Equal(t, store.StateSpent, row.State, "a double-spent output must be reported spent, not released")
		require.NotNil(t, row.SpentBy)
		assert.Equal(t, competitor, row.SpentBy.String(), "spent by the competitor, not the worker's own tx")
	}
}

// TestWorkerRejectedIn2xxHolds checks that a 2xx response carrying a
// terminal REJECTED verdict inline (no error from PostTx) is tallied and
// held, not released: arcade already accepted the submission (a 2xx), so
// this verdict alone does not prove the transaction never reached the
// network -- exactly like backpressure (see worker's doc comment). Unlike a
// PostTx error, a REJECTED verdict is a per-transaction result, not
// evidence arcade itself is unavailable, so it does not abandon the rest of
// the batch: every output is tried (and held) once.
func TestWorkerRejectedIn2xxHolds(t *testing.T) {
	srv, st, rows := newRawTestEnv(t, 1, 5)
	fk, err := client.New(srv.URL)
	require.NoError(t, err)
	fake := &fakeSubmitter{txStatus: "REJECTED"}
	rep := newReport("raw", "test", 1)
	r := &rawRunner{fk: fk, arc: fake, pool: "main", batch: 5, rep: rep, memo: []byte("x"), satPerKB: 100}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	r.worker(ctx, 0)

	assert.Zero(t, rep.Sent)
	assert.Equal(t, int64(5), rep.Failed["arcade:rejected"], "every output tried exactly once: %v", rep.Failed)
	assert.Zero(t, spentCount(t, st, "main", rows))
	got, gerr := st.GetRows(context.Background(), "main", storetest.Ops(rows))
	require.NoError(t, gerr)
	for _, row := range got {
		assert.Equal(t, store.StateReserved, row.State, "a 2xx REJECTED verdict must hold, not release")
	}
}

// TestWorkerHoldsOnUncertainSubmission checks that a PostTx failure whose
// cause leaves the transaction's fate genuinely unknown -- a transport
// failure past the dial phase, ctx ending mid-POST, an unexpected HTTP
// status, or a 2xx response this client failed to decode -- holds only the
// one output actually attempted: it is the mutation guard for worker's
// default case in the PostTx-error switch. Releasing that output would risk
// handing an already-broadcast output to a later consumer (who would then
// fail as a double-spend); reporting it spent would risk burning a row that
// in fact never reached the network. The rest of the batch -- never
// attempted, since an uncertain failure abandons the batch early rather
// than holding every remaining output the same way (see worker's doc
// comment) -- is released.
//
// It stops the worker after exactly one cycle via afterCycle, rather than a
// wall-clock cutoff: see TestWorkerBackpressureHoldsOneReleasesRest's doc
// comment for why that matters here.
func TestWorkerHoldsOnUncertainSubmission(t *testing.T) {
	cases := map[string]error{
		"connection reset":      errors.New("fake: read tcp 127.0.0.1:1->127.0.0.1:2: connection reset by peer"),
		"deadline exceeded":     fmt.Errorf("fake: post /tx: %w", context.DeadlineExceeded),
		"5xx unexpected status": &arcadeclient.UnexpectedStatusError{Method: "POST", Path: "/tx", Code: 500},
		"2xx decode error":      fmt.Errorf("arcadeclient: decode submit response: %w", errors.New("unexpected EOF")),
	}
	for name, postErr := range cases {
		t.Run(name, func(t *testing.T) {
			srv, st, rows := newRawTestEnv(t, 1, 5)
			fk, err := client.New(srv.URL)
			require.NoError(t, err)
			fake := &fakeSubmitter{err: postErr}
			rep := newReport("raw", "test", 1)
			r := &rawRunner{fk: fk, arc: fake, pool: "main", batch: 5, rep: rep, memo: []byte("x"), satPerKB: 100}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var cycles atomic.Int32
			r.afterCycle = func() {
				if cycles.Add(1) == 1 {
					cancel()
				}
			}

			done := make(chan struct{})
			go func() { defer close(done); r.worker(ctx, 0) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("worker did not stop within 5s of its first cycle completing")
			}

			assert.Zero(t, rep.Sent)
			assert.Equal(t, int64(1), rep.Failed["arcade:unknown"], "the batch is abandoned after the first uncertain failure")
			assert.Zero(t, spentCount(t, st, "main", rows), "an uncertain submission must never be reported spent")

			require.Eventually(t, func() bool {
				got, gerr := st.GetRows(context.Background(), "main", storetest.Ops(rows))
				if gerr != nil {
					return false
				}
				var reserved, available int
				for _, row := range got {
					switch row.State {
					case store.StateReserved:
						reserved++
					case store.StateAvailable:
						available++
					}
				}
				return reserved == 1 && available == 4
			}, time.Second, 5*time.Millisecond, "exactly one output -- the one actually attempted -- stays held; the rest of the batch was never attempted and must be released")
		})
	}
}

// TestWorkerDialFailureReleasesAndBacksOff is the regression test for the
// "arcade outage drains the pool" bug: previously, any PostTx failure held
// its output and the worker immediately reserved a fresh batch, so a
// sustained connection-refused outage measured holding 200 rows reserved in
// under 500ms -- each one then stuck until its reservation's own expiry
// plus unknown_requeue_age, rather than ever reaching the reaper's requeue
// rule sooner.
//
// It drives several outage cycles (via afterCycle) against a fake arcade
// that always fails to dial, and checks both halves of the fix: every
// output across every cycle is released, never held (a dial failure is
// provably never-sent -- see worker's doc comment), and the worker paces
// itself between cycles rather than looping tightly (each cycle's
// afterCycle timestamp is spaced out from the previous one by at least the
// worker's base retry pause, proving outageBackoff's sleep actually ran
// rather than the loop spinning straight back into Reserve).
func TestWorkerDialFailureReleasesAndBacksOff(t *testing.T) {
	srv, st, rows := newRawTestEnv(t, 1, 60)
	fk, err := client.New(srv.URL)
	require.NoError(t, err)
	dialErr := fmt.Errorf("arcadeclient: POST /tx: %w", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED})
	fake := &fakeSubmitter{err: dialErr}
	rep := newReport("raw", "test", 1)
	r := &rawRunner{fk: fk, arc: fake, pool: "main", batch: 10, rep: rep, memo: []byte("x"), satPerKB: 100}

	const wantCycles = 4
	var mu sync.Mutex
	var at []time.Time
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.afterCycle = func() {
		mu.Lock()
		at = append(at, time.Now())
		n := len(at)
		mu.Unlock()
		if n >= wantCycles {
			cancel()
		}
	}

	done := make(chan struct{})
	go func() { defer close(done); r.worker(ctx, 0) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not stop within 10s")
	}

	mu.Lock()
	timestamps := append([]time.Time(nil), at...)
	mu.Unlock()
	require.Len(t, timestamps, wantCycles)
	for i := 1; i < len(timestamps); i++ {
		gap := timestamps[i].Sub(timestamps[i-1])
		assert.GreaterOrEqual(t, gap, reserveRetryPause, "cycle %d did not pace itself: the worker must back off between cycles, not drain the pool in a tight loop", i)
	}

	assert.Zero(t, spentCount(t, st, "main", rows))
	got, gerr := st.GetRows(context.Background(), "main", storetest.Ops(rows))
	require.NoError(t, gerr)
	for _, row := range got {
		assert.NotEqual(t, store.StateReserved, row.State, "a dial failure must release its output, never hold it (the original drain bug)")
	}
}

// TestWorkerBackpressureHoldsOneReleasesRest checks that arcade backpressure
// is tallied, that the one output actually attempted is held (arcade
// persists a submission before the downstream publish that can answer 503,
// so backpressure does not prove the transaction never reached it -- see
// worker's doc comment), and that the rest of the batch -- never attempted,
// since backpressure abandons the batch early rather than trying every
// remaining output against a gateway already shedding load -- is released.
//
// It stops the worker deterministically after exactly one full
// reserve/process/release cycle via afterCycle, rather than racing a
// wall-clock ctx timeout against the worker's loop: cancelling ctx while a
// Reserve call is in flight is indistinguishable, from the worker's side,
// from a normal shutdown (worker's ctx.Err()-after-Reserve-error guard
// treats it as such and returns), but if the server had already committed
// that Reserve, its rows have no token to release them by and are left
// reserved until the pool's own TTL reclaims them -- correct self-healing
// behaviour, but not something this test should assert around under a race
// that a wall-clock cutoff makes more likely the harder the scheduler is
// loaded (observed as a flake under `go test -race -count=2`). afterCycle
// only ever fires between cycles, once the previous one's release has
// already completed, so cancelling from it can never race an in-flight
// Reserve.
func TestWorkerBackpressureHoldsOneReleasesRest(t *testing.T) {
	srv, st, rows := newRawTestEnv(t, 1, 5)
	fk, err := client.New(srv.URL)
	require.NoError(t, err)
	fake := &fakeSubmitter{err: &arcadeclient.BackpressureError{RetryAfter: 0}}
	rep := newReport("raw", "test", 1)
	r := &rawRunner{fk: fk, arc: fake, pool: "main", batch: 5, rep: rep, memo: []byte("x"), satPerKB: 100}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var cycles atomic.Int32
	r.afterCycle = func() {
		if cycles.Add(1) == 1 {
			cancel()
		}
	}

	done := make(chan struct{})
	go func() { defer close(done); r.worker(ctx, 0) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop within 5s of its first cycle completing")
	}

	assert.Zero(t, rep.Sent)
	assert.Equal(t, int64(1), rep.Failed["arcade:backpressure"], "the batch is abandoned after the first backpressure response")
	assert.Zero(t, spentCount(t, st, "main", rows), "backpressure must never be reported as spent")

	// afterCycle already guarantees the worker only stops between cycles
	// (never mid-Reserve), and its Release call has already returned by the
	// time done closes above; require.Eventually is an extra, cheap safety
	// margin against a slow store/scheduler rather than a load-bearing wait.
	require.Eventually(t, func() bool {
		got, gerr := st.GetRows(context.Background(), "main", storetest.Ops(rows))
		if gerr != nil {
			return false
		}
		var reserved, available int
		for _, row := range got {
			switch row.State {
			case store.StateReserved:
				reserved++
			case store.StateAvailable:
				available++
			}
		}
		return reserved == 1 && available == 4
	}, time.Second, 5*time.Millisecond, "the attempted output stays held; the rest of the batch is released")
}

// TestWorkerBackpressureReleasesRestBeforeSleeping pins the ordering fix for
// the batch-abandon path: the untried rest of the batch must be released
// right away, not merely queued for release after the Retry-After wait.
// With RetryAfter set to a full second, it uses client.WithObserver (the
// same deterministic hook pkg/client/settle_test.go's countingClient uses)
// to record when the /release call is actually seen by the server, and
// checks that timestamp lands a comfortable margin before the sleep could
// have elapsed: the pre-fix code, which only appended the untried outputs
// to a slice released after sleepBounded returned, would instead show the
// release arriving at roughly start+1s.
func TestWorkerBackpressureReleasesRestBeforeSleeping(t *testing.T) {
	srv, _, _ := newRawTestEnv(t, 1, 5)

	var mu sync.Mutex
	var releaseSeenAt time.Time
	fk, err := client.New(srv.URL, client.WithObserver(func(method, path string, _ int, _ time.Duration, _ error) {
		if method == http.MethodPost && strings.HasSuffix(path, "/release") {
			mu.Lock()
			if releaseSeenAt.IsZero() {
				releaseSeenAt = time.Now()
			}
			mu.Unlock()
		}
	}))
	require.NoError(t, err)

	const retryAfter = time.Second
	fake := &fakeSubmitter{err: &arcadeclient.BackpressureError{RetryAfter: int(retryAfter / time.Second)}}
	rep := newReport("raw", "test", 1)
	r := &rawRunner{fk: fk, arc: fake, pool: "main", batch: 5, rep: rep, memo: []byte("x"), satPerKB: 100}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var cycles atomic.Int32
	r.afterCycle = func() {
		if cycles.Add(1) == 1 {
			cancel()
		}
	}

	start := time.Now()
	done := make(chan struct{})
	go func() { defer close(done); r.worker(ctx, 0) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop within 5s of its first cycle completing")
	}

	mu.Lock()
	seenAt := releaseSeenAt
	mu.Unlock()
	require.False(t, seenAt.IsZero(), "the release call must have happened")
	assert.Less(t, seenAt.Sub(start), retryAfter/2,
		"the untried outputs must be released before the Retry-After sleep starts, not after it returns")
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
// requires. The constraint values are deliberately chosen to differ from
// buildFuelTx's own puzzle-mode defaults (version 1, lockTime 0, sequence
// 0xffffffff): using the default values here would leave this test green
// even if the whole constraints branch were deleted.
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
	constraints := &wire.Constraints{TxVersion: 2, LockTime: 600000, Sequence: 0xfffffffe}

	tx, err := buildFuelTx(o, constraints, []byte("memo"), nil, 100)
	require.NoError(t, err)

	assert.Equal(t, uint32(2), tx.Version)
	assert.Equal(t, uint32(600000), tx.LockTime)
	require.Len(t, tx.Inputs, 1)
	assert.Equal(t, uint32(0xfffffffe), tx.Inputs[0].SequenceNumber)
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
// known, hand-computed sample: 5 latencies (10,20,30,40,50 ms), nearest-rank
// index = ceil(n*p)-1 for n=5:
//
//	p50: ceil(5*0.50)-1 = ceil(2.50)-1 = 3-1 = 2 -> sorted[2] = 30ms
//	p95: ceil(5*0.95)-1 = ceil(4.75)-1 = 5-1 = 4 -> sorted[4] = 50ms
//	p99: ceil(5*0.99)-1 = ceil(4.95)-1 = 5-1 = 4 -> sorted[4] = 50ms
func TestReportFinishPercentiles(t *testing.T) {
	r := newReport("raw", "test", 1)
	for _, ms := range []int64{10, 20, 30, 40, 50} {
		r.ok(time.Duration(ms) * time.Millisecond)
	}
	r.finish()

	assert.Equal(t, int64(5), r.Sent)
	assert.InDelta(t, 30.0, r.P50Ms, 0.0001)
	assert.InDelta(t, 50.0, r.P95Ms, 0.0001)
	assert.InDelta(t, 50.0, r.P99Ms, 0.0001)
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

// TestRunReturnsNilReportOnArcadeClientFailure is the regression test for a
// bug where run returned its pre-built (all-zero) report alongside the
// error when arcadeclient.New failed: main would then write that report
// (sent=0 tps=0, etc.) to stdout/--out as if a run had actually happened,
// before going on to report the error. The FuelKeeper client construction
// succeeds here (a syntactically valid URL, never dialed); it is arcade's
// client construction that must fail (a query string, which
// arcadeclient.New rejects) and must yield a nil report, not a zero one.
func TestRunReturnsNilReportOnArcadeClientFailure(t *testing.T) {
	o := options{
		mode: "raw", url: "http://127.0.0.1:1", pool: "main",
		arcadeURL: "https://x/v1?tok=abc", // rejected: arcadeclient.New disallows a query string
		workers:   1, batch: 1, duration: time.Second,
	}
	rep, err := run(context.Background(), o)
	require.Error(t, err)
	assert.Nil(t, rep, "a client construction failure must never hand back a report")
}
