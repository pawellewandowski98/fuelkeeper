package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api"
	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// newBenchEnv builds a running FuelKeeper API server over a real pool.Pool
// and memory store, seeded with shards shards of rowsPerShard rows each, for
// fuelbench's tests to drive with run(). The manual clock never advances, so
// reservations never expire mid-test.
//
// ic.MaxShards (default 8, per pool.DefaultInstanceConfig) is raised to
// shards whenever more than that many are minted: the shard manager only
// rotates a fully-drained shard out for a fresh one on its background
// rescan (default every 5s, long past a short benchmark test's duration), so
// a "well-stocked" pool needs every one of its shards ownable up front, not
// just enough to clear the default cap.
func newBenchEnv(t *testing.T, instanceID string, shards, rowsPerShard int) (*httptest.Server, store.Store) {
	t.Helper()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	for i := 0; i < shards; i++ {
		storetest.MintShard(t, st, "main", rowsPerShard, store.ShardMined)
	}
	pc := pool.DefaultPoolConfig("main", 50)
	pc.SpendPolicy = pool.SpendTrust
	pc.MinTTL = time.Second
	require.NoError(t, pc.Validate())
	ic := pool.DefaultInstanceConfig()
	ic.InstanceID = instanceID
	if shards > ic.MaxShards {
		ic.MaxShards = shards
	}
	p := pool.NewPool(st, pc, ic, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	srv := httptest.NewServer(api.NewHandler(api.Deps{Pools: []api.PoolService{p}, Store: st}))
	t.Cleanup(func() { srv.Close(); cancel(); <-done })
	return srv, st
}

func TestRunAgainstMemoryStore(t *testing.T) {
	// 600 shards of 900 rows (540,000 rows total; kept under the store's
	// 1,000-row ListAvailable page size per shard so adoption stays a single
	// round trip) comfortably outlasts this test's 2s spend-mode run: an
	// unthrottled 8-worker loop against an in-process memory store measures
	// in the tens of thousands of reserves/s, so a "well-stocked" pool for
	// this duration needs far more than the shard manager's default 8-shard,
	// 5,000-row low-water buffer.
	srv, _ := newBenchEnv(t, "bench-test", 600, 900)

	res, err := run(context.Background(), options{urls: []string{srv.URL}, pool: "main", workers: 8, count: 5, duration: 2 * time.Second, mode: "spend"})
	require.NoError(t, err)
	assert.Greater(t, res.Reserves, int64(100))
	assert.Equal(t, res.Reserves*5, res.Outputs, "every reserve got count outputs")
	assert.Positive(t, res.P50Micros)
	assert.GreaterOrEqual(t, res.P99Micros, res.P50Micros)
	assert.Positive(t, res.SpendP50Micros, "spend mode times the spend call separately")
	assert.GreaterOrEqual(t, res.SpendP99Micros, res.SpendP50Micros)
	assert.Empty(t, res.Errors, "no errors against a well-stocked pool: %v", res.Errors)
	assert.InDelta(t, float64(res.Reserves)/res.Seconds, res.ReservesPerSec, 1e-6)
	assert.Equal(t, 1, res.Instances)
	assert.Equal(t, 8, res.Workers)
	assert.Equal(t, 5, res.Count)
	assert.Equal(t, "spend", res.Mode)

	md := renderTable([]result{res})
	assert.Contains(t, md, "| reserves/s |")
	assert.Contains(t, md, "spend")
}

// TestRunReleaseModeNeverExhausts drives a small pool (only 10 rows) in
// release mode for long enough that, if released rows never returned to the
// pool, the pool would exhaust: since release returns them, no errors occur.
func TestRunReleaseModeNeverExhausts(t *testing.T) {
	srv, _ := newBenchEnv(t, "bench-release", 1, 10)

	res, err := run(context.Background(), options{urls: []string{srv.URL}, pool: "main", workers: 4, count: 2, duration: 500 * time.Millisecond, mode: "release"})
	require.NoError(t, err)
	assert.Positive(t, res.Reserves)
	assert.Empty(t, res.Errors, "release mode must return rows so the pool never exhausts: %v", res.Errors)
}

// TestRunSpendModeExhaustsPermanently is the mutation guard for "run skips
// the spend call". A 10-row pool with count: 2 offers exactly 5 successful
// reserves; against the same tiny pool as TestRunReleaseModeNeverExhausts,
// spend mode must not just "run out" (a bug that merely held rows, like hold
// mode, would exhaust it too, over this same short duration, since the
// manual clock never advances past MinTTL) -- it must leave the store with
// every row genuinely StateSpent, not sitting in an open reservation. If
// run's spend call were skipped, Stats.Reserved would be 10 and Stats.Spent
// would be 0 instead.
func TestRunSpendModeExhaustsPermanently(t *testing.T) {
	srv, st := newBenchEnv(t, "bench-spend-exhaust", 1, 10)

	res, err := run(context.Background(), options{urls: []string{srv.URL}, pool: "main", workers: 4, count: 2, duration: 500 * time.Millisecond, mode: "spend"})
	require.NoError(t, err)
	assert.Equal(t, int64(5), res.Reserves, "10 rows at count: 2 must yield exactly 5 successful reserves")
	assert.NotEmpty(t, res.Errors, "spend mode against a 10-row pool must permanently consume it and start erroring")

	stats, err := st.Stats(context.Background(), "main")
	require.NoError(t, err)
	assert.Equal(t, int64(10), stats.Spent, "every row must have been reported spent, not merely held in an open reservation")
	assert.Zero(t, stats.Reserved, "no row should still be sitting in an open reservation")
	assert.Zero(t, stats.Available, "no row should remain available")
}

// TestRunHoldModeExhausts holds every reservation (no spend/release) against
// a small pool: it must exhaust, producing exhausted/insufficient error
// counts, without panicking, and must still stop at duration.
func TestRunHoldModeExhausts(t *testing.T) {
	srv, _ := newBenchEnv(t, "bench-hold", 1, 6)

	start := time.Now()
	res, err := run(context.Background(), options{urls: []string{srv.URL}, pool: "main", workers: 4, count: 2, duration: 500 * time.Millisecond, mode: "hold"})
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.Less(t, elapsed, 3*time.Second, "run must terminate at duration")
	assert.NotEmpty(t, res.Errors)
	var sawExpected bool
	for code := range res.Errors {
		if code == "exhausted" || code == "insufficient" {
			sawExpected = true
		}
	}
	assert.True(t, sawExpected, "expected exhausted or insufficient errors, got %v", res.Errors)
}

// TestRunRateLimit checks that a configured rate limit bounds throughput:
// against a well-stocked pool that could otherwise serve far more than 50
// reserves/s locally, rateLim: 50 over 1s must produce roughly <= 60
// reserves.
func TestRunRateLimit(t *testing.T) {
	srv, _ := newBenchEnv(t, "bench-rate", 5, 500)

	res, err := run(context.Background(), options{urls: []string{srv.URL}, pool: "main", workers: 8, count: 1, duration: time.Second, mode: "release", rateLim: 50})
	require.NoError(t, err)
	assert.LessOrEqual(t, res.Reserves, int64(60))
	assert.Positive(t, res.Reserves)
}

// countingHandler wraps h, incrementing n for every request it serves.
func countingHandler(n *atomic.Int64, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		h.ServeHTTP(w, r)
	})
}

// TestRunRoundRobinAcrossURLs checks that reserves are spread across more
// than one configured FuelKeeper URL.
func TestRunRoundRobinAcrossURLs(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)

	newServer := func(instanceID string, n *atomic.Int64) *httptest.Server {
		st := memory.New(memory.WithClock(clk.Now))
		storetest.MintShard(t, st, "main", 200, store.ShardMined)
		pc := pool.DefaultPoolConfig("main", 50)
		pc.SpendPolicy = pool.SpendTrust
		pc.MinTTL = time.Second
		require.NoError(t, pc.Validate())
		ic := pool.DefaultInstanceConfig()
		ic.InstanceID = instanceID
		p := pool.NewPool(st, pc, ic, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); _ = p.Run(ctx) }()
		h := countingHandler(n, api.NewHandler(api.Deps{Pools: []api.PoolService{p}, Store: st}))
		srv := httptest.NewServer(h)
		t.Cleanup(func() { srv.Close(); cancel(); <-done })
		return srv
	}

	var n1, n2 atomic.Int64
	srv1 := newServer("bench-rr-1", &n1)
	srv2 := newServer("bench-rr-2", &n2)

	res, err := run(context.Background(), options{urls: []string{srv1.URL, srv2.URL}, pool: "main", workers: 4, count: 1, duration: 500 * time.Millisecond, mode: "release"})
	require.NoError(t, err)
	assert.Positive(t, res.Reserves)
	assert.Positive(t, n1.Load(), "first URL should have seen requests")
	assert.Positive(t, n2.Load(), "second URL should have seen requests")
	assert.Equal(t, 2, res.Instances)
}

// TestRunValidatesOptions checks that run rejects obviously bad options
// before ever dialing a URL.
func TestRunValidatesOptions(t *testing.T) {
	base := options{urls: []string{"http://127.0.0.1:1"}, pool: "main", workers: 1, count: 1, duration: time.Second, mode: "spend"}

	t.Run("no urls", func(t *testing.T) {
		o := base
		o.urls = nil
		_, err := run(context.Background(), o)
		require.Error(t, err)
		assert.Equal(t, 2, exitCode(err), "a bad option is a usage mistake, not a runtime failure")
	})
	t.Run("zero workers", func(t *testing.T) {
		o := base
		o.workers = 0
		_, err := run(context.Background(), o)
		require.Error(t, err)
		assert.Equal(t, 2, exitCode(err))
	})
	t.Run("zero count", func(t *testing.T) {
		o := base
		o.count = 0
		_, err := run(context.Background(), o)
		require.Error(t, err)
		assert.Equal(t, 2, exitCode(err))
	})
	t.Run("bad mode", func(t *testing.T) {
		o := base
		o.mode = "bogus"
		_, err := run(context.Background(), o)
		require.Error(t, err)
		assert.Equal(t, 2, exitCode(err))
	})
	t.Run("zero duration", func(t *testing.T) {
		o := base
		o.duration = 0
		_, err := run(context.Background(), o)
		require.Error(t, err)
		assert.Equal(t, 2, exitCode(err))
	})
	t.Run("negative rate", func(t *testing.T) {
		o := base
		o.rateLim = -1
		_, err := run(context.Background(), o)
		require.Error(t, err)
		assert.Equal(t, 2, exitCode(err))
	})
	t.Run("empty pool", func(t *testing.T) {
		o := base
		o.pool = ""
		_, err := run(context.Background(), o)
		require.Error(t, err)
		assert.Equal(t, 2, exitCode(err))
	})
}

// TestExitCodeClasses pins exitCode's three classes to cmd/fuelkeeper/main.go's
// convention: 0 for success and -h/--help (flag.ErrHelp, however deeply
// wrapped), 2 for a usage mistake, 1 for anything else.
func TestExitCodeClasses(t *testing.T) {
	assert.Equal(t, 0, exitCode(nil))
	assert.Equal(t, 0, exitCode(flag.ErrHelp))
	assert.Equal(t, 0, exitCode(wrapUsageError(flag.ErrHelp)), "fs.Parse wraps -h's flag.ErrHelp; it must still exit 0")
	assert.Equal(t, 2, exitCode(usageErrorf("bad flag")))
	assert.Equal(t, 2, exitCode(wrapUsageError(errors.New("boom"))))
	assert.Equal(t, 1, exitCode(errors.New("some runtime failure")))
}

// TestRunMainExitCodes exercises runMain end to end for each exit-code class,
// using only inputs that fail before any network call (a bad flag, a bad
// option, or a missing render file), so the test stays fast and
// deterministic.
func TestRunMainExitCodes(t *testing.T) {
	t.Run("help exits 0", func(t *testing.T) {
		assert.Equal(t, 0, runMain([]string{"-h"}))
	})
	t.Run("unknown flag exits 2", func(t *testing.T) {
		assert.Equal(t, 2, runMain([]string{"--nope"}))
	})
	t.Run("bad option value exits 2", func(t *testing.T) {
		assert.Equal(t, 2, runMain([]string{"--url", "http://x", "--workers", "0"}))
	})
	t.Run("missing url exits 2", func(t *testing.T) {
		assert.Equal(t, 2, runMain([]string{"--duration", "1ms"}))
	})
	t.Run("render missing file exits 1", func(t *testing.T) {
		assert.Equal(t, 1, runMain([]string{"render", filepath.Join(t.TempDir(), "nope.json")}))
	})
}

// TestErrCode checks the error-to-code mapping used to tally Errors: an
// *client.APIError maps to its own Code, anything else maps to "transport".
func TestErrCode(t *testing.T) {
	assert.Equal(t, "exhausted", errCode(&client.APIError{Code: "exhausted"}))
	assert.Equal(t, "transport", errCode(errors.New("boom")))
	assert.Equal(t, "transport", errCode(context.DeadlineExceeded))
}

// TestRenderTableColumns checks the markdown table's header and that a row's
// mode and error summary render as expected.
func TestRenderTableColumns(t *testing.T) {
	r := result{
		Label: "run-a", Backend: "memory", Instances: 1, Workers: 8, Count: 5, Mode: "spend",
		Seconds: 2, Reserves: 100, Outputs: 500, ReservesPerSec: 50, OutputsPerSec: 250,
		P50Micros: 1200, P95Micros: 3000, P99Micros: 4500, MaxMicros: 9000,
		Errors: map[string]int64{"exhausted": 2, "insufficient": 1},
	}
	md := renderTable([]result{r})
	assert.Contains(t, md, "| label | backend | instances | workers | count | mode | reserves/s | outputs/s | p50 ms | p95 ms | p99 ms | errors |")
	assert.Contains(t, md, "| reserves/s |")
	assert.Contains(t, md, "spend")
	assert.Contains(t, md, "exhausted=2 insufficient=1")
}

// TestRenderTableNoErrorsShowsDash checks the placeholder for a clean run.
func TestRenderTableNoErrorsShowsDash(t *testing.T) {
	r := result{Label: "clean", Mode: "release", Instances: 1, Workers: 1, Count: 1}
	md := renderTable([]result{r})
	assert.Contains(t, md, "| - |")
}

// TestRunRenderCmdTwoFiles is the golden-markdown test for "fuelbench render
// a.json b.json": the table it prints must match renderTable over the two
// decoded results, exactly.
func TestRunRenderCmdTwoFiles(t *testing.T) {
	dir := t.TempDir()
	r1 := result{Label: "run1", Backend: "memory", Instances: 1, Workers: 8, Count: 5, Mode: "spend", ReservesPerSec: 100, OutputsPerSec: 500, P50Micros: 1000, P95Micros: 2000, P99Micros: 3000}
	// >= 2 error codes so the golden comparison actually pins sort.Strings'
	// ordering: a single-key map can't distinguish a sorted render from an
	// unsorted (map-iteration-order-dependent) one.
	r2 := result{Label: "run2", Backend: "sqlite", Instances: 2, Workers: 16, Count: 1, Mode: "release", ReservesPerSec: 200, OutputsPerSec: 200, P50Micros: 500, P95Micros: 900, P99Micros: 1200, Errors: map[string]int64{"exhausted": 1, "backpressure": 3}}

	f1 := filepath.Join(dir, "a.json")
	f2 := filepath.Join(dir, "b.json")
	data1, err := json.Marshal(r1)
	require.NoError(t, err)
	data2, err := json.Marshal(r2)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(f1, data1, 0o600))
	require.NoError(t, os.WriteFile(f2, data2, 0o600))

	var buf bytes.Buffer
	require.NoError(t, runRenderCmd(&buf, []string{f1, f2}))

	want := renderTable([]result{r1, r2}) + "\n"
	assert.Equal(t, want, buf.String())
	assert.Contains(t, buf.String(), "run1")
	assert.Contains(t, buf.String(), "run2")
	assert.Contains(t, buf.String(), "backpressure=3 exhausted=1", "errors must render sorted by code, deterministically")
}

func TestRunRenderCmdRequiresFiles(t *testing.T) {
	var buf bytes.Buffer
	require.Error(t, runRenderCmd(&buf, nil))
}

func TestRunRenderCmdMissingFile(t *testing.T) {
	var buf bytes.Buffer
	require.Error(t, runRenderCmd(&buf, []string{filepath.Join(t.TempDir(), "nope.json")}))
}
