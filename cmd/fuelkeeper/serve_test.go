package main

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
	"github.com/bsv-blockchain/fuelkeeper/pkg/config"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/factory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// TestServeSmokeNullnet runs the full "nullnet" pipeline end to end: a memory
// store, a mock oracle/treasury with no real chain, jobs enabled. It waits
// for the minter+watcher+mock-treasury pipeline to mint and mine a shard,
// fetches that mined shard's BEEF exactly as a real wallet would (via
// InputBEEF) and confirms it is real, parseable BEEF with a merkle path
// proving the split tx at the reported height and intact fuel-output
// locking scripts, exercises reserve/spend/release through pkg/client,
// checks readyz, and finally confirms graceful shutdown.
func TestServeSmokeNullnet(t *testing.T) {
	cfg := `
store:
  backend: memory
pools:
  - name: main
    denomination: 50
    split_size: 20
    target_available: 100
    low_water: 40
    min_ttl: 1s
    spend_policy: trust
jobs:
  enabled: true
  lease_ttl: 10s
  reaper: {interval: 500ms, grace: 1s, quarantine_min_age: 1s, min_unspent_age: 1s}
  watcher: {interval: 300ms}
  minter: {interval: 300ms, max_per_round: 5, max_pending_shards: 10}
oracle:
  kind: mock
treasury:
  kind: mock
  mock: {confirm_after: 200ms}
api:
  listen: "127.0.0.1:0"
log:
  level: warn
`
	path := filepath.Join(t.TempDir(), "fk.yaml")
	require.NoError(t, os.WriteFile(path, []byte(cfg), 0o600))

	ctx, cancel := context.WithCancel(context.Background())
	addr := make(chan string, 1)
	errc := make(chan error, 1)
	go func() { errc <- runServe(ctx, path, func(a string) { addr <- a }) }()

	var base string
	select {
	case base = <-addr:
	case err := <-errc:
		t.Fatalf("serve exited early: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not start")
	}
	c, err := client.New("http://" + base)
	require.NoError(t, err)

	// Wait for the minter+watcher+mock-treasury pipeline to produce mined fuel.
	require.Eventually(t, func() bool {
		ps, err := c.Pools(ctx)
		return err == nil && len(ps) == 1 && ps[0].Stats.Shards.Mined >= 1
	}, 15*time.Second, 100*time.Millisecond, "expected at least one mined shard")

	res, err := c.Reserve(ctx, "main", client.ReserveRequest{Count: 3})
	require.NoError(t, err)
	assert.Len(t, res.Outputs, 3)

	// The default serve_policy (mined_only) guarantees every reserved output
	// above came from a mined shard, i.e. one that has been through
	// pkg/jobs.Watcher.mine and the nullnet mock treasury's MinedBEEF. Fetch
	// that shard's BEEF exactly as a real wallet would (InputBEEF, same as
	// go-sdk's transaction.NewBeefFromBytes expects) and confirm it is real,
	// parseable BEEF -- not the unparseable "mock-mined-beef-" marker the
	// nullnet pipeline used to store.
	beef, err := c.InputBEEF(ctx, "main", res)
	require.NoError(t, err)
	parsedBEEF, err := transaction.NewBeefFromBytes(beef)
	require.NoError(t, err, "a real wallet must be able to parse a nullnet shard's BEEF")
	require.NotEmpty(t, res.Shards, "reserve names the shard(s) backing the reserved outputs")
	heights := make([]uint32, 0, len(res.Shards))
	for _, sh := range res.Shards {
		txid, err := chainhash.NewHashFromHex(sh.TxID)
		require.NoError(t, err)
		tx := parsedBEEF.FindTransaction(sh.TxID)
		require.NotNil(t, tx, "the split tx for shard %s must be in its own BEEF", sh.TxID)
		require.NotNil(t, tx.MerklePath, "a mined shard's BEEF must carry a merkle path (BUMP)")
		root, err := tx.MerklePath.ComputeRoot(txid)
		require.NoError(t, err)
		assert.Equal(t, sh.TxID, root.String(), "the BUMP's computed root is the shard's own txid (single-tx block)")
		assert.GreaterOrEqual(t, tx.MerklePath.BlockHeight, uint32(1), "the reported height is the synthetic height set when the shard was mined")
		heights = append(heights, tx.MerklePath.BlockHeight)
	}
	// If multiple shards are observed, their BUMP heights must differ (strictly increasing).
	if len(heights) > 1 {
		for i := 1; i < len(heights); i++ {
			assert.NotEqual(t, heights[i-1], heights[i], "shard heights must be strictly increasing; shard %d and %d have the same height", i-1, i)
		}
	}
	for _, o := range res.Outputs {
		tx := parsedBEEF.FindTransaction(o.TxID)
		require.NotNil(t, tx)
		require.Greater(t, len(tx.Outputs), int(o.Vout))
		wantLock, err := hex.DecodeString(o.LockingScript)
		require.NoError(t, err)
		assert.Equal(t, wantLock, []byte(*tx.Outputs[o.Vout].LockingScript), "the fuel output's locking script is intact in the mined BEEF")
	}

	n, err := c.Spend(ctx, "main", res.Token, []string{res.Outputs[0].Outpoint}, strings.Repeat("ab", 32))
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	n, err = c.Release(ctx, "main", res.Token, []string{res.Outputs[1].Outpoint, res.Outputs[2].Outpoint})
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	// readyz reports a ready pool once fuel was served.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+base+"/readyz", http.NoBody)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), `"pools_ready":1`)

	cancel()
	select {
	case err := <-errc:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not stop")
	}
}

// TestNullnetHeightCounterIncreases verifies that successive mock-treasury
// confirmations assign strictly increasing block heights. This unit test
// directly checks the height counter logic used by the nullnet mock oracle
// hook in openTreasury, ensuring two consecutive confirmations get h and h+1.
func TestNullnetHeightCounterIncreases(t *testing.T) {
	orc := oracle.NewMock()
	mine := nullnetMiner(orc)
	a, b := storetest.NewTxID(), storetest.NewTxID()
	mine(a)
	mine(b)
	heights := make([]uint32, 0, 2)
	for _, txid := range []chainhash.Hash{a, b} {
		st, err := orc.Tx(context.Background(), txid)
		require.NoError(t, err)
		require.True(t, st.Known && st.Mined)
		mp, err := transaction.NewMerklePathFromBinary(st.MerklePath)
		require.NoError(t, err)
		assert.Equal(t, st.BlockHeight, mp.BlockHeight, "BUMP height matches the reported height")
		root, err := mp.ComputeRoot(&txid)
		require.NoError(t, err)
		assert.Equal(t, txid, *root, "single-tx block: root is the txid")
		heights = append(heights, st.BlockHeight)
	}
	assert.Equal(t, []uint32{1, 2}, heights, "consecutive confirmations get strictly increasing heights")
}

// TestServeRejectsBadConfig checks two ways runServe fails fast: a config
// file that does not exist, and an oracle kind missing the URL it needs.
func TestServeRejectsBadConfig(t *testing.T) {
	t.Run("missing file", func(t *testing.T) {
		err := runServe(context.Background(), filepath.Join(t.TempDir(), "does-not-exist.yaml"), nil)
		require.Error(t, err)
	})

	t.Run("oracle kind without its url", func(t *testing.T) {
		cfg := `
store:
  backend: memory
pools:
  - name: main
    denomination: 50
oracle:
  kind: teranode
treasury:
  kind: mock
api:
  listen: "127.0.0.1:0"
log:
  level: warn
`
		path := filepath.Join(t.TempDir(), "fk.yaml")
		require.NoError(t, os.WriteFile(path, []byte(cfg), 0o600))
		err := runServe(context.Background(), path, nil)
		require.ErrorContains(t, err, "oracle.teranode_url is required")
	})
}

// TestAdminCmdParsesSubcommandBeforeOrAfterFlags unit-tests extractSubcommand
// directly (no server involved): the quarantine subcommand must be found
// whether it appears before or after the flag arguments, and a flag's own
// value (which need not start with "-") must never be mistaken for it.
func TestAdminCmdParsesSubcommandBeforeOrAfterFlags(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantSub  string
		wantRest []string
	}{
		{
			name:     "subcommand first",
			args:     []string{"list", "--pool", "main"},
			wantSub:  "list",
			wantRest: []string{"--pool", "main"},
		},
		{
			name:     "subcommand last",
			args:     []string{"--pool", "main", "list"},
			wantSub:  "list",
			wantRest: []string{"--pool", "main"},
		},
		{
			name:     "subcommand between flags",
			args:     []string{"--url", "http://x", "requeue", "--outpoints", "a:0,b:1"},
			wantSub:  "requeue",
			wantRest: []string{"--url", "http://x", "--outpoints", "a:0,b:1"},
		},
		{
			name:     "flag=value form",
			args:     []string{"--pool=main", "retire", "--txid", "aabb"},
			wantSub:  "retire",
			wantRest: []string{"--pool=main", "--txid", "aabb"},
		},
		{
			name:     "no subcommand",
			args:     []string{"--pool", "main"},
			wantSub:  "",
			wantRest: []string{"--pool", "main"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub, rest := extractSubcommand(tt.args)
			assert.Equal(t, tt.wantSub, sub)
			assert.Equal(t, tt.wantRest, rest)
		})
	}
}

// shutdownTestConfig returns a minimal, valid nullnet config for the
// shutdown-ordering tests below: serve_policy: any lets a freshly minted,
// still-unmined row be reserved immediately, so the test does not also need
// to wait out the watcher/mock-treasury confirm pipeline.
func shutdownTestConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(`
store:
  backend: memory
pools:
  - name: main
    denomination: 50
    split_size: 20
    target_available: 100
    low_water: 40
    min_ttl: 1s
    serve_policy: any
    spend_policy: trust
    release_policy: trust
jobs:
  enabled: true
  lease_ttl: 10s
  reaper: {interval: 500ms, grace: 1s, quarantine_min_age: 1s, min_unspent_age: 1s}
  watcher: {interval: 300ms}
  minter: {interval: 100ms, max_per_round: 5, max_pending_shards: 10}
oracle:
  kind: none
treasury:
  kind: mock
  mock: {confirm_after: 200ms}
api:
  listen: "127.0.0.1:0"
log:
  level: warn
`))
	require.NoError(t, err)
	return cfg
}

// TestRunServeConfigDrainsInFlightRequestBeforeStoppingPools is the
// regression test for the shutdown-ordering fix: poolCtx/jobCtx must not be
// children of the signal context, or cancelling it (simulating SIGTERM) would
// tear down the pools and jobs at the exact moment srv.Shutdown starts
// draining in-flight requests, instead of only after it finishes. A
// faultyStore (see faulty_store_test.go) pauses the Reserve call a real
// request is blocked inside, so the test can cancel ctx while that call is
// paused and confirm the request still completes with a normal 200 once
// released, and that the store is only closed -- and starts failing every
// call -- after everything has actually stopped.
func TestRunServeConfigDrainsInFlightRequestBeforeStoppingPools(t *testing.T) {
	cfg := shutdownTestConfig(t)

	var fs *faultyStore
	addr := make(chan string, 1)
	hooks := serveHooks{
		onListen: func(a string) { addr <- a },
		newStore: func(ctx context.Context, storeCfg factory.Config, now func() time.Time) (store.Store, error) {
			inner, err := factory.New(ctx, storeCfg, now)
			if err != nil {
				return nil, err
			}
			fs = newFaultyStore(inner)
			return fs, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- runServeConfig(ctx, cfg, hooks) }()

	var base string
	select {
	case base = <-addr:
	case err := <-errc:
		t.Fatalf("serve exited early: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not start")
	}
	c, err := client.New("http://" + base)
	require.NoError(t, err)

	// serve_policy: any lets the minter's very first (immediate) tick's rows
	// be reserved right away, with no need to wait for mining.
	require.Eventually(t, func() bool {
		ps, perr := c.Pools(context.Background())
		return perr == nil && len(ps) == 1 && ps[0].Stats.Available > 0
	}, 10*time.Second, 50*time.Millisecond, "expected available fuel")

	holding, release := fs.armReserveHold()

	reserveErr := make(chan error, 1)
	go func() {
		_, rerr := c.Reserve(context.Background(), "main", client.ReserveRequest{Count: 1})
		reserveErr <- rerr
	}()

	select {
	case <-holding:
	case <-time.After(5 * time.Second):
		t.Fatal("reserve did not reach the store in time")
	}

	// Simulate SIGTERM while that request is paused inside the store.
	cancel()
	// Give runServeConfig a moment to reach srv.Shutdown, which should now
	// block draining our still-in-flight (held) request rather than racing
	// ahead to cancel the pools underneath it.
	time.Sleep(200 * time.Millisecond)

	release()

	select {
	case rerr := <-reserveErr:
		require.NoError(t, rerr, "an in-flight reserve must complete successfully even though shutdown began while it was paused")
	case <-time.After(5 * time.Second):
		t.Fatal("reserve did not complete after the hold was released")
	}

	select {
	case err := <-errc:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("runServeConfig did not stop")
	}

	// Once everything has stopped, the store is closed: any further call
	// fails, proving nothing can reach it post-shutdown.
	require.ErrorIs(t, fs.Ping(context.Background()), errFaultyStoreClosed)
}

// TestRunServeConfigListenPortInUse checks that a bind failure produces a
// clear, wrapped error instead of a bare net.OpError.
func TestRunServeConfigListenPortInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	cfg := shutdownTestConfig(t)
	cfg.API.Listen = ln.Addr().String()
	cfg.Jobs.Enabled = false // irrelevant to this test; keep it fast and quiet

	err = runServeConfig(context.Background(), cfg, serveHooks{})
	require.ErrorContains(t, err, "listen")
	require.ErrorContains(t, err, ln.Addr().String())
}

// TestRunServeConfigStoreOpenFailure checks that a store-open failure is
// wrapped with a clear "store:" prefix and, crucially, happens without ever
// starting pools, jobs or the HTTP listener.
func TestRunServeConfigStoreOpenFailure(t *testing.T) {
	cfg := shutdownTestConfig(t)
	boom := errors.New("boom: simulated store-open failure")
	hooks := serveHooks{
		newStore: func(context.Context, factory.Config, func() time.Time) (store.Store, error) {
			return nil, boom
		},
	}
	err := runServeConfig(context.Background(), cfg, hooks)
	require.ErrorContains(t, err, "store:")
	require.ErrorIs(t, err, boom)
}

// TestRunServeConfigStartJobsErrorWrapping exercises startJobs' own error
// wrapping through the runServeConfig seam: a hand-mutated, otherwise-valid
// config with an invalid reaper interval (bypassing config.Config.Validate,
// which would normally catch this at load time) must surface as a "reaper:"
// error from jobs.NewReaper, not a panic or a silent no-op.
func TestRunServeConfigStartJobsErrorWrapping(t *testing.T) {
	cfg := shutdownTestConfig(t)
	cfg.Jobs.Reaper.Interval = 0 // jobs.ReaperConfig.Validate requires > 0

	err := runServeConfig(context.Background(), cfg, serveHooks{})
	require.ErrorContains(t, err, "reaper:")
}

// TestRunServeConfigToolboxTreasury serves with the go-wallet-toolbox
// treasury against a local stand-in for arcade and chaintracks (every
// endpoint 404s, so nothing leaves the machine) and an empty sqlite wallet:
// the treasury opens before the store, its Funder backs GET /v1/funding, the
// jobs run against it (an unfunded wallet only logs), and shutdown closes
// the treasury after the jobs and before the store.
func TestRunServeConfigToolboxTreasury(t *testing.T) {
	stub := httptest.NewServer(http.NotFoundHandler())
	defer stub.Close()
	cfg := shutdownTestConfig(t)
	cfg.Oracle.Kind = "arcade"
	cfg.Oracle.ArcadeURL = stub.URL
	cfg.Treasury.Kind = "toolbox"
	tb := &cfg.Treasury.Toolbox
	tb.RootKeyHex = "0000000000000000000000000000000000000000000000000000000000000002"
	tb.ArcadeURL = stub.URL
	tb.ChaintracksURL = stub.URL + "/chaintracks"
	tb.FeeSatPerKB = 1
	tb.Storage.SQLite = filepath.Join(t.TempDir(), "treasury.sqlite")
	cfg.Jobs.LeaseTTL = 90 * time.Second // must outlast a toolbox minter round
	require.NoError(t, cfg.Validate())

	var (
		mu     sync.Mutex
		events []string
	)
	record := func(e string) { mu.Lock(); events = append(events, e); mu.Unlock() }
	hooks := serveHooks{
		newStore: func(ctx context.Context, sc factory.Config, now func() time.Time) (store.Store, error) {
			record("store opened")
			st, err := factory.New(ctx, sc, now)
			if err != nil {
				return nil, err
			}
			return closeRecorder{Store: st, onClose: func() { record("store closed") }}, nil
		},
		onTreasuryClosed: func() { record("treasury closed") },
	}
	ctx, cancel := context.WithCancel(context.Background())
	addr := make(chan string, 1)
	hooks.onListen = func(a string) { addr <- a }
	errc := make(chan error, 1)
	go func() { errc <- runServeConfig(ctx, cfg, hooks) }()
	var base string
	select {
	case base = <-addr:
	case err := <-errc:
		t.Fatalf("serve exited early: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("serve did not start")
	}
	c, err := client.New("http://" + base)
	require.NoError(t, err)
	info, err := c.FundingInfo(ctx)
	require.NoError(t, err)
	assert.Equal(t, "ttn", info.Network)
	assert.Equal(t, "02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5", info.IdentityKey)
	assert.NotEmpty(t, info.DerivationPrefix)

	time.Sleep(300 * time.Millisecond) // let the minter run a few rounds against the unfunded wallet
	cancel()
	select {
	case err := <-errc:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("serve did not stop")
	}
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"store opened", "treasury closed", "store closed"}, events,
		"the treasury closes after the jobs and before the store")
	_, err = os.Stat(tb.Storage.SQLite)
	require.NoError(t, err, "the treasury wallet storage was created")
}

// TestRunServeConfigBadToolboxConfigFailsBeforeStore bypasses config.Validate
// with a toolbox treasury that has no root key: the treasury fails to open
// and the store is never opened.
func TestRunServeConfigBadToolboxConfigFailsBeforeStore(t *testing.T) {
	cfg := shutdownTestConfig(t)
	cfg.Treasury.Kind = "toolbox"
	hooks := serveHooks{
		newStore: func(context.Context, factory.Config, func() time.Time) (store.Store, error) {
			t.Fatal("store opened despite a bad treasury config")
			return nil, nil
		},
	}
	err := runServeConfig(context.Background(), cfg, hooks)
	require.ErrorContains(t, err, "treasury:")
	require.ErrorContains(t, err, "root_key_hex")
}

// closeRecorder reports Close.
type closeRecorder struct {
	store.Store
	onClose func()
}

func (c closeRecorder) Close() error {
	c.onClose()
	return c.Store.Close()
}

// leaseCounter counts job lease acquisitions.
type leaseCounter struct {
	store.Store
	jobLeases *atomic.Int32
}

func (l leaseCounter) TryAcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (time.Time, error) {
	if strings.HasPrefix(name, "jobs/") {
		l.jobLeases.Add(1)
	}
	return l.Store.TryAcquireOrRenew(ctx, name, holder, ttl)
}

// TestRunServeConfigListenFailsBeforeJobsStart checks that the listener is
// bound before any job starts: with the port taken, no job lease is ever
// requested, so no minter round can touch the treasury.
func TestRunServeConfigListenFailsBeforeJobsStart(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()
	cfg := shutdownTestConfig(t)
	cfg.API.Listen = ln.Addr().String()
	require.True(t, cfg.Jobs.Enabled)
	var leases atomic.Int32
	hooks := serveHooks{newStore: func(ctx context.Context, sc factory.Config, now func() time.Time) (store.Store, error) {
		st, err := factory.New(ctx, sc, now)
		return leaseCounter{Store: st, jobLeases: &leases}, err
	}}
	err = runServeConfig(context.Background(), cfg, hooks)
	require.ErrorContains(t, err, "listen")
	assert.Zero(t, leases.Load(), "no job ran")
}

// capturingHandler is a minimal slog.Handler that records every emitted
// slog.Record, so a test can assert on structured attributes (e.g.
// answers_outpoints as a bool) instead of scraping formatted text.
type capturingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}

func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

func (h *capturingHandler) last(t *testing.T) slog.Record {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	require.Len(t, h.records, 1)
	return h.records[0]
}

func recordAttrs(r slog.Record) map[string]any {
	m := make(map[string]any, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value.Any()
		return true
	})
	return m
}

// TestLogOracleMode covers all three shapes logOracleMode reports -- none
// (no chain source at all), arcade-only (does not answer outpoint queries,
// whose message points at jobs.reaper.unknown_requeue_age, expiry_policy:
// hold and oracle.teranode_url as the optional accelerator), and
// outpoint-capable (mock/teranode/composite+teranode_url) -- crossed with
// jobs.enabled: the unknown_requeue_age promise (the reaper's unhinted-row
// policy) only appears when the reaper actually runs.
func TestLogOracleMode(t *testing.T) {
	h := &capturingHandler{}
	log := slog.New(h)
	cfg := shutdownTestConfig(t)

	// none (jobs enabled): shutdownTestConfig's default oracle.kind. Must
	// not be mislabeled arcade-only -- it never talks to arcade at all.
	logOracleMode(log, cfg)
	rec := h.last(t)
	attrs := recordAttrs(rec)
	assert.Equal(t, "none", attrs["oracle"])
	assert.Equal(t, false, attrs["answers_outpoints"])
	assert.Equal(t, true, attrs["jobs_enabled"])
	assert.Contains(t, rec.Message, "no chain source")
	assert.Contains(t, rec.Message, "unknown_requeue_age")
	assert.NotContains(t, rec.Message, "arcade-only")

	h.records = nil
	cfg.Oracle.Kind = "arcade"
	cfg.Oracle.ArcadeURL = "http://arcade"
	logOracleMode(log, cfg)
	rec = h.last(t)
	attrs = recordAttrs(rec)
	assert.Equal(t, "arcade", attrs["oracle"])
	assert.Equal(t, false, attrs["answers_outpoints"])
	assert.Contains(t, rec.Message, "unknown_requeue_age")
	assert.Contains(t, rec.Message, "expiry_policy: hold")
	assert.Contains(t, rec.Message, "oracle.teranode_url")
	assert.NotContains(t, rec.Message, "unhinted spend")

	h.records = nil
	cfg.Oracle.Kind = "mock"
	logOracleMode(log, cfg)
	rec = h.last(t)
	attrs = recordAttrs(rec)
	assert.Equal(t, "mock", attrs["oracle"])
	assert.Equal(t, true, attrs["answers_outpoints"])
	assert.NotContains(t, rec.Message, "unknown_requeue_age")

	// jobs disabled: no reaper runs, so the unknown_requeue_age promise must
	// not appear even for the arcade-only shape that would otherwise state
	// it -- it would never come true.
	h.records = nil
	cfg.Oracle.Kind = "arcade"
	cfg.Jobs.Enabled = false
	logOracleMode(log, cfg)
	rec = h.last(t)
	attrs = recordAttrs(rec)
	assert.Equal(t, false, attrs["jobs_enabled"])
	assert.NotContains(t, rec.Message, "unknown_requeue_age")
	assert.Contains(t, rec.Message, "jobs.enabled is false")

	// unknown_requeue_age 0: the reaper runs, but the requeue itself is
	// disabled, so the message must not promise one -- it must say expired
	// reservations are held instead.
	h.records = nil
	cfg.Oracle.Kind = "arcade"
	cfg.Jobs.Enabled = true
	cfg.Jobs.Reaper.UnknownRequeueAge = 0
	logOracleMode(log, cfg)
	rec = h.last(t)
	assert.Contains(t, rec.Message, "unknown_requeue_age is 0")
	assert.Contains(t, rec.Message, "held")
	assert.NotContains(t, rec.Message, "requeued after")
}
