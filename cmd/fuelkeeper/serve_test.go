package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
	"github.com/bsv-blockchain/fuelkeeper/pkg/config"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/factory"
)

// TestServeSmokeNullnet runs the full "nullnet" pipeline end to end: a memory
// store, a mock oracle/treasury with no real chain, jobs enabled. It waits
// for the minter+watcher+mock-treasury pipeline to mint and mine a shard,
// exercises reserve/spend/release through pkg/client, checks readyz, and
// finally confirms graceful shutdown.
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

// TestServeRejectsBadConfig checks two ways runServe fails fast: a config
// file that does not exist, and a config that asks for a build kind that is
// not wired up yet (oracle.kind: teranode arrives in Plan 5).
func TestServeRejectsBadConfig(t *testing.T) {
	t.Run("missing file", func(t *testing.T) {
		err := runServe(context.Background(), filepath.Join(t.TempDir(), "does-not-exist.yaml"), nil)
		require.Error(t, err)
	})

	t.Run("oracle kind not available in this build", func(t *testing.T) {
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
		require.ErrorContains(t, err, "not available in this build")
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
