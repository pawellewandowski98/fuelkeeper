package main

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
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
