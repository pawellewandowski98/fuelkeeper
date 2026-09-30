# FuelKeeper Plan 3: Configuration, HTTP API, Go client and `serve` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make FuelKeeper runnable: a YAML configuration, the documented HTTP/JSON API over the Plan 2 `pool.Pool`, admin endpoints for operators, a Go client with BEEF caching and createAction helpers, health/readiness/metrics, and the `fuelkeeper serve` command wired to the store factory, jobs, mock treasury and configured oracle.

**Architecture:** `net/http` with Go 1.22 method patterns, no framework. Handlers are thin: decode → validate → call `pool.Pool` → encode; error mapping is one function. The API package depends on an interface (`PoolService`) so handler tests use the real `pool.Pool` over the memory store. The OpenAPI document is hand-written and served by the binary. The client package mirrors the API and adds the two things every wallet needs: a per-shard BEEF cache and a helper that turns a reservation into `createAction` inputs.

**Tech Stack:** Go 1.26 stdlib `net/http`, `log/slog`, `gopkg.in/yaml.v3`, `github.com/prometheus/client_golang/prometheus/promhttp`, go-sdk (`transaction.Beef` merge, `wallet.CreateActionInput`), testify.

**Spec:** `docs/superpowers/specs/2026-09-29-fuelkeeper-design.md` — "HTTP API", "Go client", "CLI", "Metrics". One documented deviation: BEEF is served at `GET /v1/pools/{pool}/beef/{txid}` (pool-scoped, because shards are keyed by pool) instead of `GET /v1/beef/{txid}`.

**Model tiering (user preference):** implementers on `sonnet` for Tasks 1–7 and `haiku` for Task 8 (OpenAPI YAML) and Task 9 (docs). Spec reviews on `sonnet`; code-quality reviews on `sonnet` except Task 4 (API handlers + error mapping) and Task 6 (client BEEF helpers) on the most capable model.

---

## File structure

```
pkg/config/
  config.go        Config, Load(path), env expansion, Validate (incl. mint-capacity check)
  config_test.go
  testdata/full.yaml, testdata/minimal.yaml
pkg/api/
  server.go        NewServer(deps) *http.Server; routes; middleware chain
  handlers.go      reserve/release/spend/pools/beef/admin handlers
  dto.go           request/response JSON types and encoders (hex, base64)
  errors.go        writeError, mapErr (pool errors → HTTP status + code + Retry-After)
  auth.go          bearer API-key middleware (constant-time)
  health.go        /healthz /readyz /metrics /openapi.yaml
  server_test.go   end-to-end handler tests over pool.Pool + memory store
pkg/client/
  client.go        Client, options, Reserve/Release/Spend/Pools/Pool/BEEF/Admin*
  beef.go          BEEF cache, InputBEEF, CreateActionInputs
  errors.go        APIError{Status, Code, Message, RetryAfter}, sentinel matches
  client_test.go   against httptest + real api.Server
openapi/fuelkeeper.yaml
cmd/fuelkeeper/
  main.go          subcommand dispatch
  serve.go         wire config → store → pools → jobs → api
  admin.go         pools / shards / quarantine / release subcommands over the client
docs/API.md, docs/OPERATIONS.md (Task 9)
```

Conventions:
- JSON field names are snake_case. Tx ids are display hex (`chainhash.Hash.String()`), outpoints are `"<txid>:<vout>"`, scripts are hex, tokens are the 22-char base64url form, times are RFC 3339 UTC, durations in requests are integer seconds.
- Every error response is `{"code": "...", "message": "...", "retry_after": <seconds, optional>}` with a stable `code` from `pkg/api/errors.go`.
- Handlers never sleep or block on background work; 503 responses always carry `Retry-After`.

### Task 1: Configuration (`pkg/config`)

**Files:**
- Create: `pkg/config/config.go`, `pkg/config/testdata/full.yaml`, `pkg/config/testdata/minimal.yaml`
- Modify: `pkg/pool/config.go` (add `ExpectedTPS`)
- Test: `pkg/config/config_test.go`

- [ ] **Step 1: Add `ExpectedTPS` to `pool.PoolConfig`**

In `pkg/pool/config.go` add the field after `SpendPolicy`:
```go
	ExpectedTPS     float64        `yaml:"expected_tps"` // optional; enables the mint-capacity check
```
and in `Validate` add `if c.ExpectedTPS < 0 { return fmt.Errorf("pool %s: expected_tps must be >= 0", c.Name) }`.

- [ ] **Step 2: Write the test fixtures**

`pkg/config/testdata/minimal.yaml`:
```yaml
store:
  backend: memory
pools:
  - name: main
    denomination: 50
```

`pkg/config/testdata/full.yaml`:
```yaml
instance:
  instance_id: node-1
  max_shards: 4
  low_water_rows: 2000
  lease_ttl: 20s
  rescan_interval: 4s
store:
  backend: sqlite
  sqlite:
    path: ${FK_TEST_SQLITE_PATH}
pools:
  - name: main
    denomination: 50
    split_size: 200
    target_available: 10000
    low_water: 4000
    serve_policy: any
    default_ttl: 90s
    min_ttl: 10s
    max_ttl: 5m
    max_count: 50
    release_policy: verify
    spend_policy: trust
    expected_tps: 100
  - name: big
    denomination: 500
    lock_mode: p2pkh
jobs:
  enabled: true
  lease_ttl: 45s
  reaper:
    interval: 2s
    grace: 5s
  minter:
    interval: 10s
    max_per_round: 5
    max_pending_shards: 10
oracle:
  kind: mock
treasury:
  kind: mock
api:
  listen: "127.0.0.1:9090"
  api_keys: ["k1", "k2"]
  max_body_bytes: 65536
log:
  level: debug
  format: json
```

- [ ] **Step 3: Write the failing tests**

`pkg/config/config_test.go`:
```go
package config_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/config"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/factory"
)

func TestLoadMinimalAppliesDefaults(t *testing.T) {
	c, err := config.Load("testdata/minimal.yaml")
	require.NoError(t, err)
	assert.Equal(t, factory.BackendMemory, c.Store.Backend)
	require.Len(t, c.Pools, 1)
	p := c.Pools[0]
	assert.Equal(t, "main", p.Name)
	assert.Equal(t, uint64(50), p.Denomination)
	assert.Equal(t, 500, p.SplitSize, "pool defaults applied")
	assert.Equal(t, pool.ServeMinedOnly, p.ServePolicy)
	assert.Equal(t, store.LockPuzzle, p.LockMode)
	assert.Equal(t, 8, c.Instance.MaxShards)
	assert.Equal(t, ":8080", c.API.Listen)
	assert.True(t, c.Jobs.Enabled)
	assert.Equal(t, time.Second, c.Jobs.Reaper.Interval)
	assert.Equal(t, "none", c.Oracle.Kind)
	assert.Equal(t, "mock", c.Treasury.Kind)
	assert.Equal(t, "info", c.Log.Level)
}

func TestLoadFullOverridesAndExpandsEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.sqlite")
	t.Setenv("FK_TEST_SQLITE_PATH", path)
	c, err := config.Load("testdata/full.yaml")
	require.NoError(t, err)
	assert.Equal(t, "node-1", c.Instance.InstanceID)
	assert.Equal(t, 4, c.Instance.MaxShards)
	assert.Equal(t, 20*time.Second, c.Instance.LeaseTTL)
	assert.Equal(t, factory.BackendSQLite, c.Store.Backend)
	assert.Equal(t, path, c.Store.SQLite.Path, "${VAR} expanded")
	require.Len(t, c.Pools, 2)
	assert.Equal(t, 200, c.Pools[0].SplitSize)
	assert.Equal(t, pool.ServeAny, c.Pools[0].ServePolicy)
	assert.Equal(t, pool.ReleaseVerify, c.Pools[0].ReleasePolicy)
	assert.Equal(t, pool.SpendTrust, c.Pools[0].SpendPolicy)
	assert.Equal(t, 90*time.Second, c.Pools[0].DefaultTTL)
	assert.Equal(t, 100.0, c.Pools[0].ExpectedTPS)
	assert.Equal(t, store.LockP2PKH, c.Pools[1].LockMode)
	assert.Equal(t, 500, c.Pools[1].SplitSize, "defaults still fill unspecified fields")
	assert.Equal(t, 45*time.Second, c.Jobs.LeaseTTL)
	assert.Equal(t, 2*time.Second, c.Jobs.Reaper.Interval)
	assert.Equal(t, 5*time.Second, c.Jobs.Reaper.Grace)
	assert.Equal(t, 30*time.Second, c.Jobs.Reaper.QuarantineMinAge, "unspecified reaper fields keep defaults")
	assert.Equal(t, 5, c.Jobs.Minter.MaxPerRound)
	assert.Equal(t, "mock", c.Oracle.Kind)
	assert.Equal(t, "127.0.0.1:9090", c.API.Listen)
	assert.Equal(t, []string{"k1", "k2"}, c.API.APIKeys)
	assert.Equal(t, int64(65536), c.API.MaxBodyBytes)
	assert.Equal(t, "json", c.Log.Format)
}

func TestValidateRejectsBadConfigs(t *testing.T) {
	base := func() *config.Config {
		c, err := config.Load("testdata/minimal.yaml")
		require.NoError(t, err)
		return c
	}
	c := base()
	c.Pools = nil
	require.ErrorContains(t, c.Validate(), "at least one pool")

	c = base()
	c.Pools = append(c.Pools, c.Pools[0])
	require.ErrorContains(t, c.Validate(), "duplicate pool")

	c = base()
	c.Oracle.Kind = "psychic"
	require.ErrorContains(t, c.Validate(), "oracle.kind")

	c = base()
	c.Treasury.Kind = "vault"
	require.ErrorContains(t, c.Validate(), "treasury.kind")

	c = base()
	c.Jobs.LeaseTTL = c.Jobs.Minter.Interval / 2
	require.ErrorContains(t, c.Validate(), "jobs.lease_ttl")

	c = base()
	c.Log.Level = "loud"
	require.ErrorContains(t, c.Validate(), "log.level")

	// Mint-capacity check: 500 rows × 20 splits / 30 s ≈ 333 rows/s; 1.2 × 1000 TPS exceeds it.
	c = base()
	c.Pools[0].ExpectedTPS = 1000
	err := c.Validate()
	require.ErrorContains(t, err, "mint capacity")
	c.Pools[0].ExpectedTPS = 200
	require.NoError(t, c.Validate())

	c = base()
	c.Pools[0].SpendPolicy = pool.SpendVerify
	c.Oracle.Kind = "none"
	require.ErrorContains(t, c.Validate(), "spend_policy=verify requires an oracle")

	_, err = config.Load("testdata/does-not-exist.yaml")
	require.Error(t, err)
}
```

- [ ] **Step 4: Run to verify failure**

Run: `cd /git/fuelkeeper && go test ./pkg/config/`
Expected: build FAIL.

- [ ] **Step 5: Write `pkg/config/config.go`**

```go
// Package config loads and validates FuelKeeper's YAML configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/bsv-blockchain/fuelkeeper/pkg/jobs"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/factory"
)

// Config is the whole file.
type Config struct {
	Instance pool.InstanceConfig `yaml:"instance"`
	Store    factory.Config      `yaml:"store"`
	Pools    []pool.PoolConfig   `yaml:"pools"`
	Jobs     JobsConfig          `yaml:"jobs"`
	Oracle   OracleConfig        `yaml:"oracle"`
	Treasury TreasuryConfig      `yaml:"treasury"`
	API      APIConfig           `yaml:"api"`
	Log      LogConfig           `yaml:"log"`
}

// JobsConfig enables and tunes the leased background jobs.
type JobsConfig struct {
	Enabled  bool               `yaml:"enabled"`
	LeaseTTL time.Duration      `yaml:"lease_ttl"`
	Reaper   jobs.ReaperConfig  `yaml:"reaper"`
	Watcher  jobs.WatcherConfig `yaml:"watcher"`
	Minter   jobs.MinterConfig  `yaml:"minter"`
}

// OracleConfig selects the chain oracle. teranode/arcade/composite are wired in Plan 5.
type OracleConfig struct {
	Kind     string `yaml:"kind"` // none | mock | teranode | arcade | composite
	Teranode struct {
		URL string `yaml:"url"`
	} `yaml:"teranode"`
	Arcade struct {
		URL string `yaml:"url"`
	} `yaml:"arcade"`
}

// TreasuryConfig selects the treasury. toolbox fields are added in Plan 5.
type TreasuryConfig struct {
	Kind string `yaml:"kind"` // mock | toolbox
}

// APIConfig configures the HTTP listener.
type APIConfig struct {
	Listen       string        `yaml:"listen"`
	APIKeys      []string      `yaml:"api_keys"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	IdleTimeout  time.Duration `yaml:"idle_timeout"`
	MaxBodyBytes int64         `yaml:"max_body_bytes"`
}

// LogConfig configures slog.
type LogConfig struct {
	Level  string `yaml:"level"`  // debug | info | warn | error
	Format string `yaml:"format"` // text | json
}

// Default returns a config with every default filled and no pools.
func Default() *Config {
	return &Config{
		Instance: pool.DefaultInstanceConfig(),
		Store:    factory.Config{Backend: factory.BackendMemory},
		Jobs: JobsConfig{
			Enabled:  true,
			LeaseTTL: 60 * time.Second,
			Reaper:   jobs.DefaultReaperConfig(),
			Watcher:  jobs.DefaultWatcherConfig(),
			Minter:   jobs.DefaultMinterConfig(),
		},
		Oracle:   OracleConfig{Kind: "none"},
		Treasury: TreasuryConfig{Kind: "mock"},
		API: APIConfig{
			Listen:       ":8080",
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  120 * time.Second,
			MaxBodyBytes: 1 << 20,
		},
		Log: LogConfig{Level: "info", Format: "text"},
	}
}

// Load reads a YAML file, expands ${ENV} references, applies defaults and validates.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // operator-supplied path
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	return Parse([]byte(os.ExpandEnv(string(raw))))
}

// Parse decodes YAML bytes over the defaults and validates.
func Parse(data []byte) (*Config, error) {
	c := Default()
	if err := yaml.Unmarshal(data, c); err != nil {
		return nil, fmt.Errorf("config: parse: %w", err)
	}
	for i := range c.Pools {
		applyPoolDefaults(&c.Pools[i])
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// applyPoolDefaults fills zero fields of a pool from DefaultPoolConfig.
func applyPoolDefaults(p *pool.PoolConfig) {
	d := pool.DefaultPoolConfig(p.Name, p.Denomination)
	if p.LockModeName == "" {
		p.LockModeName = d.LockModeName
	}
	if p.SplitSize == 0 {
		p.SplitSize = d.SplitSize
	}
	if p.TargetAvailable == 0 {
		p.TargetAvailable = d.TargetAvailable
	}
	if p.LowWater == 0 {
		p.LowWater = d.LowWater
	}
	if p.ServePolicy == "" {
		p.ServePolicy = d.ServePolicy
	}
	if p.DefaultTTL == 0 {
		p.DefaultTTL = d.DefaultTTL
	}
	if p.MinTTL == 0 {
		p.MinTTL = d.MinTTL
	}
	if p.MaxTTL == 0 {
		p.MaxTTL = d.MaxTTL
	}
	if p.MaxCount == 0 {
		p.MaxCount = d.MaxCount
	}
	if p.ReleasePolicy == "" {
		p.ReleasePolicy = d.ReleasePolicy
	}
	if p.SpendPolicy == "" {
		p.SpendPolicy = d.SpendPolicy
	}
}

// Validate checks cross-field rules.
func (c *Config) Validate() error {
	if err := c.Instance.Validate(); err != nil {
		return fmt.Errorf("config: instance: %w", err)
	}
	if len(c.Pools) == 0 {
		return errors.New("config: at least one pool is required")
	}
	seen := map[string]bool{}
	for i := range c.Pools {
		p := &c.Pools[i]
		if err := p.Validate(); err != nil {
			return fmt.Errorf("config: %w", err)
		}
		if seen[p.Name] {
			return fmt.Errorf("config: duplicate pool %q", p.Name)
		}
		seen[p.Name] = true
	}
	switch c.Oracle.Kind {
	case "none", "mock", "teranode", "arcade", "composite":
	default:
		return fmt.Errorf("config: oracle.kind %q is not one of none|mock|teranode|arcade|composite", c.Oracle.Kind)
	}
	switch c.Treasury.Kind {
	case "mock", "toolbox":
	default:
		return fmt.Errorf("config: treasury.kind %q is not one of mock|toolbox", c.Treasury.Kind)
	}
	if c.Jobs.Enabled {
		longest := max(c.Jobs.Reaper.Interval, c.Jobs.Watcher.Interval, c.Jobs.Minter.Interval)
		if c.Jobs.LeaseTTL <= 2*longest {
			return fmt.Errorf("config: jobs.lease_ttl (%s) must exceed twice the longest job interval (%s)", c.Jobs.LeaseTTL, longest)
		}
		if c.Jobs.Minter.Interval <= 0 || c.Jobs.Minter.MaxPerRound <= 0 || c.Jobs.Minter.MaxPendingShards <= 0 {
			return errors.New("config: jobs.minter interval, max_per_round and max_pending_shards must be > 0")
		}
		for _, p := range c.Pools {
			if p.ExpectedTPS <= 0 {
				continue
			}
			capacity := float64(p.SplitSize*c.Jobs.Minter.MaxPerRound) / c.Jobs.Minter.Interval.Seconds()
			required := 1.2 * p.ExpectedTPS
			if capacity < required {
				return fmt.Errorf("config: pool %s: mint capacity %.0f rows/s < 1.2 × expected_tps (%.0f); raise split_size or jobs.minter.max_per_round, or lower jobs.minter.interval", p.Name, capacity, required)
			}
			if p.SpendPolicy == pool.SpendVerify && c.Oracle.Kind == "none" {
				return fmt.Errorf("config: pool %s: spend_policy=verify requires an oracle (oracle.kind is none)", p.Name)
			}
		}
	}
	for _, p := range c.Pools {
		if p.SpendPolicy == pool.SpendVerify && c.Oracle.Kind == "none" {
			return fmt.Errorf("config: pool %s: spend_policy=verify requires an oracle (oracle.kind is none)", p.Name)
		}
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("config: log.level %q is not one of debug|info|warn|error", c.Log.Level)
	}
	switch c.Log.Format {
	case "text", "json":
	default:
		return fmt.Errorf("config: log.format %q is not one of text|json", c.Log.Format)
	}
	if c.API.Listen == "" || c.API.MaxBodyBytes <= 0 {
		return errors.New("config: api.listen and api.max_body_bytes are required")
	}
	return nil
}
```
Remove the duplicated spend-policy check inside the `c.Jobs.Enabled` loop (keep the standalone loop after it). Note the minimal fixture has `spend_policy` defaulting to `verify` and `oracle.kind: none`: to keep `TestLoadMinimalAppliesDefaults` valid, `applyPoolDefaults` must set `SpendPolicy` to `trust` when the oracle kind is `none`. Implement that in `Parse` after defaults: `if c.Oracle.Kind == "none" { for i := range c.Pools { if c.Pools[i].SpendPolicy == "" || c.Pools[i].SpendPolicy == pool.SpendVerify && !explicitlySet { ... } } }` — simplest correct rule: in `applyPoolDefaults`, leave `SpendPolicy` empty; in `Parse`, after defaults, `for i := range c.Pools { if c.Pools[i].SpendPolicy == "" { if c.Oracle.Kind == "none" { c.Pools[i].SpendPolicy = pool.SpendTrust } else { c.Pools[i].SpendPolicy = pool.SpendVerify } } }`. Then the standalone validation loop only rejects an explicit `verify` with `oracle.kind: none` (the test sets it explicitly). Apply this and delete the `SpendPolicy` branch from `applyPoolDefaults`.

- [ ] **Step 6: Run, lint, commit**

Run: `cd /git/fuelkeeper && go get gopkg.in/yaml.v3 && go test -race ./pkg/config/ ./pkg/pool/ && golangci-lint run ./pkg/config/ ./pkg/pool/`
Expected: PASS.

```bash
git add go.mod go.sum pkg/config pkg/pool/config.go
git commit -m "feat(config): YAML configuration with defaults, env expansion and capacity checks"
```

---

### Task 2: API foundations — DTOs, error mapping, auth, health (`pkg/api`)

**Files:**
- Create: `pkg/api/dto.go`, `pkg/api/errors.go`, `pkg/api/auth.go`, `pkg/api/health.go`
- Test: `pkg/api/errors_test.go`, `pkg/api/auth_test.go`

- [ ] **Step 1: Write the failing tests**

`pkg/api/errors_test.go`:
```go
package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

func TestMapErr(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
		retry  bool
	}{
		{pool.ErrBadRequest, 400, "bad_request", false},
		{errors.Join(pool.ErrBadRequest, errors.New("x")), 400, "bad_request", false},
		{pool.ErrUnknownPool, 404, "unknown_pool", false},
		{store.ErrNotFound, 404, "not_found", false},
		{pool.ErrInsufficient, 409, "insufficient", true},
		{pool.ErrExhausted, 503, "exhausted", true},
		{pool.ErrBackpressure, 503, "backpressure", true},
		{pool.ErrUnavailable, 503, "unavailable", true},
		{context.DeadlineExceeded, 503, "unavailable", true},
		{errors.New("boom"), 500, "internal", false},
	}
	for _, tc := range cases {
		st, code, retry := mapErr(tc.err)
		assert.Equal(t, tc.status, st, tc.err)
		assert.Equal(t, tc.code, code, tc.err)
		assert.Equal(t, tc.retry, retry > 0, tc.err)
	}
	rec := httptest.NewRecorder()
	writeError(rec, pool.ErrExhausted)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))
	assert.JSONEq(t, `{"code":"exhausted","message":"pool: exhausted","retry_after":1}`, rec.Body.String())
}
```

`pkg/api/auth_test.go`:
```go
package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBearerAuth(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	h := requireAPIKey([]string{"secret-1", "secret-2"}, ok)

	req := httptest.NewRequest(http.MethodGet, "/v1/pools", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Body.String(), `"unauthorized"`)

	req.Header.Set("Authorization", "Bearer secret-2")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	req.Header.Set("Authorization", "Bearer nope")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	open := requireAPIKey(nil, ok)
	rec = httptest.NewRecorder()
	open.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/pools", nil))
	assert.Equal(t, http.StatusNoContent, rec.Code, "no keys configured → open")
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd /git/fuelkeeper && go test ./pkg/api/`
Expected: build FAIL.

- [ ] **Step 3: Write `pkg/api/errors.go`**

```go
// Package api serves FuelKeeper's HTTP/JSON API.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// errorBody is the JSON error envelope.
type errorBody struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	RetryAfter int    `json:"retry_after,omitempty"`
}

// mapErr maps service errors to (status, code, retryAfterSeconds).
func mapErr(err error) (int, string, int) {
	switch {
	case errors.Is(err, pool.ErrBadRequest):
		return http.StatusBadRequest, "bad_request", 0
	case errors.Is(err, pool.ErrUnknownPool):
		return http.StatusNotFound, "unknown_pool", 0
	case errors.Is(err, store.ErrNotFound):
		return http.StatusNotFound, "not_found", 0
	case errors.Is(err, pool.ErrInsufficient):
		return http.StatusConflict, "insufficient", 1
	case errors.Is(err, pool.ErrExhausted):
		return http.StatusServiceUnavailable, "exhausted", 1
	case errors.Is(err, pool.ErrBackpressure):
		return http.StatusServiceUnavailable, "backpressure", 1
	case errors.Is(err, pool.ErrUnavailable), errors.Is(err, store.ErrUnavailable),
		errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return http.StatusServiceUnavailable, "unavailable", 1
	case errors.Is(err, store.ErrInvalidArgument):
		return http.StatusBadRequest, "bad_request", 0
	default:
		return http.StatusInternalServerError, "internal", 0
	}
}

func writeError(w http.ResponseWriter, err error) {
	status, code, retry := mapErr(err)
	msg := err.Error()
	if status == http.StatusInternalServerError {
		msg = "internal error"
	}
	if retry > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retry))
	}
	writeJSON(w, status, errorBody{Code: code, Message: msg, RetryAfter: retry})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
```

- [ ] **Step 4: Write `pkg/api/auth.go`**

```go
package api

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// requireAPIKey enforces "Authorization: Bearer <key>" when keys are configured.
func requireAPIKey(keys []string, next http.Handler) http.Handler {
	if len(keys) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) {
			writeJSON(w, http.StatusUnauthorized, errorBody{Code: "unauthorized", Message: "missing bearer token"})
			return
		}
		got := []byte(strings.TrimPrefix(auth, prefix))
		for _, k := range keys {
			if subtle.ConstantTimeCompare(got, []byte(k)) == 1 {
				next.ServeHTTP(w, r)
				return
			}
		}
		writeJSON(w, http.StatusUnauthorized, errorBody{Code: "unauthorized", Message: "invalid api key"})
	})
}
```

- [ ] **Step 5: Write `pkg/api/dto.go`**

```go
package api

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

type reserveRequest struct {
	Count      int `json:"count"`
	MinCount   int `json:"min_count,omitempty"`
	TTLSeconds int `json:"ttl_seconds,omitempty"`
}

// UnlockDTO carries the unlocking data for one output.
type UnlockDTO struct {
	Mode   string `json:"mode"`   // "puzzle" | "p2pkh"
	Script string `json:"script"` // hex unlocking script
	Length int    `json:"length"` // bytes, for unlockingScriptLength
}

// OutputDTO is one reserved fuel output.
type OutputDTO struct {
	TxID          string    `json:"txid"`
	Vout          uint32    `json:"vout"`
	Outpoint      string    `json:"outpoint"`
	Satoshis      uint64    `json:"satoshis"`
	LockingScript string    `json:"locking_script"`
	Unlock        UnlockDTO `json:"unlock"`
}

// ShardDTO points at a shard's BEEF.
type ShardDTO struct {
	TxID    string `json:"txid"`
	Version int64  `json:"version"`
	BEEFURL string `json:"beef_url"`
}

// ConstraintsDTO lists what a consuming tx must satisfy (presigned P2PKH only).
type ConstraintsDTO struct {
	TxVersion uint32 `json:"tx_version"`
	LockTime  uint32 `json:"lock_time"`
	Sequence  uint32 `json:"sequence"`
}

// ReserveResponse is the reserve result.
type ReserveResponse struct {
	Token       string          `json:"token"`
	ExpiresAt   time.Time       `json:"expires_at"`
	TTLSeconds  int             `json:"ttl_seconds"`
	LockMode    string          `json:"lock_mode"`
	Outputs     []OutputDTO     `json:"outputs"`
	Shards      []ShardDTO      `json:"shards"`
	Constraints *ConstraintsDTO `json:"constraints,omitempty"`
}

type releaseRequest struct {
	Token     string   `json:"token"`
	Outpoints []string `json:"outpoints"`
}

type spendRequest struct {
	Token     string   `json:"token"`
	Outpoints []string `json:"outpoints"`
	TxID      string   `json:"txid"`
}

// CountResponse reports how many rows changed.
type CountResponse struct {
	Released int `json:"released,omitempty"`
	Accepted int `json:"accepted,omitempty"`
	Requeued int `json:"requeued,omitempty"`
	Retired  int `json:"retired,omitempty"`
}

// StatsDTO mirrors store.Stats.
type StatsDTO struct {
	Available   int64 `json:"available"`
	Reserved    int64 `json:"reserved"`
	Quarantined int64 `json:"quarantined"`
	Spent       int64 `json:"spent"`
	Invalid     int64 `json:"invalid"`
	Shards      struct {
		Unmined int64 `json:"unmined"`
		Mined   int64 `json:"mined"`
		Retired int64 `json:"retired"`
		Invalid int64 `json:"invalid"`
	} `json:"shards"`
}

// PoolDTO describes a pool and its counts.
type PoolDTO struct {
	Name              string          `json:"name"`
	Denomination      uint64          `json:"denomination"`
	LockMode          string          `json:"lock_mode"`
	SplitSize         int             `json:"split_size"`
	ServePolicy       string          `json:"serve_policy"`
	ReleasePolicy     string          `json:"release_policy"`
	SpendPolicy       string          `json:"spend_policy"`
	MaxCount          int             `json:"max_count"`
	DefaultTTLSeconds int             `json:"default_ttl_seconds"`
	MinTTLSeconds     int             `json:"min_ttl_seconds"`
	MaxTTLSeconds     int             `json:"max_ttl_seconds"`
	Constraints       *ConstraintsDTO `json:"constraints,omitempty"`
	Stats             StatsDTO        `json:"stats"`
}

// ShardInfoDTO is an admin view of a shard.
type ShardInfoDTO struct {
	TxID       string     `json:"txid"`
	Size       int        `json:"size"`
	Status     string     `json:"status"`
	Owner      string     `json:"owner,omitempty"`
	LeaseUntil *time.Time `json:"lease_until,omitempty"`
	Version    int64      `json:"version"`
	CreatedAt  time.Time  `json:"created_at"`
}

// RowDTO is an admin view of a row.
type RowDTO struct {
	Outpoint      string     `json:"outpoint"`
	State         string     `json:"state"`
	ReservedBy    string     `json:"reserved_by,omitempty"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
	QuarantinedAt *time.Time `json:"quarantined_at,omitempty"`
	SpentHint     string     `json:"spent_hint,omitempty"`
	SpentBy       string     `json:"spent_by,omitempty"`
}

type outpointsRequest struct {
	Outpoints []string `json:"outpoints"`
}

func p2pkhConstraints(mode store.LockMode) *ConstraintsDTO {
	if mode != store.LockP2PKH {
		return nil
	}
	return &ConstraintsDTO{TxVersion: 1, LockTime: 0, Sequence: 0xffffffff}
}

func toReserveResponse(poolName string, r *pool.Reservation) ReserveResponse {
	out := ReserveResponse{
		Token:       r.Token.String(),
		ExpiresAt:   r.ExpiresAt.UTC(),
		TTLSeconds:  int(r.TTL / time.Second),
		LockMode:    r.LockMode.String(),
		Outputs:     make([]OutputDTO, 0, len(r.Outputs)),
		Shards:      make([]ShardDTO, 0, len(r.Shards)),
		Constraints: p2pkhConstraints(r.LockMode),
	}
	for _, o := range r.Outputs {
		out.Outputs = append(out.Outputs, OutputDTO{
			TxID:          o.TxID.String(),
			Vout:          o.Vout,
			Outpoint:      o.Outpoint.String(),
			Satoshis:      o.Satoshis,
			LockingScript: hex.EncodeToString(o.LockingScript),
			Unlock:        UnlockDTO{Mode: r.LockMode.String(), Script: hex.EncodeToString(o.UnlockingScript), Length: len(o.UnlockingScript)},
		})
	}
	for _, s := range r.Shards {
		out.Shards = append(out.Shards, ShardDTO{TxID: s.TxID.String(), Version: s.Version, BEEFURL: fmt.Sprintf("/v1/pools/%s/beef/%s", poolName, s.TxID.String())})
	}
	return out
}

func toStatsDTO(s store.Stats) StatsDTO {
	var d StatsDTO
	d.Available, d.Reserved, d.Quarantined, d.Spent, d.Invalid = s.Available, s.Reserved, s.Quarantined, s.Spent, s.Invalid
	d.Shards.Unmined, d.Shards.Mined, d.Shards.Retired, d.Shards.Invalid = s.ShardsUnmined, s.ShardsMined, s.ShardsRetired, s.ShardsInvalid
	return d
}

func toPoolDTO(c pool.PoolConfig, s store.Stats) PoolDTO {
	return PoolDTO{
		Name: c.Name, Denomination: c.Denomination, LockMode: c.LockMode.String(), SplitSize: c.SplitSize,
		ServePolicy: string(c.ServePolicy), ReleasePolicy: string(c.ReleasePolicy), SpendPolicy: string(c.SpendPolicy),
		MaxCount: c.MaxCount, DefaultTTLSeconds: int(c.DefaultTTL / time.Second), MinTTLSeconds: int(c.MinTTL / time.Second), MaxTTLSeconds: int(c.MaxTTL / time.Second),
		Constraints: p2pkhConstraints(c.LockMode), Stats: toStatsDTO(s),
	}
}

func optTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

func hashStr(h *chainhash.Hash) string {
	if h == nil || *h == (chainhash.Hash{}) {
		return ""
	}
	return h.String()
}

func toShardInfo(s store.Shard) ShardInfoDTO {
	return ShardInfoDTO{TxID: s.TxID.String(), Size: s.Size, Status: s.Status.String(), Owner: s.Owner, LeaseUntil: optTime(s.LeaseUntil), Version: s.Version, CreatedAt: s.CreatedAt.UTC()}
}

func toRowDTO(r store.Row) RowDTO {
	d := RowDTO{Outpoint: r.Outpoint.String(), State: r.State.String(), ExpiresAt: optTime(r.ExpiresAt), QuarantinedAt: optTime(r.QuarantinedAt), SpentHint: hashStr(r.SpentHint), SpentBy: hashStr(r.SpentBy)}
	if !r.ReservedBy.IsZero() {
		d.ReservedBy = r.ReservedBy.String()
	}
	return d
}

// parseOutpoints decodes and validates a list of outpoint strings.
func parseOutpoints(in []string, maxCount int) ([]store.Outpoint, error) {
	if len(in) == 0 || len(in) > maxCount {
		return nil, fmt.Errorf("%w: 1..%d outpoints required", pool.ErrBadRequest, maxCount)
	}
	out := make([]store.Outpoint, len(in))
	for i, s := range in {
		op, err := store.ParseOutpoint(s)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", pool.ErrBadRequest, err)
		}
		out[i] = op
	}
	return out, nil
}

func parseToken(s string) (store.Token, error) {
	tok, err := store.ParseToken(s)
	if err != nil {
		return store.Token{}, fmt.Errorf("%w: %w", pool.ErrBadRequest, err)
	}
	return tok, nil
}

func parseTxID(s string) (chainhash.Hash, error) {
	h, err := chainhash.NewHashFromHex(s)
	if err != nil {
		return chainhash.Hash{}, fmt.Errorf("%w: txid: %w", pool.ErrBadRequest, err)
	}
	return *h, nil
}
```

- [ ] **Step 6: Write `pkg/api/health.go`**

```go
package api

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Pinger is what readiness checks (the store).
type Pinger interface {
	Ping(ctx context.Context) error
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func readyz(p Pinger, poolCount int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if poolCount == 0 {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "no pools configured"})
			return
		}
		if err := p.Ping(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "store unavailable", "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	}
}

func metricsHandler() http.Handler { return promhttp.Handler() }

func openapiHandler(doc []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(doc)
	}
}
```

- [ ] **Step 7: Run, lint, commit**

Run: `cd /git/fuelkeeper && go test -race ./pkg/api/ && golangci-lint run ./pkg/api/`
Expected: PASS (the `dto.go` helpers are exercised in Task 4; `unused` may flag some now — if so, add `var _ = toPoolDTO` style references in a `//go:build ignore`-free way is NOT acceptable; instead implement Task 3 before running lint on the package, or accept the temporary `unused` findings and clear them in Task 3).

```bash
git add pkg/api
git commit -m "feat(api): DTOs, error mapping, bearer auth and health endpoints"
```

### Task 3: Router and handlers (`pkg/api/server.go`, `pkg/api/handlers.go`)

Move the exported DTO structs from Task 2 into a dependency-free package `pkg/api/wire` so the client (Task 5) can import them without pulling server dependencies; `pkg/api/dto.go` keeps only the conversion helpers and unexported request types.

**Files:**
- Create: `pkg/api/wire/wire.go` (move `UnlockDTO, OutputDTO, ShardDTO, ConstraintsDTO, ReserveResponse, CountResponse, StatsDTO, PoolDTO, ShardInfoDTO, RowDTO` here, renamed without the `DTO` suffix: `Unlock, Output, Shard, Constraints, ReserveResponse, CountResponse, Stats, Pool, ShardInfo, Row`; also the request bodies `ReserveRequest{Count, MinCount, TTLSeconds}`, `ReleaseRequest{Token, Outpoints}`, `SpendRequest{Token, Outpoints, TxID}`, `OutpointsRequest{Outpoints}`, and `Error{Code, Message, RetryAfter}`)
- Modify: `pkg/api/dto.go` (use `wire.*` types), `pkg/api/errors.go` (use `wire.Error`)
- Create: `pkg/api/server.go`, `pkg/api/handlers.go`

- [ ] **Step 1: Create `pkg/api/wire/wire.go`** with the moved types (JSON tags unchanged) and this package comment:

```go
// Package wire holds the JSON request and response types of the FuelKeeper
// HTTP API. It has no dependencies beyond the standard library so clients can
// import it freely.
package wire
```
Update `dto.go` conversion helpers to return `wire.ReserveResponse`, `wire.Pool`, `wire.ShardInfo`, `wire.Row` and to decode `wire.ReleaseRequest` etc.; update `errors.go` to encode `wire.Error`. Run `go build ./pkg/api/...` until clean.

- [ ] **Step 2: Write `pkg/api/server.go`**

```go
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// PoolService is what the handlers need from pool.Pool.
type PoolService interface {
	Config() pool.PoolConfig
	Reserve(ctx context.Context, req pool.ReserveRequest) (*pool.Reservation, error)
	Release(ctx context.Context, token store.Token, ops []store.Outpoint) (int, error)
	Spend(ctx context.Context, token store.Token, ops []store.Outpoint, txid chainhash.Hash) (int, error)
	Stats(ctx context.Context) (store.Stats, error)
	BEEF(ctx context.Context, txid chainhash.Hash) ([]byte, int64, store.ShardStatus, error)
}

// Deps are the server's dependencies.
type Deps struct {
	Pools        []PoolService // ordered as configured
	Store        store.Store   // admin endpoints and readiness
	Log          *slog.Logger
	APIKeys      []string
	OpenAPI      []byte
	MaxBodyBytes int64
	Version      string
}

type server struct {
	d     Deps
	pools map[string]PoolService
}

// NewHandler builds the routed, middleware-wrapped handler.
func NewHandler(d Deps) http.Handler {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.MaxBodyBytes <= 0 {
		d.MaxBodyBytes = 1 << 20
	}
	s := &server{d: d, pools: map[string]PoolService{}}
	for _, p := range d.Pools {
		s.pools[p.Config().Name] = p
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)
	mux.Handle("GET /readyz", readyz(d.Store, len(d.Pools)))
	mux.Handle("GET /metrics", metricsHandler())
	mux.Handle("GET /openapi.yaml", openapiHandler(d.OpenAPI))

	v1 := http.NewServeMux()
	v1.HandleFunc("GET /v1/pools", s.listPools)
	v1.HandleFunc("GET /v1/pools/{pool}", s.getPool)
	v1.HandleFunc("POST /v1/pools/{pool}/reserve", s.reserve)
	v1.HandleFunc("POST /v1/pools/{pool}/release", s.release)
	v1.HandleFunc("POST /v1/pools/{pool}/spend", s.spend)
	v1.HandleFunc("GET /v1/pools/{pool}/beef/{txid}", s.beef)
	v1.HandleFunc("GET /v1/pools/{pool}/shards", s.listShards)
	v1.HandleFunc("POST /v1/pools/{pool}/shards/{txid}/retire", s.retireShard)
	v1.HandleFunc("GET /v1/pools/{pool}/quarantined", s.listQuarantined)
	v1.HandleFunc("POST /v1/pools/{pool}/quarantined/requeue", s.requeue)
	mux.Handle("/v1/", requireAPIKey(d.APIKeys, v1))

	return s.recoverer(s.logging(http.MaxBytesHandler(mux, d.MaxBodyBytes)))
}

// NewServer wraps the handler in an http.Server with the configured timeouts.
func NewServer(listen string, h http.Handler, read, write, idle time.Duration) *http.Server {
	return &http.Server{Addr: listen, Handler: h, ReadHeaderTimeout: read, ReadTimeout: read, WriteTimeout: write, IdleTimeout: idle}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		if r.URL.Path == "/metrics" || r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			return
		}
		s.d.Log.Debug("http", "method", r.Method, "path", r.URL.Path, "status", rec.status, "dur", time.Since(start).String())
	})
}

func (s *server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				s.d.Log.Error("panic in handler", "path", r.URL.Path, "panic", fmt.Sprint(v))
				writeError(w, errors.New("panic"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// poolFor resolves the {pool} path value.
func (s *server) poolFor(r *http.Request) (PoolService, error) {
	p, ok := s.pools[r.PathValue("pool")]
	if !ok {
		return nil, pool.ErrUnknownPool
	}
	return p, nil
}

// decode reads a JSON body into v; empty or malformed bodies are bad requests.
func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return fmt.Errorf("%w: body too large", pool.ErrBadRequest)
		}
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: empty body", pool.ErrBadRequest)
		}
		return fmt.Errorf("%w: invalid json: %w", pool.ErrBadRequest, err)
	}
	return nil
}
```
Add the missing imports `context` and `github.com/bsv-blockchain/go-sdk/chainhash`.

- [ ] **Step 3: Write `pkg/api/handlers.go`**

```go
package api

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

func (s *server) listPools(w http.ResponseWriter, r *http.Request) {
	out := make([]wire.Pool, 0, len(s.d.Pools))
	for _, p := range s.d.Pools {
		st, err := p.Stats(r.Context())
		if err != nil {
			writeError(w, err)
			return
		}
		out = append(out, toPool(p.Config(), st))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *server) getPool(w http.ResponseWriter, r *http.Request) {
	p, err := s.poolFor(r)
	if err != nil {
		writeError(w, err)
		return
	}
	st, err := p.Stats(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toPool(p.Config(), st))
}

func (s *server) reserve(w http.ResponseWriter, r *http.Request) {
	p, err := s.poolFor(r)
	if err != nil {
		writeError(w, err)
		return
	}
	var req wire.ReserveRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	if req.TTLSeconds < 0 || req.Count < 0 || req.MinCount < 0 {
		writeError(w, fmt.Errorf("%w: negative values", pool.ErrBadRequest))
		return
	}
	res, err := p.Reserve(r.Context(), pool.ReserveRequest{Count: req.Count, MinCount: req.MinCount, TTL: time.Duration(req.TTLSeconds) * time.Second})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toReserveResponse(p.Config().Name, res))
}

func (s *server) release(w http.ResponseWriter, r *http.Request) {
	p, err := s.poolFor(r)
	if err != nil {
		writeError(w, err)
		return
	}
	var req wire.ReleaseRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	tok, err := parseToken(req.Token)
	if err != nil {
		writeError(w, err)
		return
	}
	ops, err := parseOutpoints(req.Outpoints, p.Config().MaxCount)
	if err != nil {
		writeError(w, err)
		return
	}
	n, err := p.Release(r.Context(), tok, ops)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, wire.CountResponse{Released: n})
}

func (s *server) spend(w http.ResponseWriter, r *http.Request) {
	p, err := s.poolFor(r)
	if err != nil {
		writeError(w, err)
		return
	}
	var req wire.SpendRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	tok, err := parseToken(req.Token)
	if err != nil {
		writeError(w, err)
		return
	}
	ops, err := parseOutpoints(req.Outpoints, p.Config().MaxCount)
	if err != nil {
		writeError(w, err)
		return
	}
	txid, err := parseTxID(req.TxID)
	if err != nil {
		writeError(w, err)
		return
	}
	n, err := p.Spend(r.Context(), tok, ops, txid)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, wire.CountResponse{Accepted: n})
}

func (s *server) beef(w http.ResponseWriter, r *http.Request) {
	p, err := s.poolFor(r)
	if err != nil {
		writeError(w, err)
		return
	}
	txid, err := parseTxID(r.PathValue("txid"))
	if err != nil {
		writeError(w, err)
		return
	}
	beef, version, status, err := p.BEEF(r.Context(), txid)
	if err != nil {
		writeError(w, err)
		return
	}
	etag := `"v` + strconv.FormatInt(version, 10) + `"`
	w.Header().Set("ETag", etag)
	w.Header().Set("X-Shard-Status", status.String())
	if status == store.ShardMined {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "no-cache")
	}
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(beef)
}

// ---- admin ----

func limitParam(r *http.Request, def, maxV int) int {
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 {
		return min(v, maxV)
	}
	return def
}

func (s *server) listShards(w http.ResponseWriter, r *http.Request) {
	p, err := s.poolFor(r)
	if err != nil {
		writeError(w, err)
		return
	}
	f := store.ShardFilter{Limit: limitParam(r, 100, 1000)}
	switch r.URL.Query().Get("status") {
	case "":
	case "unmined":
		f.Statuses = []store.ShardStatus{store.ShardUnmined}
	case "mined":
		f.Statuses = []store.ShardStatus{store.ShardMined}
	case "retired":
		f.Statuses = []store.ShardStatus{store.ShardRetired}
	case "invalid":
		f.Statuses = []store.ShardStatus{store.ShardInvalid}
	default:
		writeError(w, fmt.Errorf("%w: unknown status filter", pool.ErrBadRequest))
		return
	}
	shards, err := s.d.Store.ListShards(r.Context(), p.Config().Name, f)
	if err != nil {
		writeError(w, err)
		return
	}
	out := make([]wire.ShardInfo, 0, len(shards))
	for _, sh := range shards {
		out = append(out, toShardInfo(sh))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *server) retireShard(w http.ResponseWriter, r *http.Request) {
	p, err := s.poolFor(r)
	if err != nil {
		writeError(w, err)
		return
	}
	txid, err := parseTxID(r.PathValue("txid"))
	if err != nil {
		writeError(w, err)
		return
	}
	n, err := s.d.Store.RetireShard(r.Context(), p.Config().Name, txid)
	if err != nil {
		writeError(w, err)
		return
	}
	s.d.Log.Warn("shard retired by operator", "pool", p.Config().Name, "txid", txid.String(), "rows", n)
	writeJSON(w, http.StatusOK, wire.CountResponse{Retired: n})
}

func (s *server) listQuarantined(w http.ResponseWriter, r *http.Request) {
	p, err := s.poolFor(r)
	if err != nil {
		writeError(w, err)
		return
	}
	rows, err := s.d.Store.ListQuarantined(r.Context(), p.Config().Name, time.Now().Add(24*time.Hour), limitParam(r, 100, 1000))
	if err != nil {
		writeError(w, err)
		return
	}
	out := make([]wire.Row, 0, len(rows))
	for _, row := range rows {
		out = append(out, toRow(row))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *server) requeue(w http.ResponseWriter, r *http.Request) {
	p, err := s.poolFor(r)
	if err != nil {
		writeError(w, err)
		return
	}
	var req wire.OutpointsRequest
	if err := decode(r, &req); err != nil {
		writeError(w, err)
		return
	}
	ops, err := parseOutpoints(req.Outpoints, 1000)
	if err != nil {
		writeError(w, err)
		return
	}
	n, err := s.d.Store.Resolve(r.Context(), p.Config().Name, ops, store.StateAvailable, nil)
	if err != nil {
		writeError(w, err)
		return
	}
	s.d.Log.Warn("quarantined rows requeued by operator", "pool", p.Config().Name, "rows", n)
	writeJSON(w, http.StatusOK, wire.CountResponse{Requeued: n})
}
```
Rename the Task 2 helpers to match: `toReserveResponse`, `toPool`, `toShardInfo`, `toRow`, `toStats`.

- [ ] **Step 4: Build, lint, commit** (tests come in Task 4)

Run: `cd /git/fuelkeeper && go build ./... && go vet ./pkg/api/... && golangci-lint run ./pkg/api/...`
Expected: clean.

```bash
git add pkg/api
git commit -m "feat(api): router, handlers and wire types"
```

### Task 4: End-to-end handler tests (`pkg/api/server_test.go`)

Real `pool.Pool` over the memory store behind `httptest.Server`; raw HTTP so the tests pin the wire format.

**Files:**
- Test: `pkg/api/server_test.go`

- [ ] **Step 1: Write the tests**

```go
package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api"
	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

type env struct {
	srv  *httptest.Server
	st   *memory.Store
	clk  *storetest.Clock
	keys []string
}

func newEnv(t *testing.T, keys []string) *env {
	t.Helper()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	pc.MinTTL = time.Second
	require.NoError(t, pc.Validate())
	ic := pool.DefaultInstanceConfig()
	ic.InstanceID = "api-test"
	ic.AdoptTimeout = 50 * time.Millisecond
	p := pool.NewPool(st, pc, ic, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	h := api.NewHandler(api.Deps{Pools: []api.PoolService{p}, Store: st, APIKeys: keys, OpenAPI: []byte("openapi: 3.0.3\n"), MaxBodyBytes: 4096})
	srv := httptest.NewServer(h)
	t.Cleanup(func() { srv.Close(); cancel(); <-done })
	return &env{srv: srv, st: st, clk: clk, keys: keys}
}

func (e *env) do(t *testing.T, method, path string, body any, into any) *http.Response {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, e.srv.URL+path, rd)
	require.NoError(t, err)
	if len(e.keys) > 0 {
		req.Header.Set("Authorization", "Bearer "+e.keys[0])
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })
	if into != nil && resp.StatusCode < 300 {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(into))
	}
	return resp
}

func TestReserveReleaseSpendFlow(t *testing.T) {
	e := newEnv(t, nil)
	sh, _ := storetest.MintShard(t, e.st, "main", 5, store.ShardMined)

	var res wire.ReserveResponse
	resp := e.do(t, http.MethodPost, "/v1/pools/main/reserve", wire.ReserveRequest{Count: 3, TTLSeconds: 60}, &res)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Len(t, res.Token, 22)
	assert.Equal(t, 60, res.TTLSeconds)
	assert.Equal(t, "puzzle", res.LockMode)
	assert.Nil(t, res.Constraints, "puzzle mode has no consumer constraints")
	require.Len(t, res.Outputs, 3)
	require.Len(t, res.Shards, 1)
	assert.Equal(t, sh.TxID.String(), res.Shards[0].TxID)
	assert.Equal(t, "/v1/pools/main/beef/"+sh.TxID.String(), res.Shards[0].BEEFURL)
	o := res.Outputs[0]
	assert.Equal(t, sh.TxID.String()+":"+fmt.Sprint(o.Vout), o.Outpoint)
	assert.Equal(t, uint64(50), o.Satoshis)
	assert.Equal(t, "puzzle", o.Unlock.Mode)
	assert.Equal(t, 2, o.Unlock.Length)
	assert.NotEmpty(t, o.LockingScript)

	ops := []string{res.Outputs[0].Outpoint, res.Outputs[1].Outpoint}
	var cnt wire.CountResponse
	resp = e.do(t, http.MethodPost, "/v1/pools/main/release", wire.ReleaseRequest{Token: res.Token, Outpoints: ops}, &cnt)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 2, cnt.Released)

	txid := storetest.NewTxID().String()
	cnt = wire.CountResponse{}
	resp = e.do(t, http.MethodPost, "/v1/pools/main/spend", wire.SpendRequest{Token: res.Token, Outpoints: []string{res.Outputs[2].Outpoint}, TxID: txid}, &cnt)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 1, cnt.Accepted)

	var pools []wire.Pool
	resp = e.do(t, http.MethodGet, "/v1/pools", nil, &pools)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, pools, 1)
	assert.Equal(t, int64(4), pools[0].Stats.Available)
	assert.Equal(t, int64(1), pools[0].Stats.Quarantined)
	assert.Equal(t, int64(1), pools[0].Stats.Shards.Mined)

	var one wire.Pool
	resp = e.do(t, http.MethodGet, "/v1/pools/main", nil, &one)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "main", one.Name)
	assert.Equal(t, 100, one.MaxCount)
}

func TestErrorsAndStatusCodes(t *testing.T) {
	e := newEnv(t, nil)
	var body wire.Error

	resp := e.do(t, http.MethodPost, "/v1/pools/nope/reserve", wire.ReserveRequest{Count: 1}, nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "unknown_pool", body.Code)

	resp = e.do(t, http.MethodPost, "/v1/pools/main/reserve", wire.ReserveRequest{Count: 1}, nil)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, "empty pool → exhausted")
	assert.Equal(t, "1", resp.Header.Get("Retry-After"))
	body = wire.Error{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "exhausted", body.Code)
	assert.Equal(t, 1, body.RetryAfter)

	storetest.MintShard(t, e.st, "main", 2, store.ShardMined)
	resp = e.do(t, http.MethodPost, "/v1/pools/main/reserve", wire.ReserveRequest{Count: 5, MinCount: 3}, nil)
	assert.Equal(t, http.StatusConflict, resp.StatusCode, "partial below min_count → insufficient")

	resp = e.do(t, http.MethodPost, "/v1/pools/main/reserve", wire.ReserveRequest{Count: 0}, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	req, _ := http.NewRequest(http.MethodPost, e.srv.URL+"/v1/pools/main/reserve", bytes.NewBufferString("{not json"))
	resp2, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp2.StatusCode)

	resp = e.do(t, http.MethodPost, "/v1/pools/main/release", wire.ReleaseRequest{Token: "bad", Outpoints: []string{"x"}}, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp = e.do(t, http.MethodPost, "/v1/pools/main/spend", wire.SpendRequest{Token: store.NewToken().String(), Outpoints: []string{storetest.NewTxID().String() + ":0"}, TxID: "zz"}, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	big := make([]string, 200)
	for i := range big {
		big[i] = storetest.NewTxID().String() + ":0"
	}
	resp = e.do(t, http.MethodPost, "/v1/pools/main/release", wire.ReleaseRequest{Token: store.NewToken().String(), Outpoints: big}, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "over max_count outpoints or over MaxBodyBytes")
}

func TestBEEFEndpointCaching(t *testing.T) {
	e := newEnv(t, nil)
	unmined, _ := storetest.MintShard(t, e.st, "main", 1, store.ShardUnmined)
	mined, _ := storetest.MintShard(t, e.st, "main", 1, store.ShardMined)

	resp := e.do(t, http.MethodGet, "/v1/pools/main/beef/"+mined.TxID.String(), nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
	assert.Equal(t, `"v1"`, resp.Header.Get("ETag"))
	assert.Equal(t, "mined", resp.Header.Get("X-Shard-Status"))
	assert.Contains(t, resp.Header.Get("Cache-Control"), "immutable")
	got, _ := io.ReadAll(resp.Body)
	assert.Equal(t, mined.BEEF, got)

	req, _ := http.NewRequest(http.MethodGet, e.srv.URL+"/v1/pools/main/beef/"+mined.TxID.String(), nil)
	req.Header.Set("If-None-Match", `"v1"`)
	resp2, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp2.Body.Close()
	assert.Equal(t, http.StatusNotModified, resp2.StatusCode)

	resp = e.do(t, http.MethodGet, "/v1/pools/main/beef/"+unmined.TxID.String(), nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))
	assert.Equal(t, "unmined", resp.Header.Get("X-Shard-Status"))

	resp = e.do(t, http.MethodGet, "/v1/pools/main/beef/"+storetest.NewTxID().String(), nil, nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestAuthHealthAndOpenAPI(t *testing.T) {
	e := newEnv(t, []string{"k1"})
	resp, err := http.Get(e.srv.URL + "/v1/pools")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	resp = e.do(t, http.MethodGet, "/v1/pools", nil, nil) // helper adds the key
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	for _, path := range []string{"/healthz", "/readyz", "/metrics", "/openapi.yaml"} {
		r, err := http.Get(e.srv.URL + path)
		require.NoError(t, err)
		r.Body.Close()
		assert.Equal(t, http.StatusOK, r.StatusCode, path)
	}
}

func TestAdminEndpoints(t *testing.T) {
	e := newEnv(t, nil)
	sh, rows := storetest.MintShard(t, e.st, "main", 3, store.ShardMined)
	tok := store.NewToken()
	storetest.MustReserve(t, e.st, "main", rows[:2], tok, e.clk.Now().Add(time.Second))
	_, err := e.st.Spend(context.Background(), "main", tok, storetest.Ops(rows[:1]), storetest.NewTxID(), false)
	require.NoError(t, err)

	var shards []wire.ShardInfo
	resp := e.do(t, http.MethodGet, "/v1/pools/main/shards?status=mined", nil, &shards)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, shards, 1)
	assert.Equal(t, sh.TxID.String(), shards[0].TxID)
	assert.Equal(t, "mined", shards[0].Status)

	var q []wire.Row
	resp = e.do(t, http.MethodGet, "/v1/pools/main/quarantined", nil, &q)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, q, 1)
	assert.Equal(t, rows[0].Outpoint.String(), q[0].Outpoint)
	assert.NotEmpty(t, q[0].SpentHint)

	var cnt wire.CountResponse
	resp = e.do(t, http.MethodPost, "/v1/pools/main/quarantined/requeue", wire.OutpointsRequest{Outpoints: []string{q[0].Outpoint}}, &cnt)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 1, cnt.Requeued)

	cnt = wire.CountResponse{}
	resp = e.do(t, http.MethodPost, "/v1/pools/main/shards/"+sh.TxID.String()+"/retire", nil, &cnt)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 3, cnt.Retired, "available + reserved + requeued rows all invalidated")

	resp = e.do(t, http.MethodGet, "/v1/pools/main/shards?status=bogus", nil, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
```

- [ ] **Step 2: Run, lint, commit**

Run: `cd /git/fuelkeeper && go test -race -count=2 ./pkg/api/... && golangci-lint run ./pkg/api/...`
Expected: PASS. (`TestErrorsAndStatusCodes`'s last case exceeds `MaxBodyBytes: 4096` before it exceeds `max_count`; both paths return 400.)

```bash
git add pkg/api/server_test.go
git commit -m "test(api): end-to-end handler tests over pool.Pool and memory store"
```

---

### Task 5: Go client core (`pkg/client`)

**Files:**
- Create: `pkg/client/client.go`, `pkg/client/errors.go`
- Test: `pkg/client/client_test.go`

- [ ] **Step 1: Write the failing tests**

`pkg/client/client_test.go`:
```go
package client_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http/httptest"
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

func startAPI(t *testing.T, keys []string) (*httptest.Server, *memory.Store, *storetest.Clock) {
	t.Helper()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	pc.MinTTL = time.Second
	require.NoError(t, pc.Validate())
	ic := pool.DefaultInstanceConfig()
	ic.InstanceID = "client-test"
	ic.AdoptTimeout = 50 * time.Millisecond
	p := pool.NewPool(st, pc, ic, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	srv := httptest.NewServer(api.NewHandler(api.Deps{Pools: []api.PoolService{p}, Store: st, APIKeys: keys}))
	t.Cleanup(func() { srv.Close(); cancel(); <-done })
	return srv, st, clk
}

func TestClientFlow(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := startAPI(t, []string{"k1"})
	sh, _ := storetest.MintShard(t, st, "main", 4, store.ShardMined)

	c, err := client.New(srv.URL, client.WithAPIKey("k1"))
	require.NoError(t, err)

	res, err := c.Reserve(ctx, "main", client.ReserveRequest{Count: 2, TTL: 30 * time.Second})
	require.NoError(t, err)
	require.Len(t, res.Outputs, 2)
	assert.Equal(t, 30, res.TTLSeconds)
	assert.Equal(t, sh.TxID.String(), res.Shards[0].TxID)

	n, err := c.Release(ctx, "main", res.Token, []string{res.Outputs[0].Outpoint})
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	n, err = c.Spend(ctx, "main", res.Token, []string{res.Outputs[1].Outpoint}, storetest.NewTxID().String())
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	pools, err := c.Pools(ctx)
	require.NoError(t, err)
	require.Len(t, pools, 1)
	assert.Equal(t, int64(3), pools[0].Stats.Available)
	one, err := c.Pool(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, uint64(50), one.Denomination)

	beef, err := c.BEEF(ctx, "main", res.Shards[0])
	require.NoError(t, err)
	assert.Equal(t, sh.BEEF, beef)
	beef2, err := c.BEEF(ctx, "main", res.Shards[0])
	require.NoError(t, err)
	assert.Equal(t, beef, beef2)
	assert.Equal(t, 1, c.BEEFCacheMisses(), "second call served from cache")

	shards, err := c.Shards(ctx, "main", "mined", 10)
	require.NoError(t, err)
	require.Len(t, shards, 1)
	q, err := c.Quarantined(ctx, "main", 10)
	require.NoError(t, err)
	require.Len(t, q, 1)
	rq, err := c.Requeue(ctx, "main", []string{q[0].Outpoint})
	require.NoError(t, err)
	assert.Equal(t, 1, rq)
	rt, err := c.RetireShard(ctx, "main", sh.TxID.String())
	require.NoError(t, err)
	assert.Equal(t, 4, rt)
}

func TestClientErrors(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := startAPI(t, []string{"k1"})

	unauth, err := client.New(srv.URL)
	require.NoError(t, err)
	_, err = unauth.Pools(ctx)
	require.ErrorIs(t, err, client.ErrUnauthorized)

	c, err := client.New(srv.URL, client.WithAPIKey("k1"))
	require.NoError(t, err)

	_, err = c.Reserve(ctx, "main", client.ReserveRequest{Count: 1})
	require.ErrorIs(t, err, client.ErrExhausted)
	var apiErr *client.APIError
	require.True(t, errors.As(err, &apiErr))
	assert.Equal(t, 503, apiErr.Status)
	assert.Equal(t, 1, apiErr.RetryAfter)
	assert.True(t, client.IsRetryable(err))

	storetest.MintShard(t, st, "main", 1, store.ShardMined)
	_, err = c.Reserve(ctx, "main", client.ReserveRequest{Count: 3, MinCount: 2})
	require.ErrorIs(t, err, client.ErrInsufficient)
	assert.True(t, client.IsRetryable(err))

	_, err = c.Reserve(ctx, "nope", client.ReserveRequest{Count: 1})
	require.ErrorIs(t, err, client.ErrNotFound)
	assert.False(t, client.IsRetryable(err))

	_, err = c.Reserve(ctx, "main", client.ReserveRequest{Count: 0})
	require.ErrorIs(t, err, client.ErrBadRequest)

	_, err = client.New("://bad")
	require.Error(t, err)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd /git/fuelkeeper && go test ./pkg/client/`
Expected: build FAIL.

- [ ] **Step 3: Write `pkg/client/errors.go`**

```go
// Package client is the Go client for the FuelKeeper HTTP API, with a BEEF
// cache and helpers that turn a reservation into createAction inputs.
package client

import (
	"errors"
	"fmt"
)

// Sentinel errors matched with errors.Is against *APIError.
var (
	ErrBadRequest   = errors.New("fuelkeeper: bad request")
	ErrUnauthorized = errors.New("fuelkeeper: unauthorized")
	ErrNotFound     = errors.New("fuelkeeper: not found")
	ErrInsufficient = errors.New("fuelkeeper: insufficient fuel")
	ErrExhausted    = errors.New("fuelkeeper: pool exhausted")
	ErrBackpressure = errors.New("fuelkeeper: backpressure")
	ErrUnavailable  = errors.New("fuelkeeper: unavailable")
)

// APIError is a non-2xx response.
type APIError struct {
	Status     int
	Code       string
	Message    string
	RetryAfter int // seconds; 0 = none
}

func (e *APIError) Error() string {
	return fmt.Sprintf("fuelkeeper: %d %s: %s", e.Status, e.Code, e.Message)
}

// Is maps stable API codes to sentinels.
func (e *APIError) Is(target error) bool {
	switch target {
	case ErrBadRequest:
		return e.Code == "bad_request"
	case ErrUnauthorized:
		return e.Code == "unauthorized"
	case ErrNotFound:
		return e.Code == "not_found" || e.Code == "unknown_pool"
	case ErrInsufficient:
		return e.Code == "insufficient"
	case ErrExhausted:
		return e.Code == "exhausted"
	case ErrBackpressure:
		return e.Code == "backpressure"
	case ErrUnavailable:
		return e.Code == "unavailable" || e.Code == "internal"
	}
	return false
}

// IsRetryable reports whether the caller should retry after RetryAfter seconds.
func IsRetryable(err error) bool {
	var ae *APIError
	if errors.As(err, &ae) {
		return ae.RetryAfter > 0 || ae.Status == 503 || ae.Status == 409
	}
	return false
}
```

- [ ] **Step 4: Write `pkg/client/client.go`**

```go
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
)

// ReserveRequest is the client-side reserve request.
type ReserveRequest struct {
	Count    int
	MinCount int
	TTL      time.Duration
}

// Option configures New.
type Option func(*Client)

// WithAPIKey sets the bearer token.
func WithAPIKey(key string) Option { return func(c *Client) { c.apiKey = key } }

// WithHTTPClient replaces the HTTP client (keep-alive is on by default).
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

type cachedBEEF struct {
	version int64
	bytes   []byte
}

// Client talks to one FuelKeeper base URL.
type Client struct {
	base   *url.URL
	http   *http.Client
	apiKey string

	mu     sync.Mutex
	beefs  map[string]cachedBEEF // key: pool + "/" + txid
	misses int
}

// New parses baseURL and applies options.
func New(baseURL string, opts ...Option) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("fuelkeeper: invalid base url %q", baseURL)
	}
	c := &Client{
		base:  u,
		http:  &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 64, IdleConnTimeout: 90 * time.Second}},
		beefs: map[string]cachedBEEF{},
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

func (c *Client) do(ctx context.Context, method, path string, body, into any, hdr http.Header) (*http.Response, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base.JoinPath(path).String(), rd)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotModified {
		defer resp.Body.Close()
		ae := &APIError{Status: resp.StatusCode}
		var we wire.Error
		if json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&we) == nil && we.Code != "" {
			ae.Code, ae.Message, ae.RetryAfter = we.Code, we.Message, we.RetryAfter
		} else {
			ae.Code, ae.Message = "http_"+strconv.Itoa(resp.StatusCode), http.StatusText(resp.StatusCode)
		}
		if ra, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && ae.RetryAfter == 0 {
			ae.RetryAfter = ra
		}
		return nil, ae
	}
	if into != nil {
		defer resp.Body.Close()
		if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
			return nil, fmt.Errorf("fuelkeeper: decode response: %w", err)
		}
	}
	return resp, nil
}

// Reserve reserves fuel from pool.
func (c *Client) Reserve(ctx context.Context, pool string, req ReserveRequest) (*wire.ReserveResponse, error) {
	var out wire.ReserveResponse
	_, err := c.do(ctx, http.MethodPost, "/v1/pools/"+pool+"/reserve", wire.ReserveRequest{Count: req.Count, MinCount: req.MinCount, TTLSeconds: int(req.TTL / time.Second)}, &out, nil)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// Release returns outpoints held under token.
func (c *Client) Release(ctx context.Context, pool, token string, outpoints []string) (int, error) {
	var out wire.CountResponse
	if _, err := c.do(ctx, http.MethodPost, "/v1/pools/"+pool+"/release", wire.ReleaseRequest{Token: token, Outpoints: outpoints}, &out, nil); err != nil {
		return 0, err
	}
	return out.Released, nil
}

// Spend reports that txid spends the outpoints held under token.
func (c *Client) Spend(ctx context.Context, pool, token string, outpoints []string, txid string) (int, error) {
	var out wire.CountResponse
	if _, err := c.do(ctx, http.MethodPost, "/v1/pools/"+pool+"/spend", wire.SpendRequest{Token: token, Outpoints: outpoints, TxID: txid}, &out, nil); err != nil {
		return 0, err
	}
	return out.Accepted, nil
}

// Pools lists pools with stats.
func (c *Client) Pools(ctx context.Context) ([]wire.Pool, error) {
	var out []wire.Pool
	if _, err := c.do(ctx, http.MethodGet, "/v1/pools", nil, &out, nil); err != nil {
		return nil, err
	}
	return out, nil
}

// Pool describes one pool.
func (c *Client) Pool(ctx context.Context, name string) (*wire.Pool, error) {
	var out wire.Pool
	if _, err := c.do(ctx, http.MethodGet, "/v1/pools/"+name, nil, &out, nil); err != nil {
		return nil, err
	}
	return &out, nil
}

// BEEF fetches a shard's BEEF, served from cache when the version matches.
func (c *Client) BEEF(ctx context.Context, pool string, shard wire.Shard) ([]byte, error) {
	key := pool + "/" + shard.TxID
	c.mu.Lock()
	if cb, ok := c.beefs[key]; ok && cb.version >= shard.Version {
		c.mu.Unlock()
		return cb.bytes, nil
	}
	c.misses++
	c.mu.Unlock()
	resp, err := c.do(ctx, http.MethodGet, "/v1/pools/"+pool+"/beef/"+shard.TxID, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("fuelkeeper: read beef: %w", err)
	}
	version := shard.Version
	if v, err := strconv.ParseInt(trimETag(resp.Header.Get("ETag")), 10, 64); err == nil {
		version = v
	}
	c.mu.Lock()
	c.beefs[key] = cachedBEEF{version: version, bytes: b}
	c.mu.Unlock()
	return b, nil
}

func trimETag(s string) string {
	s = strings.Trim(s, `"`)
	return strings.TrimPrefix(s, "v")
}

// BEEFCacheMisses returns how many BEEF fetches hit the network.
func (c *Client) BEEFCacheMisses() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.misses
}

// ---- admin ----

// Shards lists shards (status "" = all).
func (c *Client) Shards(ctx context.Context, pool, status string, limit int) ([]wire.ShardInfo, error) {
	var out []wire.ShardInfo
	q := url.Values{}
	if status != "" {
		q.Set("status", status)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if _, err := c.do(ctx, http.MethodGet, "/v1/pools/"+pool+"/shards?"+q.Encode(), nil, &out, nil); err != nil {
		return nil, err
	}
	return out, nil
}

// Quarantined lists quarantined rows.
func (c *Client) Quarantined(ctx context.Context, pool string, limit int) ([]wire.Row, error) {
	var out []wire.Row
	if _, err := c.do(ctx, http.MethodGet, "/v1/pools/"+pool+"/quarantined?limit="+strconv.Itoa(limit), nil, &out, nil); err != nil {
		return nil, err
	}
	return out, nil
}

// Requeue moves quarantined rows back to available.
func (c *Client) Requeue(ctx context.Context, pool string, outpoints []string) (int, error) {
	var out wire.CountResponse
	if _, err := c.do(ctx, http.MethodPost, "/v1/pools/"+pool+"/quarantined/requeue", wire.OutpointsRequest{Outpoints: outpoints}, &out, nil); err != nil {
		return 0, err
	}
	return out.Requeued, nil
}

// RetireShard invalidates a shard and its live rows.
func (c *Client) RetireShard(ctx context.Context, pool, txid string) (int, error) {
	var out wire.CountResponse
	if _, err := c.do(ctx, http.MethodPost, "/v1/pools/"+pool+"/shards/"+txid+"/retire", nil, &out, nil); err != nil {
		return 0, err
	}
	return out.Retired, nil
}
```
Add `"strings"` to the imports. `url.JoinPath` on a path that already contains `?q=` would escape the query: for `Shards`, build the URL as `c.base.JoinPath("/v1/pools/"+pool+"/shards").String() + "?" + q.Encode()` inside `do` by splitting on `?` — simplest is to give `do` a `rawQuery string` parameter; do that and pass `q.Encode()` from `Shards` and `"limit=…"` from `Quarantined`.

- [ ] **Step 5: Run, lint, commit**

Run: `cd /git/fuelkeeper && go test -race ./pkg/client/ && golangci-lint run ./pkg/client/`
Expected: PASS.

```bash
git add pkg/client
git commit -m "feat(client): Go client with typed errors, BEEF cache and admin calls"
```

### Task 6: Client helpers for wallets — merged input BEEF and createAction inputs (`pkg/client/beef.go`)

**Files:**
- Create: `pkg/client/beef.go`
- Test: `pkg/client/beef_test.go`

- [ ] **Step 1: Write the failing test**

`pkg/client/beef_test.go`:
```go
package client_test

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// realShard builds a shard whose BEEF is a genuine BEEF v2 of a synthetic split tx.
func realShard(t *testing.T, st store.Store, n int) (store.Shard, []store.Row) {
	t.Helper()
	tx := transaction.NewTransaction()
	seed := fuel.NewSeed()
	rows := make([]store.Row, n)
	for i := 0; i < n; i++ {
		pre := fuel.Preimage(seed, uint32(i))
		lock := fuel.PuzzleLock(fuel.PuzzleHash(pre))
		s := script.Script(lock)
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: 50, LockingScript: &s})
		rows[i] = store.Row{Pool: "main", Satoshis: 50, LockingScript: lock, UnlockingScript: fuel.PuzzleUnlock(pre)}
	}
	txid := *tx.TxID()
	for i := range rows {
		rows[i].Outpoint = store.Outpoint{TxID: txid, Vout: uint32(i)}
	}
	beef := transaction.NewBeefV2()
	_, err := beef.MergeRawTx(tx.Bytes(), nil)
	require.NoError(t, err)
	bytes, err := beef.Bytes()
	require.NoError(t, err)
	sh := store.Shard{Pool: "main", TxID: txid, Size: n, Denomination: 50, LockMode: store.LockPuzzle, Status: store.ShardMined, BEEF: bytes}
	require.NoError(t, st.PutShard(context.Background(), sh, rows))
	return sh, rows
}

func TestInputBEEFAndCreateActionInputs(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := startAPI(t, nil)
	a, _ := realShard(t, st, 2)
	b, _ := realShard(t, st, 2)
	c, err := client.New(srv.URL)
	require.NoError(t, err)

	res, err := c.Reserve(ctx, "main", client.ReserveRequest{Count: 4})
	require.NoError(t, err)
	require.Len(t, res.Shards, 2)

	beef, err := c.InputBEEF(ctx, "main", res)
	require.NoError(t, err)
	parsed, err := transaction.NewBeefFromBytes(beef)
	require.NoError(t, err)
	assert.NotNil(t, parsed.FindTransaction(a.TxID.String()))
	assert.NotNil(t, parsed.FindTransaction(b.TxID.String()))

	inputs, err := client.CreateActionInputs(res, "fuel")
	require.NoError(t, err)
	require.Len(t, inputs, 4)
	for i, in := range inputs {
		assert.Equal(t, res.Outputs[i].Outpoint, in.Outpoint.Txid.String()+":"+itoa(in.Outpoint.Index))
		assert.Equal(t, "fuel", in.InputDescription)
		want, _ := hex.DecodeString(res.Outputs[i].Unlock.Script)
		assert.Equal(t, want, in.UnlockingScript)
		assert.Equal(t, uint32(len(want)), in.UnlockingScriptLength)
		assert.Nil(t, in.SequenceNumber, "puzzle mode leaves the sequence at the wallet default")
	}

	// P2PKH-mode constraints pin the sequence number.
	p2 := *res
	p2.Constraints = &wire.Constraints{TxVersion: 1, LockTime: 0, Sequence: 0xffffffff}
	inputs, err = client.CreateActionInputs(&p2, "fuel")
	require.NoError(t, err)
	require.NotNil(t, inputs[0].SequenceNumber)
	assert.Equal(t, uint32(0xffffffff), *inputs[0].SequenceNumber)

	bad := *res
	bad.Outputs = []wire.Output{{Outpoint: "nope", Unlock: wire.Unlock{Script: "00"}}}
	_, err = client.CreateActionInputs(&bad, "fuel")
	require.Error(t, err)

	assert.Equal(t, uint64(50-8), client.Surplus(50, 80, 100), "fee for 80 B at 100 sat/kB is 8 sat")
	assert.Equal(t, uint64(0), client.Surplus(5, 80, 100), "never negative")
	_ = storetest.Start
}

func itoa(v uint32) string { return fmt.Sprint(v) }
```
Add `"fmt"` to the imports.

- [ ] **Step 2: Run to verify failure**

Run: `cd /git/fuelkeeper && go test ./pkg/client/ -run TestInputBEEF`
Expected: FAIL, undefined `c.InputBEEF`.

- [ ] **Step 3: Write `pkg/client/beef.go`**

```go
package client

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
)

// InputBEEF fetches (with caching) and merges the BEEF of every shard referenced
// by a reservation. Pass the result as createAction's inputBEEF.
func (c *Client) InputBEEF(ctx context.Context, pool string, res *wire.ReserveResponse) ([]byte, error) {
	merged := transaction.NewBeefV2()
	for _, sh := range res.Shards {
		b, err := c.BEEF(ctx, pool, sh)
		if err != nil {
			return nil, err
		}
		if err := merged.MergeBeefBytes(b); err != nil {
			return nil, fmt.Errorf("fuelkeeper: merge beef for %s: %w", sh.TxID, err)
		}
	}
	out, err := merged.Bytes()
	if err != nil {
		return nil, fmt.Errorf("fuelkeeper: serialise beef: %w", err)
	}
	return out, nil
}

// ParseOutpoint parses "<txid>:<vout>" into a go-sdk outpoint.
func ParseOutpoint(s string) (transaction.Outpoint, error) {
	i := strings.LastIndexAny(s, ":.")
	if i <= 0 || i == len(s)-1 {
		return transaction.Outpoint{}, fmt.Errorf("fuelkeeper: bad outpoint %q", s)
	}
	h, err := chainhash.NewHashFromHex(s[:i])
	if err != nil {
		return transaction.Outpoint{}, fmt.Errorf("fuelkeeper: bad outpoint txid: %w", err)
	}
	v, err := strconv.ParseUint(s[i+1:], 10, 32)
	if err != nil {
		return transaction.Outpoint{}, fmt.Errorf("fuelkeeper: bad outpoint vout: %w", err)
	}
	return transaction.Outpoint{Txid: *h, Index: uint32(v)}, nil
}

// CreateActionInputs turns reserved outputs into go-sdk createAction inputs.
// Each input carries the full unlocking script (no signing needed). In presigned
// P2PKH mode the reservation's constraints pin the sequence number; the caller
// must also keep the transaction at version 1 and lockTime 0.
func CreateActionInputs(res *wire.ReserveResponse, description string) ([]wallet.CreateActionInput, error) {
	out := make([]wallet.CreateActionInput, 0, len(res.Outputs))
	for _, o := range res.Outputs {
		op, err := ParseOutpoint(o.Outpoint)
		if err != nil {
			return nil, err
		}
		unlock, err := hex.DecodeString(o.Unlock.Script)
		if err != nil || len(unlock) == 0 {
			return nil, fmt.Errorf("fuelkeeper: bad unlocking script for %s", o.Outpoint)
		}
		in := wallet.CreateActionInput{
			Outpoint:              op,
			InputDescription:      description,
			UnlockingScript:       unlock,
			UnlockingScriptLength: uint32(len(unlock)), //nolint:gosec // script is small
		}
		if res.Constraints != nil {
			seq := res.Constraints.Sequence
			in.SequenceNumber = &seq
		}
		out = append(out, in)
	}
	return out, nil
}

// Surplus is denomination − fee(estimatedTxBytes, satPerKB), floored at zero:
// the amount a wallet should add to one of its own outputs so the toolbox does
// not create a change output for it.
func Surplus(denomination uint64, estimatedTxBytes int, satPerKB uint64) uint64 {
	fee := fuel.FeeSats(estimatedTxBytes, satPerKB)
	if fee >= denomination {
		return 0
	}
	return denomination - fee
}
```

- [ ] **Step 4: Run, lint, commit**

Run: `cd /git/fuelkeeper && go test -race ./pkg/client/ && golangci-lint run ./pkg/client/`
Expected: PASS.

```bash
git add pkg/client/beef.go pkg/client/beef_test.go
git commit -m "feat(client): merged input BEEF and createAction input helpers"
```

---

### Task 7: `fuelkeeper serve` and admin subcommands (`cmd/fuelkeeper`)

Wires config → store → pools → jobs (with the mock treasury and mock/none oracle; toolbox treasury and chain oracles arrive in Plan 5) → API, with graceful shutdown. Adds the two small mock hooks that make "nullnet" mode useful: the mock treasury reports sends so the mock oracle can mark splits mined after a delay, and the mock oracle can assume unknown outpoints are unspent.

**Files:**
- Modify: `pkg/treasury/mock/mock.go` (`WithOnSend` option), `pkg/oracle/mock.go` (`SetAssumeUnspent`), `pkg/config/config.go` (`treasury.mock.confirm_after`, `oracle.mock.assume_unspent`)
- Create: `cmd/fuelkeeper/main.go`, `cmd/fuelkeeper/serve.go`, `cmd/fuelkeeper/admin.go`
- Test: `cmd/fuelkeeper/serve_test.go`

- [ ] **Step 1: Mock hooks**

`pkg/treasury/mock/mock.go`: add
```go
// Option configures New.
type Option func(*Treasury)

// WithOnSend registers a callback invoked after every successful Send.
func WithOnSend(fn func(txid chainhash.Hash)) Option { return func(t *Treasury) { t.onSend = fn } }
```
field `onSend func(chainhash.Hash)`, `New(opts ...Option)` applying them, and in `Send` after `t.sent[txid]++`: capture `fn := t.onSend`, unlock before calling, then `if fn != nil { fn(txid) }` (call outside the mutex: restructure `Send` so the callback runs after `t.mu.Unlock()`).

`pkg/oracle/mock.go`: add
```go
// SetAssumeUnspent makes Outpoints answer Unspent instead of Unknown for
// outpoints with no verdict. Development/benchmark mode only.
func (m *Mock) SetAssumeUnspent(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.assumeUnspent = v
}
```
and in `Outpoints`, when no verdict exists: `Status: StatusUnknown` becomes `StatusUnspent` if `m.assumeUnspent`.

`pkg/config/config.go`: extend
```go
type OracleConfig struct {
	Kind string `yaml:"kind"`
	Mock struct {
		AssumeUnspent bool `yaml:"assume_unspent"`
	} `yaml:"mock"`
	Teranode struct{ URL string `yaml:"url"` } `yaml:"teranode"`
	Arcade   struct{ URL string `yaml:"url"` } `yaml:"arcade"`
}
type TreasuryConfig struct {
	Kind string `yaml:"kind"`
	Mock struct {
		ConfirmAfter time.Duration `yaml:"confirm_after"` // mark sent splits mined after this delay
	} `yaml:"mock"`
}
```
Defaults: `Oracle.Mock.AssumeUnspent = true`, `Treasury.Mock.ConfirmAfter = 2 * time.Second`.

- [ ] **Step 2: Write the smoke test `cmd/fuelkeeper/serve_test.go`**

```go
package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
)

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

	// Wait for the minter+watcher pipeline to produce mined fuel.
	var pools []client.PoolInfo
	require.Eventually(t, func() bool {
		ps, err := c.Pools(ctx)
		if err != nil || len(ps) != 1 {
			return false
		}
		pools = nil
		for _, p := range ps {
			pools = append(pools, client.PoolInfo(p))
		}
		return ps[0].Stats.Shards.Mined >= 1
	}, 15*time.Second, 100*time.Millisecond)

	res, err := c.Reserve(ctx, "main", client.ReserveRequest{Count: 3})
	require.NoError(t, err)
	assert.Len(t, res.Outputs, 3)
	n, err := c.Spend(ctx, "main", res.Token, []string{res.Outputs[0].Outpoint}, "aa"+res.Outputs[0].Outpoint[2:64])
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	n, err = c.Release(ctx, "main", res.Token, []string{res.Outputs[1].Outpoint, res.Outputs[2].Outpoint})
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	cancel()
	select {
	case err := <-errc:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not stop")
	}
}
```
Replace the `client.PoolInfo` indirection with a plain `_ = ps` (there is no such type); the loop only needs `ps[0].Stats.Shards.Mined`. The spend txid must be 64 hex chars: build it as `strings.Repeat("ab", 32)` instead of the slice expression.

- [ ] **Step 3: Write `cmd/fuelkeeper/main.go`**

```go
// Command fuelkeeper runs the FuelKeeper service and its admin tools.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var err error
	switch os.Args[1] {
	case "serve":
		err = serveCmd(ctx, os.Args[2:])
	case "pools", "shards", "quarantine", "release":
		err = adminCmd(ctx, os.Args[1], os.Args[2:])
	case "version":
		fmt.Println(version)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  fuelkeeper serve --config fuelkeeper.yaml
  fuelkeeper pools      --url http://host:8080 [--key K]
  fuelkeeper shards     --url URL --pool P [--status S] [--limit N]
  fuelkeeper quarantine --url URL --pool P list|requeue [--outpoints a:0,b:1]|retire --txid T
  fuelkeeper release    --url URL --pool P --token T --outpoints a:0,b:1
  fuelkeeper version`)
}
```

- [ ] **Step 4: Write `cmd/fuelkeeper/serve.go`**

```go
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/openapi"
	"github.com/bsv-blockchain/fuelkeeper/pkg/api"
	"github.com/bsv-blockchain/fuelkeeper/pkg/config"
	"github.com/bsv-blockchain/fuelkeeper/pkg/jobs"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/factory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
	tmock "github.com/bsv-blockchain/fuelkeeper/pkg/treasury/mock"
)

func serveCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	cfgPath := fs.String("config", "fuelkeeper.yaml", "path to the YAML config")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return runServe(ctx, *cfgPath, nil)
}

func newLogger(c config.LogConfig) *slog.Logger {
	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(c.Level))
	opts := &slog.HandlerOptions{Level: lvl}
	if c.Format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}

// buildOracle returns the configured oracle. teranode/arcade/composite arrive in Plan 5.
func buildOracle(c config.OracleConfig) (oracle.Oracle, *oracle.Mock, error) {
	switch c.Kind {
	case "none":
		return oracle.None{}, nil, nil
	case "mock":
		m := oracle.NewMock()
		m.SetAssumeUnspent(c.Mock.AssumeUnspent)
		return m, m, nil
	default:
		return nil, nil, fmt.Errorf("oracle.kind %q is not available in this build", c.Kind)
	}
}

// buildTreasury returns the configured treasury. toolbox arrives in Plan 5.
func buildTreasury(c config.TreasuryConfig, mockOracle *oracle.Mock) (treasury.Treasury, error) {
	switch c.Kind {
	case "mock":
		var opts []tmock.Option
		if mockOracle != nil {
			delay := c.Mock.ConfirmAfter
			opts = append(opts, tmock.WithOnSend(func(txid chainhash.Hash) {
				time.AfterFunc(delay, func() {
					mockOracle.SetTx(txid, oracle.TxStatus{Known: true, Mined: true, BlockHeight: 1, MerklePath: []byte{0}})
				})
			}))
		}
		return tmock.New(opts...), nil
	default:
		return nil, fmt.Errorf("treasury.kind %q is not available in this build", c.Kind)
	}
}

// runServe runs the service until ctx is done. onListen (optional) receives the bound address.
func runServe(ctx context.Context, cfgPath string, onListen func(string)) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	log := newLogger(cfg.Log)
	clock := pool.RealClock{}

	st, err := factory.New(ctx, cfg.Store, clock.Now)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer st.Close()

	orc, mockOrc, err := buildOracle(cfg.Oracle)
	if err != nil {
		return err
	}
	tr, err := buildTreasury(cfg.Treasury, mockOrc)
	if err != nil {
		return err
	}

	pools := make([]*pool.Pool, 0, len(cfg.Pools))
	svcs := make([]api.PoolService, 0, len(cfg.Pools))
	names := make([]string, 0, len(cfg.Pools))
	for _, pc := range cfg.Pools {
		p := pool.NewPool(st, pc, cfg.Instance, clock, log)
		pools = append(pools, p)
		svcs = append(svcs, p)
		names = append(names, pc.Name)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for _, p := range pools {
		wg.Add(1)
		go func(p *pool.Pool) {
			defer wg.Done()
			if err := p.Run(runCtx); err != nil {
				log.Error("pool stopped", "pool", p.Config().Name, "err", err)
			}
		}(p)
	}

	if cfg.Jobs.Enabled {
		runner := jobs.NewRunner(st, pool.NewHolder(cfg.Instance.InstanceID), cfg.Jobs.LeaseTTL, log)
		runner.Add(jobs.NewReaper(st, orc, names, cfg.Jobs.Reaper, clock, log))
		runner.Add(jobs.NewWatcher(st, orc, tr, names, cfg.Jobs.Watcher, clock, log))
		runner.Add(jobs.NewMinter(st, tr, cfg.Pools, cfg.Jobs.Minter, clock, log))
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runner.Run(runCtx); err != nil {
				log.Error("jobs stopped", "err", err)
			}
		}()
	}

	h := api.NewHandler(api.Deps{Pools: svcs, Store: st, Log: log, APIKeys: cfg.API.APIKeys, OpenAPI: openapi.Spec, MaxBodyBytes: cfg.API.MaxBodyBytes, Version: version})
	srv := api.NewServer(cfg.API.Listen, h, cfg.API.ReadTimeout, cfg.API.WriteTimeout, cfg.API.IdleTimeout)
	ln, err := net.Listen("tcp", cfg.API.Listen)
	if err != nil {
		cancel()
		wg.Wait()
		return fmt.Errorf("listen %s: %w", cfg.API.Listen, err)
	}
	log.Info("fuelkeeper listening", "addr", ln.Addr().String(), "version", version, "pools", names, "store", cfg.Store.Backend)
	if onListen != nil {
		onListen(ln.Addr().String())
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		cancel()
		wg.Wait()
		return err
	}
	shutdownCtx, stopShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopShutdown()
	_ = srv.Shutdown(shutdownCtx) // stop accepting first
	cancel()                      // then release shards and job leases
	wg.Wait()
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
```
`openapi.Spec` is an embedded byte slice created in Task 8 (`openapi/embed.go`); until then create that file with `//go:embed fuelkeeper.yaml` and a placeholder YAML `openapi: 3.0.3` so this task compiles — Task 8 replaces the YAML.

- [ ] **Step 5: Write `cmd/fuelkeeper/admin.go`**

```go
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
)

func adminCmd(ctx context.Context, name string, args []string) error {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	url := fs.String("url", "http://127.0.0.1:8080", "FuelKeeper base URL")
	key := fs.String("key", os.Getenv("FUELKEEPER_API_KEY"), "API key (or FUELKEEPER_API_KEY)")
	poolName := fs.String("pool", "main", "pool name")
	status := fs.String("status", "", "shard status filter")
	limit := fs.Int("limit", 100, "page size")
	token := fs.String("token", "", "reservation token")
	outpoints := fs.String("outpoints", "", "comma-separated outpoints txid:vout")
	txid := fs.String("txid", "", "shard txid")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, err := client.New(*url, client.WithAPIKey(*key))
	if err != nil {
		return err
	}
	out := json.NewEncoder(os.Stdout)
	out.SetIndent("", "  ")
	split := func() []string {
		if *outpoints == "" {
			return nil
		}
		return strings.Split(*outpoints, ",")
	}
	switch name {
	case "pools":
		v, err := c.Pools(ctx)
		if err != nil {
			return err
		}
		return out.Encode(v)
	case "shards":
		v, err := c.Shards(ctx, *poolName, *status, *limit)
		if err != nil {
			return err
		}
		return out.Encode(v)
	case "release":
		n, err := c.Release(ctx, *poolName, *token, split())
		if err != nil {
			return err
		}
		return out.Encode(map[string]int{"released": n})
	case "quarantine":
		sub := "list"
		if rest := fs.Args(); len(rest) > 0 {
			sub = rest[0]
		}
		switch sub {
		case "list":
			v, err := c.Quarantined(ctx, *poolName, *limit)
			if err != nil {
				return err
			}
			return out.Encode(v)
		case "requeue":
			n, err := c.Requeue(ctx, *poolName, split())
			if err != nil {
				return err
			}
			return out.Encode(map[string]int{"requeued": n})
		case "retire":
			n, err := c.RetireShard(ctx, *poolName, *txid)
			if err != nil {
				return err
			}
			return out.Encode(map[string]int{"retired": n})
		default:
			return fmt.Errorf("unknown quarantine subcommand %q", sub)
		}
	}
	return fmt.Errorf("unknown command %q", name)
}
```
Note: `flag` stops at the first non-flag argument, so users write `fuelkeeper quarantine --url U --pool P requeue --outpoints ...` with the subcommand after the flags, or the subcommand first followed by flags. Support both by parsing: if `args[0]` does not start with `-`, treat it as the subcommand and parse the rest. Implement that at the top of the `quarantine` branch (extract before `fs.Parse`).

- [ ] **Step 6: Build, run the smoke test, lint, commit**

Run: `cd /git/fuelkeeper && go build ./... && go test -race -count=1 ./cmd/... ./pkg/... && golangci-lint run`
Expected: PASS, including `TestServeSmokeNullnet` (typically completes in 2–4 s).

Also try it by hand:
```bash
cat > /tmp/fk.yaml <<'EOF'
store: {backend: memory}
pools: [{name: main, denomination: 50, split_size: 100, target_available: 1000, low_water: 500, spend_policy: trust}]
oracle: {kind: mock}
treasury: {kind: mock}
api: {listen: "127.0.0.1:8080"}
EOF
go run ./cmd/fuelkeeper serve --config /tmp/fk.yaml &
sleep 3 && curl -s -XPOST localhost:8080/v1/pools/main/reserve -d '{"count":2}' | head -c 400; echo
go run ./cmd/fuelkeeper pools --url http://127.0.0.1:8080
kill %1
```
Expected: a reservation JSON with two outputs, then pool stats.

```bash
git add cmd/fuelkeeper openapi pkg/config pkg/oracle pkg/treasury
git commit -m "feat(cmd): fuelkeeper serve with nullnet mocks, graceful shutdown and admin subcommands"
```

### Task 8: OpenAPI document (`openapi/fuelkeeper.yaml`, `openapi/embed.go`)

Hand-written OpenAPI 3.0.3, served at `GET /openapi.yaml`. A test asserts every route registered in `pkg/api/server.go` appears in the document.

**Files:**
- Create: `openapi/embed.go`, `openapi/fuelkeeper.yaml`
- Test: `openapi/openapi_test.go`

- [ ] **Step 1: Write `openapi/embed.go`**

```go
// Package openapi embeds the FuelKeeper OpenAPI document.
package openapi

import _ "embed"

// Spec is the OpenAPI 3.0.3 document, served at GET /openapi.yaml.
//
//go:embed fuelkeeper.yaml
var Spec []byte
```

- [ ] **Step 2: Write the test `openapi/openapi_test.go`**

```go
package openapi_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/bsv-blockchain/fuelkeeper/openapi"
)

func TestSpecCoversRoutes(t *testing.T) {
	var doc struct {
		OpenAPI string                    `yaml:"openapi"`
		Paths   map[string]map[string]any `yaml:"paths"`
	}
	require.NoError(t, yaml.Unmarshal(openapi.Spec, &doc))
	require.Equal(t, "3.0.3", doc.OpenAPI)
	want := map[string][]string{
		"/healthz":                                 {"get"},
		"/readyz":                                  {"get"},
		"/metrics":                                 {"get"},
		"/openapi.yaml":                            {"get"},
		"/v1/pools":                                {"get"},
		"/v1/pools/{pool}":                         {"get"},
		"/v1/pools/{pool}/reserve":                 {"post"},
		"/v1/pools/{pool}/release":                 {"post"},
		"/v1/pools/{pool}/spend":                   {"post"},
		"/v1/pools/{pool}/beef/{txid}":             {"get"},
		"/v1/pools/{pool}/shards":                  {"get"},
		"/v1/pools/{pool}/shards/{txid}/retire":    {"post"},
		"/v1/pools/{pool}/quarantined":             {"get"},
		"/v1/pools/{pool}/quarantined/requeue":     {"post"},
	}
	for path, methods := range want {
		ops, ok := doc.Paths[path]
		require.True(t, ok, "missing path %s", path)
		for _, m := range methods {
			require.Contains(t, ops, m, "%s %s", m, path)
		}
	}
}
```

- [ ] **Step 3: Write `openapi/fuelkeeper.yaml`**

```yaml
openapi: 3.0.3
info:
  title: FuelKeeper API
  version: "1.0"
  description: |
    FuelKeeper keeps a pool of small, fixed-denomination BSV outputs ("fuel") that
    wallets reserve on demand and attach as one extra input to pay transaction fees.

    **Flow.** `POST /v1/pools/{pool}/reserve` returns a reservation token and a set of
    outputs, each with its full unlocking script. Fetch each referenced shard's BEEF from
    `beef_url` (cache it: it is immutable once the shard is mined), build your transaction
    with the fuel outputs as extra inputs, broadcast it, then call `spend` with the txid.
    If you decide not to use an output, call `release`. Reservations expire after
    `ttl_seconds`; expired fuel is quarantined and verified against the chain before it is
    handed out again.

    **Lock modes.** `puzzle` outputs are unlocked by a 32-byte preimage push and impose no
    constraints on your transaction. `p2pkh` outputs are presigned with
    `SIGHASH_NONE|ANYONECANPAY|FORKID`; the `constraints` object tells you the transaction
    version, lockTime and input sequence you must use.

    **Errors.** Every non-2xx response is `{"code","message","retry_after"?}`. `503` and
    `409` carry `Retry-After`; clients should back off and retry.
servers:
  - url: http://localhost:8080
security:
  - bearerAuth: []
tags:
  - name: fuel
  - name: pools
  - name: admin
  - name: ops
paths:
  /healthz:
    get:
      tags: [ops]
      security: []
      summary: Liveness
      responses:
        "200": {description: alive}
  /readyz:
    get:
      tags: [ops]
      security: []
      summary: Readiness (store reachable, pools configured)
      responses:
        "200": {description: ready}
        "503": {description: not ready}
  /metrics:
    get:
      tags: [ops]
      security: []
      summary: Prometheus metrics
      responses:
        "200": {description: metrics in Prometheus text format}
  /openapi.yaml:
    get:
      tags: [ops]
      security: []
      summary: This document
      responses:
        "200": {description: OpenAPI YAML}
  /v1/pools:
    get:
      tags: [pools]
      summary: List pools with counts
      responses:
        "200":
          description: pools
          content:
            application/json:
              schema: {type: array, items: {$ref: "#/components/schemas/Pool"}}
        "401": {$ref: "#/components/responses/Error"}
  /v1/pools/{pool}:
    get:
      tags: [pools]
      summary: Describe one pool
      parameters: [{$ref: "#/components/parameters/pool"}]
      responses:
        "200":
          description: pool
          content: {application/json: {schema: {$ref: "#/components/schemas/Pool"}}}
        "404": {$ref: "#/components/responses/Error"}
  /v1/pools/{pool}/reserve:
    post:
      tags: [fuel]
      summary: Reserve fuel outputs
      description: |
        Returns between `min_count` and `count` outputs under one token. `503 exhausted`
        means no fuel is available right now; `409 insufficient` means fewer than
        `min_count` could be reserved (nothing is held). Both carry `Retry-After`.
      parameters: [{$ref: "#/components/parameters/pool"}]
      requestBody:
        required: true
        content:
          application/json:
            schema: {$ref: "#/components/schemas/ReserveRequest"}
            example: {count: 5, min_count: 1, ttl_seconds: 120}
      responses:
        "200":
          description: reservation
          content: {application/json: {schema: {$ref: "#/components/schemas/ReserveResponse"}}}
        "400": {$ref: "#/components/responses/Error"}
        "404": {$ref: "#/components/responses/Error"}
        "409": {$ref: "#/components/responses/ErrorRetry"}
        "503": {$ref: "#/components/responses/ErrorRetry"}
  /v1/pools/{pool}/release:
    post:
      tags: [fuel]
      summary: Return unused outputs to the pool
      description: Idempotent. Outputs that are not reserved under this token are skipped; the count reports how many changed.
      parameters: [{$ref: "#/components/parameters/pool"}]
      requestBody:
        required: true
        content:
          application/json:
            schema: {$ref: "#/components/schemas/ReleaseRequest"}
      responses:
        "200":
          description: released count
          content: {application/json: {schema: {$ref: "#/components/schemas/CountResponse"}}}
        "400": {$ref: "#/components/responses/Error"}
        "404": {$ref: "#/components/responses/Error"}
  /v1/pools/{pool}/spend:
    post:
      tags: [fuel]
      summary: Report that a transaction spent these outputs
      description: |
        Call this after broadcasting. Under `spend_policy=verify` (default) the outputs move to
        quarantine and are proven spent by the chain oracle; under `trust` they are marked
        spent immediately. Idempotent; a second call with a different txid is ignored.
      parameters: [{$ref: "#/components/parameters/pool"}]
      requestBody:
        required: true
        content:
          application/json:
            schema: {$ref: "#/components/schemas/SpendRequest"}
      responses:
        "200":
          description: accepted count
          content: {application/json: {schema: {$ref: "#/components/schemas/CountResponse"}}}
        "400": {$ref: "#/components/responses/Error"}
        "404": {$ref: "#/components/responses/Error"}
  /v1/pools/{pool}/beef/{txid}:
    get:
      tags: [fuel]
      summary: BEEF for a shard (split transaction)
      description: |
        Binary BEEF v2. While the shard is unmined the document contains the split transaction
        and its ancestry; once mined it contains the split transaction plus its BUMP and is
        served with `Cache-Control: immutable`. `ETag` is `"v<version>"`; send `If-None-Match`
        to get `304`. `X-Shard-Status` is `unmined|mined|retired|invalid`.
      parameters:
        - {$ref: "#/components/parameters/pool"}
        - {name: txid, in: path, required: true, schema: {type: string, pattern: "^[0-9a-f]{64}$"}}
      responses:
        "200":
          description: BEEF bytes
          headers:
            ETag: {schema: {type: string}}
            Cache-Control: {schema: {type: string}}
            X-Shard-Status: {schema: {type: string}}
          content: {application/octet-stream: {schema: {type: string, format: binary}}}
        "304": {description: not modified}
        "404": {$ref: "#/components/responses/Error"}
  /v1/pools/{pool}/shards:
    get:
      tags: [admin]
      summary: List shards
      parameters:
        - {$ref: "#/components/parameters/pool"}
        - {name: status, in: query, schema: {type: string, enum: [unmined, mined, retired, invalid]}}
        - {name: limit, in: query, schema: {type: integer, default: 100, maximum: 1000}}
      responses:
        "200":
          description: shards
          content: {application/json: {schema: {type: array, items: {$ref: "#/components/schemas/ShardInfo"}}}}
  /v1/pools/{pool}/shards/{txid}/retire:
    post:
      tags: [admin]
      summary: Retire a shard (operator)
      description: Marks the shard invalid and invalidates every non-terminal row. Use when a split transaction is known to be dead.
      parameters:
        - {$ref: "#/components/parameters/pool"}
        - {name: txid, in: path, required: true, schema: {type: string}}
      responses:
        "200":
          description: retired count
          content: {application/json: {schema: {$ref: "#/components/schemas/CountResponse"}}}
        "404": {$ref: "#/components/responses/Error"}
  /v1/pools/{pool}/quarantined:
    get:
      tags: [admin]
      summary: List quarantined rows
      parameters:
        - {$ref: "#/components/parameters/pool"}
        - {name: limit, in: query, schema: {type: integer, default: 100, maximum: 1000}}
      responses:
        "200":
          description: rows
          content: {application/json: {schema: {type: array, items: {$ref: "#/components/schemas/Row"}}}}
  /v1/pools/{pool}/quarantined/requeue:
    post:
      tags: [admin]
      summary: Return quarantined rows to the pool (operator)
      description: Use only when you know the outputs are unspent (for example after the oracle was unavailable).
      parameters: [{$ref: "#/components/parameters/pool"}]
      requestBody:
        required: true
        content:
          application/json:
            schema: {$ref: "#/components/schemas/OutpointsRequest"}
      responses:
        "200":
          description: requeued count
          content: {application/json: {schema: {$ref: "#/components/schemas/CountResponse"}}}
        "400": {$ref: "#/components/responses/Error"}
components:
  securitySchemes:
    bearerAuth: {type: http, scheme: bearer}
  parameters:
    pool:
      name: pool
      in: path
      required: true
      schema: {type: string}
  responses:
    Error:
      description: error
      content: {application/json: {schema: {$ref: "#/components/schemas/Error"}}}
    ErrorRetry:
      description: retryable error
      headers:
        Retry-After: {schema: {type: integer}, description: seconds}
      content: {application/json: {schema: {$ref: "#/components/schemas/Error"}}}
  schemas:
    Error:
      type: object
      required: [code, message]
      properties:
        code:
          type: string
          enum: [bad_request, unauthorized, unknown_pool, not_found, insufficient, exhausted, backpressure, unavailable, internal]
        message: {type: string}
        retry_after: {type: integer, description: seconds to wait before retrying}
    ReserveRequest:
      type: object
      required: [count]
      properties:
        count: {type: integer, minimum: 1, description: outputs requested (max is the pool's max_count)}
        min_count: {type: integer, minimum: 1, description: accept a partial fill of at least this many (default = count)}
        ttl_seconds: {type: integer, description: reservation lifetime (pool min_ttl..max_ttl; default default_ttl)}
    Unlock:
      type: object
      required: [mode, script, length]
      properties:
        mode: {type: string, enum: [puzzle, p2pkh]}
        script: {type: string, description: hex unlocking script (complete; no signing needed)}
        length: {type: integer, description: bytes, for unlockingScriptLength}
    Output:
      type: object
      required: [txid, vout, outpoint, satoshis, locking_script, unlock]
      properties:
        txid: {type: string}
        vout: {type: integer}
        outpoint: {type: string, example: "3f…:12"}
        satoshis: {type: integer}
        locking_script: {type: string, description: hex}
        unlock: {$ref: "#/components/schemas/Unlock"}
    Shard:
      type: object
      required: [txid, version, beef_url]
      properties:
        txid: {type: string}
        version: {type: integer, description: bumps when the BEEF changes (e.g. unmined → mined)}
        beef_url: {type: string}
    Constraints:
      type: object
      description: present for p2pkh pools; the consuming transaction must match these
      properties:
        tx_version: {type: integer, example: 1}
        lock_time: {type: integer, example: 0}
        sequence: {type: integer, example: 4294967295}
    ReserveResponse:
      type: object
      required: [token, expires_at, ttl_seconds, lock_mode, outputs, shards]
      properties:
        token: {type: string, description: 22-char base64url reservation token}
        expires_at: {type: string, format: date-time}
        ttl_seconds: {type: integer}
        lock_mode: {type: string, enum: [puzzle, p2pkh]}
        outputs: {type: array, items: {$ref: "#/components/schemas/Output"}}
        shards: {type: array, items: {$ref: "#/components/schemas/Shard"}}
        constraints: {$ref: "#/components/schemas/Constraints"}
    ReleaseRequest:
      type: object
      required: [token, outpoints]
      properties:
        token: {type: string}
        outpoints: {type: array, items: {type: string}}
    SpendRequest:
      type: object
      required: [token, outpoints, txid]
      properties:
        token: {type: string}
        outpoints: {type: array, items: {type: string}}
        txid: {type: string, description: the spending transaction id}
    OutpointsRequest:
      type: object
      required: [outpoints]
      properties:
        outpoints: {type: array, items: {type: string}}
    CountResponse:
      type: object
      properties:
        released: {type: integer}
        accepted: {type: integer}
        requeued: {type: integer}
        retired: {type: integer}
    Stats:
      type: object
      properties:
        available: {type: integer}
        reserved: {type: integer}
        quarantined: {type: integer}
        spent: {type: integer}
        invalid: {type: integer}
        shards:
          type: object
          properties:
            unmined: {type: integer}
            mined: {type: integer}
            retired: {type: integer}
            invalid: {type: integer}
    Pool:
      type: object
      properties:
        name: {type: string}
        denomination: {type: integer}
        lock_mode: {type: string}
        split_size: {type: integer}
        serve_policy: {type: string, enum: [mined_only, any]}
        release_policy: {type: string, enum: [trust, verify]}
        spend_policy: {type: string, enum: [verify, trust]}
        max_count: {type: integer}
        default_ttl_seconds: {type: integer}
        min_ttl_seconds: {type: integer}
        max_ttl_seconds: {type: integer}
        constraints: {$ref: "#/components/schemas/Constraints"}
        stats: {$ref: "#/components/schemas/Stats"}
    ShardInfo:
      type: object
      properties:
        txid: {type: string}
        size: {type: integer}
        status: {type: string, enum: [unmined, mined, retired, invalid]}
        owner: {type: string}
        lease_until: {type: string, format: date-time}
        version: {type: integer}
        created_at: {type: string, format: date-time}
    Row:
      type: object
      properties:
        outpoint: {type: string}
        state: {type: string, enum: [available, reserved, quarantined, spent, invalid]}
        reserved_by: {type: string}
        expires_at: {type: string, format: date-time}
        quarantined_at: {type: string, format: date-time}
        spent_hint: {type: string}
        spent_by: {type: string}
```

- [ ] **Step 4: Run, lint, commit**

Run: `cd /git/fuelkeeper && go test -race ./openapi/ ./pkg/api/ && golangci-lint run ./openapi/`
Expected: PASS (the API test `TestAuthHealthAndOpenAPI` still passes with the real document).

```bash
git add openapi
git commit -m "docs(api): OpenAPI 3.0.3 document with route coverage test"
```

---

### Task 9: Operator and API documentation

**Files:**
- Create: `docs/API.md`, `docs/OPERATIONS.md`
- Modify: `README.md`

- [ ] **Step 1: Write `docs/API.md`**

```markdown
# FuelKeeper HTTP API

Base URL: the `api.listen` address. All `/v1/*` routes require `Authorization: Bearer <key>`
when `api.api_keys` is set. Machine-readable spec: `GET /openapi.yaml`.

## Consuming fuel in five calls

1. `POST /v1/pools/{pool}/reserve` `{"count": 1}` → token + outputs + shards.
2. `GET {shards[i].beef_url}` (cache by `txid`+`version`; mined shards are immutable).
3. Build your transaction with each output as an input: outpoint `txid:vout`, unlocking
   script `unlock.script` (complete; nothing to sign), and the merged BEEF as `inputBEEF`.
   With go-wallet-toolbox use `client.CreateActionInputs` and `client.InputBEEF`.
4. Broadcast.
5. `POST /v1/pools/{pool}/spend` `{"token","outpoints","txid"}`.
   If you did not use an output: `POST /v1/pools/{pool}/release`.

Reservations expire after `ttl_seconds`. Expired or spent fuel is quarantined and verified
against the chain before it is handed out again, so a crashed client never causes a double
hand-out.

## Fee sizing

Fuel has a fixed denomination per pool (see `GET /v1/pools`). Your transaction's fee is
`ceil(size × sat_per_kB / 1000)`; the surplus `denomination − fee` should be added to one of
your own outputs (`client.Surplus`) so the wallet does not create a change output for it.
Surpluses below the ~40-sat dust floor go to miners.

## Lock modes

| mode | unlocking script | consumer constraints | input size |
|---|---|---|---|
| `puzzle` (default) | 33-byte preimage push | none | 74 B |
| `p2pkh` | presigned `<sig><pubkey>` (NONE\|ANYONECANPAY\|FORKID) | tx version 1, lockTime 0, this input's sequence 0xffffffff | 148 B |

## Errors

| status | code | meaning | action |
|---|---|---|---|
| 400 | `bad_request` | malformed input | fix the request |
| 401 | `unauthorized` | missing/invalid API key | |
| 404 | `unknown_pool` / `not_found` | | |
| 409 | `insufficient` | fewer than `min_count` available; nothing held | retry after `Retry-After` or lower `min_count` |
| 503 | `exhausted` | no fuel available | retry after `Retry-After`; check pool stats |
| 503 | `backpressure` / `unavailable` | instance or store busy | retry after `Retry-After` |

## Admin

`GET /v1/pools/{pool}/shards[?status=]`, `GET /v1/pools/{pool}/quarantined`,
`POST /v1/pools/{pool}/quarantined/requeue`, `POST /v1/pools/{pool}/shards/{txid}/retire`.
The `fuelkeeper` binary wraps these (`fuelkeeper pools|shards|quarantine|release --url …`).
```

- [ ] **Step 2: Write `docs/OPERATIONS.md`**

```markdown
# Operating FuelKeeper

## Configuration

`fuelkeeper serve --config fuelkeeper.yaml`. `${ENV}` references are expanded. Minimal file:

    store: {backend: sqlite, sqlite: {path: /var/lib/fuelkeeper/fuel.sqlite}}
    pools: [{name: main, denomination: 50, target_available: 100000, low_water: 60000, expected_tps: 500}]
    oracle: {kind: teranode, teranode: {url: http://teranode:8090/api/v1}}   # Plan 5
    treasury: {kind: toolbox, ...}                                          # Plan 5
    api: {listen: ":8080", api_keys: ["${FUELKEEPER_API_KEY}"]}

Startup refuses configurations whose mint capacity (`split_size × jobs.minter.max_per_round /
jobs.minter.interval`) is below 1.2 × `expected_tps`, and pools with `spend_policy: verify`
when `oracle.kind` is `none`.

## Topologies

| store | processes | notes |
|---|---|---|
| sqlite, pebble | exactly one | `synchronous=FULL` by default; pragmas verified at start |
| postgres | many | `synchronous_commit=on`; `max_connections` ≥ instances × pool size |
| mongodb, aerospike | many | write concern majority / strong-consistency namespace (Plan 4) |

Every instance leases whole shards; background jobs (reaper, watcher, minter) run on one
instance at a time under a store lease. Instances can be added or removed at any time; a crashed
instance's shards become adoptable after `instance.lease_ttl`.

## Development ("nullnet") mode

`treasury.kind: mock` mints synthetic fuel; `oracle.kind: mock` marks it mined after
`treasury.mock.confirm_after` and assumes unknown outpoints are unspent. Never use in production.

## What to watch

| metric | alert when |
|---|---|
| `fuelkeeper_pool_rows{state="available"}` | falls toward `low_water` |
| `fuelkeeper_reserve_requests_total{result="exhausted"}` | rising |
| `fuelkeeper_cas_missed_total` | non-zero for long: two holders serving one shard |
| `fuelkeeper_quarantine_oldest_age_seconds` | > 10 min: oracle down or unknown outpoints |
| `fuelkeeper_watcher_oldest_unmined_age_seconds` | > 1 h: splits not confirming |
| `fuelkeeper_minter_splits_total{result!="ok"}` | rising: treasury out of funds or rejects |
| `fuelkeeper_job_lease_held` | 0 on every instance: jobs not running |

## Runbook

- **Pool exhausted**: check `fuelkeeper pools`; if `shards.unmined` is high, the watcher is
  waiting for proofs (check oracle); if the treasury is out of funds, fund it (Plan 5).
- **Rows stuck in quarantine**: `fuelkeeper quarantine list`; after confirming the outputs are
  unspent on-chain, `fuelkeeper quarantine requeue --outpoints …`.
- **Dead split** (rejected/double-spent): `fuelkeeper quarantine retire --txid …` invalidates
  the shard and its rows.
- **Graceful restart**: SIGTERM stops accepting, releases shard leases and job leases, then
  exits. In-flight reservations are unaffected (they live in the store).
```

- [ ] **Step 3: Update `README.md`** — add a "Run it" section:

```markdown
## Run it (development mode)

    go run ./cmd/fuelkeeper serve --config examples/nullnet.yaml
    curl -s -XPOST localhost:8080/v1/pools/main/reserve -d '{"count":2}'

See `docs/API.md` for the client flow and `docs/OPERATIONS.md` for production topologies.
```
and create `examples/nullnet.yaml` with the Task 7 hand-test config (memory store, mock treasury/oracle, `spend_policy: trust`).

- [ ] **Step 4: Verify links and commit**

Run: `cd /git/fuelkeeper && go run ./cmd/fuelkeeper serve --config examples/nullnet.yaml & sleep 3; curl -sf localhost:8080/readyz; kill %1`
Expected: `{"status":"ready"}`.

```bash
git add docs/API.md docs/OPERATIONS.md README.md examples/nullnet.yaml
git commit -m "docs: API guide, operations runbook and nullnet example"
```

---

## Self-review notes

- **Spec coverage:** every endpoint in the spec's API table (with the pool-scoped BEEF path deviation documented), error envelope with stable codes and Retry-After, bearer API keys, health/ready/metrics, OpenAPI served by the binary, Go client with keep-alive, per-shard BEEF cache with ETag, `CreateActionInputs`, `InputBEEF`, `Surplus`; CLI `serve`, `pools`, `shards`, `quarantine list|requeue|retire`, `release`; config with defaults, env expansion, mint-capacity check; graceful shutdown ordering (stop accepting → release shards → release job leases → close store). `fund` CLI and funding endpoints are Plan 5.
- **Type consistency:** `wire.*` names are used by both `pkg/api` and `pkg/client`; `api.PoolService` matches `pool.Pool`'s method set from Plan 2; `client.ReserveRequest{Count, MinCount, TTL}`; `client.BEEF(ctx, pool, wire.Shard)`; `runServe(ctx, cfgPath, onListen)`.
- **Placeholder scan:** none.

## Follow-on plans

4. `2026-09-29-04-stores-pebble-mongo-aerospike.md`
5. `2026-09-29-05-treasury.md` — go-wallet-toolbox treasury, BRC-29 funding endpoints and `fund` CLI, teranode/arcade/composite oracles.
6. `2026-09-29-06-demo-benchmarks-docs.md`
