# FuelKeeper Plan 6: Benchmarks, demo application and documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove and document performance: a load generator with published per-backend and multi-instance numbers, store microbenchmarks, a demo application on go-wallet-toolbox that drives thousands of transactions per second through FuelKeeper on teratestnet (raw go-sdk mode for FuelKeeper's own ceiling, toolbox mode for the BRC-100 path), and the final documentation set.

**Architecture:** `fuelbench` is an HTTP load generator that speaks only the public API (so it measures what users get), records JSON results and renders markdown; store microbenchmarks live next to the conformance suite and reuse its helpers; `fueldemo` consumes fuel exactly the way the docs tell wallets to (via `pkg/client`) and broadcasts either with go-sdk + arcade directly or through a go-wallet-toolbox wallet's `createAction`. Reports are checked into `docs/benchmarks/` with the exact commands that produced them.

**Tech Stack:** Go 1.26, `golang.org/x/time/rate`, `github.com/HdrHistogram/hdrhistogram-go` (latency percentiles), `pkg/client`, `pkg/arcadeclient`, go-sdk transaction building (EF), go-wallet-toolbox wallet + GORM SQLite provider, docker compose (Postgres + 3 FuelKeeper instances).

**Spec:** `docs/superpowers/specs/2026-09-29-fuelkeeper-design.md` — "Demo", "Benchmarks", "Metrics", success criteria in "Context".

**Model tiering (user preference):** Tasks 1–3 (fuelbench, microbenchmarks, cluster compose) on `sonnet`; Task 4 (demo raw mode) on `sonnet`; Task 5 (demo toolbox mode) on the most capable model (toolbox createAction path with caller-provided inputs and surplus handling); Tasks 6–7 (runs and docs) on `sonnet`/`haiku`. Code-quality reviews on `sonnet` except Task 5.

---

## File structure

```
cmd/fuelbench/main.go        flags, run loop, JSON output
cmd/fuelbench/render.go      `fuelbench render a.json b.json` → markdown table
cmd/fuelbench/run_test.go    against httptest FuelKeeper (memory store)
pkg/store/bench_test.go      BenchmarkReserve/Quarantine/AcquireShard per backend (tags for pg/mongo/aerospike)
deploy/docker-compose.bench.yaml   postgres + fuelkeeper x3 (nullnet mocks) 
deploy/fuelkeeper-bench.yaml       shared config for the three instances
cmd/fueldemo/main.go         flags; modes raw|toolbox; reporter
cmd/fueldemo/raw.go          go-sdk build + arcade submit
cmd/fueldemo/toolbox.go      go-wallet-toolbox wallet path
cmd/fueldemo/report.go       TPS / latency / error summary, JSON out
docs/benchmarks/README.md, docs/benchmarks/<date>-*.md, docs/PERFORMANCE.md
docs/CLIENTS.md, docs/ARCHITECTURE.md, README.md, CHANGELOG.md
```

### Task 1: `fuelbench` load generator

Measures the public API: N workers loop `reserve(count)` → optional `spend` (fake txid) or `release`, with an optional rate limit, over one or more FuelKeeper URLs (round-robin). Records reserves/s, outputs/s, latency percentiles, and error counts by code; writes JSON; `render` turns JSON files into a markdown table.

**Files:**
- Create: `cmd/fuelbench/main.go`, `cmd/fuelbench/render.go`
- Test: `cmd/fuelbench/run_test.go`

- [ ] **Step 1: Write the test `cmd/fuelbench/run_test.go`**

```go
package main

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func TestRunAgainstMemoryStore(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	for i := 0; i < 20; i++ {
		storetest.MintShard(t, st, "main", 500, store.ShardMined)
	}
	pc := pool.DefaultPoolConfig("main", 50)
	pc.SpendPolicy = pool.SpendTrust
	pc.MinTTL = time.Second
	require.NoError(t, pc.Validate())
	ic := pool.DefaultInstanceConfig()
	ic.InstanceID = "bench-test"
	p := pool.NewPool(st, pc, ic, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	srv := httptest.NewServer(api.NewHandler(api.Deps{Pools: []api.PoolService{p}, Store: st}))
	t.Cleanup(func() { srv.Close(); cancel(); <-done })

	res, err := run(ctx, options{urls: []string{srv.URL}, pool: "main", workers: 8, count: 5, duration: 2 * time.Second, mode: "spend"})
	require.NoError(t, err)
	assert.Greater(t, res.Reserves, int64(100))
	assert.Equal(t, res.Reserves*5, res.Outputs, "every reserve got count outputs")
	assert.Positive(t, res.P50Micros)
	assert.GreaterOrEqual(t, res.P99Micros, res.P50Micros)
	assert.Empty(t, res.Errors, "no errors against a well-stocked pool: %v", res.Errors)
	assert.InDelta(t, float64(res.Reserves)/res.Seconds, res.ReservesPerSec, 1e-6)

	md := renderTable([]result{res})
	assert.Contains(t, md, "| reserves/s |")
	assert.Contains(t, md, "spend")
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd /git/fuelkeeper && go get golang.org/x/time@latest github.com/HdrHistogram/hdrhistogram-go@latest && go test ./cmd/fuelbench/`
Expected: build FAIL.

- [ ] **Step 3: Write `cmd/fuelbench/main.go`**

```go
// Command fuelbench load-tests a running FuelKeeper through its public API.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"golang.org/x/time/rate"

	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
)

type options struct {
	urls     []string
	apiKey   string
	pool     string
	workers  int
	count    int
	duration time.Duration
	rateLim  float64 // reserves per second, 0 = unbounded
	mode     string  // spend | release | hold
	label    string
}

type result struct {
	Label          string           `json:"label"`
	Backend        string           `json:"backend,omitempty"`
	Instances      int              `json:"instances"`
	Workers        int              `json:"workers"`
	Count          int              `json:"count"`
	Mode           string           `json:"mode"`
	Seconds        float64          `json:"seconds"`
	Reserves       int64            `json:"reserves"`
	Outputs        int64            `json:"outputs"`
	ReservesPerSec float64          `json:"reserves_per_sec"`
	OutputsPerSec  float64          `json:"outputs_per_sec"`
	P50Micros      int64            `json:"p50_us"`
	P95Micros      int64            `json:"p95_us"`
	P99Micros      int64            `json:"p99_us"`
	MaxMicros      int64            `json:"max_us"`
	Errors         map[string]int64 `json:"errors,omitempty"`
	GoVersion      string           `json:"go_version"`
	Timestamp      time.Time        `json:"timestamp"`
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "render" {
		if err := renderCmd(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}
	fs := flag.NewFlagSet("fuelbench", flag.ExitOnError)
	urls := fs.String("url", "http://127.0.0.1:8080", "comma-separated FuelKeeper base URLs (round-robin)")
	key := fs.String("key", os.Getenv("FUELKEEPER_API_KEY"), "API key")
	poolName := fs.String("pool", "main", "pool")
	workers := fs.Int("workers", 64, "concurrent workers")
	count := fs.Int("count", 1, "outputs per reserve")
	dur := fs.Duration("duration", 30*time.Second, "test duration")
	rl := fs.Float64("rate", 0, "target reserves/s (0 = unbounded)")
	mode := fs.String("mode", "spend", "spend | release | hold")
	label := fs.String("label", "", "label for the report")
	backend := fs.String("backend", "", "store backend name for the report")
	out := fs.String("out", "", "write JSON result to this file")
	_ = fs.Parse(os.Args[1:])

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	res, err := run(ctx, options{urls: strings.Split(*urls, ","), apiKey: *key, pool: *poolName, workers: *workers, count: *count, duration: *dur, rateLim: *rl, mode: *mode, label: *label})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	res.Backend = *backend
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(res)
	if *out != "" {
		b, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(*out, b, 0o644); err != nil { //nolint:gosec // report file
			fmt.Fprintln(os.Stderr, "write:", err)
			os.Exit(1)
		}
	}
}

func run(ctx context.Context, o options) (result, error) {
	if o.workers <= 0 || o.count <= 0 || len(o.urls) == 0 {
		return result{}, errors.New("workers, count and url are required")
	}
	clients := make([]*client.Client, len(o.urls))
	for i, u := range o.urls {
		c, err := client.New(strings.TrimSpace(u), client.WithAPIKey(o.apiKey))
		if err != nil {
			return result{}, err
		}
		clients[i] = c
	}
	var limiter *rate.Limiter
	if o.rateLim > 0 {
		limiter = rate.NewLimiter(rate.Limit(o.rateLim), 1)
	}
	hist := hdrhistogram.New(1, 60_000_000, 3) // 1µs..60s
	var histMu sync.Mutex
	var reserves, outputs atomic.Int64
	errs := map[string]int64{}
	var errMu sync.Mutex
	fakeTxID := strings.Repeat("ab", 32)

	runCtx, cancel := context.WithTimeout(ctx, o.duration)
	defer cancel()
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < o.workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := clients[w%len(clients)]
			for runCtx.Err() == nil {
				if limiter != nil {
					if err := limiter.Wait(runCtx); err != nil {
						return
					}
				}
				t0 := time.Now()
				res, err := c.Reserve(runCtx, o.pool, client.ReserveRequest{Count: o.count})
				lat := time.Since(t0)
				if err != nil {
					if runCtx.Err() != nil {
						return
					}
					errMu.Lock()
					errs[errCode(err)]++
					errMu.Unlock()
					if client.IsRetryable(err) {
						time.Sleep(20 * time.Millisecond)
					}
					continue
				}
				histMu.Lock()
				_ = hist.RecordValue(lat.Microseconds())
				histMu.Unlock()
				reserves.Add(1)
				outputs.Add(int64(len(res.Outputs)))
				ops := make([]string, len(res.Outputs))
				for i, out := range res.Outputs {
					ops[i] = out.Outpoint
				}
				switch o.mode {
				case "spend":
					if _, err := c.Spend(runCtx, o.pool, res.Token, ops, fakeTxID); err != nil && runCtx.Err() == nil {
						errMu.Lock()
						errs["spend:"+errCode(err)]++
						errMu.Unlock()
					}
				case "release":
					if _, err := c.Release(runCtx, o.pool, res.Token, ops); err != nil && runCtx.Err() == nil {
						errMu.Lock()
						errs["release:"+errCode(err)]++
						errMu.Unlock()
					}
				}
			}
		}(w)
	}
	wg.Wait()
	secs := time.Since(start).Seconds()
	r := result{
		Label: o.label, Instances: len(o.urls), Workers: o.workers, Count: o.count, Mode: o.mode, Seconds: secs,
		Reserves: reserves.Load(), Outputs: outputs.Load(),
		P50Micros: hist.ValueAtQuantile(50), P95Micros: hist.ValueAtQuantile(95), P99Micros: hist.ValueAtQuantile(99), MaxMicros: hist.Max(),
		GoVersion: runtime.Version(), Timestamp: time.Now().UTC(),
	}
	r.ReservesPerSec = float64(r.Reserves) / secs
	r.OutputsPerSec = float64(r.Outputs) / secs
	if len(errs) > 0 {
		r.Errors = errs
	}
	return r, nil
}

func errCode(err error) string {
	var ae *client.APIError
	if errors.As(err, &ae) {
		return ae.Code
	}
	return "transport"
}
```

- [ ] **Step 4: Write `cmd/fuelbench/render.go`**

```go
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

func renderCmd(files []string) error {
	if len(files) == 0 {
		return errors.New("usage: fuelbench render a.json [b.json ...]")
	}
	var results []result
	for _, f := range files {
		b, err := os.ReadFile(f) //nolint:gosec // operator-supplied path
		if err != nil {
			return err
		}
		var r result
		if err := json.Unmarshal(b, &r); err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
		results = append(results, r)
	}
	fmt.Print(renderTable(results))
	return nil
}

func renderTable(results []result) string {
	var sb strings.Builder
	sb.WriteString("| label | backend | instances | workers | count | mode | reserves/s | outputs/s | p50 ms | p95 ms | p99 ms | errors |\n")
	sb.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range results {
		errs := "-"
		if len(r.Errors) > 0 {
			parts := make([]string, 0, len(r.Errors))
			for k, v := range r.Errors {
				parts = append(parts, fmt.Sprintf("%s=%d", k, v))
			}
			errs = strings.Join(parts, " ")
		}
		fmt.Fprintf(&sb, "| %s | %s | %d | %d | %d | %s | %.0f | %.0f | %.2f | %.2f | %.2f | %s |\n",
			r.Label, r.Backend, r.Instances, r.Workers, r.Count, r.Mode, r.ReservesPerSec, r.OutputsPerSec,
			float64(r.P50Micros)/1000, float64(r.P95Micros)/1000, float64(r.P99Micros)/1000, errs)
	}
	return sb.String()
}
```

- [ ] **Step 5: Run, lint, commit**

Run: `cd /git/fuelkeeper && go test -race ./cmd/fuelbench/ && golangci-lint run ./cmd/fuelbench/`
Expected: PASS.

```bash
git add cmd/fuelbench go.mod go.sum
git commit -m "feat(fuelbench): HTTP load generator with JSON results and markdown rendering"
```

---

### Task 2: Store microbenchmarks (`pkg/store/bench_test.go` and per-backend hooks)

**Files:**
- Create: `pkg/store/storetest/bench.go` (exported `RunBenchmarks(b *testing.B, newStore Factory)`), `pkg/store/memory/bench_test.go`, `pkg/store/sqlite/bench_test.go`, `pkg/store/pebble/bench_test.go`, `pkg/store/postgres/bench_test.go` (tag postgres), `pkg/store/mongodb/bench_test.go` (tag mongodb), `pkg/store/aerospike/bench_test.go` (tag aerospike)

- [ ] **Step 1: `storetest/bench.go`**

```go
package storetest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// RunBenchmarks measures the three hot operations at several batch sizes.
func RunBenchmarks(b *testing.B, newStore func(b *testing.B) store.Store) {
	ctx := context.Background()
	for _, batch := range []int{1, 16, 64, 256} {
		b.Run(fmt.Sprintf("Reserve/batch=%d", batch), func(b *testing.B) {
			s := newStore(b)
			defer s.Close()
			rows := mintMany(b, s, 20, 1000)
			exp := time.Now().Add(time.Hour)
			b.ResetTimer()
			i := 0
			for n := 0; n < b.N; n++ {
				if i+batch > len(rows) {
					b.StopTimer()
					for _, r := range rows {
						_, _ = s.Release(ctx, "bench", store.Token{}, nil) // no-op guard
						_ = r
					}
					rows = mintMany(b, s, 20, 1000)
					i = 0
					b.StartTimer()
				}
				ops := ReserveOps(rows[i:i+batch], store.NewToken(), exp)
				if _, err := s.Reserve(ctx, "bench", ops); err != nil {
					b.Fatal(err)
				}
				i += batch
			}
			b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "rows/s")
		})
	}
	b.Run("AcquireShard", func(b *testing.B) {
		s := newStore(b)
		defer s.Close()
		mintMany(b, s, 200, 10)
		now := time.Now()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			sh, err := s.AcquireShard(ctx, "bench", "h", now, now.Add(time.Hour), []store.ShardStatus{store.ShardMined}, nil)
			if err != nil {
				b.Fatal(err)
			}
			if sh == nil {
				b.StopTimer()
				mintMany(b, s, 200, 10)
				b.StartTimer()
				continue
			}
		}
	})
	b.Run("Quarantine/1000", func(b *testing.B) {
		s := newStore(b)
		defer s.Close()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			rows := mintMany(b, s, 2, 500)
			_, _ = s.Reserve(ctx, "bench", ReserveOps(rows, store.NewToken(), time.Now().Add(-time.Minute)))
			b.StartTimer()
			if _, err := s.Quarantine(ctx, "bench", time.Now(), 1000); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func mintMany(tb testing.TB, s store.Store, shards, size int) []store.Row {
	tb.Helper()
	var all []store.Row
	for i := 0; i < shards; i++ {
		_, rows := mintShardTB(tb, s, "bench", size, store.ShardMined)
		all = append(all, rows...)
	}
	return all
}
```
Refactor `MintShard` so a `testing.TB`-based `mintShardTB` does the work and `MintShard(t *testing.T, ...)` calls it (benchmarks pass `*testing.B`). Remove the odd no-op `Release` loop in the Reserve benchmark; just re-mint when rows run out.

Per-backend `bench_test.go` (memory shown):
```go
package memory_test

import (
	"testing"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func BenchmarkStore(b *testing.B) {
	storetest.RunBenchmarks(b, func(*testing.B) store.Store { return memory.New() })
}
```
SQLite/Pebble use `b.TempDir()`; Postgres/Mongo/Aerospike reuse their `isolatedStore` helpers (adapt them to `testing.TB`).

- [ ] **Step 2: Run and commit**

Run: `cd /git/fuelkeeper && go test -run '^$' -bench . -benchmem ./pkg/store/memory/ ./pkg/store/sqlite/ ./pkg/store/pebble/ | tee /tmp/bench-embedded.txt && FUELKEEPER_TEST_POSTGRES_DSN='postgres://fuelkeeper:fuelkeeper@localhost:15432/fuelkeeper?sslmode=disable' go test -tags=postgres -run '^$' -bench . -benchmem ./pkg/store/postgres/ | tee /tmp/bench-postgres.txt`
Expected: numbers printed; keep the outputs for Task 6.

```bash
git add pkg/store
git commit -m "bench(store): Reserve/AcquireShard/Quarantine microbenchmarks per backend"
```

---

### Task 3: Multi-instance cluster for benchmarks (`deploy/docker-compose.bench.yaml`)

**Files:**
- Create: `Dockerfile`, `deploy/docker-compose.bench.yaml`, `deploy/fuelkeeper-bench.yaml`
- Modify: `Makefile` (`bench-cluster-up`, `bench-cluster-down`, `bench-run`)

- [ ] **Step 1: `Dockerfile`** (multi-stage, static binary):

```dockerfile
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/fuelkeeper ./cmd/fuelkeeper && CGO_ENABLED=0 go build -o /out/fuelbench ./cmd/fuelbench

FROM alpine:3.20
RUN adduser -D fuel
COPY --from=build /out/fuelkeeper /out/fuelbench /usr/local/bin/
USER fuel
EXPOSE 8080
ENTRYPOINT ["fuelkeeper"]
CMD ["serve", "--config", "/etc/fuelkeeper/fuelkeeper.yaml"]
```

- [ ] **Step 2: `deploy/fuelkeeper-bench.yaml`**

```yaml
instance: {max_shards: 8, low_water_rows: 5000, lease_ttl: 30s, rescan_interval: 5s}
store:
  backend: postgres
  postgres: {dsn: "postgres://fuelkeeper:fuelkeeper@postgres:5432/fuelkeeper?sslmode=disable", max_conns: 32}
pools:
  - name: main
    denomination: 50
    split_size: 1000
    target_available: 300000
    low_water: 200000
    spend_policy: trust
    min_ttl: 5s
jobs:
  enabled: true
  lease_ttl: 60s
  minter: {interval: 2s, max_per_round: 20, max_pending_shards: 100}
  watcher: {interval: 1s}
oracle: {kind: mock}
treasury: {kind: mock, mock: {confirm_after: 500ms}}
api: {listen: ":8080"}
log: {level: warn}
```

- [ ] **Step 3: `deploy/docker-compose.bench.yaml`**

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment: {POSTGRES_USER: fuelkeeper, POSTGRES_PASSWORD: fuelkeeper, POSTGRES_DB: fuelkeeper}
    command: ["postgres", "-c", "max_connections=300", "-c", "synchronous_commit=on"]
    healthcheck: {test: ["CMD-SHELL", "pg_isready -U fuelkeeper"], interval: 2s, timeout: 2s, retries: 30}
  fk1: &fk
    build: {context: .., dockerfile: Dockerfile}
    volumes: ["./fuelkeeper-bench.yaml:/etc/fuelkeeper/fuelkeeper.yaml:ro"]
    depends_on: {postgres: {condition: service_healthy}}
    ports: ["8081:8080"]
  fk2:
    <<: *fk
    ports: ["8082:8080"]
  fk3:
    <<: *fk
    ports: ["8083:8080"]
```
Makefile:
```make
bench-cluster-up:
	$(COMPOSE) -f deploy/docker-compose.bench.yaml up -d --build --wait
bench-cluster-down:
	$(COMPOSE) -f deploy/docker-compose.bench.yaml down -v
bench-run:
	$(GO) run ./cmd/fuelbench --url http://localhost:8081,http://localhost:8082,http://localhost:8083 --workers $(WORKERS) --count $(COUNT) --duration $(DURATION) --label "$(LABEL)" --backend postgres --out $(OUT)
```
with defaults `WORKERS ?= 64`, `COUNT ?= 1`, `DURATION ?= 60s`, `LABEL ?= cluster`, `OUT ?= docs/benchmarks/last.json`.

- [ ] **Step 4: Verify and commit**

Run: `cd /git/fuelkeeper && make bench-cluster-up && sleep 20 && curl -s localhost:8081/v1/pools | head -c 300 && make bench-run DURATION=20s LABEL=smoke OUT=/tmp/smoke.json && make bench-cluster-down`
Expected: three instances report the same pool with growing `available`; the smoke run prints a JSON result with no errors.

```bash
git add Dockerfile deploy Makefile
git commit -m "bench: three-instance Postgres cluster compose and bench-run target"
```

(If `github.com/HdrHistogram/hdrhistogram-go` cannot be fetched, replace it in Task 1 with a `latencies []int64` slice guarded by the mutex and compute percentiles by sorting at the end; keep the JSON field names.)

### Task 4: `fueldemo` raw mode (go-sdk build + arcade submit)

Demonstrates FuelKeeper's own ceiling: each transaction spends one fuel output into an OP_RETURN, is submitted to arcade in extended format (no BEEF needed by arcade), and is reported to FuelKeeper with `spend`. Batches of fuel are reserved per worker to amortise the reserve call.

**Files:**
- Create: `cmd/fueldemo/main.go`, `cmd/fueldemo/raw.go`, `cmd/fueldemo/report.go`
- Test: `cmd/fueldemo/raw_test.go` (against httptest FuelKeeper + fake arcade)

- [ ] **Step 1: `report.go`**

```go
package main

import (
	"encoding/json"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

type report struct {
	mu        sync.Mutex
	start     time.Time
	Mode      string           `json:"mode"`
	Workers   int              `json:"workers"`
	Sent      int64            `json:"sent"`
	Failed    map[string]int64 `json:"failed,omitempty"`
	Reserves  int64            `json:"reserves"`
	lat       []int64          // µs per successful tx (build+submit)
	Seconds   float64          `json:"seconds"`
	TPS       float64          `json:"tps"`
	P50Ms     float64          `json:"p50_ms"`
	P95Ms     float64          `json:"p95_ms"`
	P99Ms     float64          `json:"p99_ms"`
	GoVersion string           `json:"go_version"`
	Network   string           `json:"network"`
	Timestamp time.Time        `json:"timestamp"`
}

func newReport(mode, network string, workers int) *report {
	return &report{start: time.Now(), Mode: mode, Network: network, Workers: workers, Failed: map[string]int64{}, GoVersion: runtime.Version()}
}

func (r *report) ok(d time.Duration) {
	r.mu.Lock()
	r.Sent++
	r.lat = append(r.lat, d.Microseconds())
	r.mu.Unlock()
}

func (r *report) fail(code string) {
	r.mu.Lock()
	r.Failed[code]++
	r.mu.Unlock()
}

func (r *report) reserved() {
	r.mu.Lock()
	r.Reserves++
	r.mu.Unlock()
}

func (r *report) finish() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Seconds = time.Since(r.start).Seconds()
	r.TPS = float64(r.Sent) / r.Seconds
	r.Timestamp = time.Now().UTC()
	if len(r.lat) == 0 {
		return
	}
	sort.Slice(r.lat, func(i, j int) bool { return r.lat[i] < r.lat[j] })
	q := func(p float64) float64 { return float64(r.lat[int(float64(len(r.lat)-1)*p)]) / 1000 }
	r.P50Ms, r.P95Ms, r.P99Ms = q(0.50), q(0.95), q(0.99)
}

func (r *report) write(path string) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if path == "" {
		_, err = os.Stdout.Write(append(b, '\n'))
		return err
	}
	return os.WriteFile(path, b, 0o644) //nolint:gosec // report file
}
```

- [ ] **Step 2: `raw.go`**

```go
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"golang.org/x/time/rate"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/arcadeclient"
	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
)

// Submitter is what raw mode needs from arcade (fakeable).
type Submitter interface {
	PostTx(ctx context.Context, tx []byte) (*arcadeclient.SubmitResponse, error)
}

type rawRunner struct {
	fk      *client.Client
	arc     Submitter
	pool    string
	batch   int
	rep     *report
	limiter *rate.Limiter
}

// buildFuelTx spends one fuel output into an OP_RETURN, honouring p2pkh constraints when present.
func buildFuelTx(o wire.Output, constraints *wire.Constraints, memo []byte) (*transaction.Transaction, error) {
	tx := transaction.NewTransaction()
	if constraints != nil {
		tx.Version = constraints.TxVersion
		tx.LockTime = constraints.LockTime
	}
	txid, err := chainhash.NewHashFromHex(o.TxID)
	if err != nil {
		return nil, err
	}
	unlock, err := hex.DecodeString(o.Unlock.Script)
	if err != nil {
		return nil, err
	}
	lock, err := hex.DecodeString(o.LockingScript)
	if err != nil {
		return nil, err
	}
	seq := uint32(0xffffffff)
	if constraints != nil {
		seq = constraints.Sequence
	}
	in := &transaction.TransactionInput{SourceTXID: txid, SourceTxOutIndex: o.Vout, SequenceNumber: seq, UnlockingScript: script.NewFromBytes(unlock)}
	in.SetSourceTxOutput(&transaction.TransactionOutput{Satoshis: o.Satoshis, LockingScript: script.NewFromBytes(lock)})
	tx.AddInput(in)
	out, err := transaction.CreateOpReturnOutput([][]byte{memo})
	if err != nil {
		return nil, err
	}
	tx.AddOutput(out)
	return tx, nil
}

func (r *rawRunner) worker(ctx context.Context, id int) {
	n := 0
	for ctx.Err() == nil {
		res, err := r.fk.Reserve(ctx, r.pool, client.ReserveRequest{Count: r.batch, MinCount: 1})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.rep.fail("reserve:" + apiCode(err))
			time.Sleep(50 * time.Millisecond)
			continue
		}
		r.rep.reserved()
		unused := make([]string, 0, len(res.Outputs))
		for _, o := range res.Outputs {
			if ctx.Err() != nil {
				unused = append(unused, o.Outpoint)
				continue
			}
			if r.limiter != nil {
				if err := r.limiter.Wait(ctx); err != nil {
					unused = append(unused, o.Outpoint)
					continue
				}
			}
			n++
			t0 := time.Now()
			tx, err := buildFuelTx(o, res.Constraints, []byte(fmt.Sprintf("fueldemo w%d n%d", id, n)))
			if err != nil {
				r.rep.fail("build")
				unused = append(unused, o.Outpoint)
				continue
			}
			ef, err := tx.EF()
			if err != nil {
				r.rep.fail("ef")
				unused = append(unused, o.Outpoint)
				continue
			}
			if _, err := r.arc.PostTx(ctx, ef); err != nil {
				var bp *arcadeclient.BackpressureError
				if errors.As(err, &bp) {
					r.rep.fail("arcade:backpressure")
					time.Sleep(time.Duration(bp.RetryAfter) * time.Second)
					unused = append(unused, o.Outpoint)
					continue
				}
				var rej *arcadeclient.RejectError
				if errors.As(err, &rej) {
					r.rep.fail(fmt.Sprintf("arcade:%d", rej.Status))
					// a rejected fuel spend is suspicious: hand the output back for the oracle to judge
					_, _ = r.fk.Spend(ctx, r.pool, res.Token, []string{o.Outpoint}, tx.TxID().String())
					continue
				}
				r.rep.fail("arcade:transport")
				unused = append(unused, o.Outpoint)
				continue
			}
			r.rep.ok(time.Since(t0))
			if _, err := r.fk.Spend(ctx, r.pool, res.Token, []string{o.Outpoint}, tx.TxID().String()); err != nil {
				r.rep.fail("spend:" + apiCode(err))
			}
		}
		if len(unused) > 0 {
			rctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, _ = r.fk.Release(rctx, r.pool, res.Token, unused)
			cancel()
		}
	}
}

func apiCode(err error) string {
	var ae *client.APIError
	if errors.As(err, &ae) {
		return ae.Code
	}
	return "transport"
}
```
Check `tx.EF()`'s exact signature in `~/go/pkg/mod/github.com/bsv-blockchain/go-sdk@v1.4.1/transaction/transaction.go` (`EF() ([]byte, error)`), and `transaction.CreateOpReturnOutput`.

- [ ] **Step 3: `main.go`**

```go
// Command fueldemo drives transactions through FuelKeeper: raw go-sdk + arcade,
// or a go-wallet-toolbox wallet's createAction.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/bsv-blockchain/fuelkeeper/pkg/arcadeclient"
	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
)

func main() {
	fs := flag.NewFlagSet("fueldemo", flag.ExitOnError)
	mode := fs.String("mode", "raw", "raw | toolbox")
	url := fs.String("url", "http://127.0.0.1:8080", "FuelKeeper base URL")
	key := fs.String("key", os.Getenv("FUELKEEPER_API_KEY"), "FuelKeeper API key")
	poolName := fs.String("pool", "main", "pool")
	arcadeURL := fs.String("arcade", os.Getenv("ARCADE_URL"), "arcade base URL (raw mode and toolbox services)")
	arcadeToken := fs.String("arcade-token", os.Getenv("ARCADE_CALLBACK_TOKEN"), "arcade callback token")
	workers := fs.Int("workers", 16, "concurrent workers")
	batch := fs.Int("batch", 20, "fuel outputs reserved per worker round")
	rl := fs.Float64("rate", 0, "target tx/s (0 = unbounded)")
	dur := fs.Duration("duration", 60*time.Second, "run time")
	out := fs.String("out", "", "JSON report path")
	network := fs.String("network", "ttn", "toolbox mode: main|test|ttn|tstn")
	walletKey := fs.String("wallet-key", os.Getenv("FUELDEMO_WALLET_KEY"), "toolbox mode: root key hex (random if empty)")
	chaintracks := fs.String("chaintracks", os.Getenv("CHAINTRACKS_URL"), "toolbox mode: chaintracks URL")
	storage := fs.String("storage", "fueldemo.sqlite", "toolbox mode: wallet storage path")
	feeKB := fs.Uint64("fee-kb", 100, "toolbox mode: fee model sat/kB")
	_ = fs.Parse(os.Args[1:])

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	fk, err := client.New(*url, client.WithAPIKey(*key))
	if err != nil {
		fatal(err)
	}
	var limiter *rate.Limiter
	if *rl > 0 {
		limiter = rate.NewLimiter(rate.Limit(*rl), 1)
	}
	rep := newReport(*mode, *network, *workers)
	runCtx, cancel := context.WithTimeout(ctx, *dur)
	defer cancel()
	var wg sync.WaitGroup
	switch *mode {
	case "raw":
		if *arcadeURL == "" {
			fatal(fmt.Errorf("--arcade is required in raw mode"))
		}
		arc, err := arcadeclient.New(*arcadeURL, arcadeclient.WithCallbackToken(*arcadeToken))
		if err != nil {
			fatal(err)
		}
		r := &rawRunner{fk: fk, arc: arc, pool: *poolName, batch: *batch, rep: rep, limiter: limiter}
		for i := 0; i < *workers; i++ {
			wg.Add(1)
			go func(i int) { defer wg.Done(); r.worker(runCtx, i) }(i)
		}
	case "toolbox":
		tr, err := newToolboxRunner(ctx, toolboxOptions{fk: fk, pool: *poolName, batch: *batch, rep: rep, limiter: limiter,
			network: *network, walletKey: *walletKey, arcadeURL: *arcadeURL, chaintracks: *chaintracks, storage: *storage, feeKB: *feeKB})
		if err != nil {
			fatal(err)
		}
		defer tr.Close()
		for i := 0; i < *workers; i++ {
			wg.Add(1)
			go func(i int) { defer wg.Done(); tr.worker(runCtx, i) }(i)
		}
	default:
		fatal(fmt.Errorf("unknown mode %q", *mode))
	}
	wg.Wait()
	rep.finish()
	if err := rep.write(*out); err != nil {
		fatal(err)
	}
	fmt.Fprintf(os.Stderr, "sent=%d tps=%.0f p50=%.1fms p99=%.1fms failed=%v\n", rep.Sent, rep.TPS, rep.P50Ms, rep.P99Ms, rep.Failed)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
```
Until Task 5 lands, add a stub `toolbox.go` with `newToolboxRunner` returning `errors.New("toolbox mode: see Task 5")` so the binary builds.

- [ ] **Step 4: Test `cmd/fueldemo/raw_test.go`**

Spin up an httptest FuelKeeper (as in `cmd/fuelbench/run_test.go`, pool `spend_policy: trust`), a fake `Submitter` that parses the EF bytes with `transaction.NewTransactionFromBytes` (EF parses as a normal tx with extra data; instead decode with `transaction.NewTransactionFromEFBytes` if present, else assert the byte prefix `0000000000EF` after the version) and counts submissions; run `rawRunner.worker` for 1 s with `batch: 5`; assert `rep.Sent > 0`, `rep.Failed` empty, every submitted tx has one input whose unlocking script verifies the puzzle (`fuel.VerifyPuzzle(lock, unlock)` using the source output attached in EF), and the store shows those rows `spent` (trust policy).

- [ ] **Step 5: Run, lint, commit**

Run: `cd /git/fuelkeeper && go test -race ./cmd/fueldemo/ && golangci-lint run ./cmd/fueldemo/`

```bash
git add cmd/fueldemo
git commit -m "feat(fueldemo): raw mode — go-sdk fuel spends submitted to arcade in extended format"
```

---

### Task 5: `fueldemo` toolbox mode (go-wallet-toolbox `createAction` with fuel inputs)

The BRC-100 path: a toolbox wallet with **no funds of its own** creates actions whose only input is a fuel output (unlocking script supplied, BEEF from FuelKeeper), and an OP_RETURN output. The wallet computes the fee; the surplus (`denomination − fee`, below the ~40-sat dust floor at 100 sat/kB for 50-sat fuel) goes to miners, so no change output is created. Several processes run in parallel to reach thousands of tx/s, because one toolbox instance is bounded by its own per-action cost (prior measurements: roughly 250–1100 TPS).

**Files:**
- Create: `cmd/fueldemo/toolbox.go` (replace the stub)
- Test: none automated (needs a network); a `-run TestToolboxWiring` compile-level test that builds the runner against a memory-backed FuelKeeper with `--network ttn` and asserts construction fails cleanly without a chaintracks URL

- [ ] **Step 1: Write `toolbox.go`**

Construction (facts from `TB` = go-wallet-toolbox v0.184.16, same as Plan 5 Task 5): services with arcade + chaintracks, `defs.FeeModel{SatPerKB, feeKB}`, `storage.NewGORMProvider` on SQLite `storage`, `Migrate`, `wallet.New(net, key, provider, WithServices(svc), WithTrustSelf("known"))`; no monitor (immediate broadcast returns the arcade verdict synchronously). Random root key when `--wallet-key` is empty (print it so the run can be repeated).

Per fuel output:
```go
inputs, _ := client.CreateActionInputs(res, "fuel")   // all outputs of the reservation
beef, _ := t.fk.InputBEEF(ctx, t.pool, res)
for i, o := range res.Outputs {
	memo, _ := transaction.CreateOpReturnOutput([][]byte{[]byte("fueldemo")})
	args := sdk.CreateActionArgs{
		Description: "fueldemo",
		InputBEEF:   beef,
		Inputs:      []sdk.CreateActionInput{inputs[i]},
		Outputs:     []sdk.CreateActionOutput{{LockingScript: *memo.LockingScript, Satoshis: 0, OutputDescription: "memo"}},
		Options:     &sdk.CreateActionOptions{AcceptDelayedBroadcast: ptr(false), RandomizeOutputs: ptr(false), ReturnTXIDOnly: ptr(true)},
	}
	if res.Constraints != nil { args.Version = res.Constraints.TxVersion; args.LockTime = res.Constraints.LockTime }
	t0 := time.Now()
	r, err := t.w.CreateAction(ctx, args, "fueldemo")
	// err classification: *pkgerrors.ProcessActionError → "reject"; wdk.ErrNotEnoughFunds → "underfunded" (fee model mismatch: raise denomination or lower --fee-kb); else "create"
	t.rep.ok(time.Since(t0)); t.fk.Spend(ctx, t.pool, res.Token, []string{o.Outpoint}, r.Txid.String())
}
```
Notes: the toolbox verifies `InputBEEF` with SPV (chaintracks) and rejects it if the shard is unmined without full ancestry; FuelKeeper's default `serve_policy: mined_only` guarantees a BUMP. `TrustSelf: known` lets repeated reservations from the same shard send txid-only stubs once the wallet knows the split (the client still merges the full BEEF; the toolbox tolerates it).

- [ ] **Step 2: Build and commit**

Run: `cd /git/fuelkeeper && go build ./... && go vet ./cmd/fueldemo/ && golangci-lint run ./cmd/fueldemo/`

```bash
git add cmd/fueldemo/toolbox.go
git commit -m "feat(fueldemo): toolbox mode — createAction with fuel inputs and merged input BEEF"
```

---

### Task 6: Benchmark and demo runs, published results

**Files:**
- Create: `docs/benchmarks/README.md`, `docs/benchmarks/<YYYYMMDD>-store-micro.md`, `docs/benchmarks/<YYYYMMDD>-api-single.md`, `docs/benchmarks/<YYYYMMDD>-api-cluster.md`, `docs/benchmarks/<YYYYMMDD>-demo-ttn.md`, `docs/PERFORMANCE.md`, result JSON files under `docs/benchmarks/results/`

- [ ] **Step 1: Store microbenchmarks** — run the Task 2 commands for memory, sqlite, pebble, postgres, mongodb, aerospike (compose up); paste `go test -bench` output into `<date>-store-micro.md` with machine info (`uname -a`, CPU model, disk type).
- [ ] **Step 2: API single instance per backend** — for each backend, start `fuelkeeper serve` with the nullnet config (`examples/nullnet.yaml` adapted to the backend, `target_available: 300000`, `split_size: 1000`), wait until `available ≥ 200000`, then run:
  ```
  fuelbench --workers 64  --count 1  --duration 60s --label <backend>-w64-c1  --backend <backend> --out docs/benchmarks/results/<backend>-w64-c1.json
  fuelbench --workers 256 --count 1  --duration 60s --label <backend>-w256-c1 ...
  fuelbench --workers 64  --count 10 --duration 60s --label <backend>-w64-c10 ...
  ```
  and `fuelbench render` them into `<date>-api-single.md`. Note memory/CPU of the FuelKeeper process (`/metrics` `process_*`) and the coalescer batch size histogram summary.
- [ ] **Step 3: Cluster** — `make bench-cluster-up`; run the same three shapes against the three instances and one against a single instance of the cluster (`--url http://localhost:8081`) for the horizontal-scaling comparison; render into `<date>-api-cluster.md`.
- [ ] **Step 4: Demo on teratestnet** — with FuelKeeper on the toolbox treasury (Plan 5 smoke config, `target_available` sized for the run: TPS × 120 s), run `fueldemo --mode raw --workers 32 --batch 50 --duration 120s --arcade $ARCADE_URL` (record arcade's acceptance and any 4xx codes), then `fueldemo --mode toolbox` in 4 processes × 16 workers; render both into `<date>-demo-ttn.md` with the arcade URL, block heights covered, and the FuelKeeper stats before/after (`available`, `spent`, `quarantined`, `cas_missed_total`, `reserve_latency` p99).
- [ ] **Step 5: `docs/PERFORMANCE.md`** — method (what each harness measures, why nullnet numbers are an upper bound for the store path, why demo numbers are bounded by arcade and the toolbox), the summary table (one row per backend: reserves/s single, reserves/s 3-instance, p99), guidance ("for N tx/s provision M instances and a pool of N × ttl"), and the audit statement (zero double hand-outs in the demo: `cas_missed_total` and the invariant test).
- [ ] **Step 6: Commit** `docs(bench): published store, API, cluster and teratestnet demo results`.

---

### Task 7: Final documentation and branch finish

**Files:**
- Create: `docs/CLIENTS.md`, `docs/ARCHITECTURE.md`, `CHANGELOG.md`
- Modify: `README.md`

- `docs/CLIENTS.md`: Go recipe (`client.New` → `Reserve` → `InputBEEF` + `CreateActionInputs` → toolbox `CreateAction` → `Spend`; error handling with `IsRetryable`; batching advice), TypeScript recipe (fetch `/reserve`, fetch `beef_url`, `wallet.createAction({inputBEEF, inputs:[{outpoint, unlockingScript, inputDescription}], outputs, options:{acceptDelayedBroadcast:false}})`, then `/spend`), fee sizing and surplus rules, p2pkh constraints, and what to do on 409/503.
- `docs/ARCHITECTURE.md`: distilled from the spec: components diagram (text), state machine, guarantees G1/G2 and where they are enforced, sharded ownership and why leases are not load-bearing, background jobs, store backends matrix with per-backend CAS primitive, treasury flow, oracle roles.
- `README.md`: elevator pitch, quick start (nullnet), fund it, consume it (link CLIENTS.md), operate it (link OPERATIONS.md), performance headline (link PERFORMANCE.md), backend matrix, development.
- `CHANGELOG.md`: `0.1.0` with the six plans' deliverables.
- Then invoke `superpowers:finishing-a-development-branch` for `feat/store-core` (merge to `main`, tag `v0.1.0`).

Commit: `docs: clients guide, architecture, changelog and README for 0.1.0`.

---

## Self-review notes

- **Spec coverage:** benchmark suite with published per-backend and multi-instance numbers; store microbenchmarks; demo on go-wallet-toolbox (toolbox mode) plus the raw mode that shows FuelKeeper's own ceiling; documented performance expectations and the double-hand-out audit; client and architecture docs.
- **Type consistency:** `fuelbench` uses only `pkg/client`; `fueldemo` uses `pkg/client`, `pkg/arcadeclient` (`PostTx`, `RejectError`, `BackpressureError`), `wire.Output/Constraints`; `client.CreateActionInputs` and `client.InputBEEF` from Plan 3.
- **Placeholders:** none; Task 6 lists the exact commands and files; numbers are produced by running them.

## Facts carried from the Plan 3 final review (2026-10-01)

- Metrics the spec lists but Plan 2 did not add: `coalescer_batch_size`, `coalescer_flush_latency_seconds`, `coalescer_inflight_ops`, `shard_adoptions_total{result}`, `shard_drops_total{reason}`, `lease_renew_failures_total`, `cas_unknown_total`, `compensating_release_total`, `store_op_latency_seconds{op}`, `clock_skew_seconds`; `fuelbench` needs at least the coalescer ones.
- The client has `WithObserver` (per-call timing hook), `ReserveWithRetry`, and a tuned transport; a load generator should still pass its own `http.Client` (`WithHTTPClient`) with a large `MaxIdleConnsPerHost`.
- There is no batch spend endpoint; the demo batches `spend` calls per token. Consider a `POST /v1/pools/{pool}/spend` body with several tokens only if profiling shows it matters.

## Facts carried from Plans 4 and 5 and from Plan 6 execution (2026-09-30)

- `treasury.Treasury.Send` is `Send(ctx, txid, beef []byte)` and is stateless
  and wallet-first: the minter passes `nil` (first send of a wallet-held
  `nosend` split), the watcher passes the stored shard BEEF for rebroadcast.
  A `Broadcaster` must never return `treasury.ErrRejected`; the oracle is the
  authority on rejection.
- `oracle.TxStatus.Inputs` (nil = unknown) plus the optional
  `oracle.InputsResolver` let the reaper verify that a mined spender really
  spends the outpoint; `composite.Tx` never fetches raw transactions on the
  watcher path. Arcade-only oracles (`arcade`, or `composite` without
  `teranode_url`) cannot answer outpoint queries: config rejects
  `release_policy: verify` for them and defaults `spend_policy` to `trust`;
  a `teranode`-only oracle is rejected whenever jobs run.
- Config keys the demo and benchmark configs use: `oracle.arcade_url`,
  `oracle.teranode_url`, `treasury.toolbox.*` (`reconcile_grace` must exceed
  `jobs.minter.build_timeout` + 10 s; `jobs.lease_ttl` must exceed
  `build_timeout` + 20 s), `store.{pebble,mongodb,aerospike}` sections,
  `api.insecure` for a keyless non-loopback listen. Deployments that link the
  toolbox should set `SETTINGS_ENV_FILE=/dev/null`; library consumers of
  `pkg/config` or `pkg/treasury/...` must copy the three `replace` lines.
- Metrics added during Plan 6 (all `fuelkeeper_`): `coalescer_batch_size`,
  `coalescer_flush_latency_seconds`, `coalescer_inflight_ops`,
  `shard_adoptions_total{pool,result}`, `shard_drops_total{pool,reason}`,
  `lease_renew_failures_total`, `cas_unknown_total`,
  `compensating_release_total`, `store_op_latency_seconds{pool,op}`,
  `pool_refilling{pool}`, `reaper_input_mismatch_total{pool,source}`.
  `clock_skew_seconds` is deferred.
- The shard manager now rotates drained shards immediately when hungry at
  `max_shards` (up to two per pass; lease release runs in the background on
  the request path), so a single instance no longer answers `exhausted` for
  a rescan interval after draining its shards; the fuelbench test fixture no
  longer needs a `MaxShards` bump.
- The minter's refill is sticky: it starts below `low_water` and continues
  across cap-interrupted rounds until `target_available` (in-memory per
  process; visible as `pool_refilling`).
- Benchmark cluster: rootless Podman needs the shared SELinux label (`:ro,z`)
  on the config bind mount; the image is about 103 MB; three instances on one
  Postgres gave 756 reserves/s (p50 37 ms, p99 274 ms) and 424 reserves/s
  (p50 67 ms, p99 390 ms) in two 15–20 s smokes with zero errors.
- The teratestnet smoke (Plan 5) and the teratestnet demo (Task 6 step 4)
  have not been run: no `FK_TTN_*` environment or funded wallet is available
  on this machine. `docs/PERFORMANCE.md` must say so and give the exact
  procedure from `docs/TREASURY.md`.

## Arcade-only lifecycle (2026-09-30)

- A direct teranode connection is not expected and must not be assumed: it
  is an optional, opt-in oracle (`oracle.kind: composite` with
  `teranode_url`), off by default. `resolveOracleDefault` sets
  `oracle.kind` to `arcade` when `treasury.kind: toolbox`, else `none`.
- The consuming go-wallet-toolbox wallet clears its own reservation when its
  transaction fails to propagate; `pkg/client/settle.go`'s `Client.Settle`
  (`Settle(ctx, pool, token, outpoints, Settlement{Outcome, TxID,
  CompetingTxIDs})`, `Outcome` one of `OutcomeBroadcast`,
  `OutcomeNotBroadcast`, `OutcomeDoubleSpend`, `OutcomeUnknown`) embodies
  that contract; the TS wallet-toolbox path reads the `WERR_REVIEW_ACTIONS`
  review results (`doubleSpend`, `invalidTx`, `serviceError`, `success`) and
  calls the same endpoints.
- With arcade only, an expired reservation (or a `release_policy: verify`
  release, which stores no hint) nobody settles is requeued after
  `jobs.reaper.unknown_requeue_age` (default 10 m, 0 disables, must be 0 or
  `>= min_unspent_age`) when the pool's `expiry_policy` is `requeue`
  (default); `hold` quarantines it for an operator instead. Pool capacity
  never leaks.
- A consumer whose broadcast is rejected as a double-spend must report the
  output SPENT (hinting the competing txid when known via
  `OutcomeDoubleSpend`), never release it: releasing would return an
  already-spent output to the pool for every later consumer to fail on (a
  poison pill). A consumer must not report spent on a rejection with no
  competitor (`OutcomeNotBroadcast` -> `Release`): under `spend_policy:
  trust` a spend is terminal.
- `spend_policy` stays `trust` by default for arcade-only; `verify` is an
  allowed opt-in, resolved from the hinted transaction's own fate
  (`pkg/jobs/reaper_hint.go`'s rules) once `hint_patience` (10 m) or
  `hint_max_wait` (default 24 h, 0 disables, must be 0 or `>=
  hint_patience`) elapse. `release_policy: verify` is now allowed with
  arcade-only too (a delayed release, requeued after
  `unknown_requeue_age`). Verify policies are still rejected outright for
  `oracle.kind: none`; a teranode-only oracle is rejected whenever
  `jobs.enabled` is true.
- Arcade never reports a competing tx synchronously: a double-spend appears
  later via GET /tx as REJECTED, ARC code 466. `rejectReasonForCode` maps
  466 -> `RejectReasonInputsSpent`; 462 (coinbase/input-script policy),
  465/467/471-474 etc -> `RejectReasonTxInvalid`; 476 (not final) and any
  unmapped code -> `RejectReasonUnknown`. `RejectReasonTxInvalid` is
  evidence about one submission to one gateway, not a global verdict.
- New metrics: `fuelkeeper_reaper_requeued_total{pool,reason}` (`unspent`,
  `unknown_aged`, `rejected`, `hint_expired`); `fuelkeeper_reaper_spent_total
  {pool,source}` (`oracle`, `hint`, `competitor`, `inputs_spent`); gauge
  `fuelkeeper_reaper_held{pool,reason}` (13 reasons: `oracle_down`,
  `unknown_fresh`, `expiry_hold`, `unspent_fresh`, `spent_no_txid`,
  `spender_pending`, `hint_pending`, `hint_unseen`, `competitor_pending`,
  `rejected_fresh`, `rejected_unknown`, `conflicting`, `tx_down`).
  `fuelkeeper_reaper_resolved_total` is unchanged.
- New pool config key: `expiry_policy: requeue|hold` (default `requeue`).
  New `jobs.reaper` keys: `unknown_requeue_age` (10 m), `hint_max_wait`
  (24 h), `tx_recheck_interval` (30 s, > 0, throttles `Tx(hint)` lookups per
  txid across ticks).
