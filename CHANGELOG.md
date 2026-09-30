# Changelog

All notable changes to FuelKeeper are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] - 2026-09-30

Initial release: a standalone Go service that splits a funding payment into
a pool of fixed-denomination BSV outputs ("fuel") and hands them out on
request under a durable reservation, so a consuming wallet can attach fuel
as an extra input to pay transaction fees without a signing service on the
hot path.

### Added

#### Core pool (`pkg/pool`)

- Fuel in two unlock modes: hash-puzzle (a 33-byte preimage push, no signing
  needed) and presigned P2PKH (`SIGHASH_NONE|ANYONECANPAY|FORKID`, with the
  transaction-level `tx_version`/`lock_time`/per-input `sequence`
  constraints a consumer must honour).
- Sharded per-instance ownership: each instance adopts and leases whole
  shards (one split transaction's worth of rows) rather than contending on
  individual rows, with pull-based adoption, targeted rescans on a CAS miss,
  and immediate rotation of drained shards.
- Batched (group-commit) reservations: a per-pool coalescer folds concurrent
  `Reserve` calls into one store write, with idempotent retry and exactly
  one compensating `Release` per in-doubt flush.
- **G1** (no fuel output is ever handed to two live reservations) and
  **G2** (every row is in exactly one state, and rows are conserved) are
  enforced by row-level compare-and-set in the store, independent of the
  in-memory allocator/lease optimizations layered on top -- see
  `docs/ARCHITECTURE.md`'s "Guarantees and Where They Are Enforced".

#### HTTP API, Go client and CLI

- The HTTP surface (`pkg/api`, `openapi/fuelkeeper.yaml`):
  `reserve`/`release`/`spend` per pool, BEEF retrieval, pool/shard listings,
  BRC-29 funding (`GET`/`POST /v1/funding`), admin quarantine
  (list/requeue/retire), `/healthz`, `/readyz`, `/metrics`.
- The Go client (`pkg/client`): `Reserve`/`ReserveWithRetry`, `Release`,
  `Spend`, and `Settle` -- the settlement contract that maps a consumer's
  own broadcast outcome (`OutcomeBroadcast`/`OutcomeNotBroadcast`/
  `OutcomeDoubleSpend`/`OutcomeUnknown`) to the right call, so a caller
  never has to get spend-vs-release backwards under pressure (see
  `docs/CLIENTS.md#when-your-broadcast-fails`) -- plus `InputBEEF` (merged,
  per-shard-cached BEEF), `CreateActionInputs`/`ApplyConstraints` (go-sdk
  `wallet.CreateActionArgs` wiring), `Surplus`/`FeeSats` (fee sizing), and
  `IsRetryable`.
- The `fuelkeeper` CLI (`cmd/fuelkeeper`): `serve` (runs the service),
  `pools`, `shards`, `quarantine` (list/requeue/retire), `release`, `fund`,
  `version`.

#### Store backends (`pkg/store`)

- Six backends behind one `Store` interface: memory, sqlite (modernc, no
  CGO), pebble (embedded LSM, differential-fuzzed against the memory
  store), postgres (`pgx`, `FOR UPDATE SKIP LOCKED` adoption, per-row CAS
  updates pipelined in one batch), mongodb (conditional single-document
  updates, majority+journal write concern), and aerospike
  (filter-expression CAS, requires a strong-consistency namespace in
  production).
- Every backend passes the same `pkg/store/storetest.RunSuite` conformance
  suite under `-race`; `make bench` runs per-backend microbenchmarks
  (`pkg/store/<backend>/bench_test.go`) for `Reserve`/`AcquireShard`/
  `Quarantine`.

#### Background jobs (`pkg/jobs`)

- Three jobs, each run on one instance at a time under a store-held lease:
  **reaper** (quarantines expired reservations, resolves them via the chain
  oracle, and, with an arcade-only oracle, requeues an unhinted row blind
  after `unknown_requeue_age` or resolves a hinted row from its own
  transaction's fate -- the full hinted-row rule set, including the
  gateway-named-spent-inputs fast path that resolves arcade's `UTXO_SPENT`
  shape in one tick, is in `docs/OPERATIONS.md`'s reaper lifecycle table),
  **watcher** (confirms unmined shards, rebroadcasts stale ones, alerts on
  very old ones), and **minter** (tops up each pool's available rows with
  sticky low-water hysteresis and rotates drained shards immediately when
  an instance is hungry at `max_shards`).

#### Treasury (`pkg/treasury`)

- `mock`: an in-memory treasury for nullnet development, instantly "mining"
  shards after a configurable delay.
- `toolbox`: an embedded go-wallet-toolbox wallet that mints real fuel,
  funded by a BRC-29 wallet-to-wallet payment from the operator's own
  BRC-100 wallet (`fuelkeeper fund`), with two-level chunking, wallet-first
  send (record-before-broadcast), and non-blocking reconciliation of
  unrecorded splits after a crash -- see `docs/TREASURY.md` and
  `docs/FUNDING.md`.

#### Chain oracles (`pkg/oracle`)

- `none` (no chain source), `mock` (nullnet), `arcade` (the default oracle
  whenever `treasury.kind: toolbox` is configured -- tracks transaction
  lifecycle, including `RejectReason` classification parsed from arcade's
  ARC error codes and, for a `UTXO_SPENT` rejection, the specific spent
  inputs and named spender), `teranode` (an optional, opt-in UTXO-set
  accelerator -- never assumed by default), and `composite` (pairs arcade's
  `Tx` lifecycle with teranode's `Outpoints`/`Inputs` when configured). A
  direct teranode connection is never required: arcade-only is the expected
  production shape, and `docs/OPERATIONS.md`'s "Oracle kinds and policies"
  documents exactly what each policy combination needs and after what
  delay.

#### Metrics

- A `fuelkeeper_`-prefixed Prometheus catalogue covering reserve/release/
  spend counters and latencies, coalescer/allocator/shard gauges,
  reaper/watcher/minter counters (including the reaper's per-reason
  requeued/spent/held breakdowns), oracle call outcomes, and job-lease
  gauges -- see `docs/OPERATIONS.md#what-to-watch`.

#### Load testing and demos

- `fuelbench` (`cmd/fuelbench`): an HTTP load generator against the public
  API -- N workers looping reserve/spend/release, reporting throughput,
  latency percentiles and error counts by code, with a `render` subcommand
  that turns saved JSON results into a markdown comparison table.
- `fueldemo` (`cmd/fueldemo`): a demand generator that drives fuel through a
  real settlement path. Raw mode spends each output into its own OP_RETURN
  transaction submitted to arcade directly (FuelKeeper's own ceiling,
  holding rather than releasing on any outcome that leaves a submission's
  fate uncertain); toolbox mode drives a go-sdk wallet's `createAction`
  over reserved fuel inputs the way a real integrator would.
- A three-instance benchmark cluster (`deploy/docker-compose.bench.yaml`,
  `make bench-cluster-up`/`bench-run`/`bench-cluster-down`) sharing one
  Postgres store, for horizontal-scaling comparisons.

#### Documentation

- `docs/API.md` (HTTP surface and Go client), `docs/CLIENTS.md` (consumer
  contract, Go and TypeScript recipes, fee sizing, the settlement
  contract), `docs/OPERATIONS.md` (configuration reference, deployment
  topologies, the reaper lifecycle and metrics, runbook), `docs/ARCHITECTURE.md`
  (components, state machine, guarantees, background jobs, oracle roles,
  store contract), `docs/TREASURY.md` and `docs/FUNDING.md` (the toolbox
  treasury and BRC-29 funding flow). Benchmark methodology and results are
  being published separately -- see `docs/PERFORMANCE.md`.

### Known limitations

- A module importing `pkg/config` or `pkg/treasury/...` must copy this
  repo's three `go.mod` `replace` lines (`github.com/libp2p/go-libp2p`,
  `github.com/quic-go/webtransport-go`, `github.com/quic-go/quic-go`),
  transitive pins that go-wallet-toolbox currently needs.
- The teratestnet demo (a funded go-wallet-toolbox treasury driving
  `fueldemo` against the public Teranode test network) was not run for this
  release: no funded teratestnet environment was available. The exact
  procedure is documented in `docs/TREASURY.md` and `docs/OPERATIONS.md`'s
  "Teratestnet (Real Treasury)" section, ready to run against a funded
  wallet.
