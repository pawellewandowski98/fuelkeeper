# Performance

Published benchmark results live under [`docs/benchmarks/`](benchmarks/README.md).
This page is the entry point: method, the headline numbers, capacity
guidance, the correctness audit statement, and caveats. The store
microbenchmarks are from commit `236642c` (2026-09-30); the API
(single-instance and cluster) numbers are from commit **`91e1331`**
(2026-09-30), re-run after two fixes landed on top of the `236642c` API
numbers this page originally published — see
[`benchmarks/20260930-api-single.md`](benchmarks/20260930-api-single.md)'s
"Before the fixes" appendix for what changed and why.

## Method

Three harnesses, each measuring a different slice of the system:

1. **Store microbenchmarks** (`go test -bench`, per `pkg/store/*` package,
   commit `236642c` —
   [`benchmarks/20260930-store-micro.md`](benchmarks/20260930-store-micro.md)).
   No HTTP, no pool coalescer, no jobs: just `Store.Reserve`,
   `Store.AcquireShard` and `Store.Quarantine` called directly, in-process.
   This isolates each backend's raw commit/lookup cost from everything
   FuelKeeper builds on top of it. **The sqlite/pebble numbers on that page
   were measured on tmpfs** (`go test`'s default temp directory, RAM-backed
   on this host), not the real SSD named in that page's "Machine" section —
   `fsync` there was already close to free, so the later group-commit fix
   (`fd4dc56`/`91e1331`, see below) does not change those particular
   numbers; its effect is visible instead in the API single-instance
   numbers, whose sqlite/pebble data directories are on the real disk.
2. **API, single instance per backend** (`cmd/fuelbench` against one
   `fuelkeeper serve` process, mock oracle/treasury, commit `91e1331` —
   [`benchmarks/20260930-api-single.md`](benchmarks/20260930-api-single.md)).
   The full public API: HTTP, auth, the pool coalescer, shard adoption, the
   minter/reaper/watcher jobs, and the store. **These numbers are an upper
   bound for the store/pool path**, not a production forecast: the mock
   oracle answers every query instantly and the mock treasury "mines" a
   split after a fixed `confirm_after` delay with no real broadcast, no fee
   negotiation, and no chain reorg handling — none of a real chain's
   latency or failure modes are present. `fuelbench` and `fuelkeeper` also
   ran on the same machine (loopback HTTP), so there is no real network hop
   either.
3. **API, three-instance cluster** (`cmd/fuelbench` against
   `deploy/docker-compose.bench.yaml`'s three instances sharing one Postgres
   store, commit `91e1331` —
   [`benchmarks/20260930-api-cluster.md`](benchmarks/20260930-api-cluster.md)).
   Same mock oracle/treasury caveat as above, plus container networking
   overhead (still loopback-adjacent: containers and client on one host).
4. **Teratestnet demo** (`cmd/fueldemo`, raw and toolbox modes — **not
   run**; procedure only, in
   [`benchmarks/20260930-demo-ttn.md`](benchmarks/20260930-demo-ttn.md)).
   This is the one harness that would measure the real ceiling: a real
   arcade broadcast, real fee/UTXO selection, and (toolbox mode) a real
   go-sdk wallet's `createAction`. Numbers from harnesses 2-3 are bounded
   above by what this harness would show once run, since arcade acceptance
   latency and the toolbox wallet's own chunking/reconciliation logic sit
   outside anything the mock treasury exercises.

## Summary

One row per store backend: single-instance `w64-c1` (64 workers, 1 output
per reserve) reserves/s, single-instance `w64-c10` (64 workers, 10 outputs
per reserve) outputs/s, and — for postgres, the only backend the cluster
benchmark used — the three-instance cluster's `w64-c1` reserves/s. p99 is
the single-instance `w64-c1` reserve latency. Full detail, error counts, and
the `-rate` caps some shapes needed are in
[`benchmarks/20260930-api-single.md`](benchmarks/20260930-api-single.md) and
[`benchmarks/20260930-api-cluster.md`](benchmarks/20260930-api-cluster.md).

| Backend | Single instance, w64-c1 reserves/s | Single instance, w64-c10 outputs/s | 3-instance cluster, w64-c1 reserves/s | p99 reserve latency (w64-c1) |
|---|---|---|---|---|
| memory | 24364 | 19311 | n/a (single-process only) | 7 ms |
| sqlite | 707 | 3030 | n/a (single-process only) | 144 ms |
| pebble | 716 | 2489 | n/a (single-process only) | 158 ms |
| postgres | 880 | 5505 | 1012 | 90 ms |
| mongodb | 970 | 6669 | n/a (not benchmarked clustered) | 85 ms |
| aerospike | 24726 | 15000 (rate-capped) | n/a (not benchmarked clustered) | 5 ms |

**memory and aerospike are the fastest by a wide margin** (single-digit
millisecond p99, 24K+ reserves/s at `w64-c1`) — both benefited directly
from the `34512a0` (memory `Stats` now O(1)) and `ba55a6f`/`a22be74`
(shard retirement) fixes, the latter of which also applies to aerospike's
adoption path. **sqlite and pebble moved from the slowest backends by two
orders of magnitude to a solid middle tier** (700+ reserves/s, tens of
milliseconds p99, down from hundreds of milliseconds) once `fd4dc56`/
`91e1331` replaced one-`fsync`-per-writer with group commit — they are
still bound by their single-writer lock relative to the multi-process
backends, just no longer catastrophically so. **Postgres is the only
backend measured clustered** (the benchmark cluster's fixed config,
`deploy/fuelkeeper-bench.yaml`, only stands up a Postgres-backed cluster);
it and mongodb are the two multi-process backends most likely to be run in
production alongside aerospike (see `docs/OPERATIONS.md`'s "Deployment
Topologies").

## Capacity guidance

**For N reserves/s, provision a pool of at least `N x reservation_ttl x
safety_factor` available rows**, where `reservation_ttl` is the TTL a client
actually requests (`pools[0].default_ttl` if unspecified — 2 minutes by
default, `pools[0].min_ttl`/`max_ttl` bound what a client may ask for; see
`docs/OPERATIONS.md`'s config template) and `safety_factor` covers burstiness
and the minter's own reaction lag (`jobs.minter.interval`,
`max_per_round`/`max_pending_shards`) — 1.5-2x is a reasonable starting
point, higher if load is bursty rather than steady. The reasoning: at a
steady N reserves/s with every reservation held for `reservation_ttl`
seconds before being spent or released, `N x reservation_ttl` outputs are
"in flight" (reserved, not yet available again) at any moment — the pool
must hold at least that many *available* rows on top of what is currently
reserved, or a legitimate burst of new reserves starves behind existing
holds. Worked example: at 1000 reserves/s with the default 120s TTL and a
1.5x safety factor, `target_available` should be at least `1000 x 120 x 1.5
= 180,000`, with `low_water` set so the minter starts refilling well before
that (e.g. half of `target_available`) — this task's own single-instance
configs used `target_available: 300000`, `low_water: 150000` for exactly
this reason. Note the fixed code can now sustain 1000 reserves/s on a
single postgres instance comfortably (880-1850 reserves/s measured,
depending on concurrency shape) and far more on memory/aerospike; size
`target_available` against the *sustained* rate you plan to run, not the
peak single-shape number above, since `w256-c1`/`w64-c10` and `w64-c1`
exercise different concurrency-vs-batch-size trade-offs.

**For horizontal scaling**, the cluster benchmark's single-instance-vs-
three-instance comparison
([`benchmarks/20260930-api-cluster.md`](benchmarks/20260930-api-cluster.md)'s
"Horizontal scaling" section) measured 571 -> 1012 reserves/s (1.77x) going
from one FuelKeeper instance to three sharing one Postgres store, all under
`w64-c1`. That gain is specific to this environment (one Postgres instance,
one host, the benchmark cluster's fixed `instance.max_shards`/
`low_water_rows` defaults) and should not be read as a universal constant —
measure again against the target Postgres/MongoDB/Aerospike deployment
before sizing a real fleet — but it demonstrates that adding stateless
FuelKeeper instances in front of one durable store does buy real additional
throughput, not none.

## Audit statement: zero double hand-outs

FuelKeeper's core correctness guarantee, **G1**, is that no fuel output is
ever live under two reservations at once. `fuelkeeper_cas_missed_total`
counts rows whose *optimistic* reservation lost a compare-and-set race —
the store had already changed the row by the time the coalescer's batch
applied, so that op is rejected and retried against a different row; this
is **not** a double hand-out, it is the CAS mechanism working as designed.
As of `91e1331`, a rescan's own store listing can no longer re-add a row a
concurrent `Reserve` already popped and applied (`pkg/pool/allocator.go`'s
bracketed listing generations), so `cas_missed_total` is now a clean signal
for a genuine second owner briefly touching a row (e.g. during a lease
hand-over) — it no longer includes the self-inflicted misses a rescan race
used to cause. It read **zero** across every single-instance and cluster
run in this re-run (six single-process backends plus the three-instance
cluster, all checked via `/metrics` after their runs), which is itself a
consistency check: the cluster runs did have genuine concurrent instances
competing for the same rows, and still saw no CAS conflicts serious enough
to register in a 60s window at the loads tested.

The actual "never handed out twice" guarantee is enforced continuously in
CI, not just asserted here: `pkg/pool/invariant_test.go`'s
**`TestInvariantsMultiInstance`** and **`TestInvariantsConcurrentClients`**
drive many concurrent clients (and, in the multi-instance case, many
FuelKeeper instances) against a shared store and fail the test the instant
any fuel outpoint is found live under two different reservation tokens
simultaneously — the `live.claim` helper in that file checks both its own
in-memory bookkeeping of "who should hold this row" and asks the store
directly whether the row really is reserved under the token it was just
handed, so a bug in either the in-memory allocator or the store's CAS logic
would be caught.

## Caveats

- **Single machine.** Every benchmark in this task ran client
  (`fuelbench`/`fuelkeeper`) and server on one host (a 13th Gen Intel
  Core i9-13900K, SATA SSD — see
  [`benchmarks/20260930-store-micro.md`](benchmarks/20260930-store-micro.md)
  for the full machine spec), sharing CPU and disk I/O between them. Real
  deployments separate client and server across a network, which changes
  the latency profile (adds a real network hop, removes host-level resource
  contention between load generator and server).
- **Client and server co-located.** Even the "cluster" benchmark's three
  containers and the load generator ran on the same host; no measurement
  here reflects real inter-host network latency or a WAN link to a remote
  Postgres/MongoDB/Aerospike.
- **Mock oracle and treasury.** Every API benchmark (single-instance and
  cluster) used `oracle.kind: mock` and `treasury.kind: mock`: no real
  arcade broadcast, no real chain confirmation latency, no real fee
  negotiation or UTXO selection. See "Method" above and
  [`benchmarks/20260930-demo-ttn.md`](benchmarks/20260930-demo-ttn.md) for
  what a real-chain run would additionally measure. The mock oracle is also
  **per-process** (`pkg/oracle.Mock` is a plain in-memory map, no backing
  store), which surfaced as a real gotcha re-running the cluster benchmark:
  see [`benchmarks/20260930-api-cluster.md`](benchmarks/20260930-api-cluster.md)'s
  "A pre-existing gotcha, not a regression" section — if the minter and
  watcher job leases land on different cluster instances, that instance's
  watcher never sees the minter's confirmations and the pool never fills.
  Not a FuelKeeper bug (a real chain oracle has no per-process state), but a
  real limitation of benchmarking a cluster with the mock oracle.
- **The tmpfs vs. real-disk split for sqlite/pebble.** The store
  microbenchmarks' sqlite/pebble numbers are on tmpfs (RAM-backed, `fsync`
  close to free); the API single-instance sqlite/pebble numbers are on the
  real SSD. Compare within a page, not across the store-micro and
  API-single pages' sqlite/pebble absolute latencies.
- **Durability settings of the local Postgres/MongoDB.** The already-running
  `localhost:15432` Postgres (`postgres:16-alpine`,
  `synchronous_commit=on` per `deploy/docker-compose.bench.yaml`'s cluster
  config; the single-instance benchmark's dedicated database ran against
  the same server's defaults) and `localhost:27017` MongoDB
  (`mongo:7`, standalone, not a replica set) are development containers on
  local SSD-backed storage, not a production HA topology — MongoDB in
  particular has no replica to make `{w: majority, j: true}` meaningfully
  redundant here, and neither server's storage is separated from the
  benchmark client's own disk I/O.
- **`/spend` is not coalesced.** Reserve requests are batched by the
  per-pool coalescer (group-commit), but every `spend` and `release` call is
  its own store write, one per call. A write-bound store therefore sees
  roughly one write transaction per output spent or released, versus
  roughly one write transaction per *batch* of outputs reserved: at a
  `count` of 20 per reserve, that is on the order of 20x more write
  transactions from spends than from reserves for the same throughput of
  outputs (see `docs/OPERATIONS.md`'s "Known Limits"). Every benchmark on
  this page ran in `spend` mode, so the numbers above already include this
  cost; size `split_size`/`target_available` and the store's write capacity
  with that ratio in mind, not just the reserve rate.
- **Shard-adoption stability: fixed, with one residual limit.** The
  permanent shard-adoption stall this page's numbers originally exposed
  (an instance's allocator could stop adopting shards forever even with
  rows available elsewhere) is fixed as of `ba55a6f`/`a22be74`: a fully
  spent-out shard (every row terminal) is now retired outright rather than
  staying adoptable forever. One narrower case remains, documented in
  `docs/OPERATIONS.md`'s **"Known Limits"**: a shard whose rows are drained
  but not yet *terminal* (still reserved under a live TTL, or quarantined)
  is only excluded from re-adoption for 60s at a time, capped at a 256-entry
  list — if more than 256 such shards precede the live ones, adoption can
  still cycle. This measurably affected pebble's `w64-c10` shape in the
  re-run (see
  [`benchmarks/20260930-api-single.md`](benchmarks/20260930-api-single.md)'s
  "Residual limit" note — a rate-independent ~20% `exhausted` rate, unique
  to pebble among the backends benchmarked). The planned fix, per
  `docs/OPERATIONS.md`, is a store-side `AcquireShard` that skips row-less
  shards server-side instead of relying on a bounded client-side exclusion
  list.

## Keeping this current

The `Makefile`'s `bench-run` target's `OUT` still defaults to `/tmp` for a
throwaway run; pass `OUT=docs/benchmarks/results/<name>.json` (or set
`LABEL`/`WORKERS`/`COUNT`/`DURATION`) to save a new result under
[`benchmarks/results/`](benchmarks/results/), then update this page's
summary table and the backend pages it links to.
