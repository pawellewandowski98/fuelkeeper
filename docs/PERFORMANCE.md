# Performance

Published benchmark results live under [`docs/benchmarks/`](benchmarks/README.md).
This page is the entry point: method, the headline numbers, capacity
guidance, the correctness audit statement, and caveats. The store
microbenchmarks are from commit `236642c` (2026-09-30); the API
(single-instance and cluster) numbers are from commit **`91e1331`**
(2026-09-30), re-run after three fixes landed on top of the `236642c` API
numbers this page originally published — see
[`benchmarks/20260930-api-single.md`](benchmarks/20260930-api-single.md)'s
"Before the fixes" appendix for what changed and why. Pebble's
single-instance numbers were re-run a second time at **`8f3bb20`** after a
fourth fix cleared a shard-adoption problem specific to that shape (see
"Summary" below).

## Method

Three harnesses, each measuring a different slice of the system:

1. **Store microbenchmarks** (`go test -bench`, per `pkg/store/*` package,
   commit `236642c` —
   [`benchmarks/20260930-store-micro.md`](benchmarks/20260930-store-micro.md)).
   No HTTP, no pool coalescer, no jobs: just `Store.Reserve`,
   `Store.AcquireShard` and `Store.Quarantine` called directly, in-process.
   This isolates each backend's raw commit/lookup cost from everything
   FuelKeeper builds on top of it. **Neither the sqlite nor the pebble
   numbers on that page pay this host's real SSD fsync cost, for two
   different reasons**: pebble's benchmarks open the store with `NoSync:
   true` (zero `fsync` calls, on any filesystem); sqlite's keep the default
   `synchronous=FULL` (an `fsync` per commit) but `go test`'s default temp
   directory is `tmpfs` (RAM-backed) on this host, not the real SSD named in
   that page's "Machine" section, so those `fsync` calls are real but cheap.
   The later group-commit fix (`fd4dc56`/`91e1331`, see below) does not
   change either page's numbers (pebble was never fsyncing there; sqlite's
   tmpfs fsync had little cost to amortize); its effect is visible instead
   in the API single-instance numbers, whose sqlite/pebble data directories
   are on the real disk with sqlite's default durability and pebble's
   `no_sync: false`.
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
| memory | 24364 | 19311 (rate-capped 4000/s) | n/a (single-process only) | 7 ms |
| sqlite | 707 | 3030 | n/a (single-process only) | 144 ms |
| pebble | 675 | 2919 | n/a (single-process only) | 201 ms |
| postgres | 880 | 5505 | 1012 | 90 ms |
| mongodb | 970 | 6669 | n/a (not benchmarked clustered) | 85 ms |
| aerospike | 24726 | 15000 (rate-capped 1500/s) | n/a (not benchmarked clustered) | 5 ms |

**memory and aerospike are the fastest by a wide margin** (single-digit
millisecond p99, 24K+ reserves/s at `w64-c1`). Memory benefited from both
`34512a0` (its row-count `Stats` reads are now O(1); the shard-status
counts `Stats` also returns stay O(shards), unchanged) and `ba55a6f`/
`a22be74` (shard retirement); aerospike benefited from `ba55a6f`/`a22be74`
only -- `34512a0` is specific to `pkg/store/memory` and does not touch
aerospike. **sqlite and pebble moved from the slowest backends by one
order of magnitude to a solid middle tier** (675+ reserves/s at `w64-c1`,
p50 in the tens of milliseconds -- 47.84/43.01 ms -- though p99 is still
144/201 ms, down from hundreds of milliseconds for both percentiles) once
`fd4dc56`/`91e1331` replaced one-`fsync`-per-writer with group commit —
they are still bound by their single-writer lock relative to the
multi-process backends, just no longer catastrophically so (pebble's
`w64-c10` needed a second fix, `e339617`/`8f3bb20`, on top of group commit
to clear a shard-adoption problem group commit's higher throughput exposed
-- see [`benchmarks/20260930-api-single.md`](benchmarks/20260930-api-single.md)
for the full mechanism). **Postgres is the only
backend measured clustered** (the benchmark cluster's fixed config,
`deploy/fuelkeeper-bench.yaml`, only stands up a Postgres-backed cluster);
it and mongodb are the two multi-process backends most likely to be run in
production alongside aerospike (see `docs/OPERATIONS.md`'s "Deployment
Topologies").

## Capacity guidance

Two separate buffers determine how many rows a pool needs, and they should
be sized separately rather than folded into one number:

1. **Reserved-in-flight capacity.** At a steady N reserves/s, an output
   stays in the `reserved` state (not `available`, not yet `spent` or
   `released`) for up to its reservation TTL (`pools[].default_ttl` if a
   client doesn't specify one — 2 minutes by default; `pools[].min_ttl`/
   `max_ttl` bound what a client may request; see `docs/OPERATIONS.md`'s
   config template). So roughly `N x hold_time` outputs (`hold_time` ≤ the
   TTL; shorter if clients spend/release promptly) are parked in `reserved`
   at any moment — capacity the minter must have produced *in addition to*
   the available buffer below, since those rows are temporarily unusable by
   anyone else.
2. **The available buffer (`target_available`).** This must cover demand
   during the minter's own reaction lag: how long it takes `jobs.minter` to
   notice `available` has dropped and produce enough newly mined rows to
   compensate (bounded by `jobs.minter.interval`, `max_per_round` and
   `max_pending_shards` — how many shards it can start and how many it will
   let sit unmined at once). Size it to `N x reaction_lag x safety_factor`,
   where `reaction_lag` is the time from a low-water trigger to new rows
   landing, and `safety_factor` (1.5-2x is a reasonable starting point,
   higher if load is bursty) covers the gap between that estimate and
   reality. `low_water` should trigger refilling well before `available`
   actually runs out — e.g. half of `target_available` — so the minter has
   the full reaction lag to work with, not zero.

Total minted supply (what the minter needs to be able to sustain, not a
single config value) is approximately the sum of both: reserved-in-flight
capacity plus the available buffer. Note the fixed code's *measured* range
on a single postgres instance across this page's three load shapes was
550-1850 reserves/s (`w64-c10`'s 550 came with real errors — 581
`exhausted` + 11 `insufficient`, not a comfortable margin — see
[`benchmarks/20260930-api-single.md`](benchmarks/20260930-api-single.md));
size `target_available` and the reserved-in-flight budget against the
*sustained* rate you actually plan to run and the concurrency shape it
will run under, not the single highest number measured here.

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
As of `a22be74`, a rescan's own store listing can no longer re-add a row a
concurrent `Reserve` already popped and applied (`pkg/pool/allocator.go`'s
bracketed listing generations), so `cas_missed_total` is now a clean signal
for a genuine second owner briefly touching a row (e.g. during a lease
hand-over) — it no longer includes the self-inflicted misses a rescan race
used to cause. It read **zero** across every single-instance and cluster
run in this re-run (three embedded backends plus three servers run
single-instance, plus the three-instance cluster, all checked via a live
`/metrics` read after their runs — observed, not recorded: not saved to a
file in this repo, unlike the `results/*.json` throughput numbers). This is
the *expected* reading, not a
surprising or notable pass: genuine second-owner contention only arises
around a lease hand-over (an instance losing and another instance adopting
the same shard), which none of these 60s runs triggered — a zero reading
here says the metric didn't fire spuriously, not that concurrent-ownership
handling was exercised and survived.

The actual "never handed out twice" guarantee is enforced continuously in
CI, not just asserted here: `pkg/pool/invariant_test.go`'s
**`TestInvariantsMultiInstance`** and **`TestInvariantsConcurrentClients`**
(`pkg/pool/invariant_test.go:192,343`) drive many concurrent clients (and,
in the multi-instance case, many FuelKeeper instances) against a shared
**memory store** (the only backend these two tests use) and fail the test
the instant any fuel outpoint is found live under two different
reservation tokens simultaneously — the `live.claim` helper in that file
checks both its own in-memory bookkeeping of "who should hold this row"
and asks the store directly whether the row really is reserved under the
token it was just handed. This catches a bug in the in-memory allocator
(backend-agnostic) or in the memory store's own CAS logic; it does not by
itself exercise postgres/mongodb/aerospike/sqlite/pebble's own CAS
implementations, each of which has separate conformance coverage under
`make test`/`make test-integration` (see the README's "Store Backends"
table).

## Caveats

- **Single machine.** Every benchmark on this page ran client
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
- **The store-micro vs. API-single split for sqlite/pebble durability.**
  The store microbenchmarks' pebble numbers make zero `fsync` calls
  (`NoSync: true`) and its sqlite numbers fsync against tmpfs (RAM-backed,
  cheap); the API single-instance sqlite/pebble numbers run with sqlite's
  default durability and pebble's `no_sync: false` against the real SSD.
  Compare within a page, not across the store-micro and API-single pages'
  sqlite/pebble absolute latencies.
- **Durability settings of the local Postgres/MongoDB.** Two separate
  Postgres containers were involved: the already-running `localhost:15432`
  dev server (`deploy/docker-compose.yaml`, `postgres:16-alpine`, no
  explicit `synchronous_commit` override -- Postgres's own built-in default
  is `on` -- the single-instance benchmark's dedicated database ran here),
  and the benchmark cluster's own postgres (`deploy/docker-compose.bench.yaml`,
  a separate container with no host port, `synchronous_commit=on` set
  explicitly). Both land on `synchronous_commit=on` in practice, but from
  different sources; neither is the same container. `localhost:27017`
  MongoDB (`mongo:7`, standalone, not a replica set) served both the
  single-instance benchmark (no cluster MongoDB config exists -- the
  cluster benchmark is postgres-only). All of these are development
  containers on local SSD-backed storage, not a production HA topology --
  MongoDB in particular has no replica to make `{w: majority, j: true}`
  meaningfully redundant here, and none of these servers' storage is
  separated from the benchmark client's own disk I/O.
- **`/spend` was not coalesced across callers when these numbers were
  taken** (it now is on memory, sqlite, pebble and postgres -- see
  `docs/OPERATIONS.md`'s "Known Limits"; the paragraph below describes the
  build these runs measured). Reserve requests from many
  concurrent callers are batched by the per-pool coalescer into one store
  write per flush; `spend`/`release` are not -- `Pool.storeSpend`
  (`pkg/pool/service.go`) makes one store write per *request* (covering
  every outpoint that request names), but each caller's request is always
  its own store call, never merged with another's. So the spend:reserve
  write-transaction ratio is roughly the coalescer's batch size divided by
  `count` (outputs per reserve) -- not a fixed multiplier, and not
  necessarily unfavorable to spend (see `docs/OPERATIONS.md`'s "Known
  Limits" for the full reasoning). Every benchmark on this page ran in
  `spend` mode, so the numbers above already include whatever this ratio
  was for that shape; sqlite/pebble's group-commit (concurrent writers
  sharing one `fsync` per flush) applies to spend/release writes at the
  store layer the same as to reserve writes, so it is not only the reserve
  side that benefited from that fix.
- **Shard-adoption stability: fixed, with one documented residual limit.**
  The permanent shard-adoption stall this page's numbers originally exposed
  (an instance's allocator could stop adopting shards forever even with
  rows available elsewhere) is fixed as of `ba55a6f`/`a22be74`: a fully
  spent-out shard (every row terminal) is now retired outright rather than
  staying adoptable forever. One narrower case remains, documented in
  `docs/OPERATIONS.md`'s **"Known Limits"**: a shard whose rows are drained
  but not yet *terminal* (still reserved under a live TTL, or quarantined)
  is only excluded from re-adoption for 60s at a time, capped at a 256-entry
  list — if more than 256 such shards precede the live ones, adoption can
  still cycle. The planned fix, per `docs/OPERATIONS.md`, is a store-side
  `AcquireShard` that skips row-less shards server-side instead of relying
  on a bounded client-side exclusion list. **Pebble's `w64-c10` shape's
  errors (fixed by `e339617`/`8f3bb20`) were a separate, second mechanism,
  not this residual limit**: Pebble's slow reads under write load (a read
  waits for the WAL fsync of any batch it may have observed) exposed two
  shard-manager problems -- adoption sharing one goroutine with a
  per-owned-shard rescan pass (starving adoption below consumption), and a
  shard consumed while owned only being retired much later, once some
  future adoption happened to re-acquire it. Neither is the drained-but-
  not-terminal exclusion-list case above (which accounted for at most ~2%
  of acquisitions in the traced run), nor Pebble's `AcquireShard` scan cost
  (measured at the same order as sqlite's there, not a differentiator). See
  [`benchmarks/20260930-api-single.md`](benchmarks/20260930-api-single.md)'s
  "Rate caps used, and why" section for the full mechanism and the fixed
  numbers.
- **A separate, still-unfixed coalescer artifact can stall reserves for
  several seconds at a load-shape transition.** When many abandoned
  in-flight reservations are released at once (e.g. `fuelbench` switching
  load shapes, or many clients timing out together), the coalescer releases
  each one with its own store `Release` call while holding the pool's
  single flush slot, so new reserves queue up behind that one-by-one
  sequence and can hit `reserve_timeout` -- see `docs/OPERATIONS.md`'s
  "Known Limits" for the mechanism. This is documented, not fixed.

## Keeping this current

The `Makefile`'s `bench-run` target's `OUT` still defaults to `/tmp` for a
throwaway run; pass `OUT=docs/benchmarks/results/<name>.json` (or set
`LABEL`/`WORKERS`/`COUNT`/`DURATION`) to save a new result under
[`benchmarks/results/`](benchmarks/results/), then update this page's
summary table and the backend pages it links to.
