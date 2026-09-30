# Performance

Published benchmark results live under [`docs/benchmarks/`](benchmarks/README.md).
This page is the entry point: method, the headline numbers, capacity
guidance, the correctness audit statement, and caveats. All numbers below
are from commit `236642c` (2026-09-30) unless stated otherwise.

## Method

Three harnesses, each measuring a different slice of the system:

1. **Store microbenchmarks** (`go test -bench`, per `pkg/store/*` package —
   [`benchmarks/20260930-store-micro.md`](benchmarks/20260930-store-micro.md)).
   No HTTP, no pool coalescer, no jobs: just `Store.Reserve`,
   `Store.AcquireShard` and `Store.Quarantine` called directly, in-process.
   This isolates each backend's raw commit/lookup cost from everything
   FuelKeeper builds on top of it.
2. **API, single instance per backend** (`cmd/fuelbench` against one
   `fuelkeeper serve` process, mock oracle/treasury —
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
   store — [`benchmarks/20260930-api-cluster.md`](benchmarks/20260930-api-cluster.md)).
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
| memory | 2885 | 3000 | n/a (single-process only) | 38 ms |
| sqlite | 59 | 526 | n/a (single-process only) | 1120 ms |
| pebble | 63 | 524 | n/a (single-process only) | 1363 ms |
| postgres | 789 | 2749 | 948 | 225 ms |
| mongodb | 975 | 4461 | n/a (not benchmarked clustered) | 86 ms |
| aerospike | 2999 (rate-capped) | 1334 (rate-capped) | n/a (not benchmarked clustered) | 1 ms |

**memory and aerospike are the fastest by a wide margin** (sub-millisecond
p99 at the store layer, and the highest sustainable rates before a
proactive `-rate` cap was needed to avoid the shard-adoption stability issue
below); **sqlite and pebble are the slowest under concurrency**, bottlenecked
by their single-writer lock, not by the pool logic on top — see
[`benchmarks/20260930-api-single.md`](benchmarks/20260930-api-single.md) for
the latency breakdown. **Postgres is the only backend measured clustered**
(the benchmark cluster's fixed config, `deploy/fuelkeeper-bench.yaml`, only
stands up a Postgres-backed cluster); it and mongodb are the two
multi-process backends most likely to be run in production alongside
aerospike (see `docs/OPERATIONS.md`'s "Deployment Topologies").

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
this reason, sized comfortably above the ~1000-3000 reserves/s measured
above.

**For horizontal scaling**, the cluster benchmark's single-instance-vs-
three-instance comparison
([`benchmarks/20260930-api-cluster.md`](benchmarks/20260930-api-cluster.md)'s
"Horizontal scaling" section) measured 512 -> 948 reserves/s (1.85x) going
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
is **not** a double hand-out, it is the CAS mechanism working as designed
(`docs/OPERATIONS.md`: "brief moments when two instances briefly serve the
same shard; it clears within one rescan cycle"). It was nonzero in two of
this task's own benchmark runs under concurrent load (479 on the aerospike
session, 191 on the mongodb session; see each backend's `/metrics` snapshot
in [`benchmarks/20260930-api-single.md`](benchmarks/20260930-api-single.md))
and is expected to be: what matters is that it stays bounded and clears, not
that it stays at zero.

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
  what a real-chain run would additionally measure.
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
- **A genuine shard-adoption stability issue was found and worked around,
  not fixed.** Under sustained high-concurrency load (many workers and/or
  `count > 1`), this instance's shard allocator can permanently stop
  adopting new shards even though the store holds plenty of available rows
  elsewhere — reproduced on memory, postgres and aerospike backends. Several
  shapes in [`benchmarks/20260930-api-single.md`](benchmarks/20260930-api-single.md)
  and [`benchmarks/20260930-api-cluster.md`](benchmarks/20260930-api-cluster.md)
  needed a `-rate` cap (found by direct trial, recorded per shape) to avoid
  it; this is very likely a real concurrency bug in `pkg/pool`'s shard
  manager (see the single-instance page's "Shard-adoption stability"
  section for the full evidence) and should be filed and root-caused
  separately from this benchmarking task.

## Keeping this current

The `Makefile`'s `bench-run` target's `OUT` still defaults to `/tmp` for a
throwaway run; pass `OUT=docs/benchmarks/results/<name>.json` (or set
`LABEL`/`WORKERS`/`COUNT`/`DURATION`) to save a new result under
[`benchmarks/results/`](benchmarks/results/), then update this page's
summary table and the backend pages it links to.
