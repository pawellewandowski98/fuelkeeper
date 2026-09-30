# API — Single Instance per Backend — 2026-09-30

`cmd/fuelbench` load-generator runs against one `fuelkeeper serve` process per
store backend (memory, sqlite, pebble, postgres, mongodb, aerospike), all on
`127.0.0.1` with a mock oracle and mock treasury (nullnet-style), at commit
`236642c`. This measures the public HTTP API end to end (coalescer, shard
adoption, store) for one instance; see
[`20260930-api-cluster.md`](20260930-api-cluster.md) for three instances
sharing one Postgres store, and [`../PERFORMANCE.md`](../PERFORMANCE.md) for
how all three benchmark pages fit together.

## Configuration

Each backend's config is `examples/nullnet.yaml` (or `examples/pebble.yaml`
for the pebble block) with:

- `pools[0]`: `denomination: 50`, `split_size: 1000`, `target_available:
  300000`, `low_water: 150000`, `spend_policy: trust`
- `jobs.minter`: `interval: 1s`, `max_per_round: 100`, `max_pending_shards:
  1000` (raised from the nullnet example's `5`/`20` so 300 shards mint in
  seconds rather than minutes)
- `jobs.reaper`/`watcher`: `interval: 1s`, short grace/quarantine ages
  (nullnet defaults), `watcher.limit: 2000` (must exceed `max_pending_shards`)
- `instance.max_shards: 64`, `instance.low_water_rows: 20000` (raised from
  the template defaults of `8`/`5000` — see "Shard-adoption stability" below
  for why)
- `oracle.kind: mock` (`assume_unspent: true`), `treasury.kind: mock`
  (`confirm_after: 500ms`)
- `store.backend` set per run; postgres used a dedicated database
  (`fuelkeeper_bench6` on the already-running `localhost:15432` server,
  dropped and recreated between the two sessions described below, dropped
  for good afterward); mongodb used a dedicated database
  (`fuelkeeper_bench6` on `localhost:27017`, dropped afterward); aerospike
  used the existing `fk_`-prefixed sets on the shared `localhost:3000`
  namespace `test` with a private `key_prefix` (`bench6api`), truncated via
  `Store.Truncate()` afterward (see "Aerospike cleanup" below); sqlite and
  pebble used per-run data directories under the task's temp dir, deleted
  afterward
- `api.listen: "127.0.0.1:<port>"` (one port per backend, 18101-18106); no
  API key needed (loopback listener, `fuelbench` ran with no `-key`)

Each server was started with `go run ./cmd/fuelkeeper serve --config
<cfg>`, polled via `GET /v1/pools` until `stats.available >= 200000` (all
six backends reached `300000`, i.e. `target_available`, within 5-20s), then
driven with:

```
fuelbench -url http://127.0.0.1:<port> -workers 64  -count 1  -duration 60s -label <backend>-w64-c1  -backend <backend> -pool main -out docs/benchmarks/results/<backend>-w64-c1.json
fuelbench -url http://127.0.0.1:<port> -workers 256 -count 1  -duration 60s -label <backend>-w256-c1 -backend <backend> -pool main -out docs/benchmarks/results/<backend>-w256-c1.json
fuelbench -url http://127.0.0.1:<port> -workers 64  -count 10 -duration 60s -label <backend>-w64-c10 -backend <backend> -pool main -out docs/benchmarks/results/<backend>-w64-c10.json
```

`-mode` defaults to `spend` (outputs are consumed, not returned), so the
pool's supply is drawn down by every successful reserve and only restored by
the minter.

### Shard-adoption stability (rate caps used)

**Finding.** Under sustained load from many concurrent workers (`-workers
256`, or `-workers 64 -count 10`), this instance's in-memory shard allocator
can permanently stop adopting new shards once its local row buffer empties,
even though the store still holds plenty of available rows in other, unowned
mined shards (`GET /v1/pools` kept reporting `stats.available` in the
hundreds of thousands while `fuelkeeper_shards_owned` and
`fuelkeeper_allocator_available` stayed at `0`, `fuelkeeper_reserve` returned
`exhausted` for every request including single manual, unloaded probes
minutes later, and `fuelkeeper_shard_adoptions_total{result=~"ok|none"}`
stopped incrementing entirely). Reproduced on three different backends
(memory, postgres, aerospike); on postgres it first surfaced behind a
transient Postgres error (`could not resize shared memory segment ... No
space left on device`, i.e. a `/dev/shm` limit on the container — itself a
separate, environmental constraint, not a FuelKeeper bug), but the instance
never recovered adoption afterward even once that condition cleared,
matching the pattern seen on memory and aerospike with no external trigger at
all. This looks like a genuine concurrency bug in `pkg/pool`'s shard manager
(most likely `ShardManager.EnsureRows`'s `adoptSem` or a goroutine in its
rotate/adopt path never releasing under contention) rather than anything
specific to one store backend; it is out of scope for this benchmarking task
to fix, and is called out here so it can be filed and root-caused separately.
`sqlite` and `pebble` never hit this exact stuck state in these runs — their
own store-side write-lock contention (see below) keeps demand far below the
threshold that triggers it — but produced plenty of transient errors of
their own.

**Workaround used.** Per shape, if an unbounded (`-rate` omitted) run showed
`exhausted` errors, the run was repeated from a freshly-restarted server
(clean pool, clean allocator) with `-rate` capped low enough to keep the
local buffer from ever reaching zero for the full 60s. Rates were found by
direct trial at full 60s duration (a clean short probe at a given rate did
not reliably predict a clean full-length run — the failure is time/adoption-
cycle-dependent, not purely a function of instantaneous rate). The rates
actually used:

| Backend   | w64-c1        | w256-c1       | w64-c10       | Notes |
|-----------|---------------|---------------|---------------|-------|
| memory    | 3000/s (capped) | 1000/s (capped) | 300/s (capped) | unbounded hit the stuck state on w256-c1 and w64-c10 |
| sqlite    | unbounded     | unbounded     | unbounded     | store-side lock contention self-limits demand; no stuck state |
| pebble    | unbounded     | unbounded     | unbounded     | same as sqlite |
| postgres  | unbounded     | unbounded     | 300/s (capped) | unbounded w64-c10 hit the stuck state (behind the transient `/dev/shm` error above); w64-c1/w256-c1 were clean unbounded |
| mongodb   | unbounded     | unbounded     | unbounded     | transient timeouts under w64-c10 but self-recovered (not the stuck state) |
| aerospike | 3000/s (capped) | 1000/s (capped) | 300/s (capped) | unbounded w64-c1 alone drained the pool fast enough (5332 reserves/s) to hit the stuck state; capped proactively for the other two shapes too |

Because of the restarts above, RSS/CPU/coalescer figures below are captured
per *session* (one or more shapes run back-to-back on one live server,
noted per backend), not uniformly across all three shapes of a backend — see
each backend's note.

## Results

| label | backend | workers | count | reserves/s | outputs/s | p50 ms | p95 ms | p99 ms | errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| memory-w64-c1 | memory | 64 | 1 | 2885 | 2885 | 0.34 | 9.25 | 37.66 | - |
| memory-w256-c1 | memory | 256 | 1 | 1000 | 1000 | 0.34 | 25.52 | 55.01 | - |
| memory-w64-c10 | memory | 64 | 10 | 300 | 3000 | 0.66 | 16.93 | 42.43 | - |
| sqlite-w64-c1 | sqlite | 64 | 1 | 59 | 59 | 535.04 | 944.13 | 1120.26 | unavailable=15 |
| sqlite-w256-c1 | sqlite | 256 | 1 | 13 | 13 | 722.43 | 1789.95 | 1949.69 | unavailable=6906 |
| sqlite-w64-c10 | sqlite | 64 | 10 | 53 | 526 | 674.30 | 1073.15 | 1488.89 | unavailable=79 |
| pebble-w64-c1 | pebble | 64 | 1 | 63 | 63 | 478.72 | 956.93 | 1362.94 | unavailable=2 |
| pebble-w256-c1 | pebble | 256 | 1 | 13 | 13 | 870.91 | 1850.37 | 1970.17 | unavailable=6369 |
| pebble-w64-c10 | pebble | 64 | 10 | 52 | 524 | 547.84 | 1075.20 | 1362.94 | exhausted=1221 insufficient=10 |
| postgres-w64-c1 | postgres | 64 | 1 | 789 | 789 | 41.44 | 73.60 | 225.15 | - |
| postgres-w256-c1 | postgres | 256 | 1 | 1788 | 1788 | 70.78 | 120.38 | 153.22 | - |
| postgres-w64-c10 | postgres | 64 | 10 | 275 | 2749 | 27.60 | 134.27 | 596.99 | - (rate-capped 300/s; unbounded attempt hit the stuck state, see above) |
| mongodb-w64-c1 | mongodb | 64 | 1 | 975 | 975 | 31.70 | 60.64 | 86.02 | - |
| mongodb-w256-c1 | mongodb | 256 | 1 | 3322 | 3322 | 39.62 | 72.96 | 97.53 | - |
| mongodb-w64-c10 | mongodb | 64 | 10 | 446 | 4461 | 51.55 | 107.01 | 264.96 | exhausted=6437 insufficient=83 (transient; self-recovered, see above) |
| aerospike-w64-c1 | aerospike | 64 | 1 | 2999 | 2999 | 0.30 | 0.71 | 1.15 | exhausted=48 (rate-capped 3000/s) |
| aerospike-w256-c1 | aerospike | 256 | 1 | 1000 | 1000 | 0.38 | 0.88 | 1.14 | exhausted=16 (rate-capped 1000/s) |
| aerospike-w64-c10 | aerospike | 64 | 10 | 133 | 1334 | 0.70 | 1.69 | 84.16 | exhausted=9554 insufficient=117 (rate-capped 300/s; transient, self-recovered) |

Raw JSON for every row is in [`results/`](results/); render any subset
yourself with `fuelbench render docs/benchmarks/results/*.json`.

**sqlite and pebble are latency-bound by their single-writer lock**, not by
the pool/coalescer: both show sub-100 reserves/s and hundreds-of-milliseconds
p50 latency under concurrent load, consistent with `busy_timeout` queuing
(sqlite) and pebble's exclusive-writer semantics under `no_sync: false`
(fsync every write batch) — see
[`20260930-store-micro.md`](20260930-store-micro.md)'s pebble/`AcquireShard`
note for a related, compounding cost. Both are the two single-process,
durable-on-a-laptop-disk backends the docs recommend for low-throughput,
single-instance deployments, not high-concurrency ones; Postgres, MongoDB and
Aerospike (all multi-process-capable) comfortably handle 1000-3000+
reserves/s here.

## Resource usage and coalescer batching

Captured from `/metrics` (`process_resident_memory_bytes`,
`process_cpu_seconds_total`, `fuelkeeper_coalescer_batch_size_{sum,count}`).
CPU delta / wall-clock gives an approximate fraction of one core consumed by
the server process (not the `fuelbench` client, which ran as a separate
process) over the session.

| Backend | Session covers | Wall-clock | CPU delta | Approx. cores | RSS at end | Coalescer mean batch (ops/flush) |
|---|---|---|---|---|---|---|
| memory | w64-c10 only (own restart; w64-c1+w256-c1 ran in an earlier session without a captured "after") | 60s | 23.49s | 39% | 841 MiB | 10.8 |
| sqlite | w64-c1 + w256-c1 + w64-c10, one continuous session | 180s | 54.35s | 30% | 393 MiB | 113.0 |
| pebble | w64-c1 + w256-c1 + w64-c10, one continuous session | 180s | 45.19s | 25% | 553 MiB | 125.4 |
| postgres | w64-c10 only (its own restarted session; "before" is approximated from the prior session's post-startup baseline, since the process itself was restarted — see the stability note above) | ~60s | ~17.45s | ~29% | 487 MiB | 12.4 |
| mongodb | w64-c1 + w256-c1 + w64-c10, one continuous session | 180s | 165.61s | 92% | 802 MiB | 16.4 |
| aerospike | w64-c1 + w256-c1 + w64-c10, one continuous (rate-capped) session | 180s | 217.18s | 121% | 668 MiB | 1.3 |

**Reading the coalescer batch size**: it is the number of concurrent
`Reserve` ops one coalescer flush covers, which drives how much a batch
amortizes a durable commit's fixed cost (see
[`20260930-store-micro.md`](20260930-store-micro.md)). Aerospike's mean
batch (1.3) is barely above 1 — its per-op latency is sub-millisecond (p50
0.3-0.7ms above), so there is rarely more than one `Reserve` call queued
when a flush fires. Sqlite and pebble show the largest mean batches (113,
125) precisely because their write path is the bottleneck: many concurrent
callers pile up behind one slow flush, so each flush that does go through
carries far more ops. Postgres/mongodb/memory fall in between.
**mongodb and aerospike's client-side CPU cost is disproportionately high**
(92% and 121% of a core respectively, vs. 25-39% for the others) — both use
Go client libraries that keep their own connection/event-loop goroutines
busy independent of the coalescer's own batching, which is worth keeping in
mind when sizing a host to run several pools/backends on one box.

## Aerospike cleanup

The aerospike runs used `set_prefix: fk_` (the fixed, shared set names) and
`key_prefix: bench6api`, matching `pkg/store/aerospike`'s test-helper
convention (`pkg/store/aerospike/aerospike_test.go`'s `testSetPrefix`/
`randKeyPrefix`). After each aerospike session, a small program built from
`pkg/store/aerospike.New` + `Store.Truncate()` (the same cleanup the
package's own tests use, `pkg/store/aerospike/fixes_test.go`'s
`TestKeyPrefixIsolationAndTruncate`) removed every record tagged with
`bench6api` from the shared `fk_*` sets without touching any other tenant's
data. `asinfo -v 'sets/test' | tr ';' '\n' | grep -c 'set='` on
`arcade-aerospike` read **2823** immediately before the first aerospike run
and **2823** again after the final truncate — no new set names were created.
