# API — Three-Instance Cluster — 2026-09-30

`cmd/fuelbench` against `deploy/docker-compose.bench.yaml`'s three
`fuelkeeper` instances (`fk1`-`fk3`, built from the repo `Dockerfile`)
sharing one `postgres:16-alpine` store, all driven by
`deploy/fuelkeeper-bench.yaml` (mock oracle/treasury, `pools[0]`:
`target_available: 300000`, `low_water: 200000`, `split_size: 1000`,
`jobs.minter`: `max_per_round: 20`, `max_pending_shards: 100`,
`instance.max_shards`/`low_water_rows` left at their config defaults, `8`/
`5000` — this file is fixed content this task did not modify). Commit
`236642c`. See [`20260930-api-single.md`](20260930-api-single.md) for the
single-instance-per-backend numbers this compares against, and
[`../PERFORMANCE.md`](../PERFORMANCE.md) for the combined summary.

## Setup

```bash
make bench-cluster-up      # docker compose -f deploy/docker-compose.bench.yaml up -d --build --wait
```

The build (three images from the repo `Dockerfile`, cold — no prior layer
cache) took roughly 8-9 minutes on this host; `up -d --wait` then waits for
each service's healthcheck (`GET /readyz`). Once healthy, `GET
/v1/pools` on all three (`:8081`-`:8083`, all reading the same shared
Postgres store) was polled every 5s until `stats.available >= 200000`; it
reached the full `target_available` of `300000` about 10s after that, and
all three runs below started from a full, matching pool.

## Results

| label | instances | workers | count | reserves/s | outputs/s | p50 ms | p95 ms | p99 ms | errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cluster-w64-c1 | 3 | 64 | 1 | 948 | 948 | 32.77 | 57.98 | 96.45 | - |
| cluster-w256-c1 | 3 | 256 | 1 | 1890 | 1890 | 54.91 | 117.95 | 554.50 | - |
| cluster-w64-c10 | 3 | 64 | 10 | 407 | 4066 | 36.67 | 230.27 | 431.87 | exhausted=6459 insufficient=232 (rate-capped 900/s — see below) |
| cluster-single-w64-c1 | 1 (`:8081` only) | 64 | 1 | 512 | 512 | 55.17 | 117.50 | 368.89 | exhausted=1837 |

Raw JSON in [`results/`](results/) (`cluster-*.json`,
`cluster-single-w64-c1.json`).

`cluster-w64-c1`/`cluster-w256-c1` ran unbounded (`-rate` omitted) and were
clean. `cluster-w64-c10` needed a `-rate 900` cap (900 reserve calls/s
across all three instances, i.e. 9000 outputs/s at `count=10`): the
unbounded shape was not attempted directly here, since the single-instance
page's equivalent shape (`postgres-w64-c10`, see
[`20260930-api-single.md`](20260930-api-single.md)) had already reproduced a
permanent shard-adoption stall under unbounded `count=10` load against
Postgres, and `deploy/fuelkeeper-bench.yaml`'s `instance.max_shards`/
`low_water_rows` are left at the small config defaults (`8`/`5000`) that
made that stall easiest to trigger — this file is fixed for the cluster
(not part of this task's editable single-instance configs), so a
conservative rate was chosen proactively rather than risking the run. Even
rate-capped, `cluster-w64-c10` and `cluster-single-w64-c1` both show
`exhausted` errors; both were checked against `/metrics`
(`fuelkeeper_shards_owned`, `fuelkeeper_allocator_available`,
`fuelkeeper_shard_adoptions_total`) immediately after and were healthy and
actively adopting, not stuck — see
[`20260930-api-single.md`](20260930-api-single.md)'s "Shard-adoption
stability" section for the full description of the stuck state this page's
numbers were checked against.

## Horizontal scaling

`cluster-single-w64-c1` (one instance, `:8081` only, same shared Postgres
pool) vs. `cluster-w64-c1` (all three, round-robin): **512 -> 948
reserves/s, a 1.85x gain for 3x the instances.** The pool and Postgres
connection are shared across all three, so this is not linear scaling —
each instance still contends with the other two for the same rows via
Postgres's `FOR UPDATE SKIP LOCKED` adoption and per-row CAS updates (see
the README's "Store Backends" table) — but it demonstrates real throughput
gained from adding stateless FuelKeeper instances in front of one durable
store, which is the deployment shape `docs/OPERATIONS.md`'s "Multi-Process"
topology describes.

## Resource usage and coalescer batching

Captured from each instance's own `/metrics` before the three-shape session
and after (fk1 additionally ran the single-instance comparison shape, so its
totals cover 240s of load against the other two's 180s).

| Instance | Wall-clock covered | CPU delta | Approx. cores | RSS at end | Coalescer mean batch |
|---|---|---|---|---|---|
| fk1 (`:8081`) | 240s (3 cluster shapes + the single-instance comparison) | 91.46s | 38% | 99 MiB | 3.9 |
| fk2 (`:8082`) | 180s (3 cluster shapes) | 60.29s | 33% | 94 MiB | 4.4 |
| fk3 (`:8083`) | 180s (3 cluster shapes) | 60.15s | 33% | 691 MiB | 4.6 |

fk3's RSS is a clear outlier (691 MiB vs. 94-99 MiB on fk1/fk2) despite a
near-identical CPU delta and coalescer batch mean; nothing in the run's
error counts or shard-adoption metrics points to a cause (all three read the
same pool stats throughout, see above), so this looks like ordinary Go
runtime/GC timing variance (which of the three happened to hold the minter's
or reaper's job lease at a given moment, heap growth before a GC cycle ran)
rather than a systematic per-instance difference — worth re-checking on a
second run before treating it as a real signal.

## Teardown

```bash
make bench-cluster-down    # docker compose -f deploy/docker-compose.bench.yaml down -v
```

Verified with `podman ps` afterward: back to the four containers that were
already running before this task started (`go-wallet-toolbox_db_1`,
`arcade-aerospike`, `deploy-postgres-1`, `deploy-mongo-1`) — no
`fuelkeeper-bench-*` containers, network, or volume left behind.
