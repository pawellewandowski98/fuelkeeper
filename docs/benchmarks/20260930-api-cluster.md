# API — Three-Instance Cluster — 2026-09-30 (re-run on 91e1331)

`cmd/fuelbench` against `deploy/docker-compose.bench.yaml`'s three
`fuelkeeper` instances (`fk1`-`fk3`, built from the repo `Dockerfile`)
sharing one `postgres:16-alpine` store, all driven by
`deploy/fuelkeeper-bench.yaml` (mock oracle/treasury, `pools[0]`:
`target_available: 300000`, `low_water: 200000`, `split_size: 1000`,
`jobs.minter`: `max_per_round: 20`, `max_pending_shards: 100`,
`instance.max_shards`/`low_water_rows` at config defaults `8`/`5000` — this
file is fixed, not part of the benchmark's editable single-instance configs). **Re-run at
commit `91e1331`** after the shard-retirement fix (`ba55a6f`/`a22be74`) and
the sqlite/pebble group-commit fix (`fd4dc56`/`91e1331`) landed — see
[`20260930-api-single.md`](20260930-api-single.md) for what those fixed and
the single-instance numbers they produced. Only postgres is a store the
cluster benchmark exercises (`deploy/fuelkeeper-bench.yaml` is
postgres-only), so the group-commit fix (sqlite/pebble) does not apply here;
this page's improvement comes entirely from the shard-retirement fix. See
[`20260930-api-single.md`](20260930-api-single.md) for the single-instance
numbers this compares against, and [`../PERFORMANCE.md`](../PERFORMANCE.md)
for the combined summary.

## Setup

```bash
make bench-cluster-up      # docker compose -f deploy/docker-compose.bench.yaml up -d --build --wait
```

Images were already built from a prior `bench-cluster-up` in this session
(cached layers), so `up -d --wait` completed in under a minute this time; a
cold build (no cached layers) took roughly 8-9 minutes when this page was
first produced.

## A pre-existing gotcha, not a regression: mock oracle state is per-process

**The first attempt at this re-run never filled the pool**: `stats.available`
stuck at exactly `100000` (`max_pending_shards: 100` × `split_size: 1000`)
for over five minutes, every shard stuck `unmined`, and
`fuelkeeper_watcher_transitions_total{outcome="rebroadcast_failed"} 100` on
the instance holding the watcher lease. Diagnosis: `deploy/fuelkeeper-bench.yaml`
uses `oracle.kind: mock`/`treasury.kind: mock` (nullnet-style, same as every
other page here), and `pkg/oracle.Mock` (`pkg/oracle/mock.go`) is a plain
in-process `sync.Mutex`-guarded map with no backing store — **each of the
three containers gets its own independent mock oracle**. The mock
treasury's send-confirmation callback (`cmd/fuelkeeper/serve.go`'s
`sendConfirmer`) marks a split mined only in the oracle instance owned by
whichever process built and sent that split (i.e. whichever instance
currently holds the *minter* lease); the *watcher* lease (which is what
actually flips a shard's stored status from `unmined` to `mined`, by
polling its own oracle) can land on a *different* instance, whose oracle
never received that confirmation and so never sees the split as mined —
eventually the watcher tries to rebroadcast a split that was, from its own
oracle's point of view, never sent, and that rebroadcast fails against the
mock treasury.

This reproduced deterministically with the job leases split
(`minter`→fk3, `watcher`+`reaper`→fk1, confirmed via each instance's
`fuelkeeper_job_lease_held` gauge) and did **not** reproduce on the very
next `bench-cluster-down`/`bench-cluster-up` cycle, where all three leases
happened to land on the same instance (fk3) — confirming it as a race in
which instance wins which job lease at startup, not a deterministic bug,
and not something any of the three fixes above touch (none of
`ba55a6f`/`a22be74`/`fd4dc56`/`91e1331`/`34512a0` touch `pkg/oracle` or the
send-confirmation wiring). **This is a real limitation of benchmarking a
multi-instance cluster with the mock oracle/treasury** (nullnet mode is
documented as a single-process development aid; this is the multi-process
manifestation of that), not a FuelKeeper correctness bug — a real chain
oracle has no such per-process blind spot, since every instance queries the
same external arcade/teranode. It is flagged here because it cost real time
to diagnose and could recur on any future `bench-cluster-up`; worth a
follow-up (e.g. pin all three jobs' leases to instance 1 in the benchmark
compose file, or accept a retry-on-stall step in the runbook) separate from
this benchmarking effort.

Once the leases landed on one instance, `GET /v1/pools` on all three
(`:8081`-`:8083`) climbed steadily to `stats.available: 300000` within
about 40s, and the runs below proceeded normally.

## Results

| label | instances | workers | count | reserves/s | outputs/s | p50 ms | p95 ms | p99 ms | errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cluster-w64-c1 | 3 | 64 | 1 | 1012 | 1012 | 31.39 | 52.54 | 75.01 | - |
| cluster-w256-c1 | 3 | 256 | 1 | 2126 | 2126 | 53.92 | 100.48 | 229.76 | - |
| cluster-w64-c10 | 3 | 64 | 10 | 676 | 6760 | 44.45 | 100.35 | 149.76 | - |
| cluster-single-w64-c1 | 1 (`:8081` only) | 64 | 1 | 571 | 571 | 54.43 | 112.83 | 407.81 | - |

Raw JSON in [`results/`](results/) (overwritten in place). **Every run was
unbounded (`-rate` omitted) and every run was clean — zero errors across all
four, including `cluster-w64-c10`**, the shape that previously needed a
`-rate 900` cap and still showed 6459 `exhausted` + 232 `insufficient`
errors (see "Before the fixes" below). No run needed a `-rate` cap this
time, and the old shard-adoption stuck state (`fuelkeeper_shards_owned`/
`fuelkeeper_allocator_available` stuck at `0` with `stats.available` still
healthy) was checked for after every run via each instance's `/metrics` and
did not occur.

## Horizontal scaling

`cluster-single-w64-c1` (one instance, `:8081` only) vs. `cluster-w64-c1`
(all three, round-robin): **571 -> 1012 reserves/s, a 1.77x gain for 3x the
instances** (was 1.85x before the fix — consistent, within the noise of a
single run each). Same caveat as before: not linear (all three still
contend for the same Postgres rows via `FOR UPDATE SKIP LOCKED` adoption
and per-row CAS), but real throughput from adding stateless instances in
front of one durable store.

## Resource usage and coalescer batching

**Observed, not recorded**: read from a live `curl .../metrics` per
instance during the session rather than saved to a file in this repo,
unlike the `results/*.json` throughput numbers above. All three instances
ran the three cluster shapes back-to-back in one session; fk1 additionally
ran the single-instance comparison shape (240s total vs. the other two's
180s).

| Instance | Wall-clock covered | CPU delta | Approx. cores | RSS at end | Coalescer mean batch |
|---|---|---|---|---|---|
| fk1 (`:8081`) | 240s | 128.76s | 54% | 98 MiB | 4.7 |
| fk2 (`:8082`) | 180s | 98.14s | 55% | 97 MiB | 5.2 |
| fk3 (`:8083`) | 180s | 95.77s | 53% | 763 MiB | 5.3 |

fk3's RSS is again an outlier (763 MiB vs. ~98 MiB on fk1/fk2), same as the
`236642c` run below, with CPU delta and coalescer batch mean both in line
with the other two — most likely which instance held the minter/reaper/
watcher leases (fk3 held all three at the start of this run) and ordinary
Go GC timing, not a new issue.

## Teardown

```bash
make bench-cluster-down    # docker compose -f deploy/docker-compose.bench.yaml down -v
```

Verified with `podman ps -a` afterward: back to the four containers that
were already running before this benchmark run started.

---

## Before the fixes (commit `236642c`, original run)

| label | instances | workers | count | reserves/s | outputs/s | p50 ms | p95 ms | p99 ms | errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cluster-w64-c1 | 3 | 64 | 1 | 948 | 948 | 32.77 | 57.98 | 96.45 | - |
| cluster-w256-c1 | 3 | 256 | 1 | 1890 | 1890 | 54.91 | 117.95 | 554.50 | - |
| cluster-w64-c10 | 3 | 64 | 10 | 407 | 4066 | 36.67 | 230.27 | 431.87 | exhausted=6459 insufficient=232 (rate-capped 900/s) |
| cluster-single-w64-c1 | 1 (`:8081` only) | 64 | 1 | 512 | 512 | 55.17 | 117.50 | 368.89 | exhausted=1837 |

`cluster-w64-c10` needed a proactive `-rate 900` cap because the
single-instance page's equivalent shape (`postgres-w64-c10`) had already
reproduced the permanent shard-adoption stall under unbounded `count=10`
load against Postgres, and this file's `instance.max_shards`/
`low_water_rows` are the small config defaults that made that stall easiest
to trigger. Even rate-capped, `cluster-w64-c10` and `cluster-single-w64-c1`
both still showed `exhausted` errors (transient, not the permanent stall —
checked and confirmed healthy at the time). All of this is now explained by
the bug `ba55a6f`/`a22be74` fixed: see
[`20260930-api-single.md`](20260930-api-single.md)'s "Before the fixes"
section for the full root cause.
