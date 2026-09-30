# API — Single Instance per Backend — 2026-09-30 (re-run on 91e1331; pebble re-run again on 8f3bb20)

`cmd/fuelbench` load-generator runs against one `fuelkeeper serve` process per
store backend (memory, sqlite, pebble, postgres, mongodb, aerospike), all on
`127.0.0.1` with a mock oracle and mock treasury (nullnet-style). Five
backends are at commit **`91e1331`**; pebble was re-run a second time at
**`8f3bb20`** once its `w64-c10` errors were root-caused (see "Rate caps
used, and why" below) — see "Before the fixes" at the bottom for the
original `236642c` data and why it looked the way it did:

- **`ba55a6f` + `a22be74`** (`pkg/pool`): a fully spent-out shard (every row
  terminal) is now retired outright instead of staying `mined` and
  adoptable forever; a rescan's own store listing can no longer re-add a row
  a concurrent `Reserve` already popped. This is the fix for the permanent
  shard-adoption stall this page originally reported.
- **`fd4dc56` + `91e1331`** (`pkg/store/sqlite`, `pkg/store/pebble`):
  concurrent writers now share one `fsync` per group-commit flush instead of
  paying one `fsync` per writer; durability is unchanged (`no_sync`/`sync`
  settings still mean what they did).
- **`34512a0`** (`pkg/store/memory`): `Stats`'s row counts now read
  incrementally maintained per-state counters in O(1) instead of a full
  scan; the shard-status counts `Stats` also returns stay O(shards),
  unchanged.
- **`e339617` + `8f3bb20`** (`pkg/pool/shardmgr.go`): adoption now runs on
  its own goroutine, separate from the renew/rescan maintenance loop; a
  rescan or `max_shards` rotation that drops a fully spent-out shard retires
  it outright instead of leaving it `"empty"` for a later adoption to find.
  This is the fix for pebble's `w64-c10` errors below.

This measures the public HTTP API end to end (coalescer, shard adoption,
store) for one instance; see
[`20260930-api-cluster.md`](20260930-api-cluster.md) for three instances
sharing one Postgres store, and [`../PERFORMANCE.md`](../PERFORMANCE.md) for
how all three benchmark pages fit together.

## Configuration

Identical to the original run: each backend's config is
`examples/nullnet.yaml` (or `examples/pebble.yaml` for the pebble block)
with `pools[0]`: `denomination: 50`, `split_size: 1000`, `target_available:
300000`, `low_water: 150000`, `spend_policy: trust`; `jobs.minter`:
`interval: 1s`, `max_per_round: 100`, `max_pending_shards: 1000`;
`instance.max_shards: 64`, `instance.low_water_rows: 20000`; `oracle.kind:
mock`, `treasury.kind: mock` (`confirm_after: 500ms`). Store setup per
backend (dedicated Postgres/MongoDB databases, Aerospike `key_prefix`,
sqlite/pebble data directories) is unchanged from the original run — see
that page's git history for the full per-backend detail if needed.

**This time**, each server ran from a binary built fresh from HEAD, not
`go run`; pebble's data was wiped and its sequence run a second time,
identically, once `git rev-parse HEAD` was `8f3bb20`:

```bash
git rev-parse HEAD   # 91e1331a65d9a031f1fa1003b1f265b821144ba1 (five backends), then 8f3bb20a674db6358a219c1783b90c749dd23d3c (pebble re-run)
go build -o <tmp>/fuelkeeper ./cmd/fuelkeeper
go build -o <tmp>/fuelbench  ./cmd/fuelbench
<tmp>/fuelkeeper serve --config <cfg>
```

Polled via `GET /v1/pools` until `stats.available >= 200000` (all six
backends reached `300000` within 20s, most within 5-10s), then driven with
the same three shapes as before, **every shape attempted unbounded
(`-rate` omitted) first**:

```
fuelbench -url http://127.0.0.1:<port> -workers 64  -count 1  -duration 60s -label <backend>-w64-c1  -backend <backend> -pool main -out docs/benchmarks/results/<backend>-w64-c1.json
fuelbench -url http://127.0.0.1:<port> -workers 256 -count 1  -duration 60s -label <backend>-w256-c1 -backend <backend> -pool main -out docs/benchmarks/results/<backend>-w256-c1.json
fuelbench -url http://127.0.0.1:<port> -workers 64  -count 10 -duration 60s -label <backend>-w64-c10 -backend <backend> -pool main -out docs/benchmarks/results/<backend>-w64-c10.json
```

## The old stuck state did not recur

Every run was checked against `/metrics`
(`fuelkeeper_shards_owned`, `fuelkeeper_allocator_available`,
`fuelkeeper_shard_adoptions_total`, `fuelkeeper_pool_refilling`) immediately
after, and any run showing `exhausted`/`insufficient` errors was
additionally checked with a manual, unloaded `POST .../reserve` a few
seconds later. In every case that succeeded and `fuelkeeper_shard_adoptions_total{result="ok"}`
was still incrementing under renewed load — the permanent "zero owned
shards forever, store still has rows" state this page originally documented
did not occur anywhere in this re-run, including on memory, postgres and
aerospike (the three backends that reproduced it before) at `w64-c10`, the
shape that triggered it most reliably. This is the expected result of
`ba55a6f`/`a22be74`.

## Rate caps used, and why

Two of eighteen runs needed a `-rate` cap; both were **legitimate** drain
(the unbounded run's own `/metrics` showed `fuelkeeper_pool_refilling{pool="main"} 1`
— the minter actively trying to catch up — while `stats.available` fell),
not the old stuck state, confirmed by a clean, responsive manual reserve
immediately after each unbounded attempt:

| Backend/shape | Unbounded result (observed, not recorded)* | Why capped | Capped result (recorded, in `results/`) |
|---|---|---|---|
| memory `w64-c10` | 5572 reserves/s, 55,722 outputs/s, 1.6% errors (`exhausted`+`insufficient`) | `pool_refilling=1` while draining: at memory's new speed, sustained `count=10` demand from 64 workers occasionally outran `jobs.minter`'s `max_per_round: 100` shards/round refill capacity | `-rate 4000`: 1931 reserves/s, 19,311 outputs/s, 7 errors total (worker-count-bound at this pacing, not rate-bound — see below) |
| aerospike `w64-c10` | 2618 reserves/s, 26,183 outputs/s, 9.4% errors | `pool_refilling=1` while draining, same pattern as memory | `-rate 1500`: exactly 1500 reserves/s, 15,000 outputs/s, **zero errors** |

\* The unbounded attempt's numbers above were read from its terminal output
during the session; that attempt was superseded by the capped rerun and no
JSON for it was kept, so treat this column as an observation, not a
verifiable artifact — only the "Capped result" column and `results/*.json`
are.

memory's capped run only reached 1931 reserves/s against a 4000/s cap, so
something other than the rate limiter was also constraining it. The run's
own p50/p99 spread (1.0ms / 1013ms -- three orders of magnitude apart) is
the relevant clue: with 64 workers at a roughly uniform round-trip latency,
p50 and p99 would sit close together, not 1000x apart. A gap this wide
means a minority of requests are individually stalling for close to a
second, not that every request is paying a uniformly higher cost -- most
likely occasional waits tied to the `pool_refilling` churn the cap was
meant to reduce, though this run doesn't pin down the exact mechanism.

**Pebble `w64-c10` was never capped, and its errors are now root-caused and
fixed** (`e339617` + `8f3bb20`). At `91e1331` it answered `exhausted=3770`,
`insufficient=37`, `unavailable=64` out of ~18.8K attempts; re-run fresh
(wiped data dir, same three shapes, same config) once `HEAD` reached
`8f3bb20`, `w64-c10` completed with **zero errors of any kind** (see
"Results" below) at 291.9 reserves/s, up from 249.

The cause: Pebble's documented durability rule means a read waits for the
WAL fsync of any batch it may have observed, so under sustained write load
`ListAvailable` averaged 35-60 ms per call (vs. ~2.4 ms on sqlite at the
same shape) -- slow enough to expose two separate problems in the shard
manager, neither of them pebble-specific in mechanism, just pebble-specific
in how badly the slow reads amplified them:

1. **Adoption and rescanning shared one goroutine.** The same loop that
   pulls in new shards on hunger also reloads every owned shard's rows once
   per `rescan_interval`. At 20-50 owned shards, one rescan pass took
   1-3.5 s at pebble's read latency, so the loop spent roughly 95% of its
   time rescanning and adopted shards more slowly than sustained
   `count=10` reserves consumed them -- the allocator drained to zero with
   the store still holding 150K+ rows.
2. **A shard consumed while owned wasn't retired until much later.** When a
   shard's rows all became terminal (spent) while this instance still owned
   it, the shard was dropped with reason `"empty"` (left `mined`, not
   retired) and only actually retired once some *later* adoption happened
   to re-acquire it after its 60-second re-adoption exclusion expired.
   Roughly every other acquisition was therefore a dead shard, and after an
   idle period the whole backlog of overdue retirements came due at once.

**What it was not**: `docs/OPERATIONS.md`'s "Known Limits" exclusion-list
residual (shards merely drained-but-not-terminal, as opposed to fully spent
out, were at most ~2% of acquisitions in the traced run -- not the
dominant effect) and not [`20260930-store-micro.md`](20260930-store-micro.md)'s
note about `AcquireShard`'s cost scaling with already-leased shard count
(measured at ~50-60 ms in the traced run, the same order as sqlite's --
not the differentiator either). The fix: adoption now runs on its own
goroutine, so it is never blocked behind a rescan pass (renew, rescan, and
flagged rescans stay together on the maintenance loop); and a rescan or
`max_shards` rotation that finds an owned shard empty now retires it
outright when every row is terminal (`reason="retired"`, no exclusion
entry needed), the same check adoption already applied, instead of leaving
it `"empty"` for a later adoption to stumble onto. See
`docs/ARCHITECTURE.md`'s "Shard manager" and `docs/OPERATIONS.md`'s
"Adoption Behavior" for the mechanism. `e339617`/`8f3bb20` touch
`pkg/pool/shardmgr.go` generally, not just pebble's path; only pebble was
re-run here, but a spot check of the memory backend's `w64-c1`/`w64-c10`
shapes at `8f3bb20` found the difference within noise (23.5K vs. 24.6K
reserves/s at `w64-c1`; identical throughput at `w64-c10`, 0 `exhausted`) --
the other backends' numbers on this page were not re-run and remain at
`91e1331`.

**`unavailable=64` was a separate, still-unfixed artifact**, not part of
the above: at the `w256-c1` -> `w64-c10` shape transition, the coalescer
holds its single flush slot while it releases the *previous* shape's
abandoned in-flight reservations, one store `Release` (one `fsync`) per
token (`pkg/pool/coalescer.go`'s `deliver`/`releaseByToken`) -- so every
new reserve queues up behind that one-by-one release sequence, and each
waiting worker can hit `reserve_timeout` (2s) once or more before its turn.
This is documented, not fixed (see `docs/OPERATIONS.md`'s "Known Limits").
It happened to register zero times in this page's recorded re-run (a
timing-dependent artifact, not deterministic); the fix authors' own
reproduction saw `unavailable=192` under the same shape transition on a
different run.

## Results

| label | backend | workers | count | reserves/s | outputs/s | p50 ms | p95 ms | p99 ms | errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| memory-w64-c1 | memory | 64 | 1 | 24364 | 24364 | 0.90 | 3.90 | 7.27 | - |
| memory-w256-c1 | memory | 256 | 1 | 40516 | 40516 | 2.89 | 10.41 | 31.34 | - |
| memory-w64-c10 | memory | 64 | 10 | 1931 | 19311 | 1.00 | 7.80 | 1013.25 | exhausted=6 insufficient=1 (rate-capped 4000/s, see above) |
| sqlite-w64-c1 | sqlite | 64 | 1 | 707 | 707 | 47.84 | 84.73 | 144.13 | - |
| sqlite-w256-c1 | sqlite | 256 | 1 | 1888 | 1888 | 77.12 | 147.46 | 205.57 | - |
| sqlite-w64-c10 | sqlite | 64 | 10 | 303 | 3030 | 130.94 | 225.02 | 430.85 | exhausted=9 insufficient=1 |
| pebble-w64-c1 | pebble | 64 | 1 | 675 | 675 | 43.01 | 117.12 | 201.22 | - (commit `8f3bb20`, re-run) |
| pebble-w256-c1 | pebble | 256 | 1 | 1892 | 1892 | 73.41 | 147.97 | 201.09 | - (commit `8f3bb20`, re-run) |
| pebble-w64-c10 | pebble | 64 | 10 | 292 | 2919 | 112.32 | 237.06 | 906.24 | - (commit `8f3bb20`, re-run; fixed by `e339617`/`8f3bb20`, see above) |
| postgres-w64-c1 | postgres | 64 | 1 | 880 | 880 | 40.32 | 64.86 | 90.24 | - |
| postgres-w256-c1 | postgres | 256 | 1 | 1850 | 1850 | 70.53 | 118.33 | 164.86 | - |
| postgres-w64-c10 | postgres | 64 | 10 | 550 | 5505 | 61.57 | 116.09 | 310.78 | exhausted=581 insufficient=11 (unbounded — this is the shape that used to hit the stuck state every time; now clean) |
| mongodb-w64-c1 | mongodb | 64 | 1 | 970 | 970 | 32.26 | 63.39 | 85.38 | - |
| mongodb-w256-c1 | mongodb | 256 | 1 | 3122 | 3122 | 43.26 | 86.21 | 114.94 | - |
| mongodb-w64-c10 | mongodb | 64 | 10 | 667 | 6669 | 51.04 | 100.54 | 241.66 | exhausted=1600 insufficient=36 |
| aerospike-w64-c1 | aerospike | 64 | 1 | 24726 | 24726 | 1.27 | 3.40 | 5.14 | - |
| aerospike-w256-c1 | aerospike | 256 | 1 | 22715 | 22715 | 5.08 | 11.82 | 36.45 | exhausted=6856 |
| aerospike-w64-c10 | aerospike | 64 | 10 | 1500 | 15000 | 0.48 | 1.51 | 5.01 | - (rate-capped 1500/s, see above) |

Raw JSON for every row is in [`results/`](results/) (overwritten in place);
render any subset yourself with `fuelbench render docs/benchmarks/results/*.json`.

**Headline gains vs. the `236642c` run** (unbounded-vs-unbounded or
capped-vs-capped, same shape): memory `w64-c1` 2885 → 24364 reserves/s
(8.4×); sqlite `w64-c1` 59 → 707 (12.0×); pebble `w64-c1` 63 → 675 (10.7×);
postgres `w64-c1` 789 → 880 (1.1×, this backend was never the
fsync/Stats()-bound one); aerospike `w64-c1` — previously rate-capped at
3000/s to dodge the stuck state, now unbounded at 24726 (8.2× the old cap).
**The group-commit fix is the sqlite/pebble story**: both moved from
single-digit-to-double-digit reserves/s with hundreds-of-milliseconds p50
(single-writer-lock-serialized, one `fsync` per writer) to 675-1900
reserves/s with tens-of-milliseconds p50, a full order of magnitude, without
changing `no_sync`/`sync` durability settings. **The shard-retirement fix
(`ba55a6f`/`a22be74`) is the postgres/memory/aerospike `w64-c10` story**:
this exact shape used to hit the permanent stuck state on all three; all
three now complete the full 60s with real (not `exhausted`-saturated)
throughput. **Pebble's `w64-c10` needed a second, separate fix**
(`e339617`/`8f3bb20`) on top of both of the above: 52 → 249 reserves/s from
group-commit alone (`91e1331`), then 249 → 292 with zero errors once
adoption stopped starving behind rescans (`8f3bb20`) — see "Rate caps used,
and why" above for the full mechanism.

## Resource usage and coalescer batching

**Observed, not recorded**: read from a live `curl .../metrics` during the
session (before/after each backend's session) rather than saved to a file
in this repo, unlike the `results/*.json` throughput numbers above. Every
backend's three shapes ran back-to-back in one continuous server session
this time (no stuck-state-driven restarts), so CPU delta and coalescer
stats below cover all three shapes' full 180s.

| Backend | CPU delta | Approx. cores | RSS at end | Coalescer mean batch (ops/flush) |
|---|---|---|---|---|
| memory | 1161.37s | 645% | 15.2 GiB | 3.8 |
| sqlite | 196.47s | 109% | 626 MiB | 80.6 |
| pebble | 194.05s | 108% | 990 MiB | 81.0 |
| postgres | 196.36s | 109% | 729 MiB | 13.9 |
| mongodb | 219.08s | 122% | 874 MiB | 19.1 |
| aerospike | 1236.60s | 687% | 4.9 GiB | 4.7 |

**Memory and aerospike's RSS and CPU cost are dramatically higher than the
other four** (15.2 GiB / 645% and 4.9 GiB / 687%, vs. 626 MiB-990 MiB and
~100-122% for sqlite/pebble/postgres/mongodb) — not a regression, but a
direct consequence of how much faster these two now are: the memory backend
retains every row it has ever created (available, reserved, or spent) in
process memory rather than on disk, and at ~24-40K reserves/s it processed
4,009,320 total reserve calls across its three 60s shapes (the JSON
results' `reserves` fields summed; vs. 171,544-285,552 for sqlite/pebble/
postgres/mongodb over the same three shapes), so its resident set
scales with lifetime throughput, not with `target_available`. Aerospike's
Go client similarly keeps per-connection/event-loop state proportional to
request volume. **This is worth real attention before running the memory
backend under sustained high-throughput load for longer than a benchmark
window** — it is documented as development/testing-only for exactly this
reason (see the README's "Store Backends" table), and this run is a
concrete illustration of why: the process actually climbed to using a
double-digit percentage of a 62 GiB host's RAM (and pushed the host's swap
to near-full) over three 60s shapes.

**Coalescer mean batch size went down for both sqlite (113.0 → 80.6) and
pebble (125.4 → 81.0, `8f3bb20`), not up** — despite much higher throughput. This is
the expected signature of the fix working as intended, not a regression:
the old, slower one-`fsync`-per-writer path is exactly what inflated the
old means, by forcing large queues of concurrent callers to pile up behind
one slow flush; group-commit means each flush now completes fast enough
that fewer callers are waiting when it fires. Lower mean batch size with
much higher total throughput means throughput no longer depends on
accumulating a large batch to amortize a slow fsync.

## Aerospike cleanup

Unchanged from the original run: `set_prefix: fk_`, `key_prefix: bench6api`,
truncated with the same `pkg/store/aerospike.New` + `Store.Truncate()`
helper after the session. `asinfo -v 'sets/test' | tr ';' '\n' | grep -c
'set='` on `arcade-aerospike` read **2823** immediately before this re-run's
aerospike session and **2823** again after the truncate (observed, not
recorded: read directly from the command's terminal output during the
session, not saved to a file in this repo).

---

## Before the fixes (commit `236642c`, original run)

The numbers below are what this page originally published, kept for the
record since they document the two bugs above. Same configuration, same
three shapes, built with `go run ./cmd/fuelkeeper` rather than a pre-built
binary (immaterial to the results).

| label | backend | workers | count | reserves/s | outputs/s | p50 ms | p95 ms | p99 ms | errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| memory-w64-c1 | memory | 64 | 1 | 2885 | 2885 | 0.34 | 9.25 | 37.66 | - |
| memory-w256-c1 | memory | 256 | 1 | 1000 | 1000 | 0.34 | 25.52 | 55.01 | - (rate-capped 1000/s) |
| memory-w64-c10 | memory | 64 | 10 | 300 | 3000 | 0.66 | 16.93 | 42.43 | - (rate-capped 300/s) |
| sqlite-w64-c1 | sqlite | 64 | 1 | 59 | 59 | 535.04 | 944.13 | 1120.26 | unavailable=15 |
| sqlite-w256-c1 | sqlite | 256 | 1 | 13 | 13 | 722.43 | 1789.95 | 1949.69 | unavailable=6906 |
| sqlite-w64-c10 | sqlite | 64 | 10 | 53 | 526 | 674.30 | 1073.15 | 1488.89 | unavailable=79 |
| pebble-w64-c1 | pebble | 64 | 1 | 63 | 63 | 478.72 | 956.93 | 1362.94 | unavailable=2 |
| pebble-w256-c1 | pebble | 256 | 1 | 13 | 13 | 870.91 | 1850.37 | 1970.17 | unavailable=6369 |
| pebble-w64-c10 | pebble | 64 | 10 | 52 | 524 | 547.84 | 1075.20 | 1362.94 | exhausted=1221 insufficient=10 |
| postgres-w64-c1 | postgres | 64 | 1 | 789 | 789 | 41.44 | 73.60 | 225.15 | - |
| postgres-w256-c1 | postgres | 256 | 1 | 1788 | 1788 | 70.78 | 120.38 | 153.22 | - |
| postgres-w64-c10 | postgres | 64 | 10 | 275 | 2749 | 27.60 | 134.27 | 596.99 | - (rate-capped 300/s; unbounded hit the permanent stuck state) |
| mongodb-w64-c1 | mongodb | 64 | 1 | 975 | 975 | 31.70 | 60.64 | 86.02 | - |
| mongodb-w256-c1 | mongodb | 256 | 1 | 3322 | 3322 | 39.62 | 72.96 | 97.53 | - |
| mongodb-w64-c10 | mongodb | 64 | 10 | 446 | 4461 | 51.55 | 107.01 | 264.96 | exhausted=6437 insufficient=83 |
| aerospike-w64-c1 | aerospike | 64 | 1 | 2999 | 2999 | 0.30 | 0.71 | 1.15 | exhausted=48 (rate-capped 3000/s; unbounded hit the permanent stuck state) |
| aerospike-w256-c1 | aerospike | 256 | 1 | 1000 | 1000 | 0.38 | 0.88 | 1.14 | exhausted=16 (rate-capped 1000/s) |
| aerospike-w64-c10 | aerospike | 64 | 10 | 133 | 1334 | 0.70 | 1.69 | 84.16 | exhausted=9554 insufficient=117 (rate-capped 300/s) |

**The bug** (fixed by `ba55a6f`/`a22be74`): under sustained load, an
instance's shard allocator could permanently stop adopting new shards even
though the store still held plenty of available rows elsewhere —
`fuelkeeper_shards_owned`/`fuelkeeper_allocator_available` stuck at `0`,
every subsequent reserve (including unloaded manual probes minutes later)
returning `exhausted`, and `fuelkeeper_shard_adoptions_total` frozen.
Reproduced on memory, postgres and aerospike; six of the eighteen runs above
needed a `-rate` cap and/or a fresh restart to work around it, found by
direct trial (a short probe at a given rate did not reliably predict a
clean full-length run). Root cause: a fully spent-out shard stayed `mined`
and adoptable forever, so once more spent-out shards preceded the live ones
than the 256-entry, 60s exclusion list could exclude, adoption could cycle
through dead shards indefinitely.

**sqlite and pebble's single-digit-to-double-digit reserves/s and
hundreds-of-milliseconds p50** (fixed by `fd4dc56`/`91e1331`): both
backends' write path took one `fsync` per concurrent writer rather than one
per group-commit flush, so throughput under concurrency was bound by the
store's single-writer lock queuing behind serialized fsyncs, not by the
pool/coalescer logic above it.
