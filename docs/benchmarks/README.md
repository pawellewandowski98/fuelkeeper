# Benchmarks

Published benchmark results for FuelKeeper, at commit `236642c` unless a
page states otherwise. Start with [`../PERFORMANCE.md`](../PERFORMANCE.md)
for the method and the headline numbers; the pages below carry the raw data
each headline number is drawn from.

| Page | What it measures |
|---|---|
| [`20260930-store-micro.md`](20260930-store-micro.md) | `go test -bench` microbenchmarks of each `pkg/store/*` backend in isolation (no HTTP, no pool, no jobs): `Reserve` at several batch sizes, `AcquireShard` at several already-leased-shard counts, `Quarantine` |
| [`20260930-api-single.md`](20260930-api-single.md) | `cmd/fuelbench` against one `fuelkeeper serve` instance per store backend (memory, sqlite, pebble, postgres, mongodb, aerospike), mock oracle/treasury, three load shapes each |
| [`20260930-api-cluster.md`](20260930-api-cluster.md) | `cmd/fuelbench` against a three-instance FuelKeeper cluster sharing one Postgres store (`deploy/docker-compose.bench.yaml`), the same three load shapes, plus a single-instance-of-the-cluster comparison run |
| [`20260930-demo-ttn.md`](20260930-demo-ttn.md) | Not run: the exact `cmd/fueldemo` (raw and toolbox mode) procedure for a real teratestnet/arcade end-to-end demo, and what to record once a funded environment is available |

Raw `fuelbench` JSON results are in [`results/`](results/), named
`<backend-or-cluster>-<shape>.json` (e.g. `postgres-w64-c1.json`,
`cluster-w256-c1.json`). Render any subset of them into a markdown
comparison table yourself with:

```bash
go run ./cmd/fuelbench render docs/benchmarks/results/*.json
```
