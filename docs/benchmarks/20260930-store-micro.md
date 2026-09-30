# Store Microbenchmarks — 2026-09-30

Per-package `go test -bench` results for every `pkg/store/*` backend, captured
at commit `236642c` (`bench(store): AcquireShard and Quarantine are
b.N-stable; leased-shard dimension; nits`). These measure the store layer in
isolation (no HTTP, no pool coalescer, no jobs) — see
[`20260930-api-single.md`](20260930-api-single.md) and
[`20260930-api-cluster.md`](20260930-api-cluster.md) for end-to-end numbers
through the public API, and [`../PERFORMANCE.md`](../PERFORMANCE.md) for how
these fit together.

## Machine

```
$ uname -a
Linux fedora 7.1.5-100.fc43.x86_64 #1 SMP PREEMPT_DYNAMIC Fri Jul 24 20:54:57 UTC 2026 x86_64 GNU/Linux

$ lscpu | grep 'Model name'
Model name:                              13th Gen Intel(R) Core(TM) i9-13900K

$ lsblk -d -o NAME,ROTA,MODEL
NAME    ROTA MODEL
sda        0 Samsung SSD 850
```

The repo (`/git/fuelkeeper-bench`) lives on `/dev/sda3`, i.e. `sda` above: a
non-rotational (`ROTA=0`) SATA SSD (Samsung 850 series), not NVMe. Postgres
and MongoDB run in containers on the same host/disk (see "Local Integration
Testing" in [`../OPERATIONS.md`](../OPERATIONS.md)); their fsync-on-commit
costs below are this SSD's fsync latency, not a remote-storage number.

```
$ go version
go version go1.26.3 linux/amd64
```

## Commands

Each backend's package was benchmarked independently with:

```bash
go test -run '^$' -bench . -benchmem -benchtime=2s ./pkg/store/<backend>/...
```

`pkg/store/memory`, `pkg/store/sqlite` and `pkg/store/pebble` need no
external server. `pkg/store/postgres` and `pkg/store/mongodb` ran with their
build tag and the already-running dev containers (`localhost:15432`,
`localhost:27017`); `pkg/store/aerospike` ran with its build tag against the
shared Aerospike server on `localhost:3000` (namespace `test`), which
predates this run and is not scoped to it:

```bash
go test -run '^$' -bench . -benchmem -benchtime=2s ./pkg/store/memory/...
go test -run '^$' -bench . -benchmem -benchtime=2s ./pkg/store/sqlite/...
go test -run '^$' -bench . -benchmem -benchtime=2s ./pkg/store/pebble/...
FUELKEEPER_TEST_POSTGRES_DSN='postgres://fuelkeeper:fuelkeeper@localhost:15432/fuelkeeper?sslmode=disable' \
  go test -run '^$' -bench . -benchmem -benchtime=2s -tags=postgres ./pkg/store/postgres/...
FUELKEEPER_TEST_MONGODB_URI='mongodb://localhost:27017' \
  go test -run '^$' -bench . -benchmem -benchtime=2s -tags=mongodb ./pkg/store/mongodb/...
FUELKEEPER_TEST_AEROSPIKE_HOST='localhost:3000' \
  go test -run '^$' -bench . -benchmem -benchtime=2s -tags=aerospike ./pkg/store/aerospike/...
```

These outputs were produced ahead of this write-up (not re-run here); this
page only transcribes and summarizes them.

## Raw output

### memory, sqlite, pebble (embedded backends)

```
goos: linux
goarch: amd64
pkg: github.com/bsv-blockchain/fuelkeeper/pkg/store/memory
cpu: 13th Gen Intel(R) Core(TM) i9-13900K
BenchmarkStore/Reserve/batch=1-32         	 5545790	       527.1 ns/op	   1897198 rows/s	     176 B/op	       3 allocs/op
BenchmarkStore/Reserve/batch=16-32        	  572271	      7748 ns/op	   2065024 rows/s	    4520 B/op	      21 allocs/op
BenchmarkStore/Reserve/batch=64-32        	  121590	     30400 ns/op	   2105233 rows/s	   17192 B/op	      69 allocs/op
BenchmarkStore/Reserve/batch=256-32       	   23888	    108914 ns/op	   2350486 rows/s	   69418 B/op	     261 allocs/op
BenchmarkStore/AcquireShard/leased=0-32   	 2639796	       892.5 ns/op	     208 B/op	       2 allocs/op
BenchmarkStore/AcquireShard/leased=100-32 	  859503	      2735 ns/op	     208 B/op	       2 allocs/op
BenchmarkStore/AcquireShard/leased=1000-32         	  129000	     18534 ns/op	     208 B/op	       2 allocs/op
BenchmarkStore/Quarantine/1000-32                  	   12238	    200713 ns/op	   17584 B/op	      13 allocs/op
PASS
ok  	github.com/bsv-blockchain/fuelkeeper/pkg/store/memory	150.653s
goos: linux
goarch: amd64
pkg: github.com/bsv-blockchain/fuelkeeper/pkg/store/sqlite
cpu: 13th Gen Intel(R) Core(TM) i9-13900K
BenchmarkStore/Reserve/batch=1-32         	   51388	     47054 ns/op	     21252 rows/s	    2457 B/op	      54 allocs/op
BenchmarkStore/Reserve/batch=16-32        	    7058	    323247 ns/op	     49498 rows/s	   31284 B/op	     638 allocs/op
BenchmarkStore/Reserve/batch=64-32        	    2208	   1193315 ns/op	     53632 rows/s	  121553 B/op	    2485 allocs/op
BenchmarkStore/Reserve/batch=256-32       	     452	   5504625 ns/op	     46506 rows/s	  484677 B/op	    9875 allocs/op
BenchmarkStore/AcquireShard/leased=0-32   	   28844	     86247 ns/op	    4024 B/op	      81 allocs/op
BenchmarkStore/AcquireShard/leased=100-32 	   24469	    101537 ns/op	    4028 B/op	      82 allocs/op
BenchmarkStore/AcquireShard/leased=1000-32         	   10000	    235209 ns/op	    4034 B/op	      82 allocs/op
BenchmarkStore/Quarantine/1000-32                  	     388	   6353253 ns/op	     844 B/op	      21 allocs/op
PASS
ok  	github.com/bsv-blockchain/fuelkeeper/pkg/store/sqlite	49.945s
goos: linux
goarch: amd64
pkg: github.com/bsv-blockchain/fuelkeeper/pkg/store/pebble
cpu: 13th Gen Intel(R) Core(TM) i9-13900K
BenchmarkReserveRowLookup/PointGet/n=10-32         	   75746	     31438 ns/op	   10737 B/op	     147 allocs/op
BenchmarkReserveRowLookup/Seek/n=10-32             	   84621	     26310 ns/op	    9271 B/op	     127 allocs/op
BenchmarkReserveRowLookup/PointGet/n=16-32         	   54122	     45806 ns/op	   16461 B/op	     224 allocs/op
BenchmarkReserveRowLookup/Seek/n=16-32             	   63523	     37985 ns/op	   15071 B/op	     199 allocs/op
BenchmarkReserveRowLookup/PointGet/n=100-32        	    8073	    288480 ns/op	  102431 B/op	    1393 allocs/op
BenchmarkReserveRowLookup/Seek/n=100-32            	   10477	    241813 ns/op	   88649 B/op	    1207 allocs/op
BenchmarkReserveRowLookup/PointGet/n=500-32        	    1743	   1406431 ns/op	  510945 B/op	    6947 allocs/op
BenchmarkReserveRowLookup/Seek/n=500-32            	    2025	   1147710 ns/op	  467342 B/op	    6007 allocs/op
BenchmarkReserveRowLookup/PointGet/n=1000-32       	     854	   2915683 ns/op	 1023414 B/op	   13919 allocs/op
BenchmarkReserveRowLookup/Seek/n=1000-32           	    1068	   2290889 ns/op	  934611 B/op	   12010 allocs/op
BenchmarkStore/Reserve/batch=1-32                  	  365624	      6838 ns/op	    146234 rows/s	    2509 B/op	      39 allocs/op
BenchmarkStore/Reserve/batch=16-32                 	   31538	     83124 ns/op	    192483 rows/s	   40241 B/op	     517 allocs/op
BenchmarkStore/Reserve/batch=64-32                 	   10000	    244428 ns/op	    261836 rows/s	  153679 B/op	    1882 allocs/op
BenchmarkStore/Reserve/batch=256-32                	    2809	    950984 ns/op	    269195 rows/s	  631962 B/op	    7468 allocs/op
BenchmarkStore/AcquireShard/leased=0-32            	   76622	     31532 ns/op	    8943 B/op	     146 allocs/op
BenchmarkStore/AcquireShard/leased=100-32          	    6999	    372074 ns/op	  107104 B/op	    1747 allocs/op
BenchmarkStore/AcquireShard/leased=1000-32         	     726	   3160787 ns/op	  989843 B/op	   16155 allocs/op
BenchmarkStore/Quarantine/1000-32                  	     417	   5515486 ns/op	 2420348 B/op	   35112 allocs/op
PASS
ok  	github.com/bsv-blockchain/fuelkeeper/pkg/store/pebble	73.825s
```

### postgres

```
goos: linux
goarch: amd64
pkg: github.com/bsv-blockchain/fuelkeeper/pkg/store/postgres
cpu: 13th Gen Intel(R) Core(TM) i9-13900K
BenchmarkStore/Reserve/batch=1-32         	     184	  14207591 ns/op	        70.38 rows/s	    1643 B/op	      42 allocs/op
BenchmarkStore/Reserve/batch=16-32        	     128	  20158735 ns/op	       793.7 rows/s	   34923 B/op	     472 allocs/op
BenchmarkStore/Reserve/batch=64-32        	     104	  21880548 ns/op	      2925 rows/s	  151494 B/op	    1811 allocs/op
BenchmarkStore/Reserve/batch=256-32       	      64	  44475264 ns/op	      5756 rows/s	  567221 B/op	    7142 allocs/op
BenchmarkStore/AcquireShard/leased=0-32   	     165	  14646724 ns/op	    2045 B/op	      41 allocs/op
BenchmarkStore/AcquireShard/leased=100-32 	     164	  17853975 ns/op	    2023 B/op	      41 allocs/op
BenchmarkStore/AcquireShard/leased=1000-32         	     100	  20406734 ns/op	    2043 B/op	      41 allocs/op
BenchmarkStore/Quarantine/1000-32                  	      94	  38126016 ns/op	     295 B/op	      10 allocs/op
PASS
ok  	github.com/bsv-blockchain/fuelkeeper/pkg/store/postgres	149.187s
```

### mongodb

```
goos: linux
goarch: amd64
pkg: github.com/bsv-blockchain/fuelkeeper/pkg/store/mongodb
cpu: 13th Gen Intel(R) Core(TM) i9-13900K
BenchmarkStore/Reserve/batch=1-32         	     134	  17306167 ns/op	        57.78 rows/s	   25468 B/op	     283 allocs/op
BenchmarkStore/Reserve/batch=16-32        	     100	  25931583 ns/op	       617.0 rows/s	  229238 B/op	    1765 allocs/op
BenchmarkStore/Reserve/batch=64-32        	     100	  24409247 ns/op	      2622 rows/s	  890478 B/op	    6395 allocs/op
BenchmarkStore/Reserve/batch=256-32       	      74	  34768918 ns/op	      7363 rows/s	 3730507 B/op	   24905 allocs/op
BenchmarkStore/AcquireShard/leased=0-32   	     170	  13821027 ns/op	   14745 B/op	     179 allocs/op
BenchmarkStore/AcquireShard/leased=100-32 	     172	  14453428 ns/op	   14788 B/op	     180 allocs/op
BenchmarkStore/AcquireShard/leased=1000-32         	     169	  15130105 ns/op	   14834 B/op	     180 allocs/op
BenchmarkStore/Quarantine/1000-32                  	      36	  56079707 ns/op	 7923118 B/op	   47344 allocs/op
PASS
ok  	github.com/bsv-blockchain/fuelkeeper/pkg/store/mongodb	341.088s
```

### aerospike

```
goos: linux
goarch: amd64
pkg: github.com/bsv-blockchain/fuelkeeper/pkg/store/aerospike
cpu: 13th Gen Intel(R) Core(TM) i9-13900K
BenchmarkStore/Reserve/batch=1-32         	   38574	     62520 ns/op	     15995 rows/s	    9026 B/op	     122 allocs/op
BenchmarkStore/Reserve/batch=16-32        	   10000	    240443 ns/op	     66544 rows/s	  110830 B/op	    1639 allocs/op
BenchmarkStore/Reserve/batch=64-32        	    4179	    596440 ns/op	    107303 rows/s	  436802 B/op	    6487 allocs/op
BenchmarkStore/Reserve/batch=256-32       	    1237	   1949701 ns/op	    131302 rows/s	 1875349 B/op	   25883 allocs/op
BenchmarkStore/AcquireShard/leased=0-32   	    2311	    954485 ns/op	  392243 B/op	    4286 allocs/op
BenchmarkStore/AcquireShard/leased=100-32 	    2629	    905320 ns/op	  392229 B/op	    4286 allocs/op
BenchmarkStore/AcquireShard/leased=1000-32         	    2370	   1106160 ns/op	  392229 B/op	    4286 allocs/op
BenchmarkStore/Quarantine/1000-32                  	     334	   7465613 ns/op	 4157224 B/op	   50680 allocs/op
PASS
ok  	github.com/bsv-blockchain/fuelkeeper/pkg/store/aerospike	67.004s
```

## Summary table

All times are `ns/op` (nanoseconds per `Reserve`/`AcquireShard`/`Quarantine`
call as benchmarked — note `Reserve`'s `ns/op` is per *call*, i.e. per batch,
not per row; `rows/s` already divides by batch size). `Quarantine/1000` moves
1000 rows per call.

| Backend   | Reserve b=1 (ns/op, rows/s) | Reserve b=16 (ns/op, rows/s) | Reserve b=64 (ns/op, rows/s) | Reserve b=256 (ns/op, rows/s) | AcquireShard leased=0 | AcquireShard leased=100 | AcquireShard leased=1000 | Quarantine/1000 |
|---|---|---|---|---|---|---|---|---|
| memory    | 527 ns, 1.90M rows/s   | 7,748 ns, 2.07M rows/s   | 30,400 ns, 2.11M rows/s   | 108,914 ns, 2.35M rows/s  | 893 ns      | 2,735 ns    | 18,534 ns     | 200,713 ns (201 µs) |
| sqlite    | 47,054 ns, 21.3K rows/s | 323,247 ns, 49.5K rows/s | 1,193,315 ns, 53.6K rows/s | 5,504,625 ns, 46.5K rows/s | 86,247 ns  | 101,537 ns  | 235,209 ns    | 6,353,253 ns (6.35 ms) |
| pebble    | 6,838 ns, 146K rows/s  | 83,124 ns, 192K rows/s   | 244,428 ns, 262K rows/s   | 950,984 ns, 269K rows/s   | 31,532 ns  | 372,074 ns  | 3,160,787 ns  | 5,515,486 ns (5.52 ms) |
| postgres  | 14,207,591 ns, 70.4 rows/s | 20,158,735 ns, 794 rows/s | 21,880,548 ns, 2,925 rows/s | 44,475,264 ns, 5,756 rows/s | 14,646,724 ns | 17,853,975 ns | 20,406,734 ns | 38,126,016 ns (38.1 ms) |
| mongodb   | 17,306,167 ns, 57.8 rows/s | 25,931,583 ns, 617 rows/s | 24,409,247 ns, 2,622 rows/s | 34,768,918 ns, 7,363 rows/s | 13,821,027 ns | 14,453,428 ns | 15,130,105 ns | 56,079,707 ns (56.1 ms) |
| aerospike | 62,520 ns, 16.0K rows/s | 240,443 ns, 66.5K rows/s | 596,440 ns, 107K rows/s | 1,949,701 ns, 131K rows/s | 954,485 ns | 905,320 ns  | 1,106,160 ns  | 7,465,613 ns (7.47 ms) |

## Interpretation

- **Pebble's `AcquireShard` cost grows with the number of already-leased
  shards.** At `leased=0` it costs 31.5 µs; at `leased=100` it is 11.8×
  slower (372 µs); at `leased=1000` it is 100× slower than the baseline
  (3.16 ms) and allocations grow from 146 to 16,155. `AcquireShard` scans
  past shards leased by any holder (not just this instance's own leases) to
  find the next unleased one, and pebble's LSM iterator pays a seek/merge
  cost per already-leased key it steps over — so the more shards are
  currently leased at all, by any instance, the more each adoption attempt
  pays. Postgres, MongoDB and memory show much flatter `AcquireShard`
  curves (memory: 893 ns → 18.5 µs, a 21× spread but from a much smaller
  base and no I/O; postgres/mongodb: under 1.5× across the same range)
  because their indexed lookups (a `WHERE` clause / index scan) don't pay a
  cost proportional to the number of already-leased rows the same way
  pebble's forward iteration does. Aerospike's `AcquireShard` is essentially
  flat (~0.9-1.1 ms) since it is a single-record read by primary key
  regardless of how many other shards are leased.

- **Postgres and MongoDB per-op latency is dominated by durable commits, not
  row count.** A single-row `Reserve` already costs 14.2 ms (postgres) /
  17.3 ms (mongodb) — both back this host's local containers, so this is
  `fsync`-class commit latency (`synchronous_commit` on for postgres,
  `{w: majority, j: true}` for mongodb; see "Durability and Consistency" in
  [`../OPERATIONS.md`](../OPERATIONS.md)), not network RTT. Batching 256 rows
  into one `Reserve` call only raises the per-call latency ~2-3× (postgres:
  14.2 ms → 44.5 ms; mongodb: 17.3 ms → 34.8 ms) even though it does 256×
  the work, because the fixed commit cost is amortized across the whole
  batch.

- **Batching amortizes the fixed per-call (and, for durable backends,
  per-commit) cost across more rows, raising `rows/s` even though `ns/op`
  also rises.** Every backend's `rows/s` climbs from `batch=1` to
  `batch=256`: postgres 70 → 5,756 rows/s (82×), mongodb 58 → 7,363 rows/s
  (127×), aerospike 16.0K → 131K rows/s (8.2×), pebble 146K → 269K rows/s
  (1.8×), sqlite 21.3K → 46.5K rows/s (2.2×), memory 1.90M → 2.35M rows/s
  (1.2×). The backends with the highest fixed per-call overhead (postgres,
  mongodb — a network round trip plus a durable commit) see the largest
  relative gain from batching; memory, with almost no fixed overhead, sees
  the least. This is the mechanism `instance.flush_window`/`max_batch` (the
  pool coalescer) exploits in production: coalescing concurrent `Reserve`
  calls into fewer, larger store batches trades a little added latency per
  caller for much higher aggregate throughput, most valuably on the
  durable, multi-process backends.

- **The sqlite and pebble numbers above do not pay a real durable-commit
  cost, for two different reasons.** `pkg/store/pebble/bench_test.go` and
  `store_bench_test.go` open pebble with `NoSync: true` -- these benchmarks
  make **zero** `fsync` calls, on any filesystem, regardless of where
  `b.TempDir()` lands. `pkg/store/sqlite`'s bench opens with no `Sync`
  override, so it keeps the default `synchronous=FULL` (an `fsync` per
  commit) -- but `go test`'s default temp directory (`os.TempDir()`, `/tmp`
  unless `TMPDIR` is set) is a `tmpfs` (RAM-backed) mount on this host, so
  those `fsync` calls are real but cheap (against RAM, not the disk named
  in "Machine"). Neither number on this page reflects this host's real SSD
  fsync cost; sqlite at least exercises the fsync *call*, pebble does not
  exercise it at all. Commit `fd4dc56`/`91e1331` (after this page,
  2026-09-30) changed sqlite/pebble to group-commit concurrent writers into
  one `fsync` per flush instead of one per writer; that fix does **not**
  change the numbers on this page (pebble was never fsyncing here, and
  sqlite's tmpfs fsync had little cost to amortize) -- its effect shows up
  instead in [`20260930-api-single.md`](20260930-api-single.md), whose
  sqlite/pebble data directories are on this host's real SSD with sqlite's
  default durability and pebble's `no_sync: false` (see that page's own
  numbers, re-run after the fix).
