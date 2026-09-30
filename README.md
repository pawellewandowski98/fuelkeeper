# FuelKeeper

A horizontally scalable pool of small, fixed-denomination BSV outputs ("fuel")
that wallets reserve on demand and attach as one extra input to pay transaction
fees. Fuel is spendable with unlocking data prepared in advance (a hash-puzzle
preimage by default), so no signing service sits on the hot path.

Status: pool core complete (Plan 2): hash-puzzle fuel, sharded per-instance
ownership with batched (group-commit) reservations, and background jobs
(reaper, proof watcher, minter) behind leased runners. HTTP API, Go client and
`serve` come next (Plan 3). Design:
`docs/superpowers/specs/2026-09-29-fuelkeeper-design.md`.

## Develop

    make test              # unit + sqlite/memory conformance, -race
    make docker-up         # Postgres 16 on :15432
    make test-integration  # postgres conformance (build tag: postgres)
    make lint

## Store backends

| Backend   | Multi-process | Conformance run          | Notes |
|-----------|---------------|--------------------------|-------|
| memory    | no            | `make test`              | tests and benchmarks |
| sqlite    | no            | `make test`              | single writer, `synchronous=FULL`, pragmas verified at open |
| pebble    | no            | `make test`              | embedded LSM, one batch per mutation, sync writes by default; differential-fuzzed against the memory store |
| postgres  | yes           | `make test-integration`  | `FOR UPDATE SKIP LOCKED` adoption, `unnest` CAS batches, partial indexes |
| mongodb   | yes           | `make test-integration`  | conditional single-document updates, write concern majority+journal, no TTL indexes; schema v2 indexes |
| aerospike | yes           | `make test-integration` (local compose) | filter-expression CAS, presence-encoded index bins, `MaxRetries=0` on writes; requires a strong-consistency namespace in production (`allow_availability_mode` only for tests) |

Every backend must pass `pkg/store/storetest.RunSuite` with `-race`. mongodb and
aerospike are exercised in CI/locally only under their own build tags
(`mongodb`, `aerospike`); aerospike is not a CI service (see
`.github/workflows/ci.yml`) — run it locally with `make docker-up && make
test-integration`.

### Configuring a store

`pkg/store/factory.Config` selects and configures a backend; every field an
operator can set has a `yaml:"snake_case"` tag (pebble, mongodb and aerospike
via small mirror structs in the factory package, since the backend `Config`
types themselves carry no YAML tags). Only the fields for the chosen
`backend` need to be set. (`time.Duration` fields are shown below in
human-readable form; the exact string-to-duration decoding depends on the
`pkg/config` YAML loader that lands in a later, post-merge phase.)

```yaml
backend: pebble
pebble:
  path: /var/lib/fuelkeeper/pebble
  no_sync: false        # leave false in production (fsync every batch)
  cache_bytes: 268435456 # 0 = 256 MiB default

# backend: mongodb
mongodb:
  uri: mongodb://localhost:27017
  database: fuelkeeper
  max_pool_size: 64      # 0 = default
  op_timeout: 5s         # 0 = default

# backend: aerospike
aerospike:
  hosts: ["localhost:3000"]
  namespace: fuelkeeper
  set_prefix: fk_        # fixed per deployment; never generate this per store
  key_prefix: ""         # optional tenant isolation on shared sets
  allow_availability_mode: false # true only for CE dev/test servers
  timeout: 3s            # 0 = default
  pool_size: 128         # 0 = default
```

## Operations

Correctness guarantee G1 (no fuel output is ever handed to two live
reservations) depends on an acknowledged commit actually being durable.
That durability requirement shapes how each backend must be deployed:

- **Postgres** requires `synchronous_commit` not to be `off`; `postgres.New`
  checks this at startup (`SHOW synchronous_commit`) and refuses to open with
  an `ErrInvalidArgument` error if it is. `on`, `local`, `remote_write` and
  `remote_apply` are all accepted — set whichever your durability posture
  calls for on the fuelkeeper role or database.
- **SQLite** defaults to `synchronous=FULL`, which fsyncs every commit.
  `Sync: NORMAL` is opt-in (`sqlite.Config.Sync`) and trades that off for
  speed: on power loss it may forget the last committed reservation. Only
  opt in when the deployment accepts that risk.
- SQLite serialises all writes through a single writer connection using
  `BEGIN IMMEDIATE`; when another process already holds that database
  file's write lock, a conflicting writer waits out `busy_timeout` (5s)
  regardless of the caller's own context deadline before SQLite reports
  `SQLITE_BUSY`. Run exactly one process per SQLite database file.
- SQLite and memory are both single-process backends
  (`Capabilities().MultiProcess == false`); only Postgres supports multiple
  processes sharing one store.

Pool behaviour that operators should know:

- **Terminal states.** A row becomes `spent` only when the oracle proves a
  mined spend (or directly under `spend_policy=trust`). Everything else stays
  `quarantined` and is visible through `fuelkeeper_quarantine_oldest_age_seconds`.
  With no oracle configured, expired reservations stay quarantined until an
  operator acts, so `release_policy=verify` and `spend_policy=verify` need an
  oracle.
- **Job leases.** Each background job (reaper, watcher, minter) runs on one
  instance at a time under a store lease named `jobs/<name>`. The lease TTL
  must exceed the longest job interval plus its run time, or leadership flaps.
  Overlapping runs during a hand-over are harmless because every job write is
  a compare-and-set.
- **Shard adoption.** An instance adopts one shard at a time while its
  in-memory rows are below `low_water_rows` and it owns fewer than
  `max_shards`; background feeding and the reserve-path fallback share one
  adoption slot, and a reserve waits at most `adopt_timeout` for it. A crashed
  instance's shards are re-adopted by others once their `lease_ttl` expires.
  A rising `fuelkeeper_cas_missed_total` means two instances briefly served
  the same shard; it clears within one rescan.
- **Test knobs.** `FUELKEEPER_STRESS=N` multiplies the rounds of the
  invariant and conformance tests; `FUELKEEPER_SEED=S` replays a specific
  invariant-test run (the seed is logged on every run).
