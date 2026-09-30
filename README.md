# FuelKeeper

A horizontally scalable pool of small, fixed-denomination BSV outputs ("fuel")
that wallets reserve on demand and attach as one extra input to pay transaction
fees. Fuel is spendable with unlocking data prepared in advance (a hash-puzzle
preimage by default), so no signing service sits on the hot path.

Status: Plans 1-5 complete: the go-wallet-toolbox treasury, BRC-29 funding
and the arcade/teranode/composite oracles (see `docs/TREASURY.md` and
`docs/FUNDING.md`), on top of the HTTP API, Go client and `serve` command
(Plan 3), the pool core (Plan 2), and six store backends -- memory, sqlite,
postgres, pebble, mongodb and aerospike (Plan 4; see "Store backends"
below): hash-puzzle fuel, sharded per-instance ownership with batched
(group-commit) reservations, and background jobs (reaper, proof watcher,
minter) behind leased runners. A module importing `pkg/config` or
`pkg/treasury/...` must copy the three `replace` lines from this `go.mod`.
Design: `docs/superpowers/specs/2026-09-29-fuelkeeper-design.md`.

## Develop

    make test              # unit + memory/sqlite/pebble conformance, -race (~90s incl. pebble's fuzz)
    make docker-up         # Postgres 16 on :15432 (always); COMPOSE_PROFILES=mongo,aerospike for the other two
    make test-integration  # postgres/mongodb/aerospike conformance (build tags: postgres, mongodb, aerospike)
    make lint

## Store backends

| Backend   | Multi-process | Conformance run          | Notes |
|-----------|---------------|--------------------------|-------|
| memory    | no            | `make test`              | tests and benchmarks |
| sqlite    | no            | `make test`              | single writer, `synchronous=FULL`, pragmas verified at open |
| pebble    | no            | `make test`              | embedded LSM, one batch per mutation, sync writes by default; differential-fuzzed against the memory store |
| postgres  | yes           | `make test-integration`  | `FOR UPDATE SKIP LOCKED` adoption; per-row CAS `UPDATE`s pipelined in one `pgx.Batch`, always in `(txid, vout)` order (never `unnest`: a combined multi-row statement would let Postgres's planner pick its own row-lock order, and two concurrent batches walking overlapping rows in different orders can then deadlock), partial indexes |
| mongodb   | yes           | `make test-integration`  | conditional single-document updates, write concern majority+journal, no TTL indexes; schema v2 indexes |
| aerospike | yes           | `make test-integration` (local compose) | filter-expression CAS, presence-encoded index bins, `MaxRetries=0` on writes; requires a strong-consistency namespace in production (`allow_availability_mode` only for tests) |

Every backend must pass `pkg/store/storetest.RunSuite` with `-race`. mongodb and
aerospike are exercised in CI/locally only under their own build tags
(`mongodb`, `aerospike`); aerospike is not a CI service (see
`.github/workflows/ci.yml`) — run it locally with `COMPOSE_PROFILES=mongo,aerospike
make docker-up && make test-integration` (`make docker-up` alone only starts
postgres; see `docs/OPERATIONS.md`'s "Local Integration Testing").

### Configuring a store

`pkg/store/factory.Config` selects and configures a backend; every field an
operator can set has a `yaml:"snake_case"` tag (pebble, mongodb and aerospike
via small mirror structs in the factory package, since the backend `Config`
types themselves carry no YAML tags). Only the fields for the chosen
`backend` need to be set. `time.Duration` fields (`op_timeout`, `timeout`,
...) are written below in human-readable form ("5s", "3s"); `pkg/config`'s
strict YAML loader (`gopkg.in/yaml.v3`, `KnownFields(true)`) decodes them
directly via `time.ParseDuration`, and rejects an unrecognized key such as a
typo'd field name. This `store:` block nests under a full config's own
`store:` key -- see `docs/OPERATIONS.md`'s Full Configuration Template.

```yaml
store:
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

## Run It (Development Mode)

Start the nullnet server (in-memory, mocked oracle and treasury):

    go run ./cmd/fuelkeeper serve --config examples/nullnet.yaml

In another terminal, verify the server is ready and make a test request:

    curl -sf localhost:8080/readyz
    curl -s -XPOST localhost:8080/v1/pools/main/reserve -H 'Content-Type: application/json' \
      -d '{"count":2}' | head -c 300

`/readyz` returns 200 as soon as the store pings and pools are configured; `pools_ready` shows which pools have adopted shards (initially 0, rises as fuel is minted and mined). The reserve response includes a reservation token, fuel outputs with complete unlocking scripts, and shard references to fetch BEEF from. See `docs/API.md` for the full API guide and `docs/OPERATIONS.md` for configuration, deployment topologies, and troubleshooting.

## Fund It

Against a real treasury (not the nullnet/mock treasury above), top up its
wallet balance with a BRC-29 payment from your own local BRC-100 wallet:

    fuelkeeper fund --sats 100000 [--url http://127.0.0.1:8080] [--key K | FUELKEEPER_API_KEY] [--wallet http://localhost:3321] [--output-index 0]

See `docs/FUNDING.md` for the full flow (including the manual HTTP/wallet
calls and recovery from a failed submission), and `docs/TREASURY.md` for how
the treasury turns that balance into fuel.

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
- **Pebble** fsyncs every write batch by default (`no_sync: false`); only set
  `no_sync: true` for throwaway stores such as tests, never in production.
- **MongoDB** always writes with `{w: "majority", j: true}` and reads with
  primary read preference (not operator-configurable), so an acknowledged
  write is durable and every subsequent read observes it.
- **Aerospike** requires a strong-consistency namespace in production
  (`New` checks every node's `strong-consistency` info property and refuses
  to start otherwise); `allow_availability_mode` opts out for CE dev/test
  servers only. Recommended SC settings: `replication-factor >= 2`,
  `commit-to-device true`, `default-ttl 0`.
- Memory, SQLite and Pebble are single-process backends
  (`Capabilities().MultiProcess == false`); Postgres, MongoDB and Aerospike
  support multiple processes sharing one store. See `docs/OPERATIONS.md`'s
  "Deployment Topologies" for what each multi-process backend needs in
  production (MongoDB: a real replica set behind its majority+journal write
  concern; Aerospike: a strong-consistency namespace).

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
