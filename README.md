# FuelKeeper

A horizontally scalable pool of small, fixed-denomination BSV outputs ("fuel")
that wallets reserve on demand and attach as one extra input to pay transaction
fees. Fuel is spendable with unlocking data prepared in advance (a hash-puzzle
preimage by default), so no signing service sits on the hot path.

Status: Plan 5 complete: the go-wallet-toolbox treasury, BRC-29 funding
and the chain oracle (see `docs/TREASURY.md` and `docs/FUNDING.md`) -- by
default an arcade-only oracle when `treasury.kind: toolbox` is configured; a
direct teranode connection is never assumed, only an optional, opt-in
accelerator (`oracle.kind: composite` with `teranode_url`) -- on top of the
HTTP API, Go client and `serve` command (Plan 3) and the pool core (Plan 2):
hash-puzzle fuel, sharded per-instance ownership with batched (group-commit)
reservations, and background jobs (reaper, proof watcher, minter) behind
leased runners. A module importing `pkg/config` or `pkg/treasury/...` must
copy the three `replace` lines from this `go.mod`. Design:
`docs/superpowers/specs/2026-09-29-fuelkeeper-design.md`.

## Develop

    make test              # unit + sqlite/memory conformance, -race
    make docker-up         # Postgres 16 on :15432
    make test-integration  # postgres conformance (build tag: postgres)
    make lint

## Store backends

| Backend  | Multi-process | Conformance run | Notes |
|----------|---------------|-----------------|-------|
| memory   | no            | always          | tests and benchmarks |
| sqlite   | no            | always          | single writer; `_pragma` DSN (`_txlock=immediate` on the writer, `synchronous` FULL by default); journal_mode/busy_timeout/synchronous/journal_size_limit/query_only read back at open |
| postgres | yes           | `make test-integration` | `FOR UPDATE SKIP LOCKED` adoption, per-row CAS UPDATEs pipelined in one `pgx.Batch` in (txid, vout) order, partial indexes |
| pebble, mongodb, aerospike | see spec | Plan 4 | |

Every backend must pass `pkg/store/storetest.RunSuite` with `-race`.

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
- SQLite and memory are both single-process backends
  (`Capabilities().MultiProcess == false`); only Postgres supports multiple
  processes sharing one store.

Pool behaviour that operators should know:

- **Terminal states.** A row becomes `spent` only when the oracle proves a
  mined spend (or directly under `spend_policy=trust`). Everything else stays
  `quarantined` and is visible through `fuelkeeper_quarantine_oldest_age_seconds`.
  With `oracle.kind: none`, `release_policy=verify` and `spend_policy=verify`
  are rejected at config load; with any other oracle kind (including the
  default arcade-only shape), an unhinted quarantined row with no verdict
  instead requeues itself after `jobs.reaper.unknown_requeue_age` (a pool's
  `expiry_policy: hold` keeps it quarantined for an operator instead). See
  `docs/OPERATIONS.md#oracle-kinds-and-policies`.
- **The settlement contract.** A consumer -- not FuelKeeper -- learns what
  actually happened to its own broadcast, so it is responsible for clearing
  its reservation once it knows: the Go client's `client.Settle` maps that
  outcome (broadcast, rejected, double-spent, or unknown) to the right
  `spend`/`release` call, and a TypeScript/go-wallet-toolbox consumer keeps
  the same contract via its own review results. Getting this backwards is
  costly (a released double-spent output poisons the pool for the next
  consumer), so see `docs/CLIENTS.md#when-your-broadcast-fails` before
  wiring up your own reporting.
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
