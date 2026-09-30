# FuelKeeper

A horizontally scalable pool of small, fixed-denomination BSV outputs ("fuel")
that wallets reserve on demand and attach as one extra input to pay transaction
fees. Fuel is spendable with unlocking data prepared in advance (a hash-puzzle
preimage by default), so no signing service sits on the hot path.

Status: store core complete (Plan 1). Design: `docs/superpowers/specs/2026-09-29-fuelkeeper-design.md`.

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
