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
