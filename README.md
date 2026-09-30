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
| sqlite   | no            | always          | single writer, `synchronous=FULL` by default, pragmas verified at open (the writer DSN uses `_txlock=immediate` and `synchronous=FULL`) |
| postgres | yes           | `make test-integration` | `FOR UPDATE SKIP LOCKED` adoption, `unnest` CAS batches, partial indexes |
| pebble, mongodb, aerospike | see spec | Plan 4 | |

Every backend must pass `pkg/store/storetest.RunSuite` with `-race`.
