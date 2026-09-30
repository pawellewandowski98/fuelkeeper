# FuelKeeper API

FuelKeeper keeps a pool of small, fixed-denomination BSV outputs ("fuel") that applications reserve on demand and attach as one extra input to pay transaction fees. This guide covers the HTTP API and the Go client.

## Consuming Fuel in Five Calls

A typical workflow:

1. **Reserve** outputs from a pool with `POST /v1/pools/{pool}/reserve`
2. **Fetch BEEF** for each shard using the server-relative `beef_url` paths
3. **Build and broadcast** your transaction with fuel outputs as inputs (each carries a complete unlocking script; no signing required)
4. **Spend** the outputs with `POST /v1/pools/{pool}/spend` to report the transaction
5. **Release** unused outputs with `POST /v1/pools/{pool}/release` if the transaction was not sent

Example request bodies:

```json
POST /v1/pools/main/reserve
{ "count": 5, "min_count": 1, "ttl_seconds": 120 }

POST /v1/pools/main/spend
{ "token": "<token>", "outpoints": ["txid:0"], "txid": "..." }

POST /v1/pools/main/release
{ "token": "<token>", "outpoints": ["txid:0"] }
```

## Reservation Semantics

### TTL and Expiry

Each reservation has a `ttl_seconds` lifetime (in the response as `expires_at`). After that time, the reservation is considered expired. Every expired or unreported-spent output is automatically quarantined and verified against the chain oracle before being handed out to another caller.

### Single-Shard-Per-Request Constraint

A single reserve request draws from the instance's in-memory rows plus at most one newly adopted shard. A large `count` on a cold instance can return `409 insufficient` if fewer than `min_count` outputs are available. In this case, the client should wait (honoring `Retry-After`) and retry.

### Idempotency

- **Release** is idempotent: outputs not reserved under the given token are silently skipped; the response reports how many changed.
- **Spend** is idempotent: a second call with a different txid for the same token is ignored; only the `accepted` field in the response is meaningful.

## Fee Sizing

FuelKeeper hands out outputs of fixed denomination (e.g., 50 satoshis). To avoid creating unnecessary change outputs:

- Calculate `fee = ceil(size_bytes × sat_per_kB / 1000)` (100 sat/kB on teratestnet/mainnet today)
- Use `Surplus(denomination, estimated_tx_bytes, sat_per_kb)` (Go client) or calculate `surplus = denomination - fee`
- Add the surplus to one of your own outputs instead of creating change from fuel outputs
- Surpluses under ~40 satoshis (dust floor) go to miners

### Input Sizes (Bytes)

- **Puzzle** (33-byte preimage push, no constraints): 74 B per input
- **P2PKH** (presigned signature + pubkey, SIGHASH_NONE|ANYONECANPAY|FORKID): 148 B per input

## Lock Modes

Two modes of operation are supported:

| Mode | Unlocking Script | Constraints | Size | Use Case |
|------|------------------|-------------|------|----------|
| **puzzle** | 33-byte preimage push (no signing) | None | 74 B | Stateless, open pools |
| **p2pkh** | Presigned `<sig><pubkey>` with SIGHASH_NONE\|ANYONECANPAY\|FORKID | `tx_version=1`, `lock_time=0`, `input_sequence=0xffffffff` | 148 B | High-security, controlled signings |

For P2PKH pools, the response includes a `constraints` object; your transaction must match those fields exactly or the presigned signature will not validate.

## Errors

All non-2xx responses are JSON with the shape `{"code", "message", "retry_after"?}`. Errors that include `Retry-After` carry retry guidance; clients should back off and retry after the indicated seconds.

| Status | Code | Meaning | Retry-After |
|--------|------|---------|-------------|
| 400 | `bad_request` | Malformed or invalid request | No |
| 401 | `unauthorized` | Invalid or missing API key (`WWW-Authenticate: Bearer` header present) | No |
| 404 | `unknown_pool` or `not_found` | Pool not found or resource missing | No |
| 405 | `method_not_allowed` | Wrong HTTP method; `Allow` header lists permitted methods | No |
| 409 | `insufficient` | Fewer than `min_count` outputs available; nothing is held | Yes (1s) |
| 499 | `client_closed_request` | Client disconnected before response sent | No |
| 500 | `internal` | Server error | No |
| 503 | `exhausted`, `backpressure`, `unavailable` | Temporarily overloaded or store unavailable | Yes (1s) |

## Authentication

All `/v1/` operations require a fuel API key in the `Authorization: Bearer <key>` header when `api.api_keys` is configured in the server. 

Admin operations (list/retire shards, list/requeue quarantined rows) require an admin key when `api.admin_keys` is configured; otherwise they accept fuel keys. 

With no keys configured, the API is open (development mode only).

Health checks (`/healthz`, `/readyz`, `/metrics`, `/openapi.yaml`) are unauthenticated.

## Admin Operations

The four admin operations require authentication (admin key when configured, else fuel key):

- **List shards**: `GET /v1/pools/{pool}/shards[?status=unmined|mined|retired|invalid][&limit=N]` → array of `ShardInfo` (txid, size, status, lease_until, version, created_at)
- **Retire a shard**: `POST /v1/pools/{pool}/shards/{txid}/retire` → `{"retired": N}` (marks shard invalid and invalidates every non-terminal row)
- **List quarantined rows**: `GET /v1/pools/{pool}/quarantined[?limit=N]` → array of `Row` (outpoint, state, reserved_by, expires_at, quarantined_at, spent_by)
- **Requeue quarantined rows**: `POST /v1/pools/{pool}/quarantined/requeue` with `{"outpoints": [...]}` → `{"requeued": N}` (returns rows to the pool; use when you know they are unspent)

Use the CLI or Go client (see below) to invoke these.

## Go Client Quick Start

The Go client is in `pkg/client`. Import and use it like this (~25 lines):

```go
package main

import (
  "context"
  "log"

  "github.com/bsv-blockchain/fuelkeeper/pkg/client"
)

func main() {
  ctx := context.Background()

  // Create a client (with an optional API key)
  c, err := client.New("http://localhost:8080", client.WithAPIKey("mykey"))
  if err != nil {
    log.Fatal(err)
  }

  // Reserve fuel (with automatic retry on 409/503)
  res, err := c.ReserveWithRetry(ctx, "main", client.ReserveRequest{Count: 5})
  if err != nil {
    log.Fatal(err)
  }

  // Fetch and merge BEEF for all shards
  beef, err := c.InputBEEF(ctx, "main", res)
  if err != nil {
    log.Fatal(err)
  }

  // Convert to go-sdk createAction inputs (complete unlocking scripts, no signing)
  inputs, err := client.CreateActionInputs(res, "paying fees")
  if err != nil {
    log.Fatal(err)
  }

  // Your wallet builds and broadcasts a transaction using inputs and beef...
  // txid := ...

  // Report the transaction
  if _, err := c.Spend(ctx, "main", res.Token, <outpoints>, txid); err != nil {
    log.Fatal(err)
  }
}
```

### Client Helpers

- `Reserve(ctx, pool, request)` — request fuel
- `ReserveWithRetry(ctx, pool, request, opts...)` — reserve with exponential backoff (respects `Retry-After`)
- `Release(ctx, pool, token, outpoints)` — return unused fuel
- `Spend(ctx, pool, token, outpoints, txid)` — report spent fuel
- `Pools(ctx)`, `Pool(ctx, poolName)` — list pools and their stats
- `BEEF(ctx, pool, shard)` — fetch one shard's BEEF (cached per version; `304 Not Modified` on cache hit)
- `InputBEEF(ctx, pool, res)` — merge BEEF from all shards in a reservation
- `CreateActionInputs(res, description)` — convert reservation to go-sdk `wallet.CreateActionInput` values
- `Surplus(denomination, estimatedTxBytes, satPerKB)` — how much to add to your own outputs instead of creating change
- `PuzzleInputSize`, `P2PKHInputSize` — per-input byte counts for fee sizing

### Typed Errors

The client returns errors that can be checked with `errors.Is()`:

- `client.ErrExhausted` — 503 exhausted (no fuel available now)
- `client.ErrInsufficient` — 409 insufficient (fewer than `min_count`)
- `client.ErrUnauthorized` — 401 unauthorized
- `client.ErrNotFound` — 404 not found
- `client.ErrBadRequest` — 400 bad request
- `client.ErrBackpressure` — 503 backpressure
- `client.ErrUnavailable` — 503 unavailable

Check `client.IsRetryable(err)` to determine whether to retry.

## TypeScript / HTTP Clients

TypeScript wallets or other HTTP clients use the same endpoints and logic:

1. `POST /v1/pools/{pool}/reserve` with `{"count": N, "min_count"?: M, "ttl_seconds"?: T}` → reservation response with outputs and shards
2. Fetch each shard's BEEF from the `beef_url` (cache it; immutable once mined, `ETag` provided for caching)
3. Build your transaction with the outputs as inputs (each output includes `unlock.script` — the complete unlocking script, no signing needed)
4. Merge the fetched BEEF and pass it as `inputBEEF` to your transaction creation
5. Broadcast the transaction
6. `POST /v1/pools/{pool}/spend` with `{"token": "...", "outpoints": [...], "txid": "..."}` to report the spend
7. On error, `POST /v1/pools/{pool}/release` to return unused outputs

## Additional Resources

- OpenAPI spec: `/openapi.yaml` (unauthenticated, available at any FuelKeeper server)
- Health probe: `GET /healthz` (alive) or `GET /readyz` (store reachable and pools ready)
- Metrics: `GET /metrics` (Prometheus format)
- Configuration: see `docs/OPERATIONS.md`
