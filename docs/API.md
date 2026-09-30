# FuelKeeper API

FuelKeeper keeps a pool of small, fixed-denomination BSV outputs ("fuel") that applications reserve on demand and attach as one extra input to pay transaction fees. This guide covers the HTTP API and the Go client.

## Consuming Fuel in Five Calls

A typical workflow:

1. **Reserve** outputs from a pool with `POST /v1/pools/{pool}/reserve`
2. **Fetch BEEF** for each shard using the server-relative `beef_url` paths
3. **Build and broadcast** your transaction with fuel outputs as inputs (each carries a complete unlocking script; no signing required)
4. **Spend** the outputs with `POST /v1/pools/{pool}/spend` to report the transaction
5. **Release** unused outputs with `POST /v1/pools/{pool}/release` — but only once you know the transaction definitely never reached a gateway; if you're not sure (a timeout, a 5xx), make no call and let the reservation expire; if it was rejected as a double-spend, `spend` with the competing txid instead (or your own if none was named) — never `release`; see docs/CLIENTS.md

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
| **p2pkh** | Presigned `<sig><pubkey>` with SIGHASH_NONE\|ANYONECANPAY\|FORKID | `tx_version=1`, `lock_time=0`, `sequence=0xffffffff` | 148 B | High-security, controlled signings |

For P2PKH pools, the response includes a `constraints` object; your transaction must match those fields exactly or the presigned signature will not validate.

## Errors

All non-2xx responses are JSON with the shape `{"code", "message", "retry_after"?}`. Errors that include `Retry-After` carry retry guidance; clients should back off and retry after the indicated seconds.

| Status | Code | Meaning | Retry-After |
|--------|------|---------|-------------|
| 400 | `bad_request` | Malformed or invalid request | No |
| 400 | `bad_funding` | `POST /v1/funding`: the treasury could not verify or accept the payment (see [Funding errors](#errors-1)) | No |
| 401 | `unauthorized` | Invalid or missing API key (`WWW-Authenticate: Bearer` header present) | No |
| 404 | `unknown_pool` or `not_found` | Pool not found or resource missing | No |
| 404 | `funding_disabled` | `/v1/funding`: this instance has no funding treasury (mock/nullnet) | No |
| 405 | `method_not_allowed` | Wrong HTTP method; `Allow` header lists permitted methods | No |
| 409 | `insufficient` | Fewer than `min_count` outputs available; nothing is held | Yes (1s) |
| 499 | `client_closed_request` | Client disconnected before response sent | No |
| 500 | `internal` | Server error | No |
| 503 | `exhausted`, `backpressure`, `unavailable` | Temporarily overloaded or store unavailable | Yes (1s) |

## Authentication

All `/v1/` operations require a fuel API key in the `Authorization: Bearer <key>` header when `api.api_keys` is configured in the server. 

Admin operations (list/retire shards, list/requeue quarantined rows) and both funding operations (`GET`/`POST /v1/funding`) require an admin key when `api.admin_keys` is configured; otherwise they accept fuel keys. 

With no keys configured, the API is open (development mode only).

Health checks (`/healthz`, `/readyz`, `/metrics`, `/openapi.yaml`) are unauthenticated.

## Admin Operations

The four admin operations require authentication (admin key when configured, else fuel key):

- **List shards**: `GET /v1/pools/{pool}/shards[?status=unmined|mined|retired|invalid][&limit=N]` → array of `ShardInfo` (txid, size, status, owner, lease_until, version, created_at)
- **Retire a shard**: `POST /v1/pools/{pool}/shards/{txid}/retire` → `{"retired": N}` (marks shard invalid and invalidates every non-terminal row)
- **List quarantined rows**: `GET /v1/pools/{pool}/quarantined[?limit=N]` → array of `Row` (outpoint, state, reserved_by, expires_at, quarantined_at, spent_hint, spent_by)
- **Requeue quarantined rows**: `POST /v1/pools/{pool}/quarantined/requeue` with `{"outpoints": [...]}` → `{"requeued": N}` (returns rows to the pool; use when you know they are unspent)

Use the CLI or Go client (see below) to invoke these.

## Funding the Treasury (BRC-29)

FuelKeeper's treasury mints fuel from its own wallet balance. `GET`/`POST /v1/funding` let an operator top that balance up via a [BRC-29](https://github.com/bitcoin-sv/BRCs/blob/master/wallet/0029.md) wallet-to-wallet payment, driven from the operator's own BRC-100 wallet (e.g. the `fuelkeeper fund` CLI below). **Both operations are admin-tier**: funding is an operator action, and gating it behind an admin key (when `api.admin_keys` is configured) prevents anonymous nonce/BEEF spam. When no treasury is configured (e.g. nullnet/mock mode), both answer `404 funding_disabled`.

### Flow

1. **Get funding instructions**: `GET /v1/funding` → the treasury's identity key, a fresh derivation-prefix nonce, and the BRC-29 protocol ID.

   ```json
   GET /v1/funding

   { "identity_key": "02ab...", "derivation_prefix": "b64nonce==",
     "network": "main", "protocol": "3241645161d8",
     "instructions": "Derive a BRC-29 key with GetPublicKey{...}, pay a P2PKH output to it, then POST the AtomicBEEF, prefix, suffix and your identity key here." }
   ```

2. **Derive a payment key** with your own wallet: `GetPublicKey{protocolID: [2, "3241645161d8"], keyID: "<derivation_prefix> <your suffix>", counterparty: identity_key}`, where `<your suffix>` is a fresh base64 value you choose (e.g. 8 random bytes).
3. **Pay** a P2PKH output to the derived key's address with your wallet's `CreateAction` (or equivalent).
4. **Submit the payment**: `POST /v1/funding` with the AtomicBEEF of the transaction you just created, the prefix and suffix, and your own identity key.

   ```json
   POST /v1/funding
   { "atomic_beef": "<base64 AtomicBEEF>", "output_index": 0,
     "derivation_prefix": "b64nonce==", "derivation_suffix": "<your base64 suffix>",
     "sender_identity_key": "<your identity key, DER hex>" }

   → { "txid": "...", "satoshis": 5000 }
   ```

   `output_index` may be omitted to ask the treasury to auto-detect the payment output; as of this version that auto-detect path is not yet supported and answers `400 bad_funding`, so pass the output's actual index explicitly.

### Errors

| Status | Code | Meaning | Retry-After |
|--------|------|---------|-------------|
| 400 | `bad_request` | Malformed request (bad base64, a `sender_identity_key` that is not 66 hex chars) | No |
| 400 | `bad_funding` | The treasury could not verify or accept the payment (wrong derivation, missing output, insufficient value, invalid sender identity key (hex but not a valid compressed point), ...) | No |
| 404 | `funding_disabled` | This instance has no funding treasury configured | No |
| 503 | `unavailable` | The treasury is temporarily unable to internalize the payment (e.g. mid-broadcast) | Yes (1s) |

**Recovery.** A `bad_funding` or `unavailable` response never means the on-chain payment itself failed -- your wallet already broadcast it. Retry `POST /v1/funding` with the **same** `derivation_prefix` and `derivation_suffix`: the derivation nonce is HMAC-based and stays valid, so retrying does not require deriving a new key or making a new payment. `fuelkeeper fund` prints both values in its error message specifically so you can do this.

### `fuelkeeper fund` CLI

```
fuelkeeper fund --sats N [--url http://127.0.0.1:8080] [--key K | FUELKEEPER_API_KEY] [--wallet http://localhost:3321] [--output-index 0]
```

Drives your local BRC-100 wallet (default `http://localhost:3321`) through the flow above: fetches funding instructions, derives a payment key, pays a P2PKH output for `--sats` satoshis, and posts the result. `--key` must be an admin key when the server has `api.admin_keys` configured. On success it prints `funded <sats> sats in <txid>`. `--output-index` defaults to `0`; `-1` (auto-detect) is not yet supported by the treasury and is rejected as `bad_funding`. If the wallet broadcasts the payment but `POST /v1/funding` then fails, the error names the already-broadcast txid and the `derivation_prefix`/`derivation_suffix` pair to retry with -- never re-run `fuelkeeper fund` from scratch in that case, since that would pay again.

## Go Client Quick Start

The Go client is in `pkg/client`. Import and use it like this (~25 lines):

```go
package main

import (
  "context"
  "log"
  "time"

  "github.com/bsv-blockchain/fuelkeeper/pkg/client"
)

func main() {
  // ReserveWithRetry without WithMaxAttempts retries until ctx ends
  ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
  defer cancel()

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
- `BEEF(ctx, pool, shard)` — fetch one shard's BEEF (cached per shard version; immutable once mined)
- `InputBEEF(ctx, pool, res)` — merge BEEF from all shards in a reservation
- `CreateActionInputs(res, description)` — convert reservation to go-sdk `wallet.CreateActionInput` values
- `Surplus(denomination, estimatedTxBytes, satPerKB)` — how much to add to your own outputs instead of creating change
- `PuzzleInputSize`, `P2PKHInputSize` — per-input byte counts for fee sizing
- `FundingInfo(ctx)` — fetch the treasury's BRC-29 payment instructions (admin key required when configured)
- `Fund(ctx, wire.FundRequest)` — submit a completed BRC-29 payment for the treasury to internalize

### Typed Errors

The client returns errors that can be checked with `errors.Is()`:

- `client.ErrExhausted` — 503 exhausted (no fuel available now)
- `client.ErrInsufficient` — 409 insufficient (fewer than `min_count`)
- `client.ErrUnauthorized` — 401 unauthorized
- `client.ErrNotFound` — 404 not found
- `client.ErrBadRequest` — 400 bad request (also covers 405 method not allowed)
- `client.ErrBackpressure` — 503 backpressure
- `client.ErrUnavailable` — 503 unavailable (also covers transport failures and 5xx without JSON body)

Check `client.IsRetryable(err)` to determine whether to retry. `client_closed_request` (499) from the server has no client sentinel; it is not retryable.

The funding endpoints' `bad_funding` (400) and `funding_disabled` (404) codes have no dedicated sentinel either; check `apiErr.Code` on the `*client.APIError` (via `errors.As`) directly. Neither is retryable.

## TypeScript / HTTP Clients

TypeScript wallets or other HTTP clients use the same endpoints and logic:

1. `POST /v1/pools/{pool}/reserve` with `{"count": N, "min_count"?: M, "ttl_seconds"?: T}` → reservation response with outputs and shards
2. Fetch each shard's BEEF from the `beef_url` (cache it; immutable once mined, `ETag` provided for caching)
3. Build your transaction with the outputs as inputs (each output includes `unlock.script` — the complete unlocking script, no signing needed)
4. Merge the fetched BEEF and pass it as `inputBEEF` to your transaction creation
5. Broadcast the transaction
6. `POST /v1/pools/{pool}/spend` with `{"token": "...", "outpoints": [...], "txid": "..."}` to report the spend
7. On a submission failure, release only if the transaction definitely never reached a gateway (`POST /v1/pools/{pool}/release`); if you're not sure (a timeout, a 5xx), make no call and let the reservation expire; on a double-spend, `spend` with the competing txid instead (or your own if none was named) — never `release`; see docs/CLIENTS.md

**Note on development mode (nullnet)**: The mock treasury emits synthetic BEEF that the go-sdk cannot parse, so `InputBEEF` and `createAction` only work against a real treasury (Plan 5). Reservation, release, and spend operations work normally in nullnet.

## Probes and Diagnostics

- **Health**: `GET /healthz` returns 200 and `{"status":"ok"}` when the process is running
- **Readiness**: `GET /readyz` returns 200 and `{"status":"ready","pools":N,"pools_ready":M}` when the store pings and at least one pool is configured. Returns 503 if not ready. `pools_ready` is the number of pools that have adopted shards or queued rows; it may be 0 even when ready, and rises as shards are mined and adopted.
- **Metrics**: `GET /metrics` (Prometheus format, unauthenticated)
- **OpenAPI**: `GET /openapi.yaml` (full spec, unauthenticated)

## Additional Resources

- Configuration: see `docs/OPERATIONS.md`
