# Client Guide

This guide is for applications that consume fuel: go-wallet-toolbox-based
services, TypeScript wallets built on `@bsv/sdk`/wallet-toolbox, or any other
BRC-100 wallet (BSV Desktop, Metanet Desktop) driven over HTTP. It covers the
consumer contract, a Go recipe against the FuelKeeper client and
go-wallet-toolbox, an illustrative TypeScript recipe, fee sizing, P2PKH pool
constraints, and error handling. The HTTP surface itself is `docs/API.md`;
this guide is about what a caller does with it.

## The Consumer Contract

A consumer reserves one or more fuel outputs from a pool
(`POST /v1/pools/{pool}/reserve`), attaches each one to its own transaction as
an extra input using the unlocking script FuelKeeper already supplied (there
is no signing step for fuel: puzzle mode is a preimage push, P2PKH mode is
presigned), builds and broadcasts that transaction, and then tells FuelKeeper
what happened: `spend` if it broadcast, or `release` if it decided not to use
the reservation. `spend` is a hint, not proof -- FuelKeeper's chain oracle is
what actually resolves the row to the terminal `spent` state (see
`docs/ARCHITECTURE.md`'s state machine); calling `spend` only lets FuelKeeper
stop waiting for the reservation to expire.

**Reservation TTL.** Every reservation carries `ttl_seconds` (defaulting to
the pool's `default_ttl` when omitted, and clamped to the pool's `min_ttl`..
`max_ttl`). If the client neither spends nor releases before `expires_at`,
the reaper quarantines the row and asks the chain oracle whether it is
actually spent, resolving it to `spent` or `available` accordingly; with an
arcade-only oracle (the default with the toolbox treasury; otherwise the
default oracle is `none`, with the same requeue behaviour -- see
`docs/OPERATIONS.md`'s "Oracle kinds and policies"), which cannot answer
that question at all, an unreported reservation is instead requeued blind after
`jobs.reaper.unknown_requeue_age` (10 m default) under a pool's
`expiry_policy: requeue` -- or held for an operator under
`expiry_policy: hold`. A client that lets a reservation expire is not
charged for it beyond that delay, in the normal case.

**`min_count`.** A reserve request can ask for `count` outputs but accept a
partial fill of at least `min_count` (default: `count`). Fewer than
`min_count` reserved is reported as `409 insufficient` and nothing is held;
`min_count`..`count` reserved is a normal `200` with however many were
actually available.

**Errors that mean "try again".** `409 insufficient` (fewer than `min_count`
available; nothing was held) and `503 exhausted` / `503 backpressure` /
`503 unavailable` (pool momentarily out of fuel, coalescer queue full, or
store unreachable) all carry a `Retry-After` header/field. The Go client's
`client.IsRetryable(err)` tells you whether a given error is one of these
(plus any `5xx` and `429`); `ReserveWithRetry` already retries on it with
backoff (see below). A TypeScript or other HTTP client should treat the same
set of codes the same way: wait `retry_after` seconds (jittered) and retry
the same reserve call with a fresh token.

## When Your Broadcast Fails

A consumer, not FuelKeeper, is the one who learns what actually happened to
its own broadcast (accepted, rejected as invalid, or rejected as a
double-spend), so a consumer is responsible for clearing its own
reservation once it knows. `pkg/client`'s `Client.Settle` is that contract
made concrete: it maps one `Settlement` (what your broadcast attempt did) to
the one FuelKeeper call the outcome calls for, so you never have to work out
spend-vs-release under pressure yourself.

```go
func (c *Client) Settle(
    ctx context.Context, pool, token string, outpoints []string, s Settlement,
) (int, error)

type Settlement struct {
    Outcome        Outcome  // see the Outcome constants below
    TxID           string   // your own txid; required for Broadcast, and for
                             // DoubleSpend when CompetingTxIDs is empty
    CompetingTxIDs []string // known double-spenders of TxID, most preferred
                             // first; only consulted for DoubleSpend
}
```

`Outcome` is one of `OutcomeBroadcast`, `OutcomeNotBroadcast`,
`OutcomeDoubleSpend` or `OutcomeUnknown`. A `TxID` `Settle` needs but does
not have, or that is not 64 lowercase hex characters, is
`ErrSettleTxIDRequired` -- caught client-side before it round-trips to the
server for the same error.

| Your broadcast attempt | Go: `client.Settle(..., Settlement{...})` | TS wallet-toolbox equivalent | What FuelKeeper does |
|---|---|---|---|
| Gateway accepted the transaction | `Outcome: OutcomeBroadcast, TxID: txid` | `createAction` returned normally (no `WERR_REVIEW_ACTIONS`), or a review result with status `success` | `Spend(token, outpoints, txid)`: reserved -> quarantined (hint), or straight to `spent` under `spend_policy: trust` |
| The transaction provably never reached a gateway: a dial-phase failure (connection refused, or any error before the request body was written -- it was never sent), or a gateway's well-formed rejection naming no competing transaction, sent before the gateway ever accepted the request (bad format/script/fee/policy; the output itself is untouched) | `Outcome: OutcomeNotBroadcast` | `WERR_REVIEW_ACTIONS` review result status `invalidTx` | `Release(token, outpoints)`: reserved -> available (or, under `release_policy: verify`, quarantined and requeued blind after `jobs.reaper.unknown_requeue_age` -- arcade alone cannot confirm it, see `docs/OPERATIONS.md`'s oracle x policy table) |
| Rejected because an input was already spent, or a competing transaction is known | `Outcome: OutcomeDoubleSpend, TxID: txid, CompetingTxIDs: [...]` (the first well-formed competitor is reported; falls back to `TxID` when none of `CompetingTxIDs` is well-formed) | `WERR_REVIEW_ACTIONS` review result status `doubleSpend` (`CompetingTxIDs` from its `competingTxs`) | `Spend(token, outpoints, hint)`: reserved -> quarantined (hint); the reaper resolves it to `spent` once it can prove the fuel output is gone -- a gateway that names the spent inputs and this outpoint is among them (checked first, resolved in one tick), a mined competitor (only when the gateway named no spent inputs), or (failing either) the bounded-loss assumption that the client's own rejected input was this one (see `docs/OPERATIONS.md`'s reaper lifecycle table, rule 3) |
| Fate not known: the request may have reached the gateway and been accepted, and its outcome is simply unclear -- a transport error *past* the dial phase (so the request, or part of it, may already be on the wire), `ctx` ending mid-request, an unexpected HTTP status such as a `5xx`, a 2xx response your own client failed to decode, **or the gateway's own backpressure/shedding response** | `Outcome: OutcomeUnknown` | `WERR_REVIEW_ACTIONS` review result status `serviceError`, or the action's own deadline expired | No call. The reservation is left in place; if it is never settled, it expires and resolves through the reaper's usual quarantine/requeue path (see below) |

Backpressure belongs under "fate not known," not "never reached a gateway":
against arcade specifically, a 503/backpressure response does not mean the
submission was refused -- arcade persists a `RECEIVED` row carrying the raw
transaction *before* the downstream publish that can answer 503, and
arcade's own propagation reaper rebroadcasts a stale `RECEIVED` row an hour
later. Releasing on a gateway's backpressure response can therefore hand an
already-queued output to the next consumer the same way releasing on any
other ambiguous failure can. Treat a gateway's backpressure response as
`OutcomeNotBroadcast` only if you know that specific gateway rejects before
persisting anything; arcade does not.

**Do not treat an ambiguous failure as "not broadcast."** Release only once
you know the transaction definitely never reached a gateway (`docs/API.md`
uses this same wording); if you're not sure -- a timeout, a `5xx`, backpressure
-- make no call and let the reservation expire instead. If the request might
have reached the gateway before failing, report `OutcomeUnknown`, not
`OutcomeNotBroadcast`: releasing an output that was in fact already
broadcast (or merely accepted and queued) hands it to the next consumer,
whose own attempt to spend it then fails as a double-spend.
`OutcomeNotBroadcast` is only for a gateway's definitive "no" (or a request
that never went out at all). `cmd/fueldemo`'s raw-mode worker
(`pkg/client`'s own reference consumer) draws this same line: a dial-phase
failure (connection refused), or a well-formed rejection naming no
competitor, is `OutcomeNotBroadcast`; a `*arcadeclient.BackpressureError`, a
transport failure past the dial phase, `ctx` ending mid-POST, an unexpected
status, a 2xx body with a terminal `REJECTED` verdict inline, or an
undecodable 2xx body is `OutcomeUnknown` -- and, since a dial failure or
backpressure or an undecodable/unexpected response is also evidence the
*gateway itself* may be struggling, the worker abandons the rest of its
current batch (releasing its untried outputs) and backs off before its next
`Reserve`, rather than retrying every remaining output individually against
a gateway that is probably still down.

**Against arcade specifically**, a double-spend is not knowable at
submission time: POST /tx's synchronous responses never carry a competing
txid, so a consumer submitting straight through arcade reports
`OutcomeBroadcast` the moment arcade accepts the transaction, exactly like
any other accepted submission. Arcade never populates `competingTxs` or a
`DOUBLE_SPEND_ATTEMPTED` status at all, synchronously or otherwise; what a
later GET /tx actually shows is a REJECTED verdict with ARC code 466 and the
underlying teranode rejection line verbatim in `extraInfo` (e.g.
`UTXO_SPENT (70): <txid>:<vout> utxo already spent by tx <spender>[<vin>]`),
which FuelKeeper's own oracle parses for the spent outpoint and, when named,
the spender -- discovered by whoever next polls the transaction's fate
(FuelKeeper's own reaper, under `spend_policy: verify`), not by the original
consumer calling `Settle` a second time. `OutcomeDoubleSpend` exists for
gateways that do name a competitor synchronously, and for a go-wallet-toolbox-backed consumer
whose own asynchronous review process (`WERR_REVIEW_ACTIONS`) learns of one
after the fact but before it calls `Settle`.

**The consequence for `spend_policy: trust`** (the default whenever the
oracle cannot answer outpoint queries directly -- arcade-only with the
toolbox treasury, or `oracle.kind: none` otherwise, with the same
behaviour; the blind spot below applies to any deployment submitting
through arcade, regardless of which of those two `oracle.kind` is
configured). Because arcade's 202 accepted response is not a guarantee -- the
transaction can still be rejected asynchronously (one of *your own*
transaction's other inputs already spent, `TX_INVALID`, or a transient
`TX_LOCKED` if you chained off a fuel output whose own split transaction was
still settling) -- reporting `OutcomeBroadcast` on that 202 under
`spend_policy: trust` moves the row straight to the terminal `spent` state,
and nothing ever re-examines it afterward. If arcade does go on to reject
the transaction, the fuel output you spent is lost for good, not merely
delayed. If your deployment cannot tolerate that, ask the operator to
configure `spend_policy: verify` for the pool: your report is then held as
a hint and resolved by the reaper from the transaction's own eventual fate
instead of trusted outright (see `docs/OPERATIONS.md`'s "The
`spend_policy: trust` blind spot with arcade-only").

**Expiry behaviour.** A reservation nobody settles (no `spend`, `release` or
`Settle` call before `expires_at`) is quarantined by the reaper like any
other expired reservation: with an arcade-only oracle (the expected shape --
see `docs/OPERATIONS.md`'s "Oracle kinds and policies"), it requeues to
`available` blind, with no proof either way, after
`jobs.reaper.unknown_requeue_age` (10 m default) under a pool's
`expiry_policy: requeue` (default), or stays quarantined for an operator
under `expiry_policy: hold`. A hinted row (you called `Spend` or `Settle`
under `spend_policy: verify`) instead follows its own transaction's fate,
requeuing after `hint_max_wait` (24 h default) only as a last resort if that
transaction is never mined or classified. This is not an absolute
guarantee that every unspent output eventually comes back, or that a
released/requeued output was never in fact spent: see "The
`spend_policy: trust` blind spot" and the reaper's rule 3 bounded loss in
`docs/OPERATIONS.md`'s "Oracle kinds and policies" for the cases where an
output is genuinely lost, not merely delayed, and `expiry_policy: hold`'s
effect of leaving a row quarantined until an operator acts rather than
requeuing it automatically.

> **This contract is the safety boundary.** FuelKeeper's reaper trusts the
> outcome you report, within these two invariants:
>
> - **Never `Release` on a double-spend.** A spent output released back to
>   the pool would be reserved by the next consumer, whose own broadcast
>   then fails the exact same way -- and the one after that, and the one
>   after that (a poison pill).
> - **Never `Spend` on a rejection that names no competing transaction.**
>   Under `spend_policy: trust`, a `Spend` is terminal the moment the server
>   accepts it -- no oracle ever re-examines it -- so an invalid-but-not-
>   double-spent output would be burned rather than returned to the pool for
>   reuse.
>
> `Settle` exists precisely so you never have to get this mapping right by
> hand under pressure: pass it the `Outcome` you actually observed and it
> picks the correct call.

## Go Recipe (go-wallet-toolbox)

```go
import (
    "context"
    "time"

    "github.com/bsv-blockchain/fuelkeeper/pkg/client"
    sdk "github.com/bsv-blockchain/go-sdk/wallet"
)

c, err := client.New("https://fuel.example.com", client.WithAPIKey(apiKey))
if err != nil {
    return err
}

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// Reserve fuel; retries 409/503 with backoff until ctx ends (or WithMaxAttempts).
res, err := c.ReserveWithRetry(ctx, "main", client.ReserveRequest{
    Count:    5,
    MinCount: 1,             // 0 = Count
    TTL:      2 * time.Minute, // 0 = pool default
})
if err != nil {
    return err
}

// Fetch and merge the BEEF of every shard the reservation touched.
inputBEEF, err := c.InputBEEF(ctx, "main", res)
if err != nil {
    return err
}

// Convert the reservation into go-sdk CreateActionInput values (unlocking
// scripts already filled in; no signing).
inputs, err := client.CreateActionInputs(res, "paying fees")
if err != nil {
    return err
}

acceptDelayed := false
args := sdk.CreateActionArgs{
    Description: "paying fees",
    InputBEEF:   inputBEEF,
    Inputs:      inputs,
    Outputs:     myOutputs, // your own outputs; add Surplus() to one of them (see below)
    Options: &sdk.CreateActionOptions{
        AcceptDelayedBroadcast: &acceptDelayed,
    },
}
// For a P2PKH pool, pin the transaction-level version/lockTime the presigned
// signature commits to; a no-op for puzzle pools (res.Constraints is nil).
client.ApplyConstraints(&args, res)

result, err := wallet.CreateAction(ctx, args, "myapp")
if err != nil {
    return err
}

// Report the outcome via Settle rather than calling Spend/Release directly
// -- see "When Your Broadcast Fails" above for the full outcome table and
// why getting spend-vs-release backwards is costly.
outpoints := make([]string, len(res.Outputs))
for i, o := range res.Outputs {
    outpoints[i] = o.Outpoint
}
s := client.Settlement{Outcome: client.OutcomeBroadcast, TxID: result.Txid.String()}
if _, err := c.Settle(ctx, "main", res.Token, outpoints, s); err != nil {
    // Non-fatal: your transaction already broadcast. Log and move on; see
    // "On Errors" below.
}
```

`go-sdk`'s `wallet.CreateActionOptions` fields are `*bool`/`*uint32`/`[]chainhash.Hash`
(`AcceptDelayedBroadcast`, `NoSend`, `RandomizeOutputs`, `SendWith`, ...), so
build the pointer explicitly as shown. `wallet` above is anything implementing
go-sdk's `wallet.Interface` (`CreateAction(ctx, args, originator)`); a
go-wallet-toolbox `*wallet.Wallet` implements it directly.

### Field names, exactly

- `client.ReserveRequest{Count int, MinCount int, TTL time.Duration}` --
  `MinCount: 0` means `Count`; `TTL: 0` means the pool default.
- `client.New(baseURL string, opts ...client.Option)` with
  `client.WithAPIKey(key)`, `client.WithHTTPClient(hc)`, `client.WithObserver(fn)`.
- `c.Reserve(ctx, pool, req)`, `c.ReserveWithRetry(ctx, pool, req, opts...)`,
  `c.Release(ctx, pool, token, outpoints)`, `c.Spend(ctx, pool, token, outpoints, txid)`,
  `c.Settle(ctx, pool, token, outpoints, client.Settlement{Outcome, TxID, CompetingTxIDs})`
  -- see "When Your Broadcast Fails" above.
- `client.InputBEEF(ctx, pool, res)` merges every shard's BEEF (fetched
  through the client's own per-shard cache, so an already-cached shard costs
  nothing).
- `client.CreateActionInputs(res, description)` -- `description` must be
  5..2000 bytes (the go-wallet-toolbox's own bound); a reservation containing
  a duplicate outpoint is rejected here rather than reaching the wallet as a
  confusing downstream error.
- `client.ApplyConstraints(args *wallet.CreateActionArgs, res)` sets
  `args.Version`/`args.LockTime` from `res.Constraints` and is a no-op when
  `res.Constraints` is nil (puzzle mode).
- `client.Surplus(denomination uint64, estimatedTxBytes int, satPerKB uint64) uint64`
  (see "Fee sizing" below).
- `client.IsRetryable(err error) bool`.

### Batching advice

Reserve `count` per worker round rather than one output per transaction: one
`Reserve` call that returns, say, 20 outputs amortizes the coalescer's
group-commit flush and the HTTP round trip across 20 future transactions
instead of paying for each individually. Keep the round's `count` a little
above what you expect to spend, and `Release` the unused tail at the end of
the round (or let it expire, if the wait is short and the pool is not tight)
rather than holding reservations open indefinitely. `ReserveWithRetry`'s
backoff already handles a pool that is temporarily out of fuel; a worker loop
does not need its own retry logic on top of it.

### Error handling table

| Client error | Meaning | What to do |
|---|---|---|
| `client.ErrInsufficient` (409) | Fewer than `min_count` available; nothing held | Retry after `Retry-After` (or use `ReserveWithRetry`) |
| `client.ErrExhausted` (503) | Pool has no fuel right now | Retry after `Retry-After` |
| `client.ErrBackpressure` (503) | Coalescer queue full | Retry after `Retry-After` |
| `client.ErrUnavailable` (503/500/transport) | Store or server unavailable | Retry with backoff |
| `client.ErrUnauthorized` (401) | Missing/invalid API key | Fix the key; not retryable |
| `client.ErrNotFound` (404) | Unknown pool or resource | Fix the pool name; not retryable |
| `client.ErrBadRequest` (400/405) | Malformed request | Fix the request; not retryable |

`client_closed_request` (499, meaning the server saw your own context end
mid-request) has no dedicated sentinel: you already know this from the
`ctx.Err()` your own call returned.

## TypeScript Recipe (`@bsv/sdk` / wallet-toolbox)

Illustrative (~20 lines); field names on the wire are snake_case exactly as
`pkg/api/wire/wire.go` defines them (see `docs/API.md`).

```ts
import { WalletClient, Beef } from '@bsv/sdk'

const wallet = new WalletClient()
const headers = { Authorization: `Bearer ${apiKey}` }

const res = await (await fetch(`${baseURL}/v1/pools/main/reserve`, {
  method: 'POST', headers: { ...headers, 'Content-Type': 'application/json' },
  body: JSON.stringify({ count: 5, min_count: 1, ttl_seconds: 120 }),
})).json()

// Fetch and merge each shard's BEEF. beef_url is server-relative
// ("/v1/pools/main/beef/<txid>"); cache by ETag ("v<version>") and send
// If-None-Match on a later fetch of the same shard (304 means unchanged).
// X-Shard-Status is unmined|mined|retired|invalid.
const merged = new Beef()
for (const shard of res.shards) {
  const beefResp = await fetch(`${baseURL}${shard.beef_url}`, { headers })
  merged.mergeBeef(new Uint8Array(await beefResp.arrayBuffer()))
}

// FuelKeeper's outpoint strings are "txid:vout"; BRC-100's OutpointString is
// "txid.vout" -- convert before building createAction's inputs. For a P2PKH
// pool, each fuel input's sequenceNumber must also be pinned to
// res.constraints.sequence (the presigned signature commits to it).
const inputs = res.outputs.map((o: any) => ({
  outpoint: o.outpoint.replace(':', '.'),
  unlockingScript: o.unlock.script, // hex; complete, no signing needed
  inputDescription: 'paying fees',
  sequenceNumber: res.constraints ? res.constraints.sequence : undefined,
}))

const { txid } = await wallet.createAction({
  description: 'paying fees',
  inputBEEF: merged.toBinary(),
  inputs,
  outputs: myOutputs, // add Surplus() worth of satoshis to one of your own outputs
  // P2PKH pools only: pin the transaction to what the presigned signature
  // commits to (tx_version 1, lock_time 0); omitted (undefined) for puzzle
  // pools, where res.constraints is absent.
  version: res.constraints?.tx_version,
  lockTime: res.constraints?.lock_time,
  options: { acceptDelayedBroadcast: false },
})

await fetch(`${baseURL}/v1/pools/main/spend`, {
  method: 'POST', headers: { ...headers, 'Content-Type': 'application/json' },
  body: JSON.stringify({ token: res.token, outpoints: res.outputs.map((o: any) => o.outpoint), txid }),
})
```

Notes:

- **BEEF caching.** `GET /v1/pools/{pool}/beef/{txid}` returns
  `Cache-Control: immutable` once the shard is mined, and its `ETag` is
  `"v<version>"` (the shard's version, which bumps when the BEEF changes,
  e.g. unmined -> mined). Send `If-None-Match` with a previously-seen ETag to
  get `304` instead of re-downloading. `X-Shard-Status` tells you the current
  status without parsing the BEEF.
- **Merging several shards.** A reservation can span more than one shard
  (`res.shards` has more than one entry) when a request draws from more than
  one owned shard. Merge every shard's BEEF into one `Beef` with
  `mergeBeef` before calling `createAction`; `Beef.mergeBeef(beef: Beef |
  number[] | Uint8Array): void` (confirmed against the installed
  `@bsv/sdk` types) replaces any transaction with the same txid and is safe
  to call once per shard in any order.
- **Outpoint format.** FuelKeeper's wire format is `"<txid>:<vout>"`
  (see `openapi/fuelkeeper.yaml`'s `Output.outpoint`); BRC-100's
  `OutpointString` (what `createAction`'s `inputs[].outpoint` expects) is
  `"<txid>.<vout>"`. Convert the separator, as shown above, before building
  `createAction`'s input list. `spend`/`release` requests, by contrast, use
  FuelKeeper's own colon form directly (the values FuelKeeper handed you).
- **Development mode caveat.** Against the mock/nullnet treasury, shard BEEF
  is synthetic and will not parse as real BEEF; `createAction` against it
  only works with a real (toolbox) treasury. Reserve/release/spend work
  normally in either mode.

## Fee Sizing and Surplus

FuelKeeper hands out fixed-denomination outputs. Two input sizes matter for
fee math:

| Lock mode | Unlocking script | Input size |
|---|---|---|
| `puzzle` | 33-byte preimage push | 74 B |
| `p2pkh` | presigned `<sig><pubkey>`, `SIGHASH_NONE\|ANYONECANPAY\|FORKID` | 148 B |

(`fuel.PuzzleInputSize` / `fuel.P2PKHInputSize`, re-exported as
`client.PuzzleInputSize` / `client.P2PKHInputSize`.)

Fee is `ceil(size_bytes * sat_per_kB / 1000)` -- `fuel.FeeSats(sizeBytes,
satPerKB)` computes exactly this rounding (the same ceiling arcade, go-sdk
and the wallet toolboxes use; node policy floors, so ceiling always clears
it). `client.Surplus(denomination, estimatedTxBytes, satPerKB)` returns
`denomination - fee`, floored at 0: it is the amount of a fuel output's value
left over once the fee is paid, meant for a caller that wants to add it to one
of its own outputs rather than create a change output out of fuel.

**Go-wallet-toolbox behaviour vs. raw builds.** The Go toolbox funder can
transact on fuel input(s) alone when `fuel >= outputs + fee`: a surplus above
the dust floor (~40 sat) becomes one change output automatically, and a
smaller surplus is simply absorbed as extra fee (never a hard failure on
small surplus, unlike the TypeScript toolbox). If you build a raw transaction
yourself (go-sdk directly, not through `createAction`), there is no such
automatic change output -- you must explicitly add the surplus to one of your
own outputs (or accept it going to miners) using `Surplus`/`FeeSats`
yourself.

**Worked example.** A pool denominated at 50 sat, feeding a 300-600 B
transaction at 100 sat/kB:

- fee = `ceil(300 * 100 / 1000)` = 30 sat .. `ceil(600 * 100 / 1000)` = 60 sat
- surplus = `50 - 30` = 20 sat .. `50 - 60` -> floored to 0

A 300 B transaction leaves 20 sat of surplus (add it to your own output or
let it go to the miner as extra fee); a 600 B transaction leaves none (the
50-sat fuel output barely covers its own fee at this rate -- size the pool's
denomination and `count` so that `denomination` comfortably exceeds
`FeeSats(estimatedTxBytes, satPerKB)` for your typical transaction, or
reserve more than one fuel output per transaction).

## P2PKH Pool Constraints

A `p2pkh`-mode pool presigns each fuel output at split time with
`SIGHASH_NONE|ANYONECANPAY|FORKID`. That sighash flag signs none of the
consuming transaction's outputs and only this one input -- but the FORKID
sighash preimage still always commits to the transaction's `nVersion`, this
input's own `nSequence`, and the transaction's `nLockTime`, even under
`ANYONECANPAY`. A presigned P2PKH fuel output is therefore only ever valid
when the consuming transaction uses exactly:

| Field | Required value |
|---|---|
| `tx_version` | 1 |
| `lock_time` | 0 |
| `sequence` (this input only) | `0xffffffff` |

The reserve response's `constraints` object carries these three values for a
P2PKH pool (`nil`/absent for puzzle pools, which impose no such constraint).
`client.ApplyConstraints` sets the transaction-level version/lockTime from it
on a `wallet.CreateActionArgs`; `client.CreateActionInputs` pins the matching
per-input `SequenceNumber` when `res.Constraints` is set. Get any of the
three wrong and the presigned signature simply fails to verify -- there is no
partial credit.

**Treat unlocking data as a bearer secret.** Because `SIGHASH_NONE` signs no
outputs at all, whoever holds a presigned P2PKH unlocking script can redirect
that output to any destination. The same is true of a puzzle preimage: anyone
holding it can spend the output. FuelKeeper hands unlocking data to whoever
reserved the row and nowhere else; treat it the same way in your own
code -- never log it, and only pass it to the wallet call that consumes it.

## On Errors

| Status/code | What it means | What to do |
|---|---|---|
| `409 insufficient` | Fewer than `min_count` reserved; nothing held | Retry with `Retry-After` |
| `503 exhausted` | No fuel available right now | Retry with `Retry-After` |
| `503 backpressure` | Server shedding load | Retry with `Retry-After` |
| `503 unavailable` | Store/server temporarily down | Retry with backoff |
| `401 unauthorized` | Missing/invalid API key | Fix the key |
| `404 not_found` / `unknown_pool` | Pool or resource missing | Fix the pool name/path |

**On a `spend` call failing** (network error, `503`, or any other non-2xx):
your transaction already broadcast, so do not undo anything. Retry `spend`
with the *same token and outpoints* -- it is idempotent (a second call, even
with a different txid, is ignored: only the response's `accepted` count is
meaningful) -- or simply let it go: if the outputs are never confirmed
`spend`, the reservation still expires normally, gets quarantined, and the
oracle resolves it from the chain either way (found spent -> `spent`; not
found -> requeued to `available` once the row has aged past
`min_unspent_age`/`hint_patience`). A missed `spend` call costs you nothing
but a slightly slower resolution.

**Idempotency in general.** Every FuelKeeper mutation is a row-level
compare-and-set keyed on the reservation token, so replaying the same
`reserve`/`release`/`spend` call (same token, same outpoints) after a timeout
or a retry is always safe: a `reserve` replay under the same token returns the
same rows without re-stamping their expiry; `release`/`spend` on a row not
currently reserved under that token is silently skipped and counted, never an
error.
