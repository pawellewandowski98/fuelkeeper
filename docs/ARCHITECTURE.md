# Architecture

FuelKeeper is a standalone Go service (module `github.com/bsv-blockchain/fuelkeeper`)
that turns fee funding into a reservation API: it splits a funding payment
into a large pool of fixed-denomination outputs whose unlocking data is
prepared in advance, and hands them out to clients on request under a durable
reservation. This document is the as-built architecture: components, the fuel
row state machine, where the two correctness guarantees are actually
enforced, the reserve request flow, the background jobs, the treasury flow,
the oracle, and the store contract. It is distilled from
`docs/superpowers/specs/2026-09-29-fuelkeeper-design.md` (the approved design)
and verified against the code in this tree. `docs/API.md`, `docs/FUNDING.md`,
`docs/TREASURY.md`, `docs/OPERATIONS.md` and `docs/CLIENTS.md` cover the HTTP
surface, funding flow, treasury tuning, operations and client usage
respectively; this document is the map connecting them.

## Component Diagram

```
                         BRC-29 funding payment
                                  |
                                  v
 client wallets          +-------------------------------------------+
 (go-wallet-toolbox,      |  FuelKeeper instance                      |
  TypeScript wallet-      |                                           |
  toolbox, BSV Desktop /  |  api      HTTP/JSON handlers, OpenAPI,    |
  Metanet Desktop,        |           auth, funding endpoints          |
  generic BRC-100)        |                                           |
        |                 |  pool     shard manager  -- leases shards |
        | reserve/        |           allocator      -- per-pool FIFO |
        | release/spend   |           coalescer      -- group commit  |
        v                 |                                           |
  -------------------->   |  jobs     reaper   (expiry -> quarantine  |
                          |                      -> oracle)           |
                          |           watcher  (unmined -> mined,     |
                          |                      rebroadcast, alert)  |
                          |           minter   (deficit -> Build ->   |
                          |                      PutShard -> Send)    |
                          |           each behind a leased runner:    |
                          |           one active runner per job       |
                          |           across instances                |
                          |                                           |
                          |  treasury embedded go-wallet-toolbox      |
                          |           wallet (mock treasury in dev)   |
                          |                                           |
                          |  oracle   arcade | teranode | composite   |
                          |           | mock | none                   |
                          +----+-------------------------+------------+
                               |                          |
                               v                          v
                         fuel store                 treasury storage
                    (memory | sqlite |          (go-wallet-toolbox GORM:
                     postgres; pebble/               sqlite | postgres)
                   mongodb/aerospike: Plan 4)
```

Many instances can share one fuel store (Postgres) and scale horizontally;
SQLite and the in-memory backend are single-instance. Background jobs run on
one instance at a time under a store-held lease (`jobs/<name>`), regardless of
how many instances are running. See `docs/OPERATIONS.md` for deployment
topologies.

## Fuel Row State Machine

Each fuel row (one output, keyed by `(txid, vout)` with `pool` as an
attribute) is in exactly one state:

```
available ---Reserve(token,ttl)---> reserved
reserved  ---Release(token)-------> available
reserved  ---Spend(token,txid)----> quarantined (SpentHint=txid)    spend is a HINT
reserved  ---expiry (reaper)------> quarantined (SpentHint=nil)
quarantined --oracle proves spent-> spent (SpentBy=txid)            terminal, oracle-proven
quarantined --oracle proves unspent (after age)--> available
quarantined --no verdict, aged past unknown_requeue_age (expiry_policy: requeue)--> available   blind requeue, unhinted
quarantined --hinted tx rejected with inputs already spent--> spent (SpentBy=hint)              terminal, see below
quarantined --hinted tx never resolved, aged past hint_max_wait--> available                     blind requeue, hinted
any non-terminal row --RetireShard--> invalid                       terminal, split tx dead
```

`spent` and `invalid` are the only terminal states. A client's `spend` call
is never itself proof: it only lets FuelKeeper stop waiting out the
reservation's TTL before checking. The row only reaches `spent` once the
chain oracle proves it (`pkg/jobs/reaper.go`'s terminal-state policy, below);
under `spend_policy=trust` (no oracle configured) `Spend` moves the row
straight to `spent`, trusting the client outright. `release_policy=verify`
routes `Release` through the same quarantine-then-oracle path as an expired
reservation instead of returning the row straight to `available` -- with an
arcade-only oracle this is a delayed release, requeued after
`unknown_requeue_age` rather than rejected by config (see
`docs/OPERATIONS.md#oracle-kinds-and-policies`).

An unhinted quarantined row with no oracle verdict (the normal case with an
arcade-only oracle, whose `Outpoints` always answers Unknown) is not stuck
forever: a pool's `expiry_policy: requeue` (default) returns it to
`available` once it has been quarantined for `jobs.reaper.unknown_requeue_age`
(default 10 m) -- a blind requeue, with no proof either way, that a rare
stale hand-out then surfaces as a double-spend rejection at its next
consumer (see `docs/CLIENTS.md#when-your-broadcast-fails`); `expiry_policy:
hold` keeps such a row quarantined for an operator instead. Pool capacity
never leaks either way.

**Reaper terminal-state policy** (`pkg/jobs/reaper.go`): only an
oracle-proven, **mined** spend retires a row, with one exception: a hinted
row (`spend_policy=verify`) whose own transaction is rejected because one of
its inputs is already spent is retired too, `SpentBy` the hint itself (not
the unknown spender), decided in this order: (a) the hint's own known inputs
exclude the fuel output -- holds the row (it says nothing about this
output); (b) the gateway named the spent inputs
(`oracle.TxStatus.SpentInputs`, parsed from arcade's `UTXO_SPENT` line) --
includes the fuel output: retire it (confirmed); excludes it: requeue it
like an invalid transaction after `hint_patience` (confirmed untouched, this
outranks (c)); (c) no named spent inputs, but a mined competitor provably
spends only some *other* input -- holds the row; (d) otherwise (the gateway
named no spent input at all) the fuel output is assumed gone and the row is
retired -- the bounded-loss case: an honest wallet whose transaction died on
an unrelated input, against a gateway that does not name which input, still
loses this one fuel output. Short of that one exception, a candidate
spending txid --
whether it is the outpoint oracle's own `SpendingTxID` or a client's
`SpentHint` used as a fallback -- is confirmed via `Oracle.Tx` and must be
`Known && Mined && !Rejected`; a mempool-only spend is never enough on its
own, even one the outpoint oracle itself reports, since it could still be
evicted or lose a double-spend race before being mined. When the mined
verdict's `Inputs` is non-nil, the reaper also requires the outpoint actually
be a member of it (the **Inputs cross-check**) before resolving to spent --
this closes the gap where a hostile client under `spend_policy=verify` could
name any mined, unrelated txid as its hint. When `Inputs` is nil (an oracle
that cannot serve raw transactions), that check cannot be performed and a
mined verdict naming the outpoint's txid is trusted as before; in that
configuration the outpoint oracle is the deployment's real safety boundary.
An `Unknown` verdict on an unhinted row (requeued instead, per above), a
`Conflicting` verdict, an oracle error, or a `Spent`
verdict naming no spending txid at all all leave the row quarantined for a
later tick -- the reaper never guesses.

**Shard statuses**: `unmined -> mined -> retired | invalid`. A shard is one
split transaction; its rows only leave `available` (become servable) once the
shard reaches `mined` under the default `serve_policy=mined_only` (or
immediately, at higher risk, under `serve_policy=any`). `retired` and
`invalid` are terminal: `RetireShard` marks the shard invalid, clears its
lease, bumps its version, and moves every non-terminal row (available,
reserved, quarantined) of that shard to `invalid`, preserving their
provenance fields.

## Guarantees and Where They Are Enforced

- **G1**: No fuel output is ever handed to two live reservations.
- **G2**: Every output is always in exactly one state, and an unspent output
  always returns to the pool or ends provably spent or invalid.

Both rest on one mechanism, stated in `pkg/store/store.go`'s contract: every
state transition is a compare-and-set on the row's current state (and, for
`Release`/`Spend`, on `reserved_by == token`). Concretely:

- **Row-level CAS in the store** is the actual correctness primitive.
  `Reserve` applies to a row only when `state == available` or
  `(state == reserved AND reserved_by == token)`; every other mutation
  re-asserts the row's current state (and owner, where relevant) in the same
  atomic write. A row that fails the predicate is silently skipped and
  reported through the applied/missed set or changed count -- never an error.
  This is backend-independent: Postgres, SQLite and the in-memory store all
  implement the identical predicate (see "Store Contract" below).
- **The allocator's no-double-in-flight invariant** (`pkg/pool/allocator.go`)
  ensures one instance never hands the same outpoint to two concurrent
  requests before the store CAS even runs: every row `Add` accepts is tracked
  as either `queued` or `inflight` until `Forget` or `RemoveShard` releases
  it, and a tracked row is never re-added, which makes background rescans
  safe while rows are in flight to the coalescer.
- **The coalescer's idempotent retry and single compensation**
  (`pkg/pool/coalescer.go`) makes an in-doubt store write safe to retry: a
  `Reserve` replay under the same token is idempotent by the CAS predicate
  above, so a flush that times out is retried (bounded attempts, exponential
  backoff) before the coalescer gives up and issues exactly one compensating
  `Release` over the ops it actually sent -- not once per request sharing
  that flush -- and fails those requests with a retryable error. A request's
  rows are only ever "unflushed" (safe to `Return` to the allocator's FIFO)
  when they never reached the store at all.
- **The reaper never guesses** (previous section): only a mined, oracle-
  proven, input-verified spend is terminal. This is what makes G2's "ends
  provably spent" hold in the presence of a lying or crashed client.
- **Leases are not load-bearing.** Shard leases (`Owner`, `LeaseUntil`) and
  the allocator's in-memory FIFOs are optimizations that make CAS conflicts
  rare, not the correctness mechanism: two instances may race `Reserve` on
  the same row (e.g. during a lease hand-over, or an instance serving a
  shard whose lease has since expired), and the row's CAS is what actually
  decides. `AcquireShard` never writes rows, so an instance crash mid-adopt
  loses nothing. This is why the store contract explicitly calls leases "an
  optimization only."

## Sharded Ownership and the Reserve Request Flow

**Shard manager** (`pkg/pool/shardmgr.go`, per instance): adopts shards
pull-based, one at a time, while in-memory available rows are below
`low_water_rows` and owned shards are below `max_shards`. `AcquireShard`
picks the oldest adoptable shard (by `created_at`, then txid) whose status is
in the serve set and whose lease is free, excluding shards this instance
recently drained. Adoption never writes fuel rows. Renewal (`RenewOnce`,
roughly every `lease_ttl/3`) drops a shard on `ErrNotOwner`/`ErrNotFound`, on
a status no longer in the serve set, or on a lease stale for more than one
full TTL under a persistent renew error; a CAS miss on a shard's row
(`NoteCASMiss`) triggers an immediate targeted rescan of just that shard
ahead of its regular turn. A shard that rescans empty
`emptyRescansMax` (2) times in a row is dropped and excluded from
re-adoption for 60 seconds. Graceful shutdown releases every owned shard.

**Allocator** (`pkg/pool/allocator.go`, per pool): the in-memory FIFO of
available rows across this instance's owned shards, oldest shard first, so a
request usually maps to one shard (one BEEF to fetch). No negative cache: an
empty FIFO triggers adoption or a rescan, never a cached "nothing available"
verdict.

**Coalescer** (`pkg/pool/coalescer.go`, per pool): batches concurrent
`Reserve` calls into group-commit store writes. Flush slots and the max batch
size come from `Store.Capabilities()` (`MaxConcurrentFlushes`, `MaxBatch`).
Backpressure trips at `max_inflight_ops` (default `4 * Slots * MaxBatch`):
above it, `Submit` returns `ErrBackpressure` (503) immediately.

**Reserve request flow** (`Pool.Reserve`, `pkg/pool/service.go`):

1. Validate `count` (1..`max_count`), `min_count` (1..`count`, default
   `count`) and `ttl_seconds` (pool `min_ttl`..`max_ttl`, default
   `default_ttl`); a violation is `400 bad_request`.
2. Generate a 16-byte token and compute `expires_at`.
3. Up to **3 rounds**: `Allocator.Pop(need)`; if short and adoption has not
   yet been tried this request, one bounded `ShardManager.EnsureRows` call
   (capped by `instance.adopt_timeout`, default 200 ms) tries to top up
   before falling through. Rows popped are submitted to the coalescer under
   the request's token; applied rows are counted toward `need`, missed rows
   (CAS lost) are forgotten and trigger a targeted rescan of their shard, and
   an unflushed batch (never reached the store, e.g. backpressure) returns
   its rows to the allocator's FIFO front.
4. Fewer than `min_count` applied: any partially-applied rows are
   best-effort released, and the request fails with `409 insufficient`
   (some, but not enough) or `503 exhausted` (none at all) or a mapped
   store/coalescer error -- both `409` and `503` here carry `Retry-After`.
5. If the context ends between the last round and building the response
   (enough rows were gathered, but nobody is left to receive them), the
   reservation is released back and the call returns that context error as-is
   (`context.Canceled` or `context.DeadlineExceeded`). At the HTTP layer this
   becomes `499 client_closed_request` when the *caller's* own request
   context ended (the client disconnected) -- not a `5xx`, since that
   reflects the caller going away, not a server failure -- or `503
   unavailable` when it was instead FuelKeeper's own `instance.reserve_timeout`
   deadline that fired.
6. Otherwise: `200` with the token, `expires_at`, `ttl_seconds`, `lock_mode`,
   the reserved `outputs[]` (each with its full unlocking script), the
   `shards[]` touched (txid + version), and, for P2PKH pools, `constraints`
   (`tx_version`, `lock_time`, `sequence`).

**BEEF is served separately**, not inlined in the reserve response:
`GET /v1/pools/{pool}/beef/{txid}` returns the shard's BEEF (split tx plus
ancestry while unmined; split tx plus BUMP once mined, with
`Cache-Control: immutable`), tagged with `ETag: "v<version>"` (the shard's
version, bumped on every `UpdateShard`) so a client can send
`If-None-Match` and get `304` once its cached copy is still current. The Go
client (`pkg/client`) caches per shard version and deduplicates concurrent
fetches of the same shard via singleflight.

## Background Jobs

Three jobs (`pkg/jobs`) each run on exactly one instance at a time under a
store lease named `jobs/<name>` (`pkg/jobs/runner.go`'s `Runner`); every job
write is itself a CAS, so an overlap during a lease hand-over is harmless by
construction, not by the leader election being perfectly exclusive.

- **Reaper** (default 1 s): `Quarantine(pool, now - grace, limit)` moves
  expired reservations to quarantine (a `grace` clock-skew guard, default
  10 s, delays this past the raw TTL expiry); then pages `ListQuarantined`
  rows older than `quarantine_min_age` (30 s) and asks the oracle in
  batches. Resolution follows the terminal-state policy above: `spent`
  (mined, input-verified) resolves to `spent`; `unspent` resolves to
  `available` only after the row has aged `min_unspent_age` (30 s, unhinted)
  or the longer `hint_patience` (10 m, hinted -- a hinted-but-unseen tx is
  more likely simply not yet propagated) since quarantine; `unknown` on an
  **unhinted** row (the normal case with an arcade-only oracle) requeues to
  `available` after `unknown_requeue_age` (10 m default) under a pool's
  `expiry_policy: requeue` (default), or holds it for an operator under
  `expiry_policy: hold`; `unknown` on a **hinted** row
  (`spend_policy: verify`) is instead resolved from the hinted transaction's
  own fate via `Tx(hint)` -- mined and spends the outpoint, a mined
  competitor, or a rejection naming a spent input all retire it; a rejection
  with the inputs untouched requeues after `hint_patience`; anything else
  requeues after `hint_max_wait` (24 h default) as a last resort (see
  `docs/OPERATIONS.md#reaper-lifecycle-in-operator-terms` for the full rule
  table); `conflicting`, or an oracle error, leave the row quarantined for a
  later tick and raise `quarantine_oldest_age_seconds`. Double runs during a
  lease hand-over are harmless: every write here is a CAS on the row's current
  state.
- **Watcher** (default 10 s): fetches up to `limit` (default 5000) `unmined`
  shards per pool and checks up to `max_checks_per_tick` (500) of them
  against the oracle's `Tx`, **rotating a per-pool checkpoint** across ticks
  (the last shard checked this tick becomes next tick's resume point, sorted
  by `created_at` then txid) so a large backlog cannot let the same leading
  shards starve newer ones -- every fetched shard is checked at least once
  within `ceil(n / max_checks_per_tick)` ticks. A mined verdict validates the
  merkle root, builds the mined BEEF (split tx + BUMP) and calls
  `UpdateShard(mined, beef)`; a rejected/double-spent verdict retires the
  shard; a shard unknown to the oracle for longer than `rebroadcast_after`
  (5 m) is resent from its stored BEEF; one unmined longer than `alert_after`
  (6 h, must be `>= rebroadcast_after`) is logged as a warning.
- **Minter** (default 30 s): per pool, `deficit = target_available -
  available` (rows already sitting in unmined shards count toward
  `available`, since `mined_only` service just can't serve them yet); when
  `deficit > 0` and unmined shards are below `max_pending_shards` (50), it
  builds up to `max_per_round` (20) splits. Refilling is hysteresis, not a
  live setpoint: a pool only tops up once `available` drops below
  `low_water` (or below `target_available` outright when `low_water` is 0),
  and once triggered it mints back up to `target_available`, not just past
  the trigger. **Order is load-bearing**: `Treasury.Build` (signs, does not
  broadcast) -> `Store.PutShard` (records the shard and its rows as
  `available` -- fuel must never exist on-chain without a shard record) ->
  `Treasury.Send`. One split's `Build` + `PutShard` is bounded by
  `build_timeout` (default 60 s). A terminal send rejection
  (`treasury.ErrRejected`) retires the shard; a transient send failure
  leaves it `unmined` for the watcher to resend; `ErrInsufficientFunds` is
  logged and the round stops for that pool, not fatal. At the start of every
  round the minter also drives the treasury's optional capabilities, when
  present: a `treasury.ChunkEnsurer.EnsureChunks` top-up, and a
  `treasury.Reconciler`'s **two-phase reconcile** (see "Treasury Flow"
  below) -- neither ever blocks a round on its own grace period.

## Treasury Flow

The toolbox treasury (`pkg/treasury/toolbox`, `treasury.kind: toolbox`)
embeds a go-wallet-toolbox wallet over its own GORM storage (SQLite or
Postgres). End to end:

1. **BRC-29 funding**: an operator's BRC-100 wallet derives a one-time key
   from the treasury's identity key and a fresh nonce (`GET /v1/funding`),
   pays a P2PKH output to it, and posts the resulting AtomicBEEF
   (`POST /v1/funding`). The treasury verifies the payment's locking script
   against the expected BRC-29 script for that prefix/suffix/sender *before*
   calling the wallet, then `InternalizeAction`s it -- unmined payments are
   accepted (storage SPV-verifies, stores and broadcasts them). See
   `docs/FUNDING.md` for the full flow and recovery from a failed
   `POST /v1/funding` after the payment already broadcast.
2. **Toolbox storage / two-level chunking**: internalized funds sit as plain
   wallet balance in the `default` basket until the chunk keeper
   (go-wallet-toolbox's `pkg/wallet/fuelkeeper.Keeper`, driven once per
   minter round via `EnsureChunks`) fans them out: `default` -> **reserve
   chunks** (`reserve` basket, `M x ChunkValue` plus fan-out fee headroom,
   `M = chunking.fanout_outputs_per_tx`, default 100) -> **leaf-sized
   chunks** (`chunks` basket, exactly `ChunkValue = LeafCost +
   chunking.headroom_sats`, refilled to `target_chunks` once below
   `low_water_percent` of it). `LeafCost` is what one split needs:
   `split_size x denomination` plus that split's own fee at the resolved
   rate.
3. **Leaf splits**: `Core.Build` claims one leaf-sized chunk and builds a
   plain `CreateAction` (`NoSend`) whose leading `split_size` outputs are the
   pool's fuel denomination (puzzle preimage hash or presigned-P2PKH lock);
   the surplus is small enough to stay below dust so it is absorbed as fee,
   not a change output. `Core.Build` also derives the shard's puzzle seed (or
   presigns each P2PKH output) so the resulting rows' unlocking data is ready
   immediately -- these are bearer secrets and are never logged. The minter
   then calls `Store.PutShard` (unmined) **before** `Core.Send` broadcasts
   it -- see the minter's "order is load-bearing" note above.
4. **Abort on failure**: any failure after the wallet has created the
   no-send split (before it is ever recorded and sent) aborts it in the
   wallet, so its puzzle seed -- which lived only in the process that built
   it -- can never reach the chain and its inputs return to the wallet.
5. **Send is wallet-first and idempotent**: `Core.Send` always asks the
   wallet about the split by txid first, so a restart or a lease hand-over
   replays safely: a no-send split still in the wallet is claimed and
   posted; a split the wallet already posted but reports "unproven" is
   re-posted from the caller-supplied BEEF; a split the wallet has no record
   of at all is `treasury.ErrUnknownTx`.
6. **Two-phase reconcile** (`treasury.Reconciler`, driven by the minter's
   `startRound`) recovers a split that was `Build`-ed but never `PutShard`-ed
   (a crash, a `PutShard` failure or timeout, or a `Build` cut off by its own
   deadline) without ever blocking a round on it:
   - **Start** (`ReconcileStart`): list the wallet's no-send `fuel-split`
     actions. Any the store already records (in any configured pool) are
     handed to `Send` immediately. The full listing becomes a snapshot with
     a due time `reconcile_grace` (default 2 m) after *this listing*
     returned, not after the round started.
   - **Finish** (`ReconcileFinish`), in the first round at or after the due
     time: list again and classify each snapshot entry -- **resent** (now
     recorded: `Send`); **aborted** (unrecorded in both listings: its
     puzzle seed lived only in the crashed process, so it must never reach
     the chain -- abort returns its inputs to the wallet); **deferred**
     (unrecorded but only in the second listing -- possibly a `Build`
     mid-flight elsewhere: take a fresh snapshot next round); **errors**
     (a lookup, send or abort failed: retried next round, nothing aborted
     on an error).
   A snapshot is taken on the first round of a new minter lease, after a
   `record_failed` split, and after a `Build` that timed out, so a lease
   held for days still eventually settles its own leftovers. The grace only
   protects a split whose `Build` + `PutShard` finish within it, which is
   why `treasury.toolbox.reconcile_grace` must exceed
   `jobs.minter.build_timeout` by at least 10 s, and why, with the toolbox
   treasury, `jobs.lease_ttl` must exceed `build_timeout + 20s` (two 10 s
   reconcile budgets) -- see `docs/OPERATIONS.md`'s validation rules and
   `docs/TREASURY.md#reconciliation` for the full detail.

## Oracle Roles

`pkg/oracle.Oracle` answers two questions -- `Outpoints(ops)` ("is this
outpoint spent?") and `Tx(txid)` ("is this txid known/mined/rejected, and
what does it spend?") -- and the reaper/watcher never *guess* from an
`Unknown` answer: an unhinted row with no verdict instead waits out
`jobs.reaper.unknown_requeue_age` before the reaper requeues it blind (or
holds it for an operator, under a pool's `expiry_policy: hold`), and a
hinted row (`spend_policy: verify`) is resolved from its own transaction's
fate via `Tx(hint)` instead -- see `docs/OPERATIONS.md`'s reaper lifecycle
table.

| Implementation | Outpoints | Tx (mined status) | Spender Inputs |
|---|---|---|---|
| `none` | never (Unknown) | never | no |
| `mock` | nullnet only | nullnet only | as configured |
| `arcade` | never (Unknown) -- no UTXO index | yes | no -- never serves raw transactions |
| `teranode` | yes | **never** -- its endpoint does not report block inclusion | yes (parses the raw tx it fetches) |
| `composite` | teranode's answer if paired with one, else Unknown | arcade's answer | teranode's, if paired |

**Arcade-only (`oracle.kind: arcade`, or `composite` without
`teranode_url`) is the expected production shape**, and `oracle.kind`
resolves to it by default whenever `treasury.kind: toolbox`: a direct
teranode connection is never assumed, only an optional, opt-in accelerator.
Arcade answers `Tx` only (mined/rejected/competing lifecycle, and the
watcher's proof source); its `Outpoints` always answers Unknown, since it
keeps no UTXO index. Rejections are classified into an `oracle.RejectReason`
from arcade's numeric ARC error code plus, for code 466, the underlying
teranode rejection line verbatim in `extraInfo` (arcade reuses 466 for four
distinct teranode verdicts, so the code alone is not enough): `UTXO_SPENT`,
`TX_CONFLICTING` and `TX_INVALID_DOUBLE_SPEND` classify `inputs_spent` (an
already-decided ownership of the input); `TX_LOCKED` (the parent
transaction's own commit still in flight, not a decided spend), 467
(ambiguous between a real invalid tx and arcade's own internal fault), 476
(not final) and an unparsable `extraInfo` all classify `unknown`; every
other classifiable code classifies `tx_invalid` (a format/fee/policy
failure, inputs untouched). When a `UTXO_SPENT` line names the spender, the
oracle also fills `oracle.TxStatus.SpentInputs` with the specific
outpoint(s) reported spent and appends the spender to `CompetingTxs` --
arcade never populates `competingTxs` or a `DOUBLE_SPEND_ATTEMPTED` status
on its own, synchronously or otherwise; what actually carries a double-spend
verdict is this REJECTED/466/`extraInfo` shape on a later GET /tx (see
`pkg/oracle/arcade/arcade.go`'s `rejectReasonForCode`/`rejectReasonFor466`
tables and `docs/CLIENTS.md#when-your-broadcast-fails`). Used alone, arcade
therefore never directly confirms an outpoint spent or unspent; the reaper instead
resolves what it can from `Tx(hint)` (hinted rows) and requeues what it
cannot after `unknown_requeue_age`/`hint_max_wait` (unhinted and
exhausted-hinted rows respectively) -- quarantine does not grow without
bound, and pool capacity never leaks. Configuring `oracle.teranode_url`
(`oracle.kind: composite`) is optional: teranode then answers `Outpoints`
directly and supplies `Inputs` for the reaper's cross-check, resolving those
same rows immediately from the UTXO set instead of waiting out the delay.
`teranode` alone can never report a shard mined, so config rejects
`oracle.kind: teranode` outright whenever jobs are enabled. The full policy
table (what each policy combination needs, and what happens without it, and
after what delay) is `docs/OPERATIONS.md#oracle-kinds-and-policies`.

## Store Contract

`pkg/store.Store` (`pkg/store/store.go`) is the correctness boundary every
backend implements identically: `PutShard`, `GetShard`, `UpdateShard`,
`RetireShard`, `AcquireShard`, `RenewShard`, `ReleaseShard`, `ListShards`,
`ListAvailable`, `GetRows`, `Reserve`, `Release`, `Spend`, `Quarantine`,
`ListQuarantined`, `Resolve`, `Stats`, `Capabilities`, plus `Leaser`
(`TryAcquireOrRenew`, `ReleaseLease`) for job/shard leases. Every batch op is
validated up front (duplicate outpoints, zero tokens, size limits) as
`ErrInvalidArgument`; every row-level predicate mismatch is silently skipped
and reported through the applied set or changed count, never an error (see
"Guarantees" above). Provenance fields (`ReservedBy`, `ReservedAt`,
`ExpiresAt`, `QuarantinedAt`, `SpentHint`) are set by `Reserve`/`Spend` and
kept across every later transition except a return to `available`. All
stored times are microsecond-precision, taken from an injected clock.

Every backend must pass `pkg/store/storetest.RunSuite` with `-race`. On this
branch, three backends exist:

| Backend | Multi-process | CAS primitive |
|---|---|---|
| **memory** (`pkg/store/memory`) | no | One mutex guards the whole store; every mutation is a plain in-process check-then-write. Reference implementation for tests and benchmarks. |
| **sqlite** (`pkg/store/sqlite`, modernc, no CGO) | no | One process, one writer: every mutation runs as a `database/sql` transaction on a dedicated single-connection handle (`MaxOpenConns(1)`, `_txlock=immediate`), per-op statements executed in sorted `(txid, vout)` order; reads use a separate read-only WAL handle running concurrently with the open write transaction. Each statement's `WHERE` re-asserts the row's current state (and owner, where relevant). |
| **postgres** (`pkg/store/postgres`, pgx) | yes | Every batch mutation (`Reserve`, `Release`, `Spend`, `Resolve`, `RetireShard`'s row cascade) issues one single-row, primary-key point `UPDATE` per op, pipelined as one `pgx.Batch` inside a single transaction, always in ascending `(txid, vout)` order. A combined multi-row `UPDATE ... FROM unnest(...)` was tried and rejected: Postgres's planner is free to pick its own row-lock order for it, so two concurrent batches touching overlapping rows in different orders could deadlock; sequencing every batch's own per-row updates into one global lock order rules that out. `AcquireShard`/`Quarantine` use `FOR UPDATE SKIP LOCKED`. |
| pebble | -- | *Plan 4 (not yet in this tree): batch-per-mutation under one store-wide write mutex, per the design spec.* |
| mongodb | -- | *Plan 4 (not yet in this tree): per-record conditional `UpdateOne`/`BulkWrite` CAS, per the design spec.* |
| aerospike | -- | *Plan 4 (not yet in this tree): per-record filter-expression CAS (`BatchOperate`), per the design spec.* |

(See `README.md`'s backend table and `docs/OPERATIONS.md` for the same three
built backends plus deployment notes; pebble/mongodb/aerospike are tracked as
Plan 4 there too.)

## Metrics and Benchmarks

The full Prometheus metric catalogue (`fuelkeeper_` prefix: reserve/release/
spend counters and latencies, coalescer/allocator/shard gauges, reaper/
watcher/minter counters, oracle and job-lease gauges) together with alerting
guidance and the operational runbook lives in `docs/OPERATIONS.md#what-to-watch`.

Benchmark methodology and results (store microbenchmarks per backend, service
load tests at 1 and 3 instances, and end-to-end demo TPS on teratestnet) are
tracked in `docs/PERFORMANCE.md`, to be published once `cmd/fuelbench` and the
demo runs land.
