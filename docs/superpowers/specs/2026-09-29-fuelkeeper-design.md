# FuelKeeper design (approved 2026-09-29)

This is the design spec produced by the brainstorming session and approved by the user. The task-level implementation plan lives in docs/superpowers/plans/.


## Context

Wallets that generate transactions at high rates spend most of their effort on
fee funding: selecting a coin, reserving it, signing it, and handling change.
go-arcade-toolbox and go-wallet-toolbox both solved this inside the wallet with a
denominated "fuel" basket and a client-side refill loop (`pkg/wallet/fuelkeeper`
in go-wallet-toolbox v0.184.16). That removed coin-selection contention, but the
fuel was still ordinary wallet-owned P2PKH change: the wallet's own key had to
sign it per transaction, the pool lived inside one wallet's storage, and the
throughput ceiling became the number of durable commits per action. The
go-arcade-toolbox reservation review showed that the atomic claim primitive was
sound on every backend and every double spend came from lifecycle gaps around
it: releases and sweeps with no committed in-flight state, SQLite pragmas the
modernc driver silently ignored, an unguarded Aerospike delete, a CAS that was
not idempotent under client auto-retry, and a per-process negative cache.

FuelKeeper is a standalone Go service that turns fee funding into a fast,
horizontally scalable reservation API. It takes a funding payment from any
BRC-100 wallet, splits it into a large pool of fixed-denomination outputs whose
unlocking data is prepared in advance (hash-puzzle preimage by default,
presigned P2PKH as an alternate mode), and hands them out on request with a
durable reservation. A client attaches one fuel output to its own transaction as
an extra input and never needs FuelKeeper to sign anything at spend time. Both
go-wallet-toolbox and the TypeScript wallet-toolbox consume fuel through the
standard `createAction` caller-provided-input path.

Success criteria from the brief: fully documented HTTP API; SQLite, Postgres,
PebbleDB, MongoDB and Aerospike stores behind one conformance suite; trivial
funding from a BRC-100 wallet; provable reservation with release ("unreserve");
never lose an unspent output; a demo application on go-wallet-toolbox that
drives thousands of transactions per second against teratestnet (scale network
later); and a benchmark suite with published numbers.

## Decisions made with the user

| Decision | Choice |
|---|---|
| Module path | `github.com/bsv-blockchain/fuelkeeper` |
| Treasury | Embedded go-wallet-toolbox wallet inside FuelKeeper (funding, splits, proofs) |
| Default lock mode | Hash puzzle `OP_SHA256 <32B> OP_EQUAL`; presigned P2PKH (`NONE\|ANYONECANPAY\|FORKID`) as alternate |
| Hot path | Fully sharded ownership: shard = one split tx; instances lease shards; row-level CAS is the correctness primitive |
| Expiry | Quarantine, verify against a chain oracle, then requeue or mark spent |
| Network | Teratestnet (teranode + arcade) first, scale network when it is back |
| API | HTTP/JSON, hand-written OpenAPI 3 spec, Go client package |

Protocol facts that shaped the design (verified against go-sdk, the SV node,
Teranode/BDK, ARC and arcade sources, plus live TAAL/GorillaPool policy):

- A P2PKH signature with `NONE|ANYONECANPAY|FORKID` commits to the fuel
  outpoint, its script and value, and to the consuming tx's `nVersion`, that
  input's `nSequence` and `nLockTime`. Presigned P2PKH fuel therefore requires
  the consumer to use version 1, sequence `0xFFFFFFFF`, lockTime 0 (toolbox
  defaults). Hash puzzles have no such coupling.
- Hash-puzzle input is 74 bytes vs 147–149 for P2PKH; both are accepted by SV
  node default policy (`acceptnonstdoutputs=true`), Teranode/BDK, ARC and
  arcade. Prefer `OP_SHA256` over `OP_HASH160` (the latter is byte-identical
  to P2SH and confuses tooling).
- Dust floor is 1 sat everywhere. Live miner fee is 100 sat/kB; a typical
  300–600 B consumer tx needs 30–60 sats of fuel.
- Consumers ship the split tx (plus BUMP once mined) as `inputBEEF`; the
  toolbox SPV-verifies it and executes scripts of unmined BEEF txs. Keep leaf
  splits small (500–1000 outputs) and hand out only mined fuel by default.
- The Go toolbox funder lets a zero-balance wallet transact on a fuel input
  alone when `fuel ≥ outputs + fee`; a surplus above the ~40-sat dust floor
  becomes one change output, a smaller surplus goes to miners (unlike the TS
  toolbox, it never fails on small surplus).

## Guarantees

- **G1** No fuel output is ever handed to two live reservations.
- **G2** Every output is always in exactly one state, and an unspent output
  always returns to the pool or ends provably spent or invalid.

Both rest on one mechanism: every state transition is a compare-and-set on the
row's current state (and on `reserved_by == token` for release/spend). Leases
and in-memory structures only make conflicts rare; they are never trusted for
correctness.

## Architecture

```
 BRC-100 wallet ──BRC-29 payment──▶ ┌──────────────────────────────┐
 (fuelkeeper fund CLI / any wallet) │ FuelKeeper instance          │
                                    │  api      HTTP/JSON, OpenAPI │
 client wallets ──reserve/spend───▶ │  pool     shard mgr, allocator,
   (go-wallet-toolbox, ts toolbox)  │           coalescer          │
                                    │  jobs     reaper, watcher,   │
                                    │           minter (leased)    │
                                    │  treasury embedded toolbox   │
                                    │           wallet             │
                                    │  oracle   arcade / teranode  │
                                    └──────┬───────────────┬───────┘
                                     fuel store        treasury storage
                        (sqlite|postgres|pebble|     (toolbox GORM: sqlite|postgres)
                         mongodb|aerospike)
```

Many instances share one fuel store (Postgres/Mongo/Aerospike) and scale
horizontally. SQLite and Pebble are single-instance stores. Background jobs run
on one instance at a time under a global lease.

### Domain model

**Pool** (config, one per denomination): `name`, `denomination`, `lock_mode`
(`puzzle` | `p2pkh`), `split_size` (default 500), `target_available`,
`low_water`, `serve_policy` (`mined_only` default | `any`), `default_ttl`,
`min_ttl` 30 s, `max_ttl` 10 m, `max_count` 100, `release_policy`
(`trust` default | `verify`), `spend_policy` (`verify` when an oracle exists,
`trust` otherwise).

**Shard** = one split transaction: key `(pool, txid)`; `size`, `denomination`,
`lock_mode`, `status` (`unmined` | `mined` | `retired` | `invalid`), `owner`,
`lease_until`, `version` (bumped on every update so renewals can detect a new
BEEF), `beef` (raw split tx + ancestry while unmined; split tx + BUMP once
mined), `created_at`, `updated_at`.

**Fuel row** = one output: key `(txid, vout)` with `pool` as an attribute;
`satoshis`, `locking_script`, `unlocking_script` (33-byte preimage push or
`<sig><pubkey>`), `state`, `reserved_by` (16-byte token), `reserved_at`,
`expires_at`, `quarantined_at`, `spent_hint`, `spent_by`, `updated_at`.

State machine:

```
available ──Reserve(token,ttl)──▶ reserved
reserved  ──Release(token)──────▶ available
reserved  ──Spend(token,txid)───▶ quarantined(hint=txid)    spend is a HINT
reserved  ──expiry (reaper)─────▶ quarantined(hint=nil)
quarantined ──oracle: spent─────▶ spent(spent_by)          terminal, oracle-proven
quarantined ──oracle: unspent───▶ available
any non-spent ──RetireShard─────▶ invalid                   terminal, split tx dead
```

A client's `spend` cannot be trusted as proof, so it only short-circuits the
wait for expiry; `spent` is reachable only through the oracle. With
`spend_policy=trust` (no oracle configured) `Spend` goes straight to `spent`.

### Sharded ownership (hot path)

**Shard manager** (per instance)
- Holder id = `instance_id/boot_nonce`, so two processes can never share one.
- Config: `max_shards` 8, `low_water` (rows in memory; default 2× expected
  reserves/s × renew interval), `lease_ttl` 30 s, renew at ttl/3,
  `rescan_interval` 5 s jittered.
- Adoption is pull-based: when in-memory available rows drop below `low_water`
  and owned shards < `max_shards`, `AcquireShard` (oldest `created_at` first,
  status in the serve set, `owner=''` or lease expired, excluding shards this
  instance dropped as empty in the last 60 s), then page `ListAvailable` into
  the allocator. At `max_shards` while hungry, the manager rotates instead
  (as built, 2026-09-30): it rescans up to 2 drained shards oldest first,
  keeps any whose rows are back, releases those the store shows empty (60 s
  re-adoption exclusion), and adopts into the freed slots. Adoption never
  writes fuel rows, so an instance crash loses nothing. Fairness across instances falls out of hunger-driven pulls and the
  cap; no coordinator.
- Renewal outcomes: ok → refresh cached status/version (refetch BEEF if
  changed); `ErrNotOwner` → drop the shard now; status `retired`/`invalid` →
  drop. If the store is unreachable, keep serving owned shards (CAS keeps it
  safe), stop adopting, and drop shards whose lease has been expired for more
  than one ttl.
- A CAS miss on a shard's row triggers an immediate renew + rescan of that
  shard. Empty after two consecutive periodic rescans → `ReleaseShard`; when
  hungry at `max_shards`, a drained shard whose store rows show zero
  available is released at once (rotation; as built, 2026-09-30). Graceful
  shutdown releases all shards.

**Allocator**
- Per pool: owned shards oldest-first, each with a FIFO of rows and a `known`
  set (in FIFO or in flight). `Pop(n)` drains the oldest shard first so a
  request usually maps to one shard (one BEEF). `Add` skips known outpoints,
  which makes rescans safe against double-pop. `Forget` after a CAS miss or a
  successful reserve. Rows return to the FIFO front only if they never reached
  a flush. No negative cache: an empty FIFO triggers adopt/rescan or rotation
  (as built, 2026-09-30), never a cached verdict.

**Coalescer** (group commit)
- `K` flush slots and batch cap `B` from `Store.Capabilities()` (Postgres/
  Mongo/Aerospike K=4–8, SQLite/Pebble K=1; B=256). Flush immediately when a
  slot is free; otherwise accumulate until a slot frees or `B` is reached.
  Optional window `W` (default 0). No cross-flush ordering is needed: the
  allocator guarantees an outpoint is in flight at most once per instance and
  the row CAS settles cross-instance races.
- `Reserve` is idempotent per `(outpoint, token)`: the CAS predicate is
  `state=available OR (state=reserved AND reserved_by=token)`, so an
  in-doubt flush is retried (R=2, 20/50 ms) and the store returns the true
  applied set. Only after retries fail does the coalescer compensate with
  `Release(token, ops)` under `context.WithoutCancel` (2 s budget) and fail
  the requests with 503. Rows of a failed batch are never re-added to the
  FIFO; rescan re-adds whatever is truly available.
- Per-op outcomes: applied → returned; missed (CAS failed) → forget, retry the
  request with other rows; unknown → retry the same op, then release. An
  outpoint appears in a 200 response only if the store returned it as applied
  for that request's token.
- Backpressure: `max_inflight_ops = 4·K·B`; above it `Reserve` returns 503 +
  `Retry-After: 1` immediately and returns its popped rows to the FIFO front.

**Reserve request flow**
```
POST /v1/pools/{pool}/reserve {count, min_count?, ttl_seconds?}
1 validate (1 ≤ min_count ≤ count ≤ max_count; min_ttl ≤ ttl ≤ max_ttl)
2 token = 16 random bytes; expires_at = now + ttl
3 rows = allocator.Pop(count); if empty: one bounded adopt (200 ms); still empty → 503 exhausted
4 coalescer.Submit(ops) and wait on the request ctx (default 2 s)
5 store error → 503 store_unavailable (coalescer already compensated)
  missed rows → Forget; pop more and resubmit under the same token (≤3 rounds)
6 applied < min_count → best-effort Release(token, applied) → 409 insufficient, Retry-After: 1
7 200 {token, expires_at, ttl_seconds, outputs[], shards[{txid, beef_version}],
       constraints{tx_version, lock_time, sequence} (p2pkh mode)}
```
BEEF is not inlined; `GET /v1/beef/{txid}` serves it with `ETag=version` and
`Cache-Control: immutable` once mined. The Go client caches per shard.

**Release / Spend** may hit any instance: token-guarded row CAS keyed by
outpoints (clients always have them, so no `reserved_by` index exists).
Release on a shard this instance owns re-adds rows locally; otherwise the
owner's rescan picks them up within one interval. Skips (wrong token, wrong
state) are reported as counts, not errors. `release_policy=verify` routes
Release through quarantine + oracle for untrusted clients.

### Background jobs (single runner per job via `Leaser`)

- **Reaper** (1 s): `Quarantine(pool, now − grace 10 s, 1000)`; then
  `ListQuarantined(older than 30 s)` in batches; rows with a spend hint are
  checked first; batch the oracle (teranode `POST /api/v1/utxos/json`,
  hundreds per call); `spent → spent(spent_by)`; `unspent → available` only
  after `min_unspent_age` (30 s unhinted, 10 min hinted, since a hinted but
  unseen tx is more likely "not yet propagated"); unknown or oracle error →
  stays quarantined, back off, raise `quarantine_oldest_age_seconds`. Double
  runs are harmless because every write is a CAS on the current state.
- **Proof watcher** (10 s): for `unmined` shards poll arcade `GET /tx/{txid}`;
  mined → validate root (`bump.ComputeRootHex` + `services.IsValidRootForHeight`),
  build BEEF v2 (`NewBeefV2`, `MergeBump`, `MergeRawTx`, `Bytes`),
  `UpdateShard(mined)`; rejected/double-spent → `RetireShard`; unknown > 5 min →
  rebroadcast from the stored BEEF; unmined > 6 h → alert.
- **Minter** (30 s): `deficit = target_available − (available + rows in unmined
  shards)`; while deficit > 0 and pending unmined shards < cap: build the split
  with the treasury using `NoSend`, `PutShard(unmined, beef)` FIRST, then send
  it (`SendWith`); a hard reject retires the shard. On startup the minter also
  reconciles treasury actions labelled `fuel-split` against the shard table to
  recover any split that was sent but not recorded. Startup validates that
  mint capacity ≥ 1.2 × configured burn rate (go-arcade-toolbox lesson).
- **Janitor** (60 s, inside the reaper): stats → metrics; shards with zero
  non-terminal rows → `retired`; available rows with no shard record → alert,
  and delete after 10 min if the oracle does not know the tx.

### Treasury (embedded go-wallet-toolbox wallet)

Pin `go-wallet-toolbox v0.184.16` (go-sdk v1.4.1). All facts below were
verified in `~/go/pkg/mod/github.com/bsv-blockchain/go-wallet-toolbox@v0.184.16`.

- **Wiring** (copy `pkg/infra/server.go`): `defs.DefaultServicesConfig(net)`
  with arcade URL/events URL, `wdk.DeriveArcadeCallbackToken(identityKey)`,
  chaintracks URL, `services.New` + `StartBackgroundServices`;
  `storage.NewGORMProvider(net, svc, WithDBConfig, WithFeeModel(100 sat/kB,
  read arcade `/policy` at boot), WithUTXOManagement(throughput), ...)` +
  `Migrate`; `wallet.New(net, rootKey, provider, WithServices,
  WithAutoKnownTxids(false))` so createAction results carry complete ancestry
  BEEF; `monitor.NewDaemonWithGORMLocker(...)` started (needed for
  delayed-broadcast retries and proof recording). Network values: `ttn`
  (public teratestnet), `tstn` (private, via env).
- **Funding**: `GET /v1/funding` returns the treasury identity key plus a
  derivation-prefix nonce (go-sdk `auth/utils.CreateNonce`, counterparty
  self). The payer derives `GetPublicKey{ProtocolID:{2,"3241645161d8"},
  KeyID:"<prefixB64> <suffixB64>", Counterparty: fuelkeeperKey}`, locks P2PKH
  to it, and posts the AtomicBEEF + prefix/suffix/sender key to
  `POST /v1/funding`. FuelKeeper verifies the nonce and calls
  `InternalizeAction` (protocol `wallet payment`, `sdk.Payment{DerivationPrefix,
  DerivationSuffix, SenderIdentityKey}`). Unmined payments are accepted:
  storage SPV-verifies, stores and broadcasts them. A static anyone-key
  address mode (as in `cmd/throughput_dashboard/internal/funding`) sits behind
  a flag; it needs WhatsOnChain to fetch the payer's BEEF, which teratestnet
  lacks, so payer-posts-BEEF is primary.
- **Two-level splits via the toolbox's own throughput strategy**: treasury
  storage runs `Strategy=throughput`, `ReserveBasket="reserve"`,
  `PoolBasket="chunks"`, `DenominationSatoshis = split_size×denomination +
  leafFee + ~20`, `FanoutOutputsPerTx=100`, `SpendPolicy=prefer_mined`,
  `TopUp.Enabled=false`. Tier 1: `w.FanOutFuel(ShapedChange{Count:R,
  Satoshis:M×chunk+headroom, Basket:"reserve"})`. Tier 2:
  `w.FanOutFuel(ShapedChange{Count:M, Satoshis:chunk, Basket:"chunks"})` per
  reserve coin (the toolbox's `pkg/wallet/fuelkeeper.Keeper` with
  `Denomination=chunk` can run this loop). Leaf: plain `w.CreateAction` with
  `split_size` puzzle/P2PKH outputs, no basket, `RandomizeOutputs=false` (fuel
  is vout `0..N-1`), `NoSend` then `SendWith`; storage funds it from `chunks`
  with one exact-match claim and the ≤40-sat surplus goes to miners. Depth is
  constant (leaf → chunk fan-out → reserve fan-out → funding tx), bounding the
  unmined-ancestry BEEF a consumer must ship. Guard: leaves wait until
  `ListOutputs(basket:"chunks")` shows enough accepted chunks and retry
  `ErrNotEnoughFunds` with backoff, because the toolbox's fallback to the
  default basket would re-chain on the deposit. Rejections surface as
  `*pkgerrors.ProcessActionError` (status, `CompetingTxs`, per-service errors).
  Implementation check: confirm `NoSend` + `SendWith` round-trip in the Go
  toolbox; fallback is `SignAndProcess` with the startup reconciliation scan.
- **Unlocking data**: puzzle mode derives `preimage = SHA256(shard_seed ||
  vout)` from a random 32-byte seed per shard and stores the 33-byte push per
  row; P2PKH mode presigns each output at split time (dummy v1/lockTime-0 tx,
  one input at seq `0xFFFFFFFF`, `NONE|ANYONECANPAY|FORKID`) with a per-pool
  fuel key derived from the treasury root key via the toolbox `KeyDeriver`.
- **Mock treasury** for benchmarks and tests: synthetic shards with real
  puzzle preimages and fake txids, no network; a mock oracle to match.

### Chain oracle (`pkg/oracle`)

```go
type Status int // Unspent, Spent, Conflicting, Unknown
type Oracle interface {
    Outpoints(ctx, ops []Outpoint) ([]Result, error)          // batch by outpoint
    Tx(ctx, txid string) (TxStatus, *transaction.MerklePath, error) // by txid (hints, watcher)
}
```
Implementations: `teranode` (`GET {asset}/api/v1/utxo/{txid}/json?vout=N`,
batch `POST /api/v1/utxos/json`; status `0` Unspent, `1` Spent with
`spendingData.txId`, `6` Conflicting, 404/`3` Unknown), `arcade`
(`GET /tx/{txid}`: `MINED|IMMUTABLE|SEEN_*` → Spent, `REJECTED` → Unspent with
competing txs, 404 → Unknown; also the watcher's proof source), `woc`
(`services.IsUtxo`, mainnet/testnet), `none`. Composite: `arcade` for hinted
rows and the watcher, `teranode` for outpoint batches. The oracle must be
mempool-aware for the `unspent → available` arm; a chain-only oracle must
lengthen `min_unspent_age` to several blocks.

Arcade contract used by the raw demo broadcaster: `POST /tx` with EF bytes
(`application/octet-stream`, `X-CallbackToken`, `X-FullStatusUpdates`), 202
`{txid, txStatus}`, 400 with ARC codes 460–476, 503 + `Retry-After`;
`POST /txs` concatenated raw txs (all-or-nothing) for independent txs.

### Store interface (`pkg/store`)

```go
type ReserveOp struct { Outpoint; Token [16]byte; ExpiresAt time.Time }
type Store interface {
    PutShard(ctx, Shard, []Row) error                                   // idempotent on txid; rows first, shard last on non-atomic backends
    UpdateShard(ctx, pool, txid string, status ShardStatus, beef []byte) error // bumps version
    RetireShard(ctx, pool, txid string) (rows int, err error)           // shard→invalid, non-terminal rows→invalid
    AcquireShard(ctx, pool, holder string, now, until time.Time, statuses []ShardStatus, exclude []string) (*Shard, error)
    RenewShard(ctx, pool, txid, holder string, until time.Time) (ShardHead, error) // ErrNotOwner; returns status+version
    ReleaseShard(ctx, pool, txid, holder string) error
    ListShards(ctx, pool string, f ShardFilter) ([]Shard, error)
    ListAvailable(ctx, pool, txid string, afterVout uint32, limit int) ([]Row, error)
    Reserve(ctx, pool string, ops []ReserveOp) (applied []ReserveOp, err error)
    Release(ctx, pool string, token [16]byte, ops []Outpoint) (int, error)
    Spend(ctx, pool string, token [16]byte, ops []Outpoint, txid string, terminal bool) (int, error) // terminal=true only under spend_policy=trust
    Quarantine(ctx, pool string, before time.Time, limit int) (int, error)
    ListQuarantined(ctx, pool string, before time.Time, limit int) ([]Row, error)
    Resolve(ctx, pool string, ops []Outpoint, to State, spentBy string) (int, error)
    Stats(ctx, pool string) (Stats, error)
    Capabilities() Caps                                                 // MaxConcurrentFlushes, MaxBatch, AtomicBatch
    Leaser                                                              // job leases, arcade shape
    Close() error
}
```

Per-backend essentials (full sketches from the design review are the
reference for implementation):

- **Postgres** (pgx): multi-row operations (`Reserve`, `Release`, `Spend`,
  `Resolve`, the `RetireShard` cascade) are per-row primary-key updates
  pipelined in one `pgx.Batch` inside one transaction, processed in sorted
  `(txid, vout)` order so lock acquisition order is global and deadlock-free
  (a single `UPDATE … FROM unnest(...)` statement deadlocked under
  contention because the planner does not honour array order); each per-row
  statement re-asserts the CAS predicate (`state=0 OR (state=1 AND
  reserved_by=$tok)`), written as `(state = 1) IS TRUE` where it would
  otherwise match a partial index and be planned onto it. `AcquireShard` and
  `Quarantine` use `FOR UPDATE SKIP LOCKED` CTEs. Partial indexes:
  `(pool, txid, vout) WHERE state=0`, `(pool, expires_at, txid, vout) WHERE
  state=1`, `(pool, quarantined_at, txid, vout) WHERE state=2`; `fuel_shards
  (pool, status, created_at, txid)` plus a unique index on `txid`. Schema is
  versioned and applied under a transaction-scoped advisory lock. EXPLAIN
  tests pin generic plans of the shipped statements. `synchronous_commit=on`
  documented as required. All stored times have microsecond precision.
- **SQLite** (modernc): writer DSN
  `file:<path>?_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=journal_size_limit(67108864)`,
  reader DSN with `query_only(1)`; writer handle `SetMaxOpenConns(1)`,
  separate reader pool; boot-time pragma read-back refuses to start on
  mismatch. Every write is a `database/sql` transaction (`BeginTx`, so a
  cancelled context rolls back rather than poisoning the writer connection)
  of per-op statements in sorted `(txid, vout)` order, single commit (K=1).
  `AcquireShard` probes each status separately (`UNION ALL` of ordered
  `LIMIT 1` probes). `synchronous=NORMAL` is opt-in because it can revert a
  committed reservation on power loss. Conformance pins all-or-nothing
  behaviour under cancellation for every `AtomicBatch` backend.
- **Pebble**: keys `r/<txid><vout>`, `s/<pool>\0<txid>`, index keys
  `is/<pool>\0<status><created><txid>`, `ie/<pool>\0<expires><txid><vout>`,
  `iq/…`; every mutation one batch with `Commit(Sync)`; one store-wide write
  mutex (K=1); `ListAvailable` scans the contiguous `r/<txid>` range.
- **MongoDB** (driver v2): `_id` = txid‖vout; partial indexes mirroring
  Postgres; write concern `majority, j:true`; `Reserve` is an unordered
  `BulkWrite` of `UpdateOne({_id, $or:[{st:0},{st:1,rb:token}]})`; if
  `MatchedCount < len(ops)` or on error, a follow-up `find({_id:{$in}, st:1},
  {rb})` classifies applied vs missed. `AcquireShard` = `FindOneAndUpdate` with
  sort by `created_at`. No TTL index on rows (expiry is a CAS, never a delete).
- **Aerospike**: key = txid‖vout; presence-encoded bins `ak="pool|shard"`
  (present iff available; STRING index → `ListAvailable` is one equality
  query), `ea` (present iff reserved; NUMERIC), `qa` (quarantined; NUMERIC);
  `Reserve` = `BatchOperate` with per-record filter expression
  `st==0 || (st==1 && rb==token)`, `UPDATE_ONLY`, `COMMIT_ALL`,
  `MaxRetries=0` (pinned by a policy test); per-record result code →
  applied/missed/unknown. Expression CAS, not generation CAS. Deletes always
  carry a filter expression. Production requires a strong-consistency
  namespace; documented.

`factory.New(cfg)` picks a backend as in arcade. `memory` backend for tests.

### Conformance suite (`pkg/store/storetest`)

`RunSuite(t, newStore, opts...)`: fresh store per parallel subtest, manual
clock, stress multiplier via env. Subtests: `LeaserContract`,
`PutShardAtomicAndIdempotent`, `UpdateShardBumpsVersion`,
`AcquireShardExclusiveConcurrent`, `RenewReturnsStatusAndVersion`,
`ListAvailablePagination`, `ReserveBasic` (incl. same-token replay idempotent),
`ReserveMixedTokensMixedShards` (catches unnest mis-pairing),
`ReserveDuplicateOutpointInBatch`, `ReserveExclusivityConcurrent` (union of
applied sets partitions the rows exactly), `ReserveReleaseCycleConcurrent`
(a second reserve only ever follows an acknowledged release),
`ReserveVsQuarantineRace`, `ReleaseGuard`, `SpendGuard`, `QuarantineByExpiry`,
`ResolveGuard` (spent is terminal for every op), `RetireShardCascade`,
`StatsCounts`. Pool-level: `ChaosCoalescerMemStore` with a fault-injecting
store wrapper (`FailAfterApplying(n)`, `InDoubtEvery(k)`, kill between commit
and delivery) asserting no request ever receives a non-reserved outpoint and
rows are conserved; `AllocatorNoDoubleInFlight`. Backend-specific: Postgres
EXPLAIN pins, SQLite pragma read-back + `EXPLAIN QUERY PLAN`, Aerospike
`MaxRetries==0`/`COMMIT_ALL` pins, Mongo write-concern pin and "no TTL index",
Pebble batch atomicity under injected commit failure.

### HTTP API (`openapi/fuelkeeper.yaml`, hand-written OpenAPI 3.0)

| Method & path | Purpose |
|---|---|
| `POST /v1/pools/{pool}/reserve` | `{count, min_count?, ttl_seconds?}` → `{token, expires_at, ttl_seconds, outputs[], shards[], constraints?}`; 409 insufficient; 503 exhausted/unavailable with `Retry-After` |
| `POST /v1/pools/{pool}/release` | `{token, outpoints}` → `{released}` |
| `POST /v1/pools/{pool}/spend` | `{token, outpoints, txid}` → `{accepted}` |
| `GET /v1/pools`, `GET /v1/pools/{pool}` | rows by state, shards by status, denomination, lock mode, constraints |
| `GET /v1/beef/{txid}` | BEEF for a shard, `ETag`, immutable once mined |
| `GET /v1/funding`, `POST /v1/funding` | BRC-29 funding handshake and internalize |
| `GET /healthz`, `/readyz`, `/metrics` | ops |

Output item: `txid, vout, satoshis, locking_script, unlock{mode, script,
length}`. Optional static bearer API keys. Errors are `{code, message,
retry_after?}` with stable codes.

### Go client (`pkg/client`)

`Reserve/Release/Spend`, keep-alive HTTP, per-shard BEEF cache with ETag,
helpers `CreateActionInputs(reservation, description)`, `InputBEEF(reservation)` (merged
BEEF), `Surplus(denomination, sizeBytes, satPerKB)` for wallets that must
not emit change.

### CLI (`cmd/fuelkeeper`)

`serve`, `fund --sats N [--wallet http://localhost:3321]` (drives the user's
local BRC-100 wallet through go-sdk `wallet/substrates.NewHTTPWalletJSON`,
originator `fuelkeeper.cli`: `GET /v1/funding` → `GetPublicKey` →
`CreateAction` with the BRC-29 output → `POST /v1/funding`), `pools`,
`release --token --outpoints`, `quarantine list|requeue|retire`, `shards list`.

### Demo (`cmd/fueldemo`)

Built on go-wallet-toolbox. Workers reserve fuel in batches, run `CreateAction`
with one fuel input (unlocking script + cached BEEF) and one P2PKH output back
to the demo wallet whose value absorbs the surplus (or lets ≤40 sat go to
miners), immediate broadcast via arcade, `TrustSelf=known`,
`ReturnTXIDOnly=true`, then batched `spend` notifications. Reports TPS,
latency percentiles, reserve latency, failures. Two modes: `--mode toolbox`
(BRC-100 path; multiple wallet processes, since prior measurements put one
toolbox instance at roughly 250–1100 TPS) and `--mode raw` (go-sdk builds +
arcade `/tx` or `/txs`) to show FuelKeeper's own ceiling. Risk flagged: the
toolbox's per-action cost, not FuelKeeper, bounds the toolbox-mode number, so
the demo scales out wallet instances against one FuelKeeper.

### Benchmarks (`cmd/fuelbench`, `docs/benchmarks/`)

1. Store microbenchmarks per backend (Reserve batch CAS, AcquireShard,
   Quarantine, ListAvailable).
2. Service load test against a running FuelKeeper with the mock treasury and
   mock oracle: N clients × reserve(count)/spend; reserves/s, outputs/s,
   p50/p99, CAS misses, coalescer batch size; 1 instance vs 3 instances on one
   store; per backend.
3. End-to-end demo TPS on teratestnet, plus an audit that no outpoint was
   handed to two live reservations (fuel rows vs arcade statuses).
JSON results with a markdown renderer, checked into `docs/benchmarks/`.

### Metrics (Prometheus, `fuelkeeper_` prefix)

`reserve_requests_total{pool,result}`, `reserve_outputs_total`,
`reserve_latency_seconds`, `coalescer_batch_size`, `coalescer_flush_latency_seconds`,
`coalescer_inflight_ops`, `cas_missed_total{pool}` (duplicate-owner alarm),
`cas_unknown_total`, `compensating_release_total`, `allocator_available{pool}`,
`shards_owned{pool}`, `shard_adoptions_total{result}`, `shard_drops_total{reason}`,
`lease_renew_failures_total`, `pool_rows{pool,state}`, `pool_shards{pool,status}`,
`reaper_resolved_total{to}`, `quarantine_oldest_age_seconds`,
`oracle_requests_total{result}`, `minter_splits_total{result}`,
`watcher_oldest_unmined_age_seconds`, `clock_skew_seconds`, `job_lease_held{job}`,
`store_op_latency_seconds{op}`.

## Repository layout

```
cmd/fuelkeeper  cmd/fueldemo  cmd/fuelbench
pkg/api        HTTP handlers, middleware, OpenAPI serving
pkg/client     Go client + createAction helpers
pkg/fuel       domain types, lock modes (puzzle, p2pkh presign), BEEF assembly
pkg/pool       shardmgr, allocator, coalescer, service facade
pkg/jobs       reaper, watcher, minter, janitor (leased runners)
pkg/treasury   toolbox wallet wiring, funding, splits; mock
pkg/oracle     teranode, arcade, woc, none, composite; mock
pkg/store      interface, errors, types; storetest/; memory/ sqlite/ postgres/ pebble/ mongodb/ aerospike/ factory/
pkg/config     YAML + env overrides, validation (incl. mint-capacity check)
pkg/metrics
openapi/  docs/  docs/benchmarks/  docs/superpowers/specs/  deploy/ (docker-compose: postgres, mongo, aerospike)
```

Tooling: Go 1.26, golangci-lint (arcade's config as the base), `Makefile`
(`build test test-integration lint bench docker-up`), GitHub Actions (unit +
sqlite/pebble/memory conformance always; Postgres and Mongo via service
containers; Aerospike behind a tag).

## Execution strategy (subagent-driven, model-tiered)

Task breakout uses the subagent-driven-development flow, and each task is
assigned to the cheapest model that can do it well (user preference):

| Tier | Model | Tasks |
|---|---|---|
| Design-sensitive core | primary (Fable) | store interface + contract comments, `postgres` reference backend, coalescer, shard manager, allocator, reaper/oracle flow, treasury wiring and split minting, invariant/chaos tests, all code reviews of subagent output |
| Pattern-following implementation | sonnet | `sqlite`, `pebble`, `mongodb`, `aerospike`, `memory` backends implemented against the finished `storetest` suite and the `postgres` reference; HTTP handlers from the OpenAPI spec; Go client; CLI subcommands; `fuelbench` harness and markdown renderer; docker-compose and CI workflows; demo raw mode |
| Mechanical / documentation | haiku | OpenAPI YAML from the endpoint table, `docs/API.md`, `docs/OPERATIONS.md`, `docs/CLIENTS.md`, README, metric registration boilerplate, config struct plumbing and validation tests, lint fixes |

Rules: a subagent task always includes the reference implementation path, the
conformance suite it must pass, and the exact commands to run; every subagent
result is reviewed by the primary model before merge; anything touching the
G1/G2 guarantees stays on the primary model.

## Implementation phases

Each phase ends with passing tests and a coherent commit series. TDD for
`pkg/store`, `pkg/pool` and `pkg/jobs`.

1. **Bootstrap**: `git init`, module, layout, lint, Makefile, config loader,
   this design written to `docs/superpowers/specs/2026-09-29-fuelkeeper-design.md`
   and committed; then the writing-plans skill produces the task-level plan.
2. **Store core**: types, errors, `storetest`, `memory`, then `postgres`
   (schema + EXPLAIN pins) and `sqlite` (pragma read-back). Compose for
   Postgres.
3. **Pool core**: allocator, coalescer (slot-based group commit, idempotent
   retry, compensation), shard manager, chaos tests on the memory store, the
   randomized multi-instance invariant test for G1/G2.
4. **Jobs + oracle**: reaper with quarantine/oracle flow, watcher, minter
   against the mock treasury, janitor, `Leaser`-based single runner; mock and
   teranode/arcade oracles.
5. **API + client + CLI**: handlers, OpenAPI spec, Go client, `serve`,
   metrics, health; service-level integration test with the mock treasury.
6. **Remaining stores**: `pebble`, `mongodb`, `aerospike` through the same
   suite; compose entries and CI tags; backend-specific pins.
7. **Treasury**: toolbox wallet wiring, BRC-29 funding endpoint, throughput
   strategy chunking, leaf splits with `NoSend`/`SendWith`, proof watcher
   wiring, `fund` CLI. Verified on teratestnet with a small pool.
8. **Demo**: `fueldemo` toolbox and raw modes on teratestnet; fix whatever the
   consumer path reveals (surplus handling, BEEF size, unmined ancestry).
9. **Benchmarks + docs**: `fuelbench`, per-backend and multi-instance runs,
   `docs/benchmarks/*.md`, `docs/API.md`, `docs/OPERATIONS.md`,
   `docs/CLIENTS.md` (Go and TS consumer recipes), README.

## Verification

- `make test`: unit tests plus sqlite/pebble/memory conformance with `-race`;
  `make test-integration`: postgres/mongo/aerospike via compose.
- Invariant test: randomized reserve/release/spend/expire/crash across several
  simulated instances on one memory store asserts G1 (no outpoint in two live
  reservations) and G2 (every outpoint in exactly one state; terminal states
  only `spent` or `invalid`; unspent rows return to `available`).
- Treasury smoke on teratestnet: `fuelkeeper fund`, watch shards go
  `unmined → mined`, reserve one output, build a tx with go-sdk, broadcast,
  call `spend`, watch it resolve to `spent` via the oracle.
- Demo: sustained run on teratestnet with N wallet processes; benchmark report
  records TPS, reserve p99, CAS misses, and the double-hand-out audit result.

## Explicitly cut (and why)

`spent_external` state (spend is a hint; oracle proves spent); shard `epoch`
(nothing needs fencing because adoption never writes rows); release-by-token
without outpoints (removes the `reserved_by` index everywhere); inline BEEF
in reserve responses (immutable endpoint + client cache); a direct-store
claim fallback when an instance owns no shard (would duplicate claim
machinery); `avail_hint`, client idempotency keys, envelope encryption of
unlocking data, gRPC (deferred until metrics or users ask).
