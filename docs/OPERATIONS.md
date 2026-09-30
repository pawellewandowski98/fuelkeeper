# Operations Guide

## Running FuelKeeper

FuelKeeper is a standalone HTTP service. Start it with:

```bash
fuelkeeper serve --config fuelkeeper.yaml
```

The YAML configuration file specifies all runtime behavior: pools, storage, jobs, oracle, treasury, API, and logging.

## Configuration

All YAML keys are strict (typos are rejected). Environment variables are expanded using `${VAR}` or `${VAR:-default}` syntax; `${VAR}` errors if unset, while `${VAR:-default}` falls back to a default value. `$$` is a literal `$`.

### Full Configuration Template

```yaml
# Instance identity and resource limits
instance:
  instance_id: myhost          # Default: system hostname; used in logs and shard adoption
  max_shards: 8                # Max shards this instance can own; default 8
  low_water_rows: 5000         # When in-memory rows fall below this, adopt a new shard; default 5000
  lease_ttl: 30s               # Duration the instance holds shard leases; must exceed rescan_interval; default 30s
  rescan_interval: 5s          # How often to reload available rows of shards this instance already owns, and drop ones that stay empty; default 5s
  reserve_timeout: 2s          # Max time a single reserve request can take; default 2s
  flush_window: 0s             # Extra time the coalescer waits to batch concurrent Reserve calls into one store write; default 0 (flush as soon as a slot is free)
  max_batch: 0                 # Max ops per coalescer flush; 0 means at least 256 (and at least pools[].max_count if that is larger), capped at the store's own max batch size
  flush_timeout: 5s            # Per-attempt timeout for the store call each flush (and each retried attempt) makes; default 5s
  adopt_timeout: 200ms         # Max time a reserve request waits for shard adoption; default 200ms

# Store backend (memory, sqlite, postgres, pebble, mongodb, aerospike)
store:
  backend: memory              # Required: memory | sqlite | postgres | pebble | mongodb | aerospike
  sqlite:
    path: /var/lib/fuelkeeper/fuel.sqlite  # Required when backend: sqlite
    sync: FULL                 # Optional: FULL (default, fsyncs every commit) | NORMAL (faster, less durable)
  postgres:
    dsn: postgres://user:pass@localhost/fuel  # Required when backend: postgres
    max_conns: 0                # Optional: pgx pool size; 0 = default (maxConcurrentFlushes + 4)
    # Note: synchronous_commit must NOT be 'off'; on, local, remote_write, remote_apply are accepted
  pebble:
    path: /var/lib/fuelkeeper/pebble  # Required when backend: pebble
    no_sync: false              # Optional: must be false in production (fsync every write batch); default false
    cache_bytes: 0               # Optional: block cache size; 0 = pebble's own default (256 MiB)
  mongodb:
    uri: mongodb://localhost:27017    # Required when backend: mongodb
    database: fuelkeeper               # Required when backend: mongodb
    max_pool_size: 0             # Optional: driver connection pool size; 0 = driver default (64)
    op_timeout: 0s                # Optional: per-operation timeout; 0 = default (5s); raise for cross-region majority writes
    # Note: the backend always writes with { w: majority, j: true }; op_timeout must leave room for that round trip
  aerospike:
    hosts: ["localhost:3000"]    # Required when backend: aerospike; "host:port" or bare "host" (defaults to port 3000)
    namespace: fuelkeeper         # Required when backend: aerospike; must be strong-consistency in production
    set_prefix: fk_               # Optional: prepended to every set/index name; fixed per deployment, never generated per store
    key_prefix: ""                 # Optional: isolates this store's keys on sets shared with others; <= 64 bytes, no "|" or NUL
    allow_availability_mode: false # Optional: opts out of the strong-consistency requirement; CE dev/test servers ONLY, never production
    timeout: 0s                    # Optional: per-operation timeout; 0 = default (3s)
    pool_size: 0                   # Optional: per-node connection pool size; 0 = default (128)

# Pools: fixed-denomination fuel outputs
pools:
  - name: main                       # Pool name; used in reserve/spend/release endpoints
    denomination: 50                 # Satoshis per output; must be > 0
    lock_mode: puzzle                # puzzle (default) | p2pkh; unlock mode
    split_size: 500                  # Outputs per split transaction (shard); default 500; must be 1..10000
    target_available: 100000         # Target number of available rows to maintain; default 100000
    low_water: 60000                 # Minter refill trigger: rows < this starts a refill; 0 = always refill below target; default 60000
                                     #   once started, refilling is sticky (in-memory, per process) and keeps minting past
                                     #   low_water each round until rows reach target_available, not just past low_water
    serve_policy: mined_only         # mined_only (default) | any; return only mined splits or any splits
    default_ttl: 2m                  # Reservation TTL when client doesn't specify; default 2m
    min_ttl: 30s                     # Minimum allowed TTL; default 30s
    max_ttl: 10m                     # Maximum allowed TTL; default 10m
    max_count: 100                   # Max outputs per reserve request; default 100
    release_policy: trust            # trust (default) | verify; verify released outputs via oracle (allowed with any
                                     #   oracle except "none"; arcade-only makes this a delayed release, see below)
    spend_policy: trust              # trust | verify; default verify when the oracle answers outpoint queries
                                     #   (mock, teranode, composite with teranode_url), else trust; see "Oracle kinds and policies"
    expiry_policy: requeue           # requeue (default) | hold; what the reaper does with a quarantined row that has
                                     #   no spend hint and no oracle verdict: requeue it after jobs.reaper.unknown_requeue_age,
                                     #   or hold it quarantined for an operator
    expected_tps: 0                  # Expected transactions per second (for capacity validation); no check if 0

# Background jobs (enabled by default)
jobs:
  enabled: true                      # Run jobs or not; default true
  lease_ttl: 90s                     # Job lease TTL; must exceed 2× longest job interval (and, with the toolbox
                                     #   treasury, minter.build_timeout + 20s); only matters when enabled
  
  reaper:
    interval: 1s                     # How often to scan for expired/quarantined rows; default 1s
    grace: 10s                       # Clock-skew guard before quarantining an expired reservation; default 10s
    quarantine_min_age: 30s          # Wait before the first oracle check on a quarantined row; default 30s
    min_unspent_age: 30s             # Quarantine age before an unhinted Unspent row is requeued; default 30s
    hint_patience: 10m               # Quarantine age before a hinted Unspent row (or a hint rejected tx_invalid) is requeued; default 10m
    unknown_requeue_age: 10m         # Quarantine age before an unhinted row with no oracle verdict is requeued, under
                                     #   pool expiry_policy: requeue; 0 disables (never requeue blind); must be 0 or >= min_unspent_age; default 10m
    hint_max_wait: 24h               # Quarantine age before a hinted row whose tx was never mined or classified is
                                     #   requeued as a last resort; 0 disables; must be 0 or >= hint_patience; default 24h
    batch: 500                       # ListQuarantined page size; default 500
    max_pages: 10                    # Max ListQuarantined pages per tick; default 10
    quarantine_batch: 1000           # Rows moved per Store.Quarantine call; default 1000
    oracle_batch: 200                # Outpoints per oracle call; default 200
    tx_recheck_interval: 30s         # Minimum interval between Tx lookups for the same hinted transaction; default 30s

  watcher:
    interval: 10s                    # How often to check unmined shards; default 10s
    rebroadcast_after: 5m            # Rebroadcast unmined splits after this time; default 5m
    alert_after: 6h                  # Log alerts for unmined splits after this time; default 6h
    limit: 5000                      # Unmined shards fetched per tick; default 5000; must exceed jobs.minter.max_pending_shards
    max_checks_per_tick: 500         # Oracle calls bounded per tick; default 500

  minter:
    interval: 30s                    # How often to mint new shards; default 30s
    max_per_round: 20                # Max new shards per minting round; default 20
    max_pending_shards: 50           # Max unmined shards before minting pauses; default 50
    build_timeout: 60s               # Bound on one split's Build + PutShard; default 60s; treasury.toolbox.reconcile_grace must exceed it + 10s

# Chain oracle (verifies spend and unspent states)
oracle:
  kind: none                         # none | mock | arcade | teranode | composite; default resolved from treasury.kind
                                     #   when left empty: arcade when treasury.kind is toolbox (below, it's mock, so
                                     #   none), else none. A direct teranode connection is never assumed -- it is an
                                     #   optional, opt-in accelerator (composite with teranode_url); see "Oracle kinds
                                     #   and policies" below and docs/CLIENTS.md's settlement contract
  mock:
    assume_unspent: true             # When true, assume unknown outpoints are unspent (nullnet only)
  arcade_url: ""                     # Arcade base URL; required for arcade and composite (default: the
                                     #   toolbox treasury's arcade_url when treasury.kind is toolbox)
  teranode_url: ""                   # Teranode asset API base (e.g. http://teranode:8090/api/v1);
                                     #   required for teranode, optional for composite
  teranode_batch_threshold: 0        # Outpoints above which teranode batches lookups; 0 = 8, < 0 disables

# Treasury (produces split transactions)
treasury:
  kind: mock                         # mock (default, nullnet only) | toolbox
  mock:
    confirm_after: 2s                # How long after send to mark splits mined (nullnet only)
  toolbox:                           # go-wallet-toolbox wallet; see docs/TREASURY.md
    network: ttn                     # main | test | ttn (default) | tstn
    root_key_hex: ${FUELKEEPER_TREASURY_KEY}  # 64 hex chars; required for kind toolbox; never commit it
    originator: fuelkeeper           # BRC-100 originator; default fuelkeeper
    fee_sat_per_kb: 0                # 0 = read arcade /policy at start (floor 1)
    arcade_url: ""                   # Arcade base URL (broadcast, status); default on main/ttn: the toolbox's
                                     #   public arcade; required on test/tstn
    arcade_events_url: ""            # Arcade SSE events base; default arcade_url
    chaintracks_url: ""              # Chaintracks (headers, SPV); default on main/ttn: <arcade host>/chaintracks;
                                     #   required on test/tstn
    storage:
      engine: sqlite                 # sqlite (default) | postgres
      sqlite_path: treasury.sqlite   # Wallet database for engine sqlite
      postgres: {host: "", port: 0, dbname: "", user: "", password: "", sslmode: ""}  # port 0 = 5432; sslmode "" = verify-full
    chunking:
      fanout_outputs_per_tx: 100     # Chunks per reserve chunk (M); default 100
      target_chunks: 50              # Leaf-sized chunks kept ready; default 50
      low_water_percent: 60          # Refill below this share of target_chunks; default 60
      headroom_sats: 20              # Added to each chunk so the leaf surplus stays below dust; default 20
      interval: 10s                  # Keeper cadence (the minter drives it each round); default 10s
      reserve_basket: reserve        # Wallet basket for reserve chunks; default reserve
      chunk_basket: chunks           # Wallet basket for leaf-sized chunks; default chunks
    reconcile_grace: 2m              # Minimum gap between Reconcile's two listings; must exceed jobs.minter.build_timeout + 10s

# HTTP API listener
api:
  listen: ":8080"                    # Host:port to bind; default :8080
  insecure: false                    # Allow access without API keys (non-loopback listen requires explicit insecure: true)
  api_keys: ["${FUELKEEPER_API_KEY}"]      # Bearer tokens for /v1/ endpoints; required unless listen is loopback or insecure: true
  admin_keys: ["${FUELKEEPER_ADMIN_KEY}"]  # Bearer tokens for admin endpoints (funding, quarantine); empty = use api_keys
  read_timeout: 10s                  # HTTP read timeout; default 10s
  write_timeout: 30s                 # HTTP write timeout; default 30s
  idle_timeout: 120s                 # HTTP idle keep-alive timeout; default 120s
  max_body_bytes: 1048576            # Max request body size; limits input only (1 MiB default)
  request_timeout: 10s               # Max time per HTTP request; must exceed instance.reserve_timeout; default 10s

# Logging
log:
  level: info                        # debug | info (default) | warn | error
  format: text                       # text (default) | json
```

### Validation Rules

On startup, FuelKeeper validates the configuration:

- `store.backend` must be one of `memory`, `sqlite`, `postgres`, `pebble`, `mongodb`, `aerospike`; an empty or unrecognized value names the full list in the error
- The chosen backend's required fields are checked at config load, before FuelKeeper ever opens the store, and named `store.<backend>.<field>`: `store.sqlite.path`, `store.postgres.dsn`, `store.pebble.path`, `store.mongodb.uri` and `store.mongodb.database`, `store.aerospike.hosts` (at least one) and `store.aerospike.namespace`
- Postgres requires `synchronous_commit` not set to `off` (checked at connect time)
- SQLite defaults to `synchronous=FULL` (fsync on every commit); set `sync: NORMAL` to opt out; `store.sqlite.sync` is validated case-insensitively and normalized to `FULL`/`NORMAL`
- Aerospike requires a strong-consistency namespace (checked at connect time, on every node) unless `store.aerospike.allow_availability_mode: true` (CE dev/test servers only); `key_prefix` must be valid UTF-8, at most 64 bytes, and contain neither `|` nor a NUL byte
- `jobs.lease_ttl` must exceed twice the longest job interval (e.g., if minter.interval is 30s, lease_ttl must be > 60s)
- `jobs.watcher.limit` must exceed `jobs.minter.max_pending_shards`
- Mint capacity must be ≥ 1.2× `expected_tps` for each pool (when expected_tps > 0)
- Pools with `spend_policy: verify` or `release_policy: verify` require an oracle (`oracle.kind` != "none"): `oracle.kind: none` is the only oracle that rejects either policy at config load -- an arcade-only oracle (`arcade`, or `composite` without `teranode_url`) accepts both (see "Oracle kinds and policies")
- `spend_policy` defaults to `verify` only when the oracle answers outpoint queries directly (`mock`, `teranode`, `composite` with `teranode_url`); it defaults to `trust` otherwise (`none`, `arcade`, `composite` without `teranode_url` -- the expected arcade-only production shape). An explicit `spend_policy: verify` is kept even with an arcade-only oracle, not overridden
- `oracle.kind: arcade` and `composite` need `oracle.arcade_url` (or a toolbox treasury, whose arcade they then use); `teranode` needs `oracle.teranode_url`
- `oracle.kind: teranode` is rejected whenever `jobs.enabled` is true: teranode's asset API never reports a mined status, so the watcher could never confirm a shard mined and the reaper could never confirm a spend mined; use `oracle.kind: composite` with both `arcade_url` and `teranode_url` instead
- Per pool, `expiry_policy` must be `requeue` or `hold`
- `jobs.reaper.unknown_requeue_age` must be `0` (never requeue blind) or `>= min_unspent_age`: a row with no evidence is never requeued faster than one the oracle actually confirmed Unspent
- `jobs.reaper.hint_max_wait` must be `0` (never requeue a stuck hint) or `>= hint_patience`
- `treasury.kind: toolbox` needs a valid `root_key_hex` (plus `arcade_url` and `chaintracks_url` on `test`/`tstn`; `main` and `ttn` default to the toolbox's public endpoints), and every pool must share one leaf cost class (same `split_size` × `denomination`, same leaf size): one treasury keeps one chunk denomination (see [TREASURY.md](TREASURY.md))
- `treasury.toolbox.reconcile_grace` must exceed `jobs.minter.build_timeout` + 10 s (the reconcile budget), or Reconcile could abort a split whose Build and PutShard were merely slow
- With the toolbox treasury and jobs enabled, `jobs.lease_ttl` must exceed `jobs.minter.build_timeout` + 20 s (two 10 s reconcile phases), the longest minter round the lease has to outlast

**Config keys changed with the toolbox treasury.** The oracle URLs moved from
nested blocks to flat keys: `oracle.arcade.url` is now `oracle.arcade_url`
and `oracle.teranode.url` is now `oracle.teranode_url`. The old keys are
rejected as unknown fields. `oracle.mock.assume_unspent` and
`treasury.mock.confirm_after` are unchanged.

### Oracle Kinds and Policies

**Arcade-only (`oracle.kind: arcade`, or `composite` without `teranode_url`)
is the expected production shape.** A direct teranode connection is never
assumed -- it is an optional, opt-in accelerator. `oracle.kind` defaults to
`arcade` when `treasury.kind: toolbox` (else `none`); `serve` logs one Info
line at start-up (`logOracleMode`) naming the resolved kind and whether it
answers outpoint queries directly.

The reaper settles quarantined rows with two oracle questions: *is this
outpoint spent?* (`Outpoints`) and *is this txid mined, and what does it
spend?* (`Tx`, plus the inputs lookup). Not every oracle answers both:

| `oracle.kind` | Outpoints | Mined status | Spender inputs |
|---|---|---|---|
| `none` | never (Unknown) | never | no |
| `mock` | nullnet only | nullnet only | as configured |
| `arcade` | never (Unknown) -- no UTXO index | yes | no -- never serves raw transactions |
| `composite` without `teranode_url` | never (Unknown) | yes (arcade) | no |
| `composite` with `teranode_url` | yes (teranode) | yes (arcade) | yes (teranode raw tx, fetched only by the reaper) |
| `teranode` | yes | **never** | yes; **rejected by config when jobs are enabled** |

Arcade never reports a competing transaction synchronously, and never
populates `competingTxs` or a `DOUBLE_SPEND_ATTEMPTED` status at all, even
asynchronously: POST /tx's 4xx and 202 bodies never carry a competitor, so a
double-spend only appears later, via a follow-up GET /tx answering REJECTED
with ARC code 466 and the underlying teranode rejection line verbatim in
`extraInfo` (e.g. `UTXO_SPENT (70): <txid>:<vout> utxo already spent by tx
<spender>[<vin>]`). 466 alone is not enough to classify: arcade reuses it
for four distinct teranode verdicts, so the oracle parses `extraInfo`'s
leading name to tell them apart -- `UTXO_SPENT`, `TX_CONFLICTING` and
`TX_INVALID_DOUBLE_SPEND` map to `RejectReasonInputsSpent` (an
already-decided ownership of the input); `TX_LOCKED` maps to
`RejectReasonUnknown` (transient: the parent transaction's own commit is
still in flight in teranode's store, not a decided spend) -- as does an
empty or unparsable `extraInfo`. Every other classifiable code maps to
`RejectReasonTxInvalid`, except 467 (generic: ambiguous between a real
`TX_INVALID` and arcade's own internal processing fault, so it maps to
`RejectReasonUnknown` too) and 476 "not final" (also `RejectReasonUnknown`,
because the transaction may still become valid once its locktime passes).
When a `UTXO_SPENT` line names the spender, the oracle also fills
`oracle.TxStatus.SpentInputs` (the specific outpoint(s) reported spent) and
appends the spender to `CompetingTxs`; `TX_CONFLICTING`/
`TX_INVALID_DOUBLE_SPEND` lines classify `InputsSpent` but are not parsed
for detail, so `SpentInputs` stays nil for those. See
`pkg/oracle/arcade/arcade.go`'s `rejectReasonForCode`/`rejectReasonFor466`
for the full code table. A consumer submitting through arcade therefore
reports its broadcast as accepted the moment arcade accepts it; a later
double-spend is discovered by whoever next polls the transaction's fate
(FuelKeeper's own reaper, or the consumer's own review process) -- see
[CLIENTS.md](CLIENTS.md#when-your-broadcast-fails).

**What each policy does, per oracle, and after what delay:**

| `oracle.kind` | Expired reservation (unhinted) | `release_policy: verify` | `spend_policy: trust` | `spend_policy: verify` (opt-in unless noted) |
|---|---|---|---|---|
| `none` | Requeued after `unknown_requeue_age` (`expiry_policy: requeue`, default) or held for an operator (`expiry_policy: hold`, or `unknown_requeue_age: 0`) | **rejected at config load** (`release_policy=verify requires an oracle`) | spent at once, no oracle call -- the only policy this oracle allows, and a later-discovered problem with the client's report is unrecoverable: there is no `verify` opt-in with this oracle | **rejected at config load** (`spend_policy=verify requires an oracle`) |
| `arcade`, or `composite` without `teranode_url` | Same as `none`: `Outpoints` always answers Unknown, so the row is unhinted and follows the same `unknown_requeue_age`/`expiry_policy` rule | allowed: a **delayed release** -- `Release` stores no hint, so the row is quarantined and follows the same unhinted rule as an expired reservation above | spent at once, no oracle call -- **see the trust trade-off below: an asynchronous arcade rejection after this point is never seen again** | allowed (default here is `trust`): the row is hinted and resolved from `Tx(hint)` (see the reaper lifecycle table below) -- mined & spends the outpoint, a mined competitor, or a rejection naming a spent input all resolve to spent; a rejection with the inputs untouched requeues after `hint_patience`; anything still unresolved requeues after `hint_max_wait` (24h default) as a last resort |
| `composite` with `teranode_url` | Resolved directly from teranode's UTXO-set verdict: Unspent requeues after `min_unspent_age` (30s), Spent (mined, inputs verified) resolves to spent -- no `unknown_requeue_age` wait | allowed, same delay as an expired reservation with this oracle (`min_unspent_age`, not `unknown_requeue_age`) | spent at once, no oracle call -- **the same trust trade-off as arcade-only, unmitigated by teranode**: `spend_policy: trust` never consults any oracle regardless of kind, so having `teranode_url` configured makes no difference to this row once it is trusted-spent | allowed (default here): resolved once mined and the spender's inputs (from teranode) include the outpoint -- no blind trust of an unverified hint |
| `teranode` alone | N/A -- **config refuses to start whenever `jobs.enabled` is true**: teranode's asset API never reports a mined status, so the watcher could never confirm a shard mined and the reaper could never confirm a spend mined; use `composite` with both `arcade_url` and `teranode_url` instead | | | |

**The `spend_policy: trust` blind spot with arcade-only.** Arcade's 202
response only means the transaction was accepted for validation, not that it
is safe from a later, asynchronous rejection (an input the wallet's own
transaction already spent, `TX_INVALID`, or `TX_LOCKED` on the wallet's own
still-settling parent). Under the default `spend_policy: trust`, the
consumer's `Settle(OutcomeBroadcast)` call on that 202 moves the row
straight to the terminal `spent` state, and nothing ever re-examines it: if
arcade goes on to reject the transaction, the fuel output is lost --
marked `spent` forever, not merely delayed. `spend_policy: verify` is the
opt-in for an arcade-only deployment that must not lose outputs this way:
the row is quarantined as a hint instead of resolved immediately, and the
reaper polls `GET /tx` for it every `jobs.reaper.tx_recheck_interval` until
it is mined or rejected (a rejection with the input untouched requeues the
output; see the reaper lifecycle table) -- the cost is that per-hinted-row
polling load, not a correctness cost. A `TX_LOCKED` rejection specifically
classifies `RejectReasonUnknown` (rule 5, held `rejected_unknown`), not
`inputs_spent` or `tx_invalid`: it is transient (the parent transaction's
own commit is still in flight in teranode's store, not a decided spend), so
the row is held rather than requeued or retired until it resolves itself on
a later poll, or, failing that, `hint_max_wait` requeues it as a last
resort. The default `serve_policy: mined_only`
already rules out `TX_LOCKED` on the fuel output's *own* parent (a served
row's split transaction is always mined already, so its own UTXO commit has
settled by the time a client reserves it); `serve_policy: any` removes that
guarantee. **`serve_policy: any` combined with arcade-only and
`spend_policy: trust` is the combination to avoid; prefer
`spend_policy: verify` with `serve_policy: any`.** The same blind spot
exists, with no `verify` opt-in at all, under `oracle.kind: none` -- it has
no oracle to ever examine a trusted spend regardless of policy.

Split shards (the watcher's mined check) follow the "Mined status" column of
the first table above: never mined with `none` (use the mock treasury, or
give the oracle an arcade/teranode source); mined via arcade with `arcade`
or `composite`; never mined with `teranode` alone, which is exactly why
config rejects that combination whenever jobs run.

Configuring `oracle.teranode_url` (`oracle.kind: composite`) is optional and
resolves the rows in the "expired reservation" / `release_policy: verify` /
`spend_policy: verify` columns immediately from the UTXO set, instead of
waiting out `unknown_requeue_age`/`hint_patience`/`hint_max_wait`. It is an
accelerator, not a requirement: arcade-only deployments are expected to run
indefinitely with these delays. Pool capacity does not otherwise leak:
an unreported reservation is requeued after `unknown_requeue_age` under
`expiry_policy: requeue` (held under `expiry_policy: hold` until an
operator acts); an output is actually lost only in the documented cases --
the `spend_policy: trust` blind spot above, and the hinted rule 3 bounded
loss (see the reaper lifecycle table).

## Deployment Topologies

A config file has no notion of "several instances" in one place — each
FuelKeeper process reads its own file (with its own `instance.instance_id`)
and points `store:` at the same backend/database the other processes share.
Which backends that's safe for is a property of the backend, captured by
`store.Caps.MultiProcess` (see `pkg/store/factory.Config`'s doc comment):

### Single-Process (memory, SQLite or Pebble)

- One process per database/directory
- SQLite serializes writes through one in-process writer (group-committed, see [Durability and Consistency](#durability-and-consistency)); `busy_timeout` (5s) only matters against another process holding the file's lock
- Pebble locks its directory for exclusive access; a second process opening
  the same `pebble.path` fails at store-open time rather than corrupting data
- Memory backend: all state lost on restart; for development and testing only
- Running two processes against the same sqlite file or pebble directory, or
  sharing a memory store across processes (it is in-process only), is a
  configuration error, not something FuelKeeper detects and refuses at
  startup — keep exactly one process per single-process backend instance

### Multi-Process (Postgres, MongoDB or Aerospike)

- Multiple processes share the same backing database/cluster
- Each instance acquires a lease on whole shards and job responsibilities
- Shards from a crashed instance are re-adopted after `instance.lease_ttl`
- Every store operation is a compare-and-set; no pessimistic locking
- With `treasury.kind: toolbox`, every instance must use the same treasury storage: one postgres database in `treasury.toolbox.storage` and the same root key. The minter runs wherever the lease is and funding is served by whichever instance a request reaches, so with per-instance sqlite files a payment can land in a wallet the minter never spends (see [TREASURY.md](TREASURY.md#several-instances))

**MongoDB.** The backend always writes with `{w: "majority", j: true}` and
reads with read preference primary (not operator-configurable), so an
acknowledged write is durable and visible to every subsequent read; there is
deliberately no TTL index (`fuel_rows` never expires a document out from under
a live reservation — cleanup is driven by explicit state transitions only).
The package opens no session or multi-document transaction, so even a
standalone `mongod` works for development; production should run a real
replica set so `w: majority` has somewhere to replicate to and survives a
primary failover. Schema v2 indexes (partial indexes on state) are created
automatically at open.

**Aerospike.** Production **must** run a namespace configured with
`strong-consistency true` (Enterprise Edition; Community Edition cannot run
SC) — `New` reads the namespace's `strong-consistency` info property on every
node and refuses to start unless it is true. `allow_availability_mode` is the
explicit opt-out, for single-node Community Edition development and test
servers only; never set it in production, since availability (AP) mode lets
two sides of a partition accept conflicting writes to the same record and a
migration can resurrect an older copy, which breaks the "one row, one owner"
invariant (G1). Recommended SC namespace settings:

- `strong-consistency true`, with a roster covering every node
- `replication-factor` 2 or more (`replication-factor 1` survives no node
  loss: every partition on a failed node becomes unavailable, surfacing as
  request errors rather than wrong answers)
- `commit-to-device true` on the storage engine if the deployment must survive
  a simultaneous crash of every replica of a partition without losing
  acknowledged writes
- `default-ttl 0` (rows and shards never expire; the backend never sets a
  record TTL)

Set names are fixed per deployment (`set_prefix` + `{rows, shards, txids,
leases, meta}`; Aerospike never frees a set name once created, and a
namespace is capped at roughly 4K of them) — never derive `set_prefix` per
store instance. `key_prefix` isolates one FuelKeeper deployment's keys from
another's on sets it shares with them (for example, several test runs against
one server); it must be valid UTF-8, at most 64 bytes, and contain no `|` or
NUL byte.

## Local Integration Testing

`deploy/docker-compose.yaml` defines the three multi-process backends'
servers:

| Service   | Port(s)         | Notes |
|-----------|------------------|-------|
| postgres  | 15432 (→ 5432)   | `postgres:16-alpine`; always started (no profile) |
| mongo     | 27017            | `mongo:7`; behind the `mongo` compose profile |
| aerospike | 3000-3002        | `aerospike/aerospike-server`; behind the `aerospike` compose profile; namespace `test`, allow-availability-mode configuration for CE (see `deploy/aerospike.conf`) |

`make docker-up` (`docker compose -f deploy/docker-compose.yaml up -d --wait`)
starts only postgres by default, so it works unmodified on a host where ports
3000-3002 or 27017 are already taken by something else (including another
FuelKeeper checkout's own aerospike/mongo, or a shared long-lived Aerospike
test server other tooling depends on). Opt in to the other two with
`COMPOSE_PROFILES`:

```bash
COMPOSE_PROFILES=mongo,aerospike make docker-up
```

`make test-integration` runs every backend's conformance suite under its
build tag, plus the factory package's own tagged tests (which open a store
through `pkg/store/factory.New` the way `pkg/config`/`cmd/fuelkeeper` do,
rather than through the backend package directly):

```bash
FUELKEEPER_TEST_POSTGRES_DSN=...  go test -tags=postgres  ./pkg/store/postgres/... ./pkg/store/factory/...
FUELKEEPER_TEST_MONGODB_URI=...   go test -tags=mongodb   ./pkg/store/mongodb/...  ./pkg/store/factory/...
FUELKEEPER_TEST_AEROSPIKE_HOST=... go test -tags=aerospike ./pkg/store/aerospike/... ./pkg/store/factory/...
```

(see the Makefile for the exact commands and default DSN/URI/host values).
Every mongodb/aerospike-tagged test skips itself when its env var is unset,
so `make test-integration` without `COMPOSE_PROFILES` still runs cleanly
against whatever subset is up. Aerospike's tests never create a new set name
(fixed `fk_*` sets; see [Deployment Topologies](#deployment-topologies)) and
isolate themselves with a random `key_prefix`, so it is safe to point them at
a long-lived, shared Aerospike server instead of a disposable one — do not
restart or `docker compose down` a shared server other tooling also depends
on; verify with `docker exec <container> asinfo -v sets/<namespace> | tr ';'
'\n' | grep -c set=` that the set-name count is unchanged after a run.

### Benchmark cluster

`deploy/docker-compose.bench.yaml` runs three `fuelkeeper` instances (`fk1`-`fk3`, built from the repo `Dockerfile`) sharing one `postgres:16-alpine` store, all driven by `deploy/fuelkeeper-bench.yaml` (mock oracle and treasury, so throughput measures the store/pool path, not chain latency). It is a separate compose project (`name: fuelkeeper-bench`) from `deploy/docker-compose.yaml`'s dev Postgres: its own network, and host ports 8081-8083 only (the bench Postgres exposes no host port, unlike the dev one's 15432).

```bash
make bench-cluster-up                     # build + start fk1..fk3 + postgres, wait for healthy
make bench-run                            # cmd/fuelbench against :8081-8083; WORKERS/COUNT/DURATION/LABEL/OUT override
make bench-cluster-down                   # stop and remove volumes
```

`bench-run`'s `OUT` defaults to `/tmp/fuelbench-last.json` for a throwaway run; pass `OUT=docs/benchmarks/results/<name>.json` to save a result alongside the published ones (see `docs/PERFORMANCE.md` and `docs/benchmarks/README.md`). Give the minter a few seconds after `bench-cluster-up` to fill `pools[0].available` toward `target_available` before a long run, or lower `target_available` in the bench config for a quick smoke test; a handful of `exhausted` errors right after start-up, before the mock treasury's first splits are mined, is expected and not a bug. Every service sets `SETTINGS_ENV_FILE=/dev/null` (see "Teratestnet" above) so the toolbox's settings package does not pick up a stray `.env`/`settings.conf`.

`fk1`-`fk3` bind-mount the same `deploy/fuelkeeper-bench.yaml` read-only with the SELinux relabel option `z` (lowercase), not `Z`: all three containers read the one host file, and `Z` relabels it privately for whichever container starts last, leaving the other two unable to read it (`permission denied` on startup) under rootless Podman.

## Durability and Consistency

**Correctness guarantee G1** (no fuel output is ever handed to two live reservations) depends on durability:

- **Postgres**: Enable `synchronous_commit` (not `off`). On, local, remote_write, or remote_apply all provide durable commits.
- **SQLite**: Default `synchronous=FULL` fsyncs every commit. Opt-in `sync: NORMAL` trades durability for speed (on power loss, may forget the last reservation). Writes are group-committed: the single writer runs every write queued while its previous commit was in flight in one transaction (each in its own savepoint, so each is still all-or-nothing on its own) and commits once, so concurrent writers share one WAL fsync; no write is acknowledged before that commit returns, so `FULL` keeps exactly its per-write durability. Throughput is therefore group size divided by fsync latency rather than one write per fsync (a 15 ms fsync used to cap every durable write, and so every `/spend` or `/release`, at ≈65/s).
- **Pebble**: `no_sync: false` (the default) fsyncs every write batch, which G1 depends on; only set `no_sync: true` for throwaway stores such as tests, never in production. A batch is applied under the store's lock and its WAL fsync is awaited outside it, so concurrent calls' fsyncs coalesce in Pebble's WAL writer; a call returns only once its own batch and every batch applied before it are durable, and a read waits the same way for any batch it may have observed, so no result is ever based on a write a crash could revert.
- **MongoDB**: Every write uses write concern `{w: "majority", j: true}` (journaled, not operator-configurable), so an acknowledged write is durable and every subsequent read (primary read preference) observes it.
- **Aerospike**: Production requires a strong-consistency namespace (see [Deployment Topologies](#deployment-topologies)); within that, `commit-to-device true` covers a simultaneous crash of every replica of a partition, and `replication-factor >= 2` covers a single node's loss. `allow_availability_mode` trades this guarantee away and is for CE dev/test servers only.

## Jobs and Leases

Three background jobs run under store leases for coordination. The `instance.lease_ttl` is separate and manages shard ownership: a crashed instance's shards are re-adopted by others after `instance.lease_ttl` expires (must exceed `instance.rescan_interval`).

When jobs are enabled, `jobs.lease_ttl` coordinates which instance runs each job; it must exceed twice the longest job interval to avoid rapid leader flaps.

### Reaper

Runs every `reaper.interval`. Responsibilities:

- Expire old reservations (`>= ttl_seconds` old) and move them to quarantine
- Move quarantined rows to "spent" only after the oracle confirms a MINED spend
- Requeue rows marked Unspent by the oracle (after `min_unspent_age` for unhinted rows, or `hint_patience` for hinted rows)
- Requeue an unhinted row with no oracle verdict (arcade-only: every unhinted row) after `unknown_requeue_age`, under a pool's `expiry_policy: requeue` (default); hold it for an operator under `expiry_policy: hold`
- Resolve a hinted row (`spend_policy: verify`) from its own transaction's fate when the oracle cannot answer the outpoint directly (arcade-only)

#### Reaper lifecycle, in operator terms

**Unhinted rows** (an expired reservation, or a `release_policy: verify`
release -- neither stores a hint):

| Evidence this tick | Outcome | Delay | Metric |
|---|---|---|---|
| Oracle: Unspent | requeued | `min_unspent_age` (30s default) since quarantined | `reaper_requeued_total{reason="unspent"}` |
| Oracle: Unknown, pool `expiry_policy: requeue` (default), `unknown_requeue_age > 0` | requeued (blind: no proof either way) | `unknown_requeue_age` (10m default) since quarantined | `reaper_requeued_total{reason="unknown_aged"}` |
| Oracle: Unknown, pool `expiry_policy: hold`, or `unknown_requeue_age: 0` | held for an operator | -- | `reaper_held{reason="expiry_hold"}` (or `"unknown_fresh"` while still younger than `unknown_requeue_age` under `requeue`) |
| Oracle: Spent, names a mined spending txid that spends the outpoint (its own `SpendingTxID`, or a fallback hint if it named none) | resolved to spent | immediate once mined | `reaper_spent_total{source="oracle"}` (or `"hint"` when a fallback hint resolved it) |
| Oracle: Spent, names no txid and there is no hint to fall back on | held (a `SpentBy` cannot be fabricated) | -- | `reaper_held{reason="spent_no_txid"}` |
| Oracle: Conflicting | held for an operator | -- | `reaper_held{reason="conflicting"}` |
| Oracle call failed, or answered mismatched | every remaining row this tick is left untouched | -- | `reaper_held{reason="oracle_down"}` |

**Hinted rows** (`spend_policy: verify`; with an arcade-only oracle every
hinted row's outpoint verdict is Unknown, so it is resolved from
`Tx(hint)` instead -- rule numbers match `pkg/jobs/reaper.go`'s "Hinted
rows" doc comment). Checked in this order: 1, then 3a/3b (only for an
`inputs_spent` rejection whose gateway named the spent inputs --
`SpentInputs` non-nil -- decided *before* the competitor lookup), then 2,
then 3a/3c/3d (the `SpentInputs`-nil path), then 4, then 5. This ordering
exists so that arcade's actual `UTXO_SPENT` shape (`SpentInputs` set, the
named spender appended to -- but not distinguished within -- `CompetingTxs`)
resolves in one tick: running the competitor lookup first would either
wrongly trust a mined spender the gateway's own list excludes, or hold the
row as `competitor_pending` when the list already decides it.

| Rule | Evidence this tick | Outcome | Delay | Metric |
|---|---|---|---|---|
| 1 | Hint mined, spends the outpoint | spent | immediate | `reaper_spent_total{source="hint"}` |
| 1 | Hint mined, but its inputs exclude the outpoint (mismatch) | held | -- | `reaper_held{reason="hint_pending"}` |
| 3a | Hint rejected `inputs_spent`, gateway named the spent inputs (`SpentInputs` non-nil), and the hint's own known inputs exclude the fuel output | held (checked before rule 2's competitor lookup) | -- | `reaper_held{reason="hint_pending"}` |
| 3b | Hint rejected `inputs_spent`, gateway named the spent inputs and they *include* the fuel output | spent, resolved in this one tick without a competitor lookup | immediate | `reaper_spent_total{source="inputs_spent"}` |
| 3b | Hint rejected `inputs_spent`, gateway named the spent inputs and they *exclude* the fuel output (or `SpentInputs` is an empty, non-nil list) | requeued, like an invalid tx, resolved in this one tick | `hint_patience` (10m default); held (`rejected_fresh`) before that | `reaper_requeued_total{reason="rejected"}` |
| 2 | *(Reached whenever the hint is not an `inputs_spent` rejection with named spent inputs: a hint that is not rejected at all but still carries competing transactions, is rejected for a different reason, or is rejected `inputs_spent` with the gateway naming no spent inputs.)* Hint has competing transactions; one is mined and spends the outpoint (or its inputs are unknown, trusted) | spent | immediate | `reaper_spent_total{source="competitor"}` |
| 2 | Hint has competing transactions; none is mined yet | held | -- | `reaper_held{reason="competitor_pending"}` |
| 3a | Hint rejected `inputs_spent`, gateway named no spent inputs (`SpentInputs` nil), and the hint's own known inputs exclude the fuel output | held | -- | `reaper_held{reason="hint_pending"}` |
| 3c | Hint rejected `inputs_spent`, gateway named no spent inputs, but a mined competitor provably spends only some *other* input | held | -- | `reaper_held{reason="hint_pending"}` |
| 3d | Hint rejected `inputs_spent`, gateway named no spent inputs and no mined competitor excludes the fuel output | spent (bounded-loss case: the fuel output is assumed gone; `SpentBy` is the hint itself, not the unnamed spender) | immediate | `reaper_spent_total{source="inputs_spent"}` |
| 4 | Hint rejected `tx_invalid` (fee/format/policy; inputs untouched) | requeued | `hint_patience` (10m default) since quarantined; held (`rejected_fresh`) before that | `reaper_requeued_total{reason="rejected"}` |
| 5 | Hint rejected, unclassified reason | held | -- | `reaper_held{reason="rejected_unknown"}` |
| 5 | Hint known, not yet mined or rejected | held | -- | `reaper_held{reason="hint_pending"}` |
| 5 | Hint not known to the oracle yet | held | -- | `reaper_held{reason="hint_unseen"}` |
| -- | Any held hinted row above reaches `hint_max_wait` (> 0) since quarantined | requeued as a last resort | `hint_max_wait` (24h default) | `reaper_requeued_total{reason="hint_expired"}` |

A `Tx` lookup failure this tick holds the row(s) waiting on it as
`reaper_held{reason="tx_down"}` and an outpoint-oracle `SpendingTxID` still
awaiting confirmation as `reaper_held{reason="spender_pending"}`, same as
`oracle_down`/`tx_down` above: transient, and expected to clear on the next
successful tick.

#### Reaper metrics

| Metric | Labels | Meaning | Operator action |
|---|---|---|---|
| `fuelkeeper_reaper_requeued_total` | `pool`, `reason` (`unspent`, `unknown_aged`, `rejected`, `hint_expired`) | Quarantined rows returned to `available` | `unspent`/`rejected`: normal, oracle- or patience-driven (`rejected` now also covers a hint rejected `inputs_spent` where the gateway named the actual spent input and it was not this row's -- the fuel output is confirmed untouched, not merely assumed). `unknown_aged`: normal under an arcade-only oracle -- a blind requeue with no proof either way, after `unknown_requeue_age`, per `expiry_policy: requeue`; watch `reaper_spent_total{source="inputs_spent"}`/`{source="competitor"}` for a rise right after, which would mean requeued rows are coming back stale. That delay is the safety/latency trade-off of arcade-only; configuring `oracle.teranode_url` resolves these rows immediately instead, if the stale-requeue rate matters for your deployment. `hint_expired`: a hinted transaction was never mined or classified within `hint_max_wait`; a sustained nonzero rate suggests stuck or abandoned client transactions, or an arcade outage swallowing GET /tx answers -- worth investigating |
| `fuelkeeper_reaper_spent_total` | `pool`, `source` (`oracle`, `hint`, `competitor`, `inputs_spent`) | Quarantined rows resolved to the terminal `spent` state | `oracle`/`hint`: normal terminal resolution, no action. `competitor`: a double-spend was detected and resolved -- a sustained nonzero rate points at a client population racing itself or reusing reservations; investigate the client, not FuelKeeper. `inputs_spent`: the client's own hinted transaction was rejected because an input was already spent -- either confirmed (the gateway named the spent inputs and this row's outpoint is among them) or assumed (the gateway did not name which input, the bounded-loss case: an honest wallet whose transaction died on an unrelated input still loses this one fuel output) -- a sustained nonzero rate can mean clients are reusing fuel past expiry or racing each other; check client behaviour |
| `fuelkeeper_reaper_held` (gauge) | `pool`, `reason` (13 values: `oracle_down`, `unknown_fresh`, `expiry_hold`, `unspent_fresh`, `spent_no_txid`, `spender_pending`, `hint_pending`, `hint_unseen`, `competitor_pending`, `rejected_fresh`, `rejected_unknown`, `conflicting`, `tx_down`) | Rows the reaper examined this tick and left quarantined, by why | `unknown_fresh`/`unspent_fresh`/`rejected_fresh`: normal, still waiting out a delay, no action. `expiry_hold`: deliberate under `expiry_policy: hold` -- verify on-chain and requeue by hand via `fuelkeeper quarantine requeue`, or leave it for an operator decision. `spent_no_txid`/`conflicting`/`rejected_unknown`: the oracle could not give the reaper enough to resolve automatically -- inspect on-chain (or arcade's GET /tx for the classification code) and requeue or retire by hand. `spender_pending`/`hint_pending`/`hint_unseen`/`tx_down`: normal, transient, waiting on a transaction to propagate or mine -- watch only if it grows without bound. `competitor_pending`: a double-spend race is in progress -- **never hand-requeue these**; wait for a competitor to be mined (the row then resolves to spent under `source="competitor"`), or, absent that, it eventually requeues on its own after `hint_max_wait` (24h default) like any other unresolved hinted row -- it does not clear on its own just because a competitor check comes up empty on some later tick. `oracle_down`: the oracle call itself failed or was malformed this tick -- check oracle/arcade connectivity if sustained |

### Watcher

Runs every `watcher.interval`. Responsibilities:

- Fetch up to `limit` unmined shards and check them with the oracle, update status to "mined" when confirmed
- Rebroadcast shards older than `rebroadcast_after`
- Alert (log warnings) for shards older than `alert_after`
- Make up to `max_checks_per_tick` oracle calls per tick; old unknown shards cannot starve newer ones

### Minter

Runs every `minter.interval`. Responsibilities:

- Reconcile the toolbox treasury without blocking: take a snapshot of built-but-unsent splits on the first round of each lease (and after a failed PutShard or a timed-out Build), and finish it in the first round at least `reconcile_grace` after the snapshot's listing; unrecorded splits seen in both listings are aborted, recorded ones are sent (see [TREASURY.md](TREASURY.md#reconciliation))
- At the start of every round, top up the treasury's leaf-sized funding chunks (toolbox treasury)
- Build, sign and send new split transactions ("shards") to keep each pool's available rows near target; one split's Build + PutShard is bounded by `build_timeout`
- Stop minting when unmined shards exceed `max_pending_shards`
- Retire shards if the treasury rejects them (terminal failure)

## Adoption Behavior

The shard manager per instance (not the minter) owns shards and replenishes in-memory rows:

- Adopts one shard at a time when in-memory rows < `low_water_rows` and owned shards < `max_shards`, on its own goroutine so adoption never waits behind a rescan; at `max_shards` it rescans up to 2 drained shards (oldest first), keeps any whose rows are back, and releases one the store shows empty — retired outright if every row is terminal (`fuelkeeper_shard_drops_total{reason="retired"}`), otherwise excluded from re-adoption by this instance for 60 s (`reason="empty"`) — then adopts into the freed slots
- Every `rescan_interval` it reloads owned shards' rows; a shard that stays empty for two rescans is retired outright if every row is terminal (spent or invalid; no exclusion entry), otherwise released with the 60 s exclusion. An adoption that finds a shard empty applies the same check.
- A reserve request waits at most `instance.adopt_timeout` for adoption to complete; lease releases from that rotation run in the background (each bounded at 1 s) and are not charged to the request
- A crashed instance's shards are re-adopted by others after `instance.lease_ttl` expires

Retiring spent-out shards keeps a high-throughput instance from cycling
through hundreds of dead shards ahead of its live ones: `AcquireShard`
always tries the oldest adoptable shard first, and before this a spent-out
shard stayed `mined` and adoptable forever, with only the 60-second,
256-entry exclusion list above keeping adoption away from it -- once more
spent-out shards preceded the live ones than that list holds, an instance
could answer `exhausted` indefinitely even with fuel available further down
the queue. See `docs/ARCHITECTURE.md`'s "Shard statuses" and "Shard
manager" for the mechanism, and "Known Limits" below for what retirement
does not yet cover.

The metric `fuelkeeper_cas_missed_total` now counts only a genuine second
owner briefly touching a row (e.g. during a lease hand-over): a rescan's own
store listing can no longer re-add a row that a concurrent `Reserve` already
popped and applied, so it no longer includes those self-inflicted misses
(bracketed listing generations, `pkg/pool/allocator.go`). This makes it the
audit signal `docs/PERFORMANCE.md` relies on to confirm zero double
hand-outs during a benchmark run; it clears (stops incrementing) within one
rescan cycle once the overlap resolves.

## Known Limits

- **Adoption can still cycle through dead shards in one case.** Only a
  fully spent-out shard (every row terminal) is retired outright. A shard
  whose rows are merely *drained but not yet terminal* -- still reserved or
  quarantined, e.g. under very long reservation TTLs at a high reservation
  rate -- cannot be retired this way, only excluded from re-adoption for
  60 s at a time, and that exclusion list is capped at 256 entries (see
  "Adoption Behavior" above). If more than 256 such shards precede the live
  ones, adoption can still cycle indefinitely. The planned fix is a
  store-side `AcquireShard` that skips row-less shards server-side instead
  of relying on a bounded client-side exclusion list.
- **`/spend` is not coalesced.** Reserve requests from many concurrent
  callers are batched by the per-pool coalescer into one store write per
  flush (group-commit); `spend` and `release` are not -- `Pool.storeSpend`
  (`pkg/pool/service.go`) makes one store write per *request*, covering
  every outpoint that request names, but each caller's request is always
  its own store call, never merged with another caller's. So the
  spend:reserve write-transaction ratio is roughly the coalescer's batch
  size divided by `count` (outputs per reserve): a store write is cheaper
  per output on the reserve side whenever concurrent demand fills a batch
  bigger than `count`, and the two sides are roughly even otherwise. A
  write-bound store (see "Durability and Consistency" above) still pays
  for this per request, though sqlite and pebble's group-commit (concurrent
  writers sharing one `fsync` per flush) applies at the store layer to
  spend/release writes the same as to reserve writes, so the per-request
  cost is not one `fsync` per writer there either. Watch
  `fuelkeeper_coalescer_batch_size` against `count` to see which side is
  paying more per output in a given deployment, rather than assuming a
  fixed multiplier.
- **Abandoned reservations are released inside the reserve flush.** When
  many clients abandon in-flight reserves at once (client timeouts, a load
  generator stopping), the coalescer releases each abandoned request's rows
  with its own store `Release` -- one durable write each -- while holding
  the pool's single flush slot (`pkg/pool/coalescer.go` `deliver`/
  `releaseByToken`). On a store with a fsync-bound write path this can hold
  every new reserve for several seconds, so each waiting request fails with
  `unavailable` at `reserve_timeout`.

## Development Mode (Nullnet)

For testing and development, use the nullnet configuration:

```yaml
store: {backend: memory}
pools:
  - {name: main, denomination: 50, split_size: 100, target_available: 1000, low_water: 500, spend_policy: trust}
jobs:
  enabled: true
  lease_ttl: 10s
  reaper: {interval: 1s, grace: 2s, quarantine_min_age: 2s, min_unspent_age: 2s}
  watcher: {interval: 1s}
  minter: {interval: 2s, max_per_round: 5, max_pending_shards: 20}
oracle: {kind: mock, mock: {assume_unspent: true}}
treasury: {kind: mock, mock: {confirm_after: 500ms}}
api: {listen: "127.0.0.1:8080"}
log: {level: info}
```

Key differences from production:

- **Mock oracle** assumes unknown outputs are unspent (never to production)
- **Mock treasury** instantly "mines" shards after `confirm_after` (never to production)
- **Memory backend** loses all state on restart
- Shorter job intervals enable faster testing

## Teratestnet (Real Treasury)

`examples/teratestnet.yaml` runs the go-wallet-toolbox treasury on the public
Teranode test network with a composite (arcade + teranode) oracle and sqlite
storage. It needs `FUELKEEPER_TREASURY_KEY`, `FUELKEEPER_API_KEY` and
`FUELKEEPER_ADMIN_KEY` in the environment; the arcade and chaintracks URLs
default to the toolbox's public ttn endpoints. Fund the treasury with
`fuelkeeper fund` ([FUNDING.md](FUNDING.md)) before expecting fuel; until
then the minter logs "treasury cannot fund a split" each round. Key
material, chunk sizing, backups and reconciliation are in
[TREASURY.md](TREASURY.md).

**Side effects of the toolbox's dependencies at start-up.** The toolbox's
chaintracks client pulls in a settings package (`github.com/ordishs/gocore`)
that runs while the binary initialises, before FuelKeeper reads its config:

- It loads a `.env` file from the **working directory** into the process
  environment (existing variables are not overridden), or the file named by
  `SETTINGS_ENV_FILE`. A stray `.env` can therefore supply values for the
  `${VAR}` references in FuelKeeper's config, API keys included.
- It searches for `settings.conf`, `settings_test.conf` and
  `settings_local.conf` next to the binary and in its parent directories,
  then in the working directory and its parents, and prints
  `WARN: No config file 'settings.conf'` and
  `WARN: No local config file 'settings_local.conf'` when none is found.
  FuelKeeper never reads those settings, but any file found on that path is
  loaded into the settings package, so keep them out of the binary's and the
  working directory's ancestry.

In deployments, set `SETTINGS_ENV_FILE=/dev/null` (or any path that does not
exist) in the service environment, e.g. `Environment=SETTINGS_ENV_FILE=/dev/null`
in a systemd unit or `SETTINGS_ENV_FILE: /dev/null` under a compose
service's `environment:`, and run from a working directory that holds no
`.env` or `settings*.conf`. The two WARN lines are then harmless.

## What to Watch

Monitor these Prometheus metrics to stay on top of pool health:

| Metric | Type | Labels | Alert | Notes |
|--------|------|--------|-------|-------|
| `fuelkeeper_pool_rows` | gauge | pool, state (available\|reserved\|quarantined\|spent\|invalid) | available < low_water × 0.5? | Row count by state |
| `fuelkeeper_pool_shards` | gauge | pool, status (unmined\|mined\|retired\|invalid) | track | Shard count by status |
| `fuelkeeper_reserve_requests_total` | counter | pool, result (ok\|bad_request\|insufficient\|exhausted\|error\|canceled) | insufficient rate ↑? | Reserve attempts by outcome |
| `fuelkeeper_reserve_outputs_total` | counter | pool | track | Total outputs handed out (cumulative) |
| `fuelkeeper_reserve_latency_seconds` | histogram | pool | p99 > 100ms? | Reserve request latency |
| `fuelkeeper_cas_missed_total` | counter | pool | > 0? | A second owner touched the row (cross-instance overlap only -- self-inflicted rescan misses are no longer counted); the audit signal for zero double hand-outs, see [Adoption Behavior](#adoption-behavior) |
| `fuelkeeper_release_total` | counter | pool, result | track | Release calls by outcome |
| `fuelkeeper_spend_total` | counter | pool, result | track | Spend calls by outcome |
| `fuelkeeper_allocator_available` | gauge | pool | track | Rows queued in memory |
| `fuelkeeper_shards_owned` | gauge | pool | track | Shards leased by this instance |
| `fuelkeeper_coalescer_batch_size` | histogram | pool | track | Ops grouped into one coalescer flush |
| `fuelkeeper_coalescer_flush_latency_seconds` | histogram | pool | p99 high? | Time from the first attempt to the final outcome, including retry backoff |
| `fuelkeeper_coalescer_inflight_ops` | gauge | pool | near MaxInflightOps? | Ops queued plus ops currently in a flush |
| `fuelkeeper_cas_unknown_total` | counter | pool | > 0 sustained? | Ops whose Reserve outcome was unknown after retries were exhausted |
| `fuelkeeper_compensating_release_total` | counter | pool | track | Compensating Release calls issued by the coalescer after an in-doubt flush |
| `fuelkeeper_shard_adoptions_total` | counter | pool, result (ok\|none\|error) | error rate ↑? | AdoptOnce attempts by result |
| `fuelkeeper_shard_drops_total` | counter | pool, reason (not_owner\|retired\|empty\|stale\|closed) | not_owner/stale rate ↑? | Owned shards dropped, by reason |
| `fuelkeeper_lease_renew_failures_total` | counter | pool | > 0 sustained? | RenewShard calls that failed transiently (excludes lease-loss) |
| `fuelkeeper_store_op_latency_seconds` | histogram | pool, op (reserve\|release\|spend\|acquire_shard\|renew_shard\|release_shard\|list_available) | p99 high? | Store call latency observed at the pool's call sites |
| `fuelkeeper_quarantine_oldest_age_seconds` | gauge | pool | > 5 minutes? | Oldest stuck quarantined row |
| `fuelkeeper_watcher_unmined_shards` | gauge | pool | > limit × 0.8? | Current unmined shard count |
| `fuelkeeper_watcher_oldest_unmined_age_seconds` | gauge | pool | > 2 hours? | Age of oldest unmined shard |
| `fuelkeeper_watcher_transitions_total` | counter | pool, result | track | Shard state changes by outcome |
| `fuelkeeper_minter_splits_total` | counter | pool, result (ok\|build_failed\|record_failed\|rejected\|send_failed) | failed rate ↑? | Minted splits by outcome |
| `fuelkeeper_minter_last_success_timestamp` | gauge | pool | track | Unix time of last successful split |
| `fuelkeeper_pool_refilling` | gauge | pool | stuck at 1 for a long time? | 1 while the pool is mid-refill (sticky hysteresis, see `low_water`), 0 otherwise |
| `fuelkeeper_reaper_quarantined_total` | counter | pool | track | Rows moved to quarantine |
| `fuelkeeper_reaper_resolved_total` | counter | pool, result | track | Quarantined rows resolved by state |
| `fuelkeeper_reaper_requeued_total` | counter | pool, reason (unspent\|unknown_aged\|rejected\|hint_expired) | `unknown_aged`/`hint_expired` sustained? | Quarantined rows returned to available, by reason; see [Reaper metrics](#reaper-metrics) |
| `fuelkeeper_reaper_spent_total` | counter | pool, source (oracle\|hint\|competitor\|inputs_spent) | `competitor`/`inputs_spent` rate ↑? | Quarantined rows resolved to spent, by source; see [Reaper metrics](#reaper-metrics) |
| `fuelkeeper_reaper_held` | gauge | pool, reason (13 values; see [Reaper metrics](#reaper-metrics)) | `competitor_pending`/`spent_no_txid`/`conflicting`/`rejected_unknown` nonzero for a long time? | Quarantined rows left unresolved this tick, by reason |
| `fuelkeeper_oracle_requests_total` | counter | kind, result | track | Oracle calls by type and outcome |
| `fuelkeeper_job_lease_held` | gauge | job (reaper\|watcher\|minter) | max by (job) < 1 for > 1 min? | 1 if this instance holds the job lease, 0 otherwise |

`fuelkeeper_clock_skew_seconds` (spec) is deferred, not implemented: local clock vs. store `now()` would matter for lease takeover timing (a large skew could make a lease look expired, or not, earlier or later than the store believes), but `pkg/pool` has no independent server-clock-vs-store-clock notion yet to measure it from.

## Runbook

### Pool Exhausted (No Outputs Available)

**Symptom**: `reserve` returns 503 exhausted or 409 insufficient repeatedly.

**Diagnosis**:

1. Check `fuelkeeper_pool_rows{state="available"}` — is it zero or very low?
2. Check `fuelkeeper_watcher_unmined_shards` — are there many unmined shards waiting?
3. Check logs for minter failures or treasury rejections.

**Remedies**:

- Increase `split_size` or `target_available` (may require restart)
- Check treasury (BSV node) connectivity
- Temporarily disable `serve_policy: mined_only` to serve unmined shards (risky: unmined shards can be double-spent)
- Increase `jobs.minter.max_per_round` to mint faster

### Rows Stuck in Quarantine

**Symptom**: `fuelkeeper_quarantine_oldest_age_seconds` > 5 minutes, or
`fuelkeeper_reaper_held{reason=...}` nonzero for a long time.

With an arcade-only oracle (the expected shape), quarantine holding rows for
a while is normal, not a symptom on its own: an unhinted row waits up to
`unknown_requeue_age` (10m default) before it requeues itself, and a hinted
row can wait up to `hint_max_wait` (24h default) before it does. Only treat
this as an incident once rows sit well past those windows, or
`fuelkeeper_reaper_held` is nonzero under a reason the reaper cannot clear on
its own (`spent_no_txid`, `conflicting`, `rejected_unknown`, or
`expiry_hold` under a pool configured `expiry_policy: hold`).

**Diagnosis**:

1. `fuelkeeper quarantine list --url http://host:8080 --pool main --key <key>` -- each row shows `outpoint`, `state`, `quarantined_at` and, when hinted, `spent_hint`. The list itself does not carry a held *reason*; cross-reference age and whether `spent_hint` is set against the `fuelkeeper_reaper_held{pool,reason}` gauge (broken down by reason, not by row) and the [Reaper lifecycle](#reaper-lifecycle-in-operator-terms) table above to work out which reason applies to a given row: a set `spent_hint` means one of the hinted-row reasons, an unset one means `unknown_fresh`/`expiry_hold`/`spent_no_txid`/`conflicting`/`oracle_down`.
2. Manually verify on-chain whether listed outpoints are spent or unspent

**Remedies**:

- If truly unspent and the row has already aged past what the automatic rules would need (i.e. this is an operator-held reason, not one that is still waiting out a delay), requeue with `fuelkeeper quarantine requeue --url ... --pool main --outpoints a:0,b:1 --key <key>`
- If spent (expected), no action needed; unhinted rows age out to `spent` once the oracle proves it, hinted rows once `Tx(hint)` (or a competitor) is mined
- **Never hand-requeue a row held as `competitor_pending`**: a double-spend race is in progress and the row will resolve to `spent{source="competitor"}` once a competitor is mined; requeuing it early can hand an about-to-be-spent output back to the pool (a poison pill for whoever reserves it next). Left alone, it does not stay quarantined forever either: with no competitor ever mined, it requeues itself after `hint_max_wait` (24h default) like any other unresolved hinted row
- **Never try to "undo" a row retired as `spent{source="inputs_spent"}`**: it is already in the terminal `spent` state, not quarantined. The reaper checks whether the gateway named which input was spent (`oracle.TxStatus.SpentInputs`, parsed from arcade's `UTXO_SPENT` line) *before* it ever looks at mined competitors: if it names this row's outpoint, the retirement is confirmed, resolved in that one tick; if `SpentInputs` excludes it, the reaper requeues the row automatically instead (reason `rejected`) -- no operator action needed either way. Only when the gateway names no spent input at all (`SpentInputs` nil, and no mined competitor excludes this outpoint) does the reaper fall back to the bounded-loss assumption and retire the row without confirmation; trust that classification rather than trying to recover the row
- Verify on-chain before requeuing; do not requeue spent rows

### Dead Split (Known-Unminable Shard)

**Symptom**: Operator discovers a shard that will never mine (double-spend, bad nLockTime, etc.).

**Remedy**:

1. Verify it's dead on-chain
2. Retire it: `fuelkeeper quarantine retire --url http://host:8080 --pool main --txid <txid> --key <key>`
3. This marks all outputs as invalid and prevents re-handing them

### Graceful Restart

FuelKeeper handles SIGTERM gracefully with a fixed 10-second shutdown timeout:

1. Stops accepting new `/v1/` requests (logs: "shutting down")
2. Drains in-flight reserves (waits up to 10 seconds)
3. Stops pools, releasing shard leases (logs: "http drained", "pools stopped")
4. Stops jobs, releasing job leases (logs: "jobs stopped")
5. Closes the store (logs: "store closed")

A second SIGINT/SIGTERM forces exit immediately.

In-flight reservations live in the store and survive the restart.

To restart:

```bash
# Gracefully shut down
kill -TERM <pid>

# Wait for shutdown to complete (logs should show "store closed")
sleep 12

# Restart
fuelkeeper serve --config fuelkeeper.yaml &
```

## Test Knobs

Environment variables for testing:

- `FUELKEEPER_STRESS=N` — Run invariant tests N times instead of 1 (default: 1)
- `FUELKEEPER_SEED=S` — Replay a specific invariant test run (seed is logged at test start)

Example:

```bash
FUELKEEPER_STRESS=100 go test -race ./pkg/pool
FUELKEEPER_SEED=12345 go test -race ./pkg/pool
```

## Troubleshooting Common Issues

### High Latency on `reserve`

Check:

- `fuelkeeper_reserve_latency_seconds{pool="..."}` histogram (p50, p99)
- Are there many CAS misses? (`fuelkeeper_cas_missed_total` rising)
- Is the store slow (Postgres connection pool full, SQLite contention)?
- Are jobs consuming CPU (high minter or reaper load)?

### "Lease already held by ..." (Job Can't Acquire Lease)

- Check logs for the instance name that is holding the lease
- Verify that instance is still running and healthy
- If stuck, wait `jobs.lease_ttl` for it to auto-release (or restart)

### Store Connection Errors

**Postgres**: Check DSN, credentials, network, and `synchronous_commit` setting.

**SQLite**: Check file permissions, disk space, and ensure only one process is writing.

**Pebble**: Check directory permissions and disk space, and ensure only one process opens `pebble.path` at a time.

**MongoDB**: Check the URI, credentials, and network; if writes hang or time out, `op_timeout` may be too low for a cross-region majority write to replicate in time — raise it.

**Aerospike**: Check `hosts` and network; a refusal to start naming "strong-consistency" means the namespace is not configured for SC — either fix the namespace (production) or set `allow_availability_mode: true` (dev/test only). A `key_prefix` rejected as invalid must be valid UTF-8, at most 64 bytes, with no `|` or NUL byte.

### Verify Policies Without Oracle

Pools with `spend_policy: verify` need an oracle that is not "none"; FuelKeeper will refuse to start with error `"spend_policy=verify requires an oracle"`.

Similarly, `release_policy: verify` requires an oracle with error `"release_policy=verify requires an oracle"`.

Remedy: set `oracle.kind: arcade` (or `composite`, with or without `teranode_url`) -- an arcade-only oracle now accepts both policies (see [Oracle Kinds and Policies](#oracle-kinds-and-policies)): `release_policy: verify` becomes a delayed release and `spend_policy: verify` resolves hinted rows from the client's own transaction, both requeued/resolved automatically by the reaper. Add `oracle.teranode_url` to resolve those rows immediately instead of waiting out the delay. `mock` with `assume_unspent: true` also works for nullnet, or change the policies to "trust" if verification is not wanted at all.

### Configuration Validation Errors

- **Pool name validation**: pool names must match `^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$` (alphanumeric start, then alphanumeric/underscore/dot/dash, max 64 chars)
- **Empty key strings rejected**: an empty string inside `api_keys` or `admin_keys` (for example from `${KEY:-}`) is a configuration error; leaving the list empty is allowed only on a loopback listen, or anywhere with `insecure: true`
- **Non-loopback requires insecure**: if `api.listen` is not loopback (127.0.0.1 or ::1), you must set `api.insecure: true` to run without API keys
- **SQLite/Postgres DSN required**: `store.sqlite.path` or `store.postgres.dsn` must be set when using those backends
- **Pebble/MongoDB/Aerospike fields required**: `store.pebble.path`; `store.mongodb.uri` and `store.mongodb.database`; `store.aerospike.hosts` (at least one) and `store.aerospike.namespace` must be set when using those backends
- **Postgres synchronous_commit**: Postgres checks `synchronous_commit` at connect time and rejects `off` with an error
- **Aerospike strong consistency**: Aerospike checks every node's namespace `strong-consistency` info property at connect time and refuses to start unless it is true, unless `store.aerospike.allow_availability_mode: true` (dev/test only)
