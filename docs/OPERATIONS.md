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
  rescan_interval: 5s          # How often to rescan the store for new shards; default 5s
  reserve_timeout: 2s          # Max time a single reserve request can take; default 2s
  flush_window: 0s             # Extra time the coalescer waits to batch concurrent Reserve calls into one store write; default 0 (flush as soon as a slot is free)
  max_batch: 0                 # Max rows to batch before flushing; default 0 (no limit)
  flush_timeout: 5s            # Max time to hold a partial batch before flushing; default 5s
  adopt_timeout: 200ms         # Max time a reserve request waits for shard adoption; default 200ms

# Store backend (memory, sqlite, postgres; pebble/mongodb/aerospike in Plan 4)
store:
  backend: memory              # Required: memory | sqlite | postgres
  sqlite:
    path: /var/lib/fuelkeeper/fuel.sqlite  # Required when backend: sqlite
    sync: FULL                 # Optional: FULL (default, fsyncs every commit) | NORMAL (faster, less durable)
  postgres:
    dsn: postgres://user:pass@localhost/fuel  # Required when backend: postgres
    # Note: synchronous_commit must NOT be 'off'; on, local, remote_write, remote_apply are accepted

# Pools: fixed-denomination fuel outputs
pools:
  - name: main                       # Pool name; used in reserve/spend/release endpoints
    denomination: 50                 # Satoshis per output; must be > 0
    lock_mode: puzzle                # puzzle (default) | p2pkh; unlock mode
    split_size: 500                  # Outputs per split transaction (shard); default 500; must be 1..10000
    target_available: 100000         # Target number of available rows to maintain; default 100000
    low_water: 60000                 # Minter refill trigger: mint when rows < this; 0 = always refill below target; default 60000
    serve_policy: mined_only         # mined_only (default) | any; return only mined splits or any splits
    default_ttl: 2m                  # Reservation TTL when client doesn't specify; default 2m
    min_ttl: 30s                     # Minimum allowed TTL; default 30s
    max_ttl: 10m                     # Maximum allowed TTL; default 10m
    max_count: 100                   # Max outputs per reserve request; default 100
    release_policy: trust            # trust (default) | verify; verify released outputs via oracle
    spend_policy: trust              # trust | verify; default verify when the oracle answers outpoint queries
                                     #   (mock, teranode, composite with teranode_url), else trust; see "Oracle kinds and policies"
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
    hint_patience: 10m               # Quarantine age before a hinted Unspent row is requeued; default 10m
    batch: 500                       # ListQuarantined page size; default 500
    max_pages: 10                    # Max ListQuarantined pages per tick; default 10
    quarantine_batch: 1000           # Rows moved per Store.Quarantine call; default 1000
    oracle_batch: 200                # Outpoints per oracle call; default 200

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
  kind: none                         # none (default) | mock | arcade | teranode | composite
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

- Postgres requires `synchronous_commit` not set to `off` (checked at connect time)
- SQLite defaults to `synchronous=FULL` (fsync on every commit); set `sync: NORMAL` to opt out
- `jobs.lease_ttl` must exceed twice the longest job interval (e.g., if minter.interval is 30s, lease_ttl must be > 60s)
- `jobs.watcher.limit` must exceed `jobs.minter.max_pending_shards`
- Mint capacity must be ≥ 1.2× `expected_tps` for each pool (when expected_tps > 0)
- Pools with `spend_policy: verify` or `release_policy: verify` require an oracle (`oracle.kind` != "none")
- `release_policy: verify` requires an oracle that answers outpoint queries (`teranode`, `composite` with `teranode_url`, or `mock`); `spend_policy` defaults to `verify` only for such an oracle and to `trust` otherwise
- `oracle.kind: arcade` and `composite` need `oracle.arcade_url` (or a toolbox treasury, whose arcade they then use); `teranode` needs `oracle.teranode_url`
- `treasury.kind: toolbox` needs a valid `root_key_hex` (plus `arcade_url` and `chaintracks_url` on `test`/`tstn`; `main` and `ttn` default to the toolbox's public endpoints), and every pool must share one leaf cost class (same `split_size` × `denomination`, same leaf size): one treasury keeps one chunk denomination (see [TREASURY.md](TREASURY.md))
- `treasury.toolbox.reconcile_grace` must exceed `jobs.minter.build_timeout` + 10 s (the reconcile budget), or Reconcile could abort a split whose Build and PutShard were merely slow
- With the toolbox treasury and jobs enabled, `jobs.lease_ttl` must exceed `jobs.minter.build_timeout` + 20 s (two 10 s reconcile phases), the longest minter round the lease has to outlast

**Config keys changed with the toolbox treasury.** The oracle URLs moved from
nested blocks to flat keys: `oracle.arcade.url` is now `oracle.arcade_url`
and `oracle.teranode.url` is now `oracle.teranode_url`. The old keys are
rejected as unknown fields. `oracle.mock.assume_unspent` and
`treasury.mock.confirm_after` are unchanged.

### Oracle Kinds and Policies

The reaper settles quarantined rows with two oracle questions: *is this
outpoint spent?* (`Outpoints`) and *is this txid mined, and what does it
spend?* (`Tx`, plus the inputs lookup). Not every oracle answers both:

| `oracle.kind` | Outpoints | Mined status | Spender inputs |
|---|---|---|---|
| `none` | never (Unknown) | never | no |
| `mock` | nullnet only | nullnet only | as configured |
| `arcade` | never (Unknown) | yes | no |
| `composite` without `teranode_url` | never (Unknown) | yes (arcade) | no |
| `composite` with `teranode_url` | yes (teranode) | yes (arcade) | yes (teranode raw tx, fetched only by the reaper) |
| `teranode` | yes | **never** | yes; **rejected by config when jobs are enabled** |

What that means for rows (and shards):

| Situation | Needs | none / arcade / composite without teranode | composite with teranode | teranode alone |
|---|---|---|---|---|
| Expired reservation (every policy) | an Unspent outpoint verdict | stays quarantined until requeued by hand (`fuelkeeper quarantine requeue`) | requeued after `min_unspent_age` | requeued |
| Release, `release_policy: trust` | nothing | available at once | available at once | available at once |
| Release, `release_policy: verify` | an Unspent outpoint verdict | **rejected by config** | requeued after `min_unspent_age` | requeued |
| Spend, `spend_policy: trust` | nothing | spent at once | spent at once | spent at once |
| Spend, `spend_policy: verify` | the hinted txid mined, or a Spent verdict | resolved once the hinted txid is mined, **with no input check**: a client hint naming *any* mined txid is trusted; a false hint leaves the row quarantined (none: always quarantined; config rejects verify with none) | resolved once mined, and only if the mined spender's inputs include the outpoint | stays quarantined: teranode never reports Mined |
| Split shards (watcher) | Mined status | never mined with `none` (use the mock treasury); mined via arcade | mined via arcade | **never mined**, so config rejects `oracle.kind: teranode` whenever jobs are enabled: "oracle.kind teranode cannot report transactions as mined ...; use kind: composite with arcade_url and teranode_url" |

So: use `composite` with `teranode_url` for production. An arcade-only
oracle (`arcade`, or `composite` without `teranode_url`) is only suitable
with `release_policy: trust` and `spend_policy: trust` (the defaults it
gets), and even then expired reservations accumulate in quarantine until an
operator requeues them. Config rejects `release_policy: verify` with such an
oracle, and `serve` logs a Warn for `spend_policy: verify` with one.

## Deployment Topologies

### Single-Process (SQLite or memory)

- One process per database
- SQLite uses a write lock; a second writer waits for `busy_timeout` (5s)
- Memory backend: all state lost on restart; for development and testing only

### Multi-Process (Postgres)

- Multiple processes share the same Postgres database
- Each instance acquires a lease on whole shards and job responsibilities
- Shards from a crashed instance are re-adopted after `instance.lease_ttl`
- Every store operation is a compare-and-set; no pessimistic locking
- With `treasury.kind: toolbox`, every instance must use the same treasury storage: one postgres database in `treasury.toolbox.storage` and the same root key. The minter runs wherever the lease is and funding is served by whichever instance a request reaches, so with per-instance sqlite files a payment can land in a wallet the minter never spends (see [TREASURY.md](TREASURY.md#several-instances))

### Benchmark cluster

`deploy/docker-compose.bench.yaml` runs three `fuelkeeper` instances (`fk1`-`fk3`, built from the repo `Dockerfile`) sharing one `postgres:16-alpine` store, all driven by `deploy/fuelkeeper-bench.yaml` (mock oracle and treasury, so throughput measures the store/pool path, not chain latency). It is a separate compose project (`name: fuelkeeper-bench`) from `deploy/docker-compose.yaml`'s dev Postgres: its own network, and host ports 8081-8083 only (the bench Postgres exposes no host port, unlike the dev one's 15432).

```bash
make bench-cluster-up                     # build + start fk1..fk3 + postgres, wait for healthy
make bench-run                            # cmd/fuelbench against :8081-8083; WORKERS/COUNT/DURATION/LABEL/OUT override
make bench-cluster-down                   # stop and remove volumes
```

`bench-run`'s `OUT` defaults to `/tmp/fuelbench-last.json` (`docs/benchmarks/` does not exist in this tree). Give the minter a few seconds after `bench-cluster-up` to fill `pools[0].available` toward `target_available` before a long run, or lower `target_available` in the bench config for a quick smoke test; a handful of `exhausted` errors right after start-up, before the mock treasury's first splits are mined, is expected and not a bug. Every service sets `SETTINGS_ENV_FILE=/dev/null` (see "Teratestnet" above) so the toolbox's settings package does not pick up a stray `.env`/`settings.conf`.

## Durability and Consistency

**Correctness guarantee G1** (no fuel output is ever handed to two live reservations) depends on durability:

- **Postgres**: Enable `synchronous_commit` (not `off`). On, local, remote_write, or remote_apply all provide durable commits.
- **SQLite**: Default `synchronous=FULL` fsyncs every commit. Opt-in `sync: NORMAL` trades durability for speed (on power loss, may forget the last reservation).

## Jobs and Leases

Three background jobs run under store leases for coordination. The `instance.lease_ttl` is separate and manages shard ownership: a crashed instance's shards are re-adopted by others after `instance.lease_ttl` expires (must exceed `instance.rescan_interval`).

When jobs are enabled, `jobs.lease_ttl` coordinates which instance runs each job; it must exceed twice the longest job interval to avoid rapid leader flaps.

### Reaper

Runs every `reaper.interval`. Responsibilities:

- Expire old reservations (`>= ttl_seconds` old) and move them to quarantine
- Move quarantined rows to "spent" only after the oracle confirms a MINED spend
- Requeue rows marked Unspent by the oracle (after `min_unspent_age` for unhinted rows, or `hint_patience` for hinted rows)

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

- Adopts one shard at a time when in-memory rows < `low_water_rows` and owned shards < `max_shards`
- A reserve request waits at most `instance.adopt_timeout` for adoption to complete
- A crashed instance's shards are re-adopted by others after `instance.lease_ttl` expires

The metric `fuelkeeper_cas_missed_total` counts brief moments when two instances briefly serve the same shard; it clears within one rescan cycle.

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
| `fuelkeeper_cas_missed_total` | counter | pool | > 0? | Concurrent shard ownership (clears within rescan) |
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
| `fuelkeeper_reaper_quarantined_total` | counter | pool | track | Rows moved to quarantine |
| `fuelkeeper_reaper_resolved_total` | counter | pool, result | track | Quarantined rows resolved by state |
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

**Symptom**: `fuelkeeper_quarantine_oldest_age_seconds` > 5 minutes.

**Diagnosis**:

1. `fuelkeeper quarantine list --url http://host:8080 --pool main --key <key>`
2. Manually verify on-chain whether listed outpoints are spent or unspent

**Remedies**:

- If truly unspent, requeue with `fuelkeeper quarantine requeue --url ... --pool main --outpoints a:0,b:1 --key <key>`
- If spent (expected), no action needed; they will age out when `min_unspent_age` expires
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

### Verify Policies Without Oracle

Pools with `spend_policy: verify` need an oracle that is not "none"; FuelKeeper will refuse to start with error `"spend_policy=verify requires an oracle"`.

Similarly, `release_policy: verify` requires an oracle with error `"release_policy=verify requires an oracle"`.

Remedy: set `oracle.kind: composite` with `oracle.teranode_url` (arcade alone cannot answer outpoint queries, so `release_policy: verify` is rejected with it and `spend_policy: verify` trusts any mined hint; see [Oracle Kinds and Policies](#oracle-kinds-and-policies)), use `mock` with `assume_unspent: true` for nullnet, or change the policies to "trust".

### Configuration Validation Errors

- **Pool name validation**: pool names must match `^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$` (alphanumeric start, then alphanumeric/underscore/dot/dash, max 64 chars)
- **Empty key strings rejected**: an empty string inside `api_keys` or `admin_keys` (for example from `${KEY:-}`) is a configuration error; leaving the list empty is allowed only on a loopback listen, or anywhere with `insecure: true`
- **Non-loopback requires insecure**: if `api.listen` is not loopback (127.0.0.1 or ::1), you must set `api.insecure: true` to run without API keys
- **SQLite/Postgres DSN required**: `store.sqlite.path` or `store.postgres.dsn` must be set when using those backends
- **Postgres synchronous_commit**: Postgres checks `synchronous_commit` at connect time and rejects `off` with an error
