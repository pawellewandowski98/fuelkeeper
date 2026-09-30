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
  low_water_rows: 5000         # Adoption threshold: adopt a new shard when in-memory rows < this; default 5000
  lease_ttl: 30s               # Duration the instance holds its shard leases; default 30s; must be > longest job interval
  rescan_interval: 5s          # How often to rescan the store for new shards; default 5s
  reserve_timeout: 2s          # Max time a single reserve request can take; default 2s

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
    low_water: 60000                 # Adoption trigger: mint when rows < this; 0 = always refill below target; default 60000
    serve_policy: mined_only         # mined_only (default) | any; return only mined splits or any splits
    default_ttl: 2m                  # Reservation TTL when client doesn't specify; default 2m
    min_ttl: 30s                     # Minimum allowed TTL; default 30s
    max_ttl: 10m                     # Maximum allowed TTL; default 10m
    max_count: 100                   # Max outputs per reserve request; default 100
    release_policy: trust            # trust (default) | verify; verify released outputs via oracle
    spend_policy: verify             # trust | verify (default unless oracle.kind: none); verify spent outputs via oracle
    expected_tps: 0                  # Expected transactions per second (for capacity validation); no check if 0

# Background jobs (disabled by default in development)
jobs:
  enabled: true                      # Run jobs or not
  lease_ttl: 90s                     # Lease TTL for job coordination; must be > 2× longest job interval
  
  reaper:
    interval: 1s                     # How often to scan for expired/quarantined rows; default 1s
    grace: 10s                       # Grace period before retiring an expired reservation; default 10s
    quarantine_min_age: 30s          # Min age before a quarantined row can be verified; default 30s
    min_unspent_age: 30s             # Min time a row must be "unspent" before being re-handed out; default 30s
    hint_patience: 10m               # How long to trust a "spent" hint before re-checking; default 10m
    batch: 500                       # Max rows per reaper query batch; default 500
    max_pages: 10                    # Max store pages to scan per run; default 10
    quarantine_batch: 1000           # Max quarantined rows per oracle verification batch; default 1000
    oracle_batch: 200                # Max rows to send to oracle in one request; default 200

  watcher:
    interval: 10s                    # How often to check unmined shards; default 10s
    rebroadcast_after: 5m            # Rebroadcast unmined splits after this time; default 5m
    alert_after: 6h                  # Log alerts for unmined splits after this time; default 6h
    limit: 5000                      # Max unmined shards to track; default 5000; must exceed jobs.minter.max_pending_shards
    max_checks_per_tick: 500         # Max shards to oracle-check per interval; default 500

  minter:
    interval: 30s                    # How often to mint new shards; default 30s
    max_per_round: 20                # Max new shards per minting round; default 20
    max_pending_shards: 50           # Max unmined shards before minting pauses; default 50

# Chain oracle (verifies spend and unspent states)
oracle:
  kind: none                         # none | mock | teranode | arcade | composite (Plan 5+)
  mock:
    assume_unspent: true             # When true, assume unknown outpoints are unspent (nullnet only)
  # teranode, arcade fields come in Plan 5

# Treasury (produces split transactions)
treasury:
  kind: mock                         # mock (nullnet only) | toolbox (Plan 5)
  mock:
    confirm_after: 2s                # How long after send to mark splits mined (nullnet only)

# HTTP API listener
api:
  listen: ":8080"                    # Host:port to bind; default :8080
  api_keys: []                       # Bearer tokens for /v1/ endpoints; empty = open (dev only)
  admin_keys: []                     # Bearer tokens for admin endpoints; empty = use fuel keys
  read_timeout: 10s                  # HTTP read timeout; default 10s
  write_timeout: 30s                 # HTTP write timeout; default 30s
  idle_timeout: 120s                 # HTTP idle keep-alive timeout; default 120s
  max_body_bytes: 1048576            # Max request/response body size (1 MiB default)
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

## Durability and Consistency

**Correctness guarantee G1** (no fuel output is ever handed to two live reservations) depends on durability:

- **Postgres**: Enable `synchronous_commit` (not `off`). On, local, remote_write, or remote_apply all provide durable commits.
- **SQLite**: Default `synchronous=FULL` fsyncs every commit. Opt-in `sync: NORMAL` trades durability for speed (on power loss, may forget the last reservation).

## Jobs and Leases

Three background jobs run under store leases for coordination:

### Reaper

Runs every `reaper.interval`. Responsibilities:

- Expire old reservations (`>= ttl_seconds` old) and move them to quarantine
- Move old quarantined rows to "spent" if the oracle confirms they are spent
- Move very old quarantined rows back to "available" (safety fallback when oracle is unavailable)

### Watcher

Runs every `watcher.interval`. Responsibilities:

- Check unmined shards with the oracle, update status to "mined" when confirmed
- Rebroadcast shards older than `rebroadcast_after`
- Alert (log warnings) for shards older than `alert_after`
- Ensure the unmined shard count stays below `limit`

### Minter

Runs every `minter.interval`. Responsibilities:

- Build, sign and send new split transactions ("shards") to keep each pool's available rows near target
- Stop minting when unmined shards exceed `max_pending_shards`
- Retire shards if the treasury rejects them (terminal failure)

All jobs are leased to one instance at a time. The lease TTL must exceed the longest job interval plus its runtime, or leadership flaps. Overlapping runs during hand-over are harmless because every job write is a compare-and-set.

## Adoption Behavior

An instance adopts shards to replenish in-memory rows:

- Adopts one shard at a time when in-memory rows < `low_water_rows` and owned shards < `max_shards`
- The adoption slot is shared between background feeding (minter) and on-demand reserve-path adoption
- A reserve request waits at most `instance.adopt_timeout` for the adoption to complete
- A crashed instance's shards are re-adopted by others after `instance.lease_ttl` expires

The metric `fuelkeeper_cas_missed_total` counts brief moments when two instances are serving the same shard; it clears within one rescan cycle.

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

## What to Watch

Monitor these Prometheus metrics to stay on top of pool health:

| Metric | Type | Labels | Alert | Notes |
|--------|------|--------|-------|-------|
| `fuelkeeper_pool_rows` | gauge | state (available\|reserved\|quarantined\|spent\|invalid) | rows < low_water × 0.5? | Available output count by state |
| `fuelkeeper_reserve_requests_total` | counter | result (ok\|insufficient\|exhausted\|error) | insufficient rate ↑? | Reserve success and failure counts |
| `fuelkeeper_cas_missed_total` | counter | pool | > 0? | Concurrent shard ownership (clears quickly) |
| `fuelkeeper_quarantine_oldest_age_seconds` | gauge | pool | > 5 minutes? | Oldest stuck quarantined row (indicates oracle or manual action needed) |
| `fuelkeeper_watcher_oldest_unmined_age_seconds` | gauge | pool | > 2 hours? | Oldest unmined shard (may need to retire manually) |
| `fuelkeeper_watcher_unmined_shards` | gauge | pool | > limit × 0.8? | Current unmined shard count |
| `fuelkeeper_minter_splits_total` | counter | result (ok\|failed) | failed rate ↑? | Minted splits by outcome |
| `fuelkeeper_job_lease_held` | gauge | job (reaper\|watcher\|minter) | any ≤ 0 for > 1 min? | Job lease indicator (0 = not held, 1 = held) |
| `fuelkeeper_reserve_latency_seconds` | histogram | pool | p99 > 100ms? | Reserve request latency |
| `fuelkeeper_allocator_available` | gauge | pool | track for trends | Total available rows in allocator (in-memory) |
| `fuelkeeper_shards_owned` | gauge | pool | track for trends | Shards owned by this instance |

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
- If oracle is down, set `oracle.kind: mock` temporarily and requeue

### Dead Split (Known-Unminable Shard)

**Symptom**: Operator discovers a shard that will never mine (double-spend, bad nLockTime, etc.).

**Remedy**:

1. Verify it's dead on-chain
2. Retire it: `fuelkeeper quarantine retire --url http://host:8080 --pool main --txid <txid> --key <key>`
3. This marks all outputs as invalid and prevents re-handing them

### Graceful Restart

FuelKeeper handles SIGTERM gracefully:

1. Stops accepting new `/v1/` requests
2. Waits for in-flight reserves to complete (up to `instance.reserve_timeout`)
3. Releases shard leases (shard adoption can restart on another instance)
4. Releases job leases (jobs can resume on another instance)
5. Closes the store

In-flight reservations live in the store and survive the restart.

To restart:

```bash
# Gracefully shut down
kill -TERM <pid>

# Wait for shutdown to complete (logs: "... signaled to shut down")
sleep 5

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

### "verify policies without an oracle" Error

Pools with `spend_policy: verify` or `release_policy: verify` need an oracle. Set `oracle.kind` to something other than "none" or change the policies to "trust".
