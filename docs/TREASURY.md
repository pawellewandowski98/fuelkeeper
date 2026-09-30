# The Toolbox Treasury

`treasury.kind: toolbox` mints fuel from an embedded go-wallet-toolbox
wallet (`pkg/treasury/toolbox`). This guide covers how it is wired, how it
sizes its coins, what it stores, how it recovers from a crash, and how to
verify it on teratestnet. Funding the wallet is in [FUNDING.md](FUNDING.md);
the configuration reference is in [OPERATIONS.md](OPERATIONS.md).

## What Runs

`toolbox.Open` builds, in the order of the toolbox's own server:

1. **Services** (`defs.DefaultServicesConfig(network)`) with the configured
   arcade URL (broadcast, status, SSE events) and chaintracks URL (block
   headers for SPV), both enabled. On `main` and `ttn`, an empty
   `arcade_url` or `chaintracks_url` takes the toolbox's public default
   (`defs.ArcadeURL` / `defs.ArcadeTTNURL` and that host's `/chaintracks`);
   `test` and `tstn` need both set. An `arcade` or `composite` oracle with
   no `oracle.arcade_url` uses the treasury's. The arcade health probe and the
   chaintracks subscriptions start in the background.
2. **Storage**: a GORM provider on sqlite or postgres, migrated under the
   name `fuelkeeper-treasury`, with the throughput UTXO strategy (below), the
   resolved fee model and the toolbox's background broadcaster.
3. **Wallet** over the root key and that storage. Known-txid shortcuts are
   off, so every split's BEEF carries full ancestry.
4. **Monitor daemon** with the toolbox's default tasks: `check_for_proofs`
   (1 min), `send_waiting` (5 min, also at start), `fail_abandoned` (5 min;
   it fails unsigned/unprocessed actions, never a no-send split) and
   `un_fail` (10 min), plus the arcade SSE status stream and chaintracks
   reorg/tip events. The daemon holds per-task leases in the wallet
   database, so FuelKeeper processes sharing one postgres treasury (see
   "Several Instances") run each task once.
5. **Chunk keeper** (`pkg/wallet/fuelkeeper.Keeper`), run once per minter
   round.

`Close` stops the monitor, cancels the background services, closes the
wallet and stops the storage (background broadcaster, database). `serve`
closes the treasury after the jobs stop and before FuelKeeper's store
closes.

## Two-Level Chunking

A leaf split turns one exact-value coin into `split_size` fuel outputs. The
toolbox keeps such coins ready in two levels:

```
default basket (funding, change)
   └─ chunk fan-out ─> reserve basket: reserve chunks of M × ChunkValue + keeper headroom
         └─ leaf fan-out ─> chunk basket: M chunks of exactly ChunkValue
               └─ Build (one leaf split) ─> split_size fuel outputs of denomination
```

- **LeafCost** is what one split needs: `split_size × denomination` plus the
  split's fee at the resolved rate. The split is one P2PKH input,
  `split_size` fuel outputs (44 B puzzle, 34 B P2PKH), one change output and
  the fixed fields (`toolbox.LeafSizeBytes`).
- **ChunkValue** is `LeafCost + chunking.headroom_sats` (default 20): the
  surplus of a split stays below dust, so it is absorbed as fee instead of
  creating a change output.
- `M` is `chunking.fanout_outputs_per_tx` (default 100). A reserve chunk
  holds `M × ChunkValue` plus the keeper's leaf fan-out fee allowance: the
  larger of `max(1000, 8 × ChunkValue)` (the toolbox's `FromThroughput`
  default) and twice the fee of a leaf fan-out (one input, `M` + 1 P2PKH
  outputs) at the resolved rate. What the fan-out fee does not use returns
  to the default basket as change.
- The keeper refills the chunk basket to `chunking.target_chunks` (default
  50) once it drops below `low_water_percent` (default 60) of it, making up
  to 100 leaf fan-outs per round.

Numbers (puzzle lock mode):

| Pool | Leaf size | Fee rate | LeafCost | ChunkValue |
|------|-----------|----------|----------|------------|
| denomination 50, split_size 50 | 2 392 B | 1 sat/kB | 2 503 | 2 523 |
| denomination 50, split_size 50 | 2 392 B | 100 sat/kB | 2 740 | 2 760 |
| denomination 50, split_size 500 | 22 194 B | 1 sat/kB | 25 023 | 25 043 |
| denomination 50, split_size 500 | 22 194 B | 100 sat/kB | 27 220 | 27 240 |

At 100 sat/kB with the 50 × 50 pool and M = 100, one reserve chunk is
`100 × 2 760 + 22 080 = 298 080` sat and yields 100 splits, i.e. 5 000 fuel
outputs. `target_chunks: 50` keeps about 138 000 sat ready in the chunk
basket.

**One leaf cost class per treasury.** The throughput strategy keeps one
exact denomination per wallet storage, so every pool of a FuelKeeper that
uses the toolbox treasury must have the same `split_size × denomination`
and the same leaf size (same `split_size` and lock mode). `config` refuses
anything else. Run one FuelKeeper (with its own treasury storage) per class.

## Fee Rate

`fee_sat_per_kb: 0` (default) reads arcade's `GET /policy` once at start and
uses its mining fee, rounded up per kB, with a floor of 1. A non-zero value
skips the lookup. The rate fixes ChunkValue, so a change needs a restart,
and chunks already made at the old rate stay at the old value: after
changing the rate (or anything else that changes ChunkValue), drain or
consolidate the old chunk basket, or start on a new storage.

## Key Material

- **Root key**: `root_key_hex`, 64 hex characters, normally
  `${FUELKEEPER_TREASURY_KEY}` in the config. It is the wallet's identity:
  the identity key payers see in `GET /v1/funding` is its public key, and
  every other key derives from it. It is redacted from every config print
  and log line; keep it in a secret store.
- **Per-pool P2PKH keys**: `lock_mode: p2pkh` fuel is locked to a key
  derived with BRC-42 under protocol `fuelkeeper` (security level 2), key id
  `pool <name>`, counterparty self. It is deterministic, so a restarted
  treasury presigns to the same lock; the presigned unlocking scripts are
  bearer secrets stored in FuelKeeper's store, not the treasury's.
- **Funding nonces**: BRC-29 derivation prefixes are go-sdk HMAC nonces
  (protocol `server hmac`, counterparty self): they survive restarts and are
  not one-shot.
- **Arcade callback token**: derived from the identity key
  (`wdk.DeriveArcadeCallbackToken`), so it is stable across restarts. Arcade
  uses it to scope the SSE status stream to this wallet, and the monitor
  resumes the stream from a cursor stored in the wallet database.

## What the Treasury Storage Holds

The toolbox database (`storage.sqlite_path` or the postgres database) holds
the wallet: its transactions and actions (every split is labelled
`fuelkeeper`, `fuel-split` and `pool:<name>`; funding payments `funding`),
outputs by basket (`default`, `reserve`, `chunks`), the derivation data of
every output (BRC-29 prefixes and suffixes, change derivations), broadcast
state, merkle proofs, the monitor's task leases and the SSE cursor.

**Back up the database, not only the key.** The root key alone does not
recover the balance: spending a received or change output needs the
derivation data stored with it. For sqlite, copy the file while FuelKeeper is
stopped, or use `sqlite3 treasury.sqlite ".backup treasury.bak"` while it
runs; for postgres, `pg_dump`. The toolbox's own guidance is that the
throughput strategy is rated for postgres; sqlite is single-writer and fine
for one FuelKeeper process and moderate rates.

## Reconciliation

Build creates a split as a no-send action, the minter records the shard,
then Send asks the wallet to broadcast it. A crash in between, a PutShard
that fails or times out, or a Build cut off by its deadline, can leave a
no-send split in the wallet that no shard records. The minter settles such
splits in two phases, never waiting inside a round:

1. **Snapshot** (`ReconcileStart`): list the wallet's `fuel-split` actions in
   status no-send. Each one FuelKeeper's store records (in any configured
   pool) is sent at once; the whole listing is kept as the snapshot, with a
   due time `reconcile_grace` (default 2 min) after the listing returned
   (not after the round started, which may have begun with a slow finish).
2. **Finish** (`ReconcileFinish`), in the first minter round that starts at
   or after the due time: list again and look each split up afresh:
   - **resent**: recorded; handed to Send (the wallet claims and posts it).
   - **aborted**: unrecorded and present in both listings. Its puzzle seed
     lived only in the process that built it, so it must never reach the
     chain; the abort returns its inputs to the wallet and parks it as
     invalid.
   - **deferred**: unrecorded but only in the second listing: possibly a
     Build whose PutShard is still running (in this or another instance
     during a lease hand-over). The next round takes a new snapshot.
   - **errors**: the store lookup or the send/abort failed; nothing is
     aborted on an error, and the next round takes a new snapshot.

A snapshot is taken on the first round of each minter lease (and at process
start), after a `record_failed` split (PutShard failed after a successful
Build), and after a Build that failed with a deadline, so an instance that
keeps the lease for days still settles its own leftovers a grace later.
When the first listing is empty there is nothing to finish.

The grace only protects a split whose Build and PutShard finish within it,
so the minter bounds that window with `jobs.minter.build_timeout` (default
60 s), and the config requires `reconcile_grace > build_timeout + 10 s`
(a first listing, bounded by the reconcile budget, may overlap such a
Build). Each phase
is bounded by a 10 s budget (`jobs.ReconcileBudget`), and a round runs at
most one finish and one snapshot, so with the toolbox treasury the config
requires `jobs.lease_ttl > build_timeout + 20 s` (90 s > 80 s by default).
The job lease is renewed only between rounds; because no phase waits for
the grace, a round never outlives the lease on account of reconciliation,
and two instances never mint concurrently because of it. (A round that
builds several splits can still take up to `max_per_round` ×
`build_timeout` in the worst case; every store write is a CAS, so an
overlap in that case is harmless, as it is during any lease hand-over.)

A failed phase is logged and retried next round, keeping its snapshot;
minting continues. The minter logs the counts at Info (Warn when anything
was aborted or failed) and the txids at Debug.

The minter detects a new lease by a gap of more than 1.5 × `minter.interval`
since its previous round (the runner calls it only while it holds the
lease). A false positive costs one extra wallet listing, and a second
listing a grace later only if the first found no-send splits; it never
delays a round.

## Several Instances

Every FuelKeeper instance that may hold the minter lease must use **the
same treasury storage**: one postgres database in `treasury.toolbox.storage`
(and the same root key). The minter runs on whichever instance holds the
lease, and funding (`POST /v1/funding`) is served by whichever instance a
request reaches; with per-instance sqlite files, a payment received by one
instance lands in a wallet the minter on another instance never spends. The
monitor's task leases live in that shared database, so each monitor task
still runs once.

## Using the Packages as a Library

`pkg/treasury/toolbox`, `pkg/treasury/factory` and `pkg/config` link
go-wallet-toolbox's services, which pull in go-libp2p. A module importing
them must copy the three `replace` lines at the end of FuelKeeper's
`go.mod` (go-libp2p, quic-go/webtransport-go and quic-go, as
go-wallet-toolbox v0.184.16's own `go.mod` asks of its consumers); Go
ignores `replace` directives of dependencies, and without them the build
fails in go-libp2p's webtransport transport.

## Teratestnet Smoke

`pkg/treasury/toolbox/wiring_test.go` (build tag `ttn`) opens the treasury
on teratestnet, reconciles, runs one keeper round, builds and sends one
5-output split, and waits for arcade to know it.

```
export FK_TTN_ROOT_KEY=<64 hex chars; a dedicated test key>
export FK_TTN_ARCADE_URL=https://arcade-v2-ttn-us-1.bsvblockchain.tech
export FK_TTN_CHAINTRACKS_URL=https://arcade-v2-ttn-us-1.bsvblockchain.tech/chaintracks
export FK_TTN_TERANODE_URL=<teranode asset API base, e.g. http://host:8090/api/v1>
export FK_TTN_STORAGE=$HOME/fk-ttn-treasury.sqlite   # persistent: funding lands here
go test -tags ttn -run TestTeratestnetSmoke -v -timeout 15m ./pkg/treasury/toolbox/
```

The test skips when a variable is unset. The first run on a new storage
prints the treasury identity key and skips with "has no funds". To fund it,
run FuelKeeper with the same key and treasury storage
(`examples/teratestnet.yaml` with `FUELKEEPER_TREASURY_KEY=$FK_TTN_ROOT_KEY`
and `FUELKEEPER_TREASURY_DB=$FK_TTN_STORAGE`), fund it with
`fuelkeeper fund --sats 1000000` ([FUNDING.md](FUNDING.md)), stop it, and run
the test again. It logs the resolved fee, the chunk value, the reconcile
counts, the split txid and arcade's status for it.

## Verified on Teratestnet

Not yet run: the environment above was not available when the wiring was
written (the wiring was verified against local stand-ins for arcade and
chaintracks only). Record here:

| Date | Commit | Fee (sat/kB) | Chunk value | Funding txid | Split txid | Arcade status |
|------|--------|--------------|-------------|--------------|------------|---------------|
| | | | | | | |
