package store

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// Store is the fuel store contract. Every implementation must pass storetest.RunSuite.
//
// # Correctness model
//
// Each Row has exactly one State. Every mutation is a compare-and-set on the row's
// current state (and on ReservedBy == token for Release and Spend). Backends must
// never overwrite a row without re-asserting that predicate in the same atomic
// write. Rows that fail the predicate are silently skipped and reported through
// the returned count / applied set; they are never errors.
//
// Shards carry a lease (Owner, LeaseUntil) that lets one instance serve a shard's
// rows without contention. The lease is an optimisation only: two instances may
// race Reserve on the same row and the row CAS decides. Adoption never writes rows.
//
// # Idempotency
//
// Reserve applies to a row when state == available OR (state == reserved AND
// reserved_by == op.Token). A replay under the same token is therefore reported
// as applied without changing reserved_at / expires_at. Callers retry Reserve on
// ErrUnavailable instead of compensating.
//
// # Clocks
//
// Backends stamp created_at / reserved_at / quarantined_at / updated_at and lease
// expiry from their injected clock (time.Now by default). Callers pass now, before
// and until explicitly where a comparison is made, so a single clock policy holds
// across processes (see the design spec, "Restart / recovery, identity, clocks").
type Store interface {
	// Ping reports connectivity. It must not allocate or mutate.
	Ping(ctx context.Context) error
	// Capabilities is constant for the life of the store.
	Capabilities() Caps

	// PutShard inserts a shard and all its rows (state available). It is idempotent
	// on (pool, txid): an identical replay returns nil without touching existing
	// rows; a replay with different size/denomination/lock mode returns
	// ErrShardExists. Backends without multi-record atomicity write rows first and
	// the shard record last, so a visible shard implies its rows exist.
	PutShard(ctx context.Context, shard Shard, rows []Row) error
	// GetShard returns the full record including BEEF, or ErrNotFound.
	GetShard(ctx context.Context, pool string, txid chainhash.Hash) (*Shard, error)
	// UpdateShard sets status (unmined, mined or retired) and, when beef != nil,
	// replaces BEEF; it bumps Version. Retired/invalid shards return ErrTerminalShard.
	UpdateShard(ctx context.Context, pool string, txid chainhash.Hash, status ShardStatus, beef []byte) error
	// RetireShard marks the shard invalid, clears its lease, bumps Version, and moves
	// every non-terminal row (available, reserved, quarantined) to invalid. It returns
	// the number of rows changed and is idempotent. Missing shard: ErrNotFound.
	RetireShard(ctx context.Context, pool string, txid chainhash.Hash) (int, error)
	// AcquireShard atomically claims the oldest shard (by created_at, then txid) whose
	// status is in statuses, whose lease is free (owner == "" or lease_until < now),
	// and whose txid is not in exclude. It sets Owner = holder, LeaseUntil = until and
	// returns the full record. (nil, nil) means nothing was adoptable.
	AcquireShard(ctx context.Context, pool, holder string, now, until time.Time, statuses []ShardStatus, exclude []chainhash.Hash) (*Shard, error)
	// RenewShard extends the lease if Owner == holder (even if it had expired) and
	// returns the current status and version. Otherwise ErrNotOwner; missing: ErrNotFound.
	RenewShard(ctx context.Context, pool string, txid chainhash.Hash, holder string, until time.Time) (ShardHead, error)
	// ReleaseShard clears the lease if Owner == holder; otherwise it is a no-op.
	// Missing shard: ErrNotFound.
	ReleaseShard(ctx context.Context, pool string, txid chainhash.Hash, holder string) error
	// ListShards returns shards ordered by (created_at, txid) with BEEF omitted.
	ListShards(ctx context.Context, pool string, f ShardFilter) ([]Shard, error)

	// ListAvailable returns available rows of one shard with vout >= fromVout,
	// ordered by vout, at most limit.
	ListAvailable(ctx context.Context, pool string, txid chainhash.Hash, fromVout uint32, limit int) ([]Row, error)
	// GetRows returns the rows that exist in pool among ops, in any order. Unknown
	// outpoints are omitted.
	GetRows(ctx context.Context, pool string, ops []Outpoint) ([]Row, error)
	// Reserve performs the reservation CAS for each op and returns the applied ops
	// (ExpiresAt reflects the stored value, which on a replay is the original). Ops
	// whose row is missing, in another state, or reserved under another token are
	// omitted. Batch-level ErrInvalidArgument for duplicates, zero tokens or size.
	Reserve(ctx context.Context, pool string, ops []ReserveOp) ([]ReserveOp, error)
	// Release moves rows from reserved(token) back to available, clearing the
	// reservation fields. Returns the number of rows changed.
	Release(ctx context.Context, pool string, token Token, ops []Outpoint) (int, error)
	// Spend moves rows from reserved(token) to quarantined with SpentHint = txid, or,
	// when terminal is true (spend_policy=trust), directly to spent with SpentBy = txid.
	// ReservedBy is kept for provenance. Returns the number of rows changed.
	Spend(ctx context.Context, pool string, token Token, ops []Outpoint, txid chainhash.Hash, terminal bool) (int, error)
	// Quarantine moves up to limit reserved rows whose expires_at < before to
	// quarantined (oldest expiry first) and returns how many it moved.
	Quarantine(ctx context.Context, pool string, before time.Time, limit int) (int, error)
	// ListQuarantined returns quarantined rows with quarantined_at < before, oldest
	// first, at most limit.
	ListQuarantined(ctx context.Context, pool string, before time.Time, limit int) ([]Row, error)
	// Resolve moves quarantined rows to available (clearing reservation fields and
	// SpentHint) or to spent (setting SpentBy). Returns the number of rows changed.
	Resolve(ctx context.Context, pool string, ops []Outpoint, to State, spentBy *chainhash.Hash) (int, error)
	// Stats counts rows by state and shards by status for one pool.
	Stats(ctx context.Context, pool string) (Stats, error)

	Leaser
	// Close releases resources. It is idempotent.
	Close() error
}

// Leaser is single-runner election for background jobs. It has no fencing token:
// only gate work that is safe if two holders briefly overlap (every job write in
// FuelKeeper is a CAS, so that holds).
type Leaser interface {
	// TryAcquireOrRenew grants or extends the named lease for holder until now+ttl
	// when the lease is free, expired, or already held by holder. It returns the new
	// expiry, or the zero time when another holder owns an unexpired lease. Errors
	// are for infrastructure failures only.
	TryAcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (time.Time, error)
	// ReleaseLease drops the lease if holder owns it; otherwise it is a no-op.
	// (Named ReleaseLease because Store.Release is the row operation.)
	ReleaseLease(ctx context.Context, name, holder string) error
}
