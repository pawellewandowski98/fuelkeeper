package store

import (
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

const maxPoolNameLen = 64

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidArgument}, args...)...)
}

// ValidatePool checks a pool name: non-empty, at most 64 bytes.
func ValidatePool(pool string) error {
	if pool == "" || len(pool) > maxPoolNameLen {
		return invalid("pool name must be 1..%d bytes", maxPoolNameLen)
	}
	return nil
}

// ValidateHolder checks an instance holder id.
func ValidateHolder(holder string) error {
	if holder == "" || len(holder) > 128 {
		return invalid("holder must be 1..128 bytes")
	}
	return nil
}

// ValidateLimit checks a positive page size.
func ValidateLimit(limit int) error {
	if limit <= 0 {
		return invalid("limit must be > 0")
	}
	return nil
}

// ValidateLeaseName checks a lease/job name (TryAcquireOrRenew, ReleaseLease).
func ValidateLeaseName(name string) error {
	if name == "" || len(name) > 128 {
		return invalid("lease name must be 1..128 bytes")
	}
	return nil
}

// ValidateLease checks TryAcquireOrRenew arguments.
func ValidateLease(name, holder string, ttl time.Duration) error {
	if err := ValidateLeaseName(name); err != nil {
		return err
	}
	if err := ValidateHolder(holder); err != nil {
		return err
	}
	if ttl <= 0 {
		return invalid("lease ttl must be > 0")
	}
	return nil
}

// ValidateAcquireUntil checks the lease expiry passed to AcquireShard: it must be
// strictly after now, so a freshly granted lease is immediately held.
func ValidateAcquireUntil(until, now time.Time) error {
	if !until.After(now) {
		return invalid("acquire until must be after now")
	}
	return nil
}

// ValidateRenewUntil checks the lease expiry passed to RenewShard: it must be
// non-zero (a zero time would be indistinguishable from "unowned").
func ValidateRenewUntil(until time.Time) error {
	if until.IsZero() {
		return invalid("renew until must not be zero")
	}
	return nil
}

// ValidateShardStatuses checks an AcquireShard status set: at least one status,
// every entry known.
func ValidateShardStatuses(statuses []ShardStatus) error {
	if len(statuses) == 0 {
		return invalid("at least one shard status is required")
	}
	return validateKnownShardStatuses(statuses)
}

func validateKnownShardStatuses(statuses []ShardStatus) error {
	for _, s := range statuses {
		if s > ShardInvalid {
			return invalid("unknown shard status %d", s)
		}
	}
	return nil
}

// ValidateListShardsFilter checks a ListShards filter: a non-negative limit (0 means
// "no limit") and, when present, only known statuses (nil/empty Statuses means "all").
func ValidateListShardsFilter(f ShardFilter) error {
	if f.Limit < 0 {
		return invalid("limit must be >= 0")
	}
	return validateKnownShardStatuses(f.Statuses)
}

// ValidateUpdateStatus checks the status passed to UpdateShard. Invalid is only
// reachable through RetireShard.
func ValidateUpdateStatus(s ShardStatus) error {
	if s == ShardInvalid || s > ShardInvalid {
		return invalid("UpdateShard cannot set status %s", s)
	}
	return nil
}

// ValidatePutShard checks a shard and its rows before insertion.
func ValidatePutShard(shard Shard, rows []Row) error {
	if err := ValidatePool(shard.Pool); err != nil {
		return err
	}
	if shard.Size <= 0 || shard.Size != len(rows) {
		return invalid("shard size %d must equal len(rows) %d and be > 0", shard.Size, len(rows))
	}
	if shard.Denomination == 0 {
		return invalid("shard denomination must be > 0")
	}
	if shard.Status != ShardUnmined && shard.Status != ShardMined {
		return invalid("new shard status must be unmined or mined, got %s", shard.Status)
	}
	if shard.LockMode > LockP2PKH {
		return invalid("unknown lock mode %d", shard.LockMode)
	}
	seen := make(map[uint32]struct{}, len(rows))
	for i := range rows {
		r := &rows[i]
		if r.Pool != shard.Pool {
			return invalid("row %d pool %q != shard pool %q", i, r.Pool, shard.Pool)
		}
		if r.TxID != shard.TxID {
			return invalid("row %d txid != shard txid", i)
		}
		if _, dup := seen[r.Vout]; dup {
			return invalid("duplicate vout %d", r.Vout)
		}
		seen[r.Vout] = struct{}{}
		if r.Satoshis == 0 {
			return invalid("row %d satoshis must be > 0", i)
		}
		if len(r.LockingScript) == 0 || len(r.UnlockingScript) == 0 {
			return invalid("row %d needs locking and unlocking scripts", i)
		}
	}
	return nil
}

// ValidateReserveOps checks a Reserve batch: 1..maxBatch ops, distinct outpoints,
// non-zero tokens, non-zero expiry.
func ValidateReserveOps(ops []ReserveOp, maxBatch int) error {
	if len(ops) == 0 || len(ops) > maxBatch {
		return invalid("reserve batch must have 1..%d ops, got %d", maxBatch, len(ops))
	}
	seen := make(map[Outpoint]struct{}, len(ops))
	for i, op := range ops {
		if op.Token.IsZero() {
			return invalid("op %d has a zero token", i)
		}
		if op.ExpiresAt.IsZero() {
			return invalid("op %d has no expiry", i)
		}
		if _, dup := seen[op.Outpoint]; dup {
			return invalid("duplicate outpoint %s in batch", op.Outpoint)
		}
		seen[op.Outpoint] = struct{}{}
	}
	return nil
}

// ValidateOutpoints checks a Release/Spend/Resolve/GetRows batch.
func ValidateOutpoints(ops []Outpoint, maxBatch int) error {
	if len(ops) == 0 || len(ops) > maxBatch {
		return invalid("batch must have 1..%d outpoints, got %d", maxBatch, len(ops))
	}
	seen := make(map[Outpoint]struct{}, len(ops))
	for _, op := range ops {
		if _, dup := seen[op]; dup {
			return invalid("duplicate outpoint %s in batch", op)
		}
		seen[op] = struct{}{}
	}
	return nil
}

// ValidateResolve checks the Resolve target: available (spentBy ignored) or spent (spentBy required).
func ValidateResolve(to State, spentBy *chainhash.Hash) error {
	switch to {
	case StateAvailable:
		return nil
	case StateSpent:
		if spentBy == nil {
			return invalid("resolve to spent requires spentBy")
		}
		return nil
	default:
		return invalid("resolve target must be available or spent, got %s", to)
	}
}
