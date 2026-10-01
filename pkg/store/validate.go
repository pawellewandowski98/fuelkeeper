package store

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

const (
	maxPoolNameLen = 64
	maxNameLen     = 128
)

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidArgument}, args...)...)
}

// validateName checks a pool/holder/lease name shared by every backend:
// non-empty, at most maxLen bytes, valid UTF-8, and free of NUL bytes.
// Postgres and SQLite TEXT columns can store neither an invalid UTF-8 byte
// sequence nor a NUL byte, and a JSON-backed backend's string encoding
// silently replaces an invalid UTF-8 byte with the Unicode replacement
// character U+FFFD instead of failing (see store.go, "Names"), so this is
// checked once here rather than left to surface differently, or not at all,
// per backend.
func validateName(s string, maxLen int, what string) error {
	if s == "" || len(s) > maxLen {
		return invalidf("%s must be 1..%d bytes", what, maxLen)
	}
	if !utf8.ValidString(s) {
		return invalidf("%s must be valid UTF-8", what)
	}
	if strings.IndexByte(s, 0) >= 0 {
		return invalidf("%s must not contain a NUL byte", what)
	}
	return nil
}

// ValidatePool checks a pool name: non-empty, at most 64 bytes, valid UTF-8,
// and free of NUL bytes (see validateName and store.go, "Names").
func ValidatePool(pool string) error {
	return validateName(pool, maxPoolNameLen, "pool name")
}

// ValidateHolder checks an instance holder id: non-empty, at most 128 bytes,
// valid UTF-8, and free of NUL bytes (see validateName and store.go, "Names").
func ValidateHolder(holder string) error {
	return validateName(holder, maxNameLen, "holder")
}

// ValidateLimit checks a positive page size.
func ValidateLimit(limit int) error {
	if limit <= 0 {
		return invalidf("limit must be > 0")
	}
	return nil
}

// ValidateLeaseName checks a lease/job name (TryAcquireOrRenew, ReleaseLease):
// non-empty, at most 128 bytes, valid UTF-8, and free of NUL bytes (see
// validateName and store.go, "Names").
func ValidateLeaseName(name string) error {
	return validateName(name, maxNameLen, "lease name")
}

// unixEpoch is time.Unix(0, 0) in UTC: see ValidateTime.
var unixEpoch = time.Unix(0, 0).UTC()

// ValidateTime checks a non-sentinel time argument (store.go, "Clocks"): the
// zero time.Time{} is always allowed, since backends use it throughout as
// "unset" (ReservedAt/ExpiresAt/QuarantinedAt before their field is ever set,
// LeaseUntil while a shard is unowned, ...), but any other value must be
// strictly after the Unix epoch. A time at or before the epoch is rejected
// rather than accepted: every backend's on-disk time encoding stores the zero
// time and the exact epoch instant identically (both as 0), so a caller-
// supplied epoch-or-earlier value would be indistinguishable from "unset"
// once read back, and naively encoding a negative Unix time as an unsigned
// microsecond count (every backend's key/counter layout needs unsigned,
// order-preserving timestamps) would otherwise silently wrap around to a
// huge, wrongly-ordered value instead of failing loudly here.
func ValidateTime(t time.Time, what string) error {
	if t.IsZero() {
		return nil
	}
	if !t.After(unixEpoch) {
		return invalidf("%s must be strictly after the Unix epoch", what)
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
		return invalidf("lease ttl must be > 0")
	}
	return nil
}

// ValidateAcquireUntil checks the lease expiry passed to AcquireShard: it must
// be strictly after the Unix epoch (ValidateTime) and strictly after now, so a
// freshly granted lease is immediately held. The after-now comparison
// truncates both sides to microsecond precision first (store.go, "Clocks"):
// every backend stores/compares at microsecond resolution, so an until only a
// few hundred nanoseconds after now would truncate to the same stored instant
// and be immediately free, not "immediately held".
func ValidateAcquireUntil(until, now time.Time) error {
	if err := ValidateTime(until, "acquire until"); err != nil {
		return err
	}
	if !until.Truncate(time.Microsecond).After(now.Truncate(time.Microsecond)) {
		return invalidf("acquire until must be after now")
	}
	return nil
}

// ValidateRenewUntil checks the lease expiry passed to RenewShard: it must be
// non-zero (a zero time would be indistinguishable from "unowned") and, like
// every other time argument stored as data, strictly after the Unix epoch
// (ValidateTime). RenewShard stores until as the shard's LeaseUntil, so an
// epoch-or-earlier until would otherwise round-trip indistinguishably from
// "unset" (store.go, "Clocks"), breaking "Owner is empty iff LeaseUntil is
// zero": Owner would stay set while LeaseUntil read back as the zero time.
func ValidateRenewUntil(until time.Time) error {
	if until.IsZero() {
		return invalidf("renew until must not be zero")
	}
	return ValidateTime(until, "renew until")
}

// ValidateShardStatuses checks an AcquireShard status set: at least one status,
// every entry known.
func ValidateShardStatuses(statuses []ShardStatus) error {
	if len(statuses) == 0 {
		return invalidf("at least one shard status is required")
	}
	return validateKnownShardStatuses(statuses)
}

func validateKnownShardStatuses(statuses []ShardStatus) error {
	for _, s := range statuses {
		if s > ShardInvalid {
			return invalidf("unknown shard status %d", s)
		}
	}
	return nil
}

// ValidateListShardsFilter checks a ListShards filter: a non-negative limit (0 means
// "no limit") and, when present, only known statuses (nil/empty Statuses means "all").
func ValidateListShardsFilter(f ShardFilter) error {
	if f.Limit < 0 {
		return invalidf("limit must be >= 0")
	}
	return validateKnownShardStatuses(f.Statuses)
}

// ValidateUpdateStatus checks the status passed to UpdateShard. Invalid is only
// reachable through RetireShard.
func ValidateUpdateStatus(s ShardStatus) error {
	if s == ShardInvalid || s > ShardInvalid {
		return invalidf("UpdateShard cannot set status %s", s)
	}
	return nil
}

// ValidatePutShard checks a shard and its rows before insertion.
func ValidatePutShard(shard Shard, rows []Row) error {
	if err := ValidatePool(shard.Pool); err != nil {
		return err
	}
	if shard.Size <= 0 || shard.Size != len(rows) {
		return invalidf("shard size %d must equal len(rows) %d and be > 0", shard.Size, len(rows))
	}
	if shard.Denomination == 0 {
		return invalidf("shard denomination must be > 0")
	}
	if len(shard.BEEF) == 0 {
		return invalidf("shard BEEF must not be empty")
	}
	if shard.Status != ShardUnmined && shard.Status != ShardMined {
		return invalidf("new shard status must be unmined or mined, got %s", shard.Status)
	}
	if shard.LockMode > LockP2PKH {
		return invalidf("unknown lock mode %d", shard.LockMode)
	}
	seen := make(map[uint32]struct{}, len(rows))
	for i := range rows {
		r := &rows[i]
		if r.Pool != shard.Pool {
			return invalidf("row %d pool %q != shard pool %q", i, r.Pool, shard.Pool)
		}
		if r.TxID != shard.TxID {
			return invalidf("row %d txid != shard txid", i)
		}
		if _, dup := seen[r.Vout]; dup {
			return invalidf("duplicate vout %d", r.Vout)
		}
		seen[r.Vout] = struct{}{}
		if r.Satoshis == 0 {
			return invalidf("row %d satoshis must be > 0", i)
		}
		if len(r.LockingScript) == 0 || len(r.UnlockingScript) == 0 {
			return invalidf("row %d needs locking and unlocking scripts", i)
		}
	}
	return nil
}

// ValidateReserveOps checks a Reserve batch: 1..maxBatch ops, distinct
// outpoints, non-zero tokens, non-zero expiry strictly after the Unix epoch
// (ValidateTime).
func ValidateReserveOps(ops []ReserveOp, maxBatch int) error {
	if len(ops) == 0 || len(ops) > maxBatch {
		return invalidf("reserve batch must have 1..%d ops, got %d", maxBatch, len(ops))
	}
	seen := make(map[Outpoint]struct{}, len(ops))
	for i, op := range ops {
		if op.Token.IsZero() {
			return invalidf("op %d has a zero token", i)
		}
		if op.ExpiresAt.IsZero() {
			return invalidf("op %d has no expiry", i)
		}
		if err := ValidateTime(op.ExpiresAt, fmt.Sprintf("op %d expiry", i)); err != nil {
			return err
		}
		if _, dup := seen[op.Outpoint]; dup {
			return invalidf("duplicate outpoint %s in batch", op.Outpoint)
		}
		seen[op.Outpoint] = struct{}{}
	}
	return nil
}

// ValidateOutpoints checks a Release/Spend/Resolve/GetRows batch.
func ValidateOutpoints(ops []Outpoint, maxBatch int) error {
	if len(ops) == 0 || len(ops) > maxBatch {
		return invalidf("batch must have 1..%d outpoints, got %d", maxBatch, len(ops))
	}
	seen := make(map[Outpoint]struct{}, len(ops))
	for _, op := range ops {
		if _, dup := seen[op]; dup {
			return invalidf("duplicate outpoint %s in batch", op)
		}
		seen[op] = struct{}{}
	}
	return nil
}

// ValidateSpend checks the txid/terminal combination passed to Spend: a
// terminal spend (spend_policy=trust) always names the spending txid, since
// SpentBy is meant to be an oracle-proven fact, not an absent one. A
// non-terminal spend's txid is only a client-supplied hint, so a zero txid is
// accepted there and simply records no hint (see Spend's doc comment).
func ValidateSpend(txid chainhash.Hash, terminal bool) error {
	if terminal && txid == (chainhash.Hash{}) {
		return invalidf("terminal spend requires a non-zero txid")
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
			return invalidf("resolve to spent requires spentBy")
		}
		return nil
	default:
		return invalidf("resolve target must be available or spent, got %s", to)
	}
}

// ValidateSpendRequest checks one SpendRequest exactly as Spend checks its
// own arguments: a non-zero token, 1..maxBatch distinct outpoints
// (ValidateOutpoints), and a txid/terminal combination ValidateSpend accepts.
func ValidateSpendRequest(r SpendRequest, maxBatch int) error {
	if err := ValidateOutpoints(r.Ops, maxBatch); err != nil {
		return err
	}
	if r.Token.IsZero() {
		return invalidf("spend request has a zero token")
	}
	return ValidateSpend(r.TxID, r.Terminal)
}

// ValidateSpendBatch checks a SpendBatch call's requests: 1..maxBatch
// requests whose outpoints total at most maxBatch, every one valid by
// ValidateSpendRequest. The same outpoint may appear in more than one request
// (the later request just finds it no longer reserved); it may not appear
// twice within one request. One invalid request rejects the whole batch.
func ValidateSpendBatch(reqs []SpendRequest, maxBatch int) error {
	if len(reqs) == 0 || len(reqs) > maxBatch {
		return invalidf("spend batch must have 1..%d requests, got %d", maxBatch, len(reqs))
	}
	total := 0
	for i, r := range reqs {
		if err := ValidateSpendRequest(r, maxBatch); err != nil {
			return fmt.Errorf("spend request %d: %w", i, err)
		}
		total += len(r.Ops)
	}
	if total > maxBatch {
		return invalidf("spend batch must have at most %d outpoints in total, got %d", maxBatch, total)
	}
	return nil
}
