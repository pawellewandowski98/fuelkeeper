// Package store defines the fuel store contract shared by every backend.
package store

import "errors"

var (
	// ErrNotFound is returned when a shard or lease the caller named does not exist.
	ErrNotFound = errors.New("store: not found")
	// ErrNotOwner is returned by RenewShard when Owner != holder: another holder owns
	// the lease, nobody does, or the caller's own lease was cleared by ReleaseShard or
	// reassigned by AcquireShard.
	ErrNotOwner = errors.New("store: not the shard owner")
	// ErrShardExists is returned by PutShard when a shard with the same txid already
	// exists: under the same pool with a different size, denomination or lock mode, or
	// under a different pool (a shard's txid is globally unique across pools).
	ErrShardExists = errors.New("store: shard exists with different parameters")
	// ErrTerminalShard is returned by UpdateShard when the shard is retired or invalid.
	ErrTerminalShard = errors.New("store: shard is retired or invalid")
	// ErrInvalidArgument is returned for malformed input; it is never retryable.
	ErrInvalidArgument = errors.New("store: invalid argument")
	// ErrUnavailable wraps transient backend failures (lock timeouts, connection loss).
	// Callers may retry an ErrUnavailable operation; Reserve is idempotent per (outpoint, token).
	ErrUnavailable = errors.New("store: temporarily unavailable")
)
