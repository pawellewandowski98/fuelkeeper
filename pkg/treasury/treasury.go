// Package treasury defines how FuelKeeper obtains new fuel: a Treasury builds a
// split transaction (without broadcasting), the minter records the shard, and
// only then the treasury sends it. That ordering is load-bearing: fuel must never
// exist on-chain without a shard record.
package treasury

import (
	"context"
	"errors"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

var (
	// ErrInvalidRequest is returned for a malformed MintRequest.
	ErrInvalidRequest = errors.New("treasury: invalid mint request")
	// ErrUnsupportedLockMode is returned when the treasury cannot mint the requested lock mode.
	ErrUnsupportedLockMode = errors.New("treasury: unsupported lock mode")
	// ErrInsufficientFunds means the treasury wallet cannot fund a split right now.
	ErrInsufficientFunds = errors.New("treasury: insufficient funds")
	// ErrRejected means the network terminally rejected the split transaction.
	ErrRejected = errors.New("treasury: split rejected")
	// ErrUnknownTx is returned by Send for a txid this treasury did not build.
	ErrUnknownTx = errors.New("treasury: unknown transaction")
)

// MintRequest asks for one split transaction.
type MintRequest struct {
	// Pool is the pool the minted rows belong to.
	Pool string
	// Denomination is the satoshi value of each minted row.
	Denomination uint64
	// Count is the number of rows to mint.
	Count int
	// LockMode selects how the minted rows are locked. The zero value is LockPuzzle.
	LockMode store.LockMode
}

// Validate checks the request.
func (r MintRequest) Validate() error {
	if err := store.ValidatePool(r.Pool); err != nil {
		return errors.Join(ErrInvalidRequest, err)
	}
	if r.Denomination == 0 || r.Count <= 0 {
		return ErrInvalidRequest
	}
	return nil
}

// BuiltShard is a signed, not-yet-broadcast split: the shard record (status
// unmined, BEEF = split tx + ancestry) and its rows (state available, with
// unlocking data).
type BuiltShard struct {
	// Shard is the split's shard record, ready for the store's PutShard.
	Shard store.Shard
	// Rows are the split's fuel rows, ready for the store's PutShard.
	Rows []store.Row
	// RawTx is the raw split transaction bytes.
	RawTx []byte
}

// Treasury builds and sends split transactions.
type Treasury interface {
	// Build creates and signs a split without broadcasting it.
	Build(ctx context.Context, req MintRequest) (*BuiltShard, error)
	// Send broadcasts a previously built split. It is idempotent; a terminal
	// network rejection returns ErrRejected.
	Send(ctx context.Context, txid chainhash.Hash) error
	// MinedBEEF converts a shard's stored unmined BEEF plus the BUMP reported by
	// the oracle into the compact mined BEEF (split tx + BUMP).
	MinedBEEF(ctx context.Context, txid chainhash.Hash, unminedBEEF, merklePath []byte) ([]byte, error)
}
