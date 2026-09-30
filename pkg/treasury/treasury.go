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
	// Send broadcasts a previously built split. beef is the shard's stored
	// BEEF (store.Shard.BEEF) and may be nil when the caller does not have it;
	// a treasury uses it only to re-post a split its wallet no longer
	// broadcasts itself. Send is idempotent and safe to call again after a
	// restart; a terminal network rejection returns ErrRejected, and a split
	// the treasury has no record of returns ErrUnknownTx.
	Send(ctx context.Context, txid chainhash.Hash, beef []byte) error
	// MinedBEEF converts a shard's stored unmined BEEF plus the BUMP reported by
	// the oracle into the compact mined BEEF (split tx + BUMP).
	MinedBEEF(ctx context.Context, txid chainhash.Hash, unminedBEEF, merklePath []byte) ([]byte, error)
}

// FundingInfo is what a payer needs to build a BRC-29 payment to the treasury.
type FundingInfo struct {
	// IdentityKeyHex is the treasury wallet's compressed identity public key (66 hex chars).
	IdentityKeyHex string
	// DerivationPrefixB64 is a fresh, verifiable nonce the payer uses as the BRC-29 derivation prefix.
	DerivationPrefixB64 string
	// Network is the treasury's network (main | test | ttn | tstn).
	Network string
}

// FundRequest is a payer's completed payment.
type FundRequest struct {
	// AtomicBEEF is the payment transaction with its ancestry (BRC-95).
	AtomicBEEF []byte
	// OutputIndex is the payment output's index; -1 asks for auto-detection by
	// matching the BRC-29 script (available only where the wiring supports it).
	OutputIndex int
	// DerivationPrefixB64 is the prefix handed out by FundingInfo.
	DerivationPrefixB64 string
	// DerivationSuffixB64 is the payer-chosen BRC-29 derivation suffix.
	DerivationSuffixB64 string
	// SenderIdentityKeyHex is the payer's identity public key (hex).
	SenderIdentityKeyHex string
}

// FundResult reports what was internalized.
type FundResult struct {
	// TxID is the payment transaction id (hex).
	TxID string
	// Satoshis is the value of the internalized output.
	Satoshis uint64
}

// ErrBadFunding is returned for a malformed or unverifiable funding request.
var ErrBadFunding = errors.New("treasury: bad funding request")

// Funder is implemented by treasuries that accept BRC-29 payments.
type Funder interface {
	// FundingInfo returns the treasury identity key and a fresh derivation prefix.
	FundingInfo(ctx context.Context) (FundingInfo, error)
	// Internalize accepts a completed payment into the treasury wallet.
	Internalize(ctx context.Context, req FundRequest) (FundResult, error)
}
