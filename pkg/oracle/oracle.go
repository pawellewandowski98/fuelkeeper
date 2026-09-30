// Package oracle answers "is this outpoint spent?" and "what happened to this tx?"
// from the chain/mempool. The reaper and watcher never guess: an Unknown answer
// leaves rows quarantined.
package oracle

import (
	"context"
	"errors"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// ErrUnavailable means the oracle could not answer right now; retry later.
var ErrUnavailable = errors.New("oracle: unavailable")

// Status of one outpoint.
type Status int

// Status values. StatusUnknown means no verdict; callers must leave the row
// alone. Only StatusUnspent, StatusSpent and StatusConflicting may drive a
// state change.
const (
	// StatusUnknown means the oracle has no verdict; leave the row alone.
	StatusUnknown Status = iota
	// StatusUnspent means the outpoint was seen in the UTXO set or mempool as unspent.
	StatusUnspent
	// StatusSpent means the outpoint is spent; SpendingTxID is set when known.
	StatusSpent
	// StatusConflicting means a double-spend attempt was observed; treat as not safe to requeue.
	StatusConflicting
)

// String returns a lower-case name for s, or "unknown" for any unrecognized value.
func (s Status) String() string {
	switch s {
	case StatusUnspent:
		return "unspent"
	case StatusSpent:
		return "spent"
	case StatusConflicting:
		return "conflicting"
	}
	return "unknown"
}

// Result is the verdict for one outpoint.
type Result struct {
	// Outpoint is the outpoint this verdict is about.
	Outpoint store.Outpoint
	// Status is the verdict for Outpoint.
	Status Status
	// SpendingTxID is set when Status is StatusSpent and the spending
	// transaction id is known.
	SpendingTxID *chainhash.Hash
}

// TxStatus is the verdict for one transaction id.
type TxStatus struct {
	// Known reports whether the oracle has seen this txid.
	Known bool
	// Mined reports whether the transaction is included in a block.
	Mined bool
	// Rejected reports whether the transaction was terminally rejected
	// (invalid or lost a double-spend).
	Rejected bool
	// Status is the backend status string, for logs/metrics.
	Status string
	// BlockHeight is the height of the block the transaction was mined in,
	// when Mined is true.
	BlockHeight uint32
	// MerklePath holds BUMP bytes when Mined is true and a proof is available.
	MerklePath []byte
	// CompetingTxs lists txids competing for the same inputs, when known.
	CompetingTxs []string
}

// Oracle is implemented by teranode/arcade/woc clients (Plan 5), Mock and None.
type Oracle interface {
	// Outpoints returns one Result per input outpoint, in input order.
	Outpoints(ctx context.Context, ops []store.Outpoint) ([]Result, error)
	// Tx returns the status of a transaction.
	Tx(ctx context.Context, txid chainhash.Hash) (TxStatus, error)
}
