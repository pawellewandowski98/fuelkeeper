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

// RejectReason classifies a Rejected verdict for the reaper: whether the
// fuel output a client's spend hint names is gone (an input was already
// spent) or untouched (the transaction itself was invalid), or the oracle
// could not tell. Meaningful only when TxStatus.Rejected is true.
//
// RejectReasonTxInvalid is evidence about one submission to one gateway, not
// a global verdict on the transaction: a gateway-local policy code (e.g.
// arcade's fee, generic, or frozen-input codes) can reject a transaction
// that a different gateway, or the same gateway under different policy,
// would accept. A requeue driven by RejectReasonTxInvalid can therefore
// still be resubmitted successfully elsewhere. The safety boundary this
// package guarantees is narrower than "never reject a spendable output": it
// is that RejectReasonInputsSpent and StatusSpent are never asserted
// without real evidence the output is gone. Whether requeueing on
// RejectReasonTxInvalid is safe for a given deployment is the consumer's
// contract to keep (e.g. only requeueing once, or only after confirming no
// other path for the same funds is in flight), not something this
// classification enforces on its own.
type RejectReason int

// RejectReason values. RejectReasonUnknown means the oracle could not
// classify the rejection; the reaper must hold the row rather than guess.
const (
	// RejectReasonUnknown means the rejection could not be classified: hold.
	RejectReasonUnknown RejectReason = iota
	// RejectReasonTxInvalid means the transaction itself was invalid (bad
	// format, script, fee, size, or similar policy failure); its inputs are
	// untouched and the row may be requeued. See the type doc: this is
	// gateway-local evidence, not a global verdict.
	RejectReasonTxInvalid
	// RejectReasonInputsSpent means an input was already spent — missing,
	// non-existent, or conflicting with another transaction; the fuel
	// output this row names is gone.
	RejectReasonInputsSpent
)

// String returns a lower-case name for r, or "unknown" for any unrecognized
// value.
func (r RejectReason) String() string {
	switch r {
	case RejectReasonTxInvalid:
		return "tx_invalid"
	case RejectReasonInputsSpent:
		return "inputs_spent"
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
	// RejectReason classifies the rejection. Meaningful only when Rejected
	// is true; RejectReasonUnknown otherwise.
	RejectReason RejectReason
	// Status is the backend status string, for logs/metrics.
	Status string
	// BlockHeight is the height of the block the transaction was mined in,
	// when Mined is true.
	BlockHeight uint32
	// MerklePath holds BUMP bytes when Mined is true and a proof is available.
	MerklePath []byte
	// CompetingTxs lists txids competing for the same inputs, when known.
	CompetingTxs []string
	// Inputs lists the outpoints the transaction spends, when the oracle can
	// supply them (a node that serves the raw transaction can; a gateway that
	// only tracks broadcast status cannot). nil means unknown, never "no
	// inputs". Callers that resolve a fuel row to spent because a txid is
	// mined must, when Inputs is non-nil, also check that the row's outpoint
	// is among them; otherwise any mined txid could be passed off as the
	// spender of an unrelated outpoint.
	Inputs []store.Outpoint
}

// InputsResolver is implemented by oracles that can list the outpoints a
// transaction spends (a node that serves raw transactions). It is optional:
// the reaper asks it only when it is about to resolve a row to spent by a
// mined txid whose TxStatus.Inputs is nil, so the watcher's frequent Tx
// checks never pay for a raw-transaction fetch. It returns nil (unknown)
// with a nil error when the transaction is not known, and a non-nil,
// possibly empty slice otherwise; an error means it could not check.
type InputsResolver interface {
	TxInputs(ctx context.Context, txid chainhash.Hash) ([]store.Outpoint, error)
}

// Oracle is implemented by teranode/arcade/composite clients, Mock and None.
type Oracle interface {
	// Outpoints returns one Result per input outpoint, in input order.
	Outpoints(ctx context.Context, ops []store.Outpoint) ([]Result, error)
	// Tx returns the status of a transaction.
	Tx(ctx context.Context, txid chainhash.Hash) (TxStatus, error)
}
