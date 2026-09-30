package oracle

import (
	"context"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// None answers Unknown for everything. Quarantined rows then stay quarantined
// until an operator requeues them.
type None struct{}

var _ Oracle = None{}

// Outpoints returns StatusUnknown for every outpoint.
func (None) Outpoints(_ context.Context, ops []store.Outpoint) ([]Result, error) {
	out := make([]Result, len(ops))
	for i, op := range ops {
		out[i] = Result{Outpoint: op, Status: StatusUnknown}
	}
	return out, nil
}

// Tx reports every transaction as not known.
func (None) Tx(context.Context, chainhash.Hash) (TxStatus, error) { return TxStatus{}, nil }
