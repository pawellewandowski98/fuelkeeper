package oracle

import (
	"context"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// None answers Unknown for everything: no chain source is configured at all.
// A quarantined row still follows the reaper's ordinary unhinted-row policy
// (see pkg/jobs.Reaper's doc comment): it requeues to available after
// jobs.reaper.unknown_requeue_age under a pool's expiry_policy: requeue (the
// default), or stays quarantined for an operator under expiry_policy: hold.
// Config separately rejects spend_policy: verify and release_policy: verify
// outright for this oracle kind (pkg/config's Validate), since with no
// chain source at all neither could ever be confirmed.
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
