// Package composite pairs two oracle.Oracle backends behind one Oracle: one
// answers Outpoints, the other answers Tx. This lets a deployment use, say,
// an arcade broadcast gateway for Tx lifecycle (mined/rejected/competing)
// while a teranode node answers Outpoints and fills in the Inputs that
// arcade cannot supply.
package composite

import (
	"context"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// Oracle routes Outpoints and Tx queries to two configured backends.
type Oracle struct {
	tx        oracle.Oracle
	outpoints oracle.Oracle
}

var _ oracle.Oracle = (*Oracle)(nil)

// New returns an Oracle that answers Tx from tx and Outpoints from
// outpoints. outpoints may be nil, in which case Outpoints also falls back
// to tx. tx must not be nil.
func New(tx oracle.Oracle, outpoints oracle.Oracle) *Oracle {
	return &Oracle{tx: tx, outpoints: outpoints}
}

// Outpoints delegates to the outpoints backend when one is configured,
// otherwise to the tx backend.
func (o *Oracle) Outpoints(ctx context.Context, ops []store.Outpoint) ([]oracle.Result, error) {
	if o.outpoints != nil {
		return o.outpoints.Outpoints(ctx, ops)
	}
	return o.tx.Outpoints(ctx, ops)
}

// Tx asks the tx backend for txid's verdict. When that verdict is Known but
// has no Inputs (a broadcast gateway tracks lifecycle but cannot serve raw
// transactions), and a secondary outpoints backend is configured, Tx also
// asks the secondary for its own verdict and copies its Inputs across. A
// secondary that errors, or itself reports no Inputs, leaves the primary
// verdict untouched: an unavailable secondary must never fail the primary
// verdict, so on a nil Inputs the reaper falls back to its documented trust
// model (SpentHint plus quarantine) instead of resolving the row.
func (o *Oracle) Tx(ctx context.Context, txid chainhash.Hash) (oracle.TxStatus, error) {
	st, err := o.tx.Tx(ctx, txid)
	if err != nil {
		return oracle.TxStatus{}, err
	}
	if st.Known && st.Inputs == nil && o.outpoints != nil {
		if sec, secErr := o.outpoints.Tx(ctx, txid); secErr == nil && sec.Inputs != nil {
			st.Inputs = sec.Inputs
		}
	}
	return st, nil
}
