// Package composite pairs two oracle.Oracle backends behind one Oracle: one
// answers Outpoints, the other answers Tx. This lets a deployment use, say,
// an arcade broadcast gateway for Tx lifecycle (mined/rejected/competing)
// while a teranode node answers Outpoints and, through TxInputs, supplies the Inputs that
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
// to tx: with a tx-only oracle such as arcade and no outpoints oracle,
// Outpoints then always reports Unknown, so the reaper never resolves a row
// via a spent verdict and quarantine grows without bound (only the
// mined-hint path can still move a row forward). tx must not be nil; New
// panics if it is.
func New(tx oracle.Oracle, outpoints oracle.Oracle) *Oracle {
	if tx == nil {
		panic("composite: tx oracle must not be nil")
	}
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

// Tx asks the tx backend for txid's verdict. It never calls the outpoints
// backend: the watcher checks Tx for every unmined shard each tick and never
// needs Inputs, so fetching raw transactions there would be wasted. The
// reaper gets Inputs, when it needs them, from TxInputs.
func (o *Oracle) Tx(ctx context.Context, txid chainhash.Hash) (oracle.TxStatus, error) {
	return o.tx.Tx(ctx, txid)
}

// TxInputs implements oracle.InputsResolver from the outpoints backend: its
// own TxInputs when it has one, else the Inputs of its Tx verdict. Without
// an outpoints backend (a tx-only gateway such as arcade) it reports nil
// (unknown). The result is a copy: a non-nil, empty slice ("known, spends
// nothing we could find") stays non-nil, and the caller cannot alias the
// backend's state.
func (o *Oracle) TxInputs(ctx context.Context, txid chainhash.Hash) ([]store.Outpoint, error) {
	if o.outpoints == nil {
		return nil, nil
	}
	var ins []store.Outpoint
	if r, ok := o.outpoints.(oracle.InputsResolver); ok {
		got, err := r.TxInputs(ctx, txid)
		if err != nil {
			return nil, err
		}
		ins = got
	} else {
		st, err := o.outpoints.Tx(ctx, txid)
		if err != nil {
			return nil, err
		}
		if !st.Known {
			return nil, nil
		}
		ins = st.Inputs
	}
	if ins == nil {
		return nil, nil
	}
	return append(make([]store.Outpoint, 0, len(ins)), ins...), nil
}

var _ oracle.InputsResolver = (*Oracle)(nil)
