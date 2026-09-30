// Package arcade adapts pkg/arcadeclient's arcade broadcast-gateway client
// to the oracle.Oracle interface.
//
// arcade tracks transaction lifecycle (submitted/mined/rejected/competing)
// but has no UTXO index, so it cannot answer "is this outpoint spent"; its
// Outpoints always reports StatusUnknown. It also never supplies Inputs on
// Tx, since it does not serve raw transactions. Pair this oracle with
// pkg/oracle/composite and a teranode oracle to get outpoint verdicts and
// input details alongside arcade's lifecycle verdict. Used alone, or paired
// via composite with a nil outpoints oracle, no oracle ever confirms an
// outpoint spent, so the reaper never requeues a row through that path and
// quarantine grows without bound; only the mined-hint path can still move a
// row forward.
package arcade

import (
	"context"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/arcadeclient"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// Oracle adapts an arcadeclient.Client to oracle.Oracle.
type Oracle struct {
	c *arcadeclient.Client
}

var _ oracle.Oracle = (*Oracle)(nil)

// New returns an Oracle backed by c.
func New(c *arcadeclient.Client) *Oracle {
	return &Oracle{c: c}
}

// Tx returns arcade's verdict for txid. A 404 (arcade has never seen this
// txid) is reported as the zero TxStatus with a nil error, per the oracle
// contract: unknown is not an error. Any other failure (transport, 5xx, an
// undecodable body) is wrapped in oracle.ErrUnavailable.
func (o *Oracle) Tx(ctx context.Context, txid chainhash.Hash) (oracle.TxStatus, error) {
	st, err := o.c.GetTx(ctx, txid.String())
	if err != nil {
		if errors.Is(err, arcadeclient.ErrNotFound) {
			return oracle.TxStatus{}, nil
		}
		return oracle.TxStatus{}, fmt.Errorf("%w: arcade GetTx %s: %w", oracle.ErrUnavailable, txid.String(), err)
	}
	return oracle.TxStatus{
		Known:        true,
		Mined:        st.Mined(),
		Rejected:     st.Rejected(),
		Status:       st.TxStatus,
		BlockHeight:  st.BlockHeight,
		MerklePath:   st.MerklePathBytes(),
		CompetingTxs: st.CompetingTxs,
		Inputs:       nil,
	}, nil
}

// Outpoints reports StatusUnknown for every outpoint: arcade has no UTXO
// index to consult. See the package comment for the recommended pairing
// with a teranode oracle via pkg/oracle/composite.
func (o *Oracle) Outpoints(_ context.Context, ops []store.Outpoint) ([]oracle.Result, error) {
	out := make([]oracle.Result, len(ops))
	for i, op := range ops {
		out[i] = oracle.Result{Outpoint: op, Status: oracle.StatusUnknown}
	}
	return out, nil
}
