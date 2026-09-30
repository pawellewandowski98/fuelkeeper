package teranode

import (
	"testing"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// TestInputsFromTxNilSourceTXIDIsUnavailable covers N1. A wire-parsed
// transaction never actually has a nil SourceTXID (the raw format always
// carries 32 txid bytes; see input.go's readFrom), so this defensive branch
// can only be exercised by handing inputsFromTx a hand-built Transaction
// directly, in this white-box (internal package) test. The point is to pin
// that a nil SourceTXID becomes ErrUnavailable, never a fabricated
// zero-hash outpoint that could be mistaken for a real spend of the
// all-zero txid.
func TestInputsFromTxNilSourceTXIDIsUnavailable(t *testing.T) {
	txid := storetest.NewTxID()
	other := storetest.NewTxID()
	tx := &transaction.Transaction{
		Version: 1,
		Inputs: []*transaction.TransactionInput{
			{SourceTXID: &other, SourceTxOutIndex: 0},
			{SourceTXID: nil, SourceTxOutIndex: 1},
		},
	}

	inputs, err := inputsFromTx(tx, txid)
	require.ErrorIs(t, err, oracle.ErrUnavailable)
	assert.Nil(t, inputs)
}

func TestInputsFromTxAllPresent(t *testing.T) {
	txid := storetest.NewTxID()
	prev1 := storetest.NewTxID()
	prev2 := storetest.NewTxID()
	tx := &transaction.Transaction{
		Version: 1,
		Inputs: []*transaction.TransactionInput{
			{SourceTXID: &prev1, SourceTxOutIndex: 0},
			{SourceTXID: &prev2, SourceTxOutIndex: 5},
		},
	}

	inputs, err := inputsFromTx(tx, txid)
	require.NoError(t, err)
	require.Len(t, inputs, 2)
	assert.Equal(t, prev1, inputs[0].TxID)
	assert.Equal(t, uint32(0), inputs[0].Vout)
	assert.Equal(t, prev2, inputs[1].TxID)
	assert.Equal(t, uint32(5), inputs[1].Vout)
}
