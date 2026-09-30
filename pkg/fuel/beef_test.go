package fuel_test

import (
	"testing"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
)

func TestMinedBEEF(t *testing.T) {
	// A synthetic split tx with two puzzle outputs and no inputs (fine for BEEF framing).
	tx := transaction.NewTransaction()
	for i := uint32(0); i < 2; i++ {
		pre := fuel.Preimage(fuel.NewSeed(), i)
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: 50, LockingScript: scriptOf(fuel.PuzzleLock(fuel.PuzzleHash(pre)))})
	}
	txid := tx.TxID()
	raw := tx.Bytes()

	// Unmined BEEF as the treasury would store it: just the raw tx (no bump).
	unmined := transaction.NewBeefV2()
	_, err := unmined.MergeRawTx(raw, nil)
	require.NoError(t, err)
	unminedBytes, err := unmined.Bytes()
	require.NoError(t, err)

	// One-leaf merkle path: the root is the txid itself.
	mp := transaction.NewMerklePath(100, [][]*transaction.PathElement{{{Offset: 0, Hash: txid, Txid: boolPtr(true)}}})
	root, err := mp.ComputeRoot(txid)
	require.NoError(t, err)
	assert.Equal(t, txid.String(), root.String())

	mined, err := fuel.MinedBEEF(unminedBytes, *txid, mp.Bytes())
	require.NoError(t, err)

	parsed, err := transaction.NewBeefFromBytes(mined)
	require.NoError(t, err)
	found := parsed.FindTransaction(txid.String())
	require.NotNil(t, found)
	require.NotNil(t, found.MerklePath, "mined BEEF carries the bump")
	assert.Equal(t, uint32(100), found.MerklePath.BlockHeight)
	assert.Equal(t, raw, found.Bytes())

	rawBack, err := fuel.RawTxFromBEEF(unminedBytes, *txid)
	require.NoError(t, err)
	assert.Equal(t, raw, rawBack)

	_, err = fuel.MinedBEEF([]byte("garbage"), *txid, mp.Bytes())
	require.Error(t, err)
	_, err = fuel.MinedBEEF(unminedBytes, *txid, []byte{0xff})
	require.Error(t, err)
	_, err = fuel.RawTxFromBEEF(unminedBytes, *transaction.NewTransaction().TxID())
	require.ErrorIs(t, err, fuel.ErrTxNotInBEEF)
}

func scriptOf(b []byte) *script.Script { s := script.Script(b); return &s }
func boolPtr(b bool) *bool             { return &b }
