package oracle_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func TestMockOracle(t *testing.T) {
	ctx := context.Background()
	m := oracle.NewMock()
	op := store.Outpoint{TxID: storetest.NewTxID(), Vout: 3}
	spender := storetest.NewTxID()

	res, err := m.Outpoints(ctx, []store.Outpoint{op})
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, oracle.StatusUnknown, res[0].Status, "unset outpoints are unknown")

	m.SetUnspent(op)
	res, err = m.Outpoints(ctx, []store.Outpoint{op})
	require.NoError(t, err)
	assert.Equal(t, oracle.StatusUnspent, res[0].Status)

	m.SetSpent(op, spender)
	res, err = m.Outpoints(ctx, []store.Outpoint{op})
	require.NoError(t, err)
	assert.Equal(t, oracle.StatusSpent, res[0].Status)
	require.NotNil(t, res[0].SpendingTxID)
	assert.Equal(t, spender, *res[0].SpendingTxID)

	ts, err := m.Tx(ctx, spender)
	require.NoError(t, err)
	assert.False(t, ts.Known)
	m.SetTx(spender, oracle.TxStatus{Known: true, Mined: true, BlockHeight: 10, MerklePath: []byte{1, 2, 3}})
	ts, err = m.Tx(ctx, spender)
	require.NoError(t, err)
	assert.True(t, ts.Mined)
	assert.Equal(t, uint32(10), ts.BlockHeight)

	// Ordering: results must line up with the input order, including a
	// repeated outpoint.
	opUnspent := store.Outpoint{TxID: storetest.NewTxID(), Vout: 1}
	opSpent := store.Outpoint{TxID: storetest.NewTxID(), Vout: 2}
	m.SetUnspent(opUnspent)
	m.SetSpent(opSpent, spender)
	ordered := []store.Outpoint{opUnspent, opSpent, opUnspent}
	res, err = m.Outpoints(ctx, ordered)
	require.NoError(t, err)
	require.Len(t, res, 3)
	for i := range ordered {
		assert.Equal(t, ordered[i], res[i].Outpoint, "result order must match input order")
	}
	assert.Equal(t, oracle.StatusUnspent, res[0].Status)
	assert.Equal(t, oracle.StatusSpent, res[1].Status)
	assert.Equal(t, oracle.StatusUnspent, res[2].Status)

	// Forget reverts a verdict back to Unknown.
	m.SetSpent(op, spender)
	m.Forget(op)
	res, err = m.Outpoints(ctx, []store.Outpoint{op})
	require.NoError(t, err)
	assert.Equal(t, oracle.StatusUnknown, res[0].Status, "forgotten outpoints are unknown")

	// Aliasing: mutating a slice the caller holds, whether returned from Tx
	// or passed into SetTx, must not change what the mock later reports.
	aliasTxID := storetest.NewTxID()
	mp := []byte{9, 9, 9}
	m.SetTx(aliasTxID, oracle.TxStatus{Known: true, MerklePath: mp})
	aliasTS, err := m.Tx(ctx, aliasTxID)
	require.NoError(t, err)
	require.Len(t, aliasTS.MerklePath, 3)
	aliasTS.MerklePath[0] = 0xFF
	mp[0] = 0xEE
	aliasTS2, err := m.Tx(ctx, aliasTxID)
	require.NoError(t, err)
	assert.Equal(t, byte(9), aliasTS2.MerklePath[0], "mutating a returned or source slice must not affect stored state")

	m.SetDown(true)
	_, err = m.Outpoints(ctx, []store.Outpoint{op})
	require.ErrorIs(t, err, oracle.ErrUnavailable)
	_, err = m.Tx(ctx, spender)
	require.ErrorIs(t, err, oracle.ErrUnavailable)
	assert.Equal(t, 11, m.Calls(), "every call is counted, including failed ones")

	n := oracle.None{}
	res, err = n.Outpoints(ctx, []store.Outpoint{op})
	require.NoError(t, err)
	assert.Equal(t, oracle.StatusUnknown, res[0].Status)
	ts, err = n.Tx(ctx, spender)
	require.NoError(t, err)
	assert.False(t, ts.Known)
}

// TestMockOracleAssumeUnspent covers the "nullnet" development mode: with
// SetAssumeUnspent(true), an outpoint with no verdict answers Unspent instead
// of Unknown, but an outpoint with an explicit verdict (spent or
// conflicting) keeps that verdict.
func TestMockOracleAssumeUnspent(t *testing.T) {
	ctx := context.Background()
	m := oracle.NewMock()
	unset := store.Outpoint{TxID: storetest.NewTxID(), Vout: 0}
	spent := store.Outpoint{TxID: storetest.NewTxID(), Vout: 1}
	spender := storetest.NewTxID()
	m.SetSpent(spent, spender)

	res, err := m.Outpoints(ctx, []store.Outpoint{unset, spent})
	require.NoError(t, err)
	assert.Equal(t, oracle.StatusUnknown, res[0].Status, "default: no verdict means unknown")
	assert.Equal(t, oracle.StatusSpent, res[1].Status)

	m.SetAssumeUnspent(true)
	res, err = m.Outpoints(ctx, []store.Outpoint{unset, spent})
	require.NoError(t, err)
	assert.Equal(t, oracle.StatusUnspent, res[0].Status, "assume-unspent: no verdict now means unspent")
	assert.Equal(t, oracle.StatusSpent, res[1].Status, "an explicit verdict is never overridden")

	m.SetAssumeUnspent(false)
	res, err = m.Outpoints(ctx, []store.Outpoint{unset})
	require.NoError(t, err)
	assert.Equal(t, oracle.StatusUnknown, res[0].Status, "turning it back off restores unknown")
}

// TestMockOracleTxStatusInputsAreCopied pins two properties of the mock's
// TxStatus handling that the reaper relies on: Inputs is never shared with
// the caller's slice, and an empty non-nil Inputs stays non-nil (nil means
// "unknown", empty means "verified: spends nothing").
func TestMockOracleTxStatusInputsAreCopied(t *testing.T) {
	ctx := context.Background()
	m := oracle.NewMock()
	txid := storetest.NewTxID()
	a, b := storetest.NewTxID(), storetest.NewTxID()

	src := []store.Outpoint{{TxID: a, Vout: 1}}
	m.SetTx(txid, oracle.TxStatus{Known: true, Mined: true, Inputs: src})
	src[0] = store.Outpoint{TxID: b, Vout: 9} // caller mutates its own slice after SetTx

	got, err := m.Tx(ctx, txid)
	require.NoError(t, err)
	require.Len(t, got.Inputs, 1)
	assert.Equal(t, store.Outpoint{TxID: a, Vout: 1}, got.Inputs[0], "SetTx must copy Inputs")
	got.Inputs[0] = store.Outpoint{TxID: b, Vout: 9} // caller mutates the returned slice
	again, err := m.Tx(ctx, txid)
	require.NoError(t, err)
	assert.Equal(t, store.Outpoint{TxID: a, Vout: 1}, again.Inputs[0], "Tx must return a fresh copy")

	empty := storetest.NewTxID()
	m.SetTx(empty, oracle.TxStatus{Known: true, Mined: true, Inputs: []store.Outpoint{}})
	got, err = m.Tx(ctx, empty)
	require.NoError(t, err)
	assert.NotNil(t, got.Inputs, "an empty non-nil Inputs must stay non-nil")
	assert.Empty(t, got.Inputs)

	unset := storetest.NewTxID()
	m.SetTx(unset, oracle.TxStatus{Known: true})
	got, err = m.Tx(ctx, unset)
	require.NoError(t, err)
	assert.Nil(t, got.Inputs, "nil Inputs stays nil")
}

// TestMockOracleTxStatusRejectReasonRoundTrips pins that RejectReason, a
// scalar field on TxStatus, survives SetTx/Tx unchanged for every value,
// including the zero value (RejectReasonUnknown) on a rejected verdict.
func TestMockOracleTxStatusRejectReasonRoundTrips(t *testing.T) {
	ctx := context.Background()
	m := oracle.NewMock()

	tests := []oracle.RejectReason{
		oracle.RejectReasonUnknown,
		oracle.RejectReasonTxInvalid,
		oracle.RejectReasonInputsSpent,
	}
	for _, reason := range tests {
		t.Run(reason.String(), func(t *testing.T) {
			txid := storetest.NewTxID()
			m.SetTx(txid, oracle.TxStatus{Known: true, Rejected: true, RejectReason: reason})
			got, err := m.Tx(ctx, txid)
			require.NoError(t, err)
			assert.True(t, got.Rejected)
			assert.Equal(t, reason, got.RejectReason)
		})
	}
}
