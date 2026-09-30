package composite_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle/composite"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func TestOutpointsUsesSecondaryWhenSet(t *testing.T) {
	ctx := context.Background()
	txOracle := oracle.NewMock()
	opsOracle := oracle.NewMock()
	op := store.Outpoint{TxID: storetest.NewTxID(), Vout: 0}
	opsOracle.SetUnspent(op)

	c := composite.New(txOracle, opsOracle)
	res, err := c.Outpoints(ctx, []store.Outpoint{op})
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, oracle.StatusUnspent, res[0].Status)
	assert.Equal(t, 1, opsOracle.Calls())
	assert.Equal(t, 0, txOracle.Calls())
}

func TestOutpointsFallsBackToTxWhenNoSecondary(t *testing.T) {
	ctx := context.Background()
	txOracle := oracle.NewMock()
	op := store.Outpoint{TxID: storetest.NewTxID(), Vout: 0}
	txOracle.SetSpent(op, storetest.NewTxID())

	c := composite.New(txOracle, nil)
	res, err := c.Outpoints(ctx, []store.Outpoint{op})
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, oracle.StatusSpent, res[0].Status)
	assert.Equal(t, 1, txOracle.Calls())
}

func TestOutpointsPreservesOrderAndLength(t *testing.T) {
	ctx := context.Background()
	opsOracle := oracle.NewMock()
	ops := []store.Outpoint{
		{TxID: storetest.NewTxID(), Vout: 0},
		{TxID: storetest.NewTxID(), Vout: 1},
	}
	opsOracle.SetUnspent(ops[0])
	opsOracle.SetSpent(ops[1], storetest.NewTxID())

	c := composite.New(oracle.NewMock(), opsOracle)
	res, err := c.Outpoints(ctx, ops)
	require.NoError(t, err)
	require.Len(t, res, 2)
	assert.Equal(t, ops[0], res[0].Outpoint)
	assert.Equal(t, ops[1], res[1].Outpoint)
}

func TestTxFillsInputsFromSecondary(t *testing.T) {
	ctx := context.Background()
	txid := storetest.NewTxID()
	txOracle := oracle.NewMock()
	opsOracle := oracle.NewMock()
	txOracle.SetTx(txid, oracle.TxStatus{Known: true, Mined: true, Status: "MINED", BlockHeight: 42})
	inputs := []store.Outpoint{{TxID: storetest.NewTxID(), Vout: 1}}
	opsOracle.SetTx(txid, oracle.TxStatus{Known: true, Inputs: inputs})

	c := composite.New(txOracle, opsOracle)
	st, err := c.Tx(ctx, txid)
	require.NoError(t, err)
	assert.True(t, st.Known)
	assert.True(t, st.Mined)
	assert.Equal(t, "MINED", st.Status)
	assert.Equal(t, uint32(42), st.BlockHeight)
	assert.Equal(t, inputs, st.Inputs)
	assert.Equal(t, 1, opsOracle.Calls())
}

func TestTxSecondaryDownKeepsPrimary(t *testing.T) {
	ctx := context.Background()
	txid := storetest.NewTxID()
	txOracle := oracle.NewMock()
	opsOracle := oracle.NewMock()
	txOracle.SetTx(txid, oracle.TxStatus{Known: true, Mined: true})
	opsOracle.SetDown(true)

	c := composite.New(txOracle, opsOracle)
	st, err := c.Tx(ctx, txid)
	require.NoError(t, err)
	assert.True(t, st.Known)
	assert.True(t, st.Mined)
	assert.Nil(t, st.Inputs)
}

func TestTxPrimaryUnknownSkipsSecondary(t *testing.T) {
	ctx := context.Background()
	txid := storetest.NewTxID()
	txOracle := oracle.NewMock() // no SetTx: Known stays false.
	opsOracle := oracle.NewMock()

	c := composite.New(txOracle, opsOracle)
	st, err := c.Tx(ctx, txid)
	require.NoError(t, err)
	assert.False(t, st.Known)
	assert.Equal(t, 0, opsOracle.Calls())
}

func TestTxPrimaryAlreadyHasInputsSkipsSecondary(t *testing.T) {
	ctx := context.Background()
	txid := storetest.NewTxID()
	txOracle := oracle.NewMock()
	opsOracle := oracle.NewMock()
	existing := []store.Outpoint{{TxID: storetest.NewTxID(), Vout: 0}}
	txOracle.SetTx(txid, oracle.TxStatus{Known: true, Inputs: existing})

	c := composite.New(txOracle, opsOracle)
	st, err := c.Tx(ctx, txid)
	require.NoError(t, err)
	assert.Equal(t, existing, st.Inputs)
	assert.Equal(t, 0, opsOracle.Calls())
}

func TestTxPrimaryErrorPropagates(t *testing.T) {
	ctx := context.Background()
	txid := storetest.NewTxID()
	txOracle := oracle.NewMock()
	txOracle.SetDown(true)
	opsOracle := oracle.NewMock()

	c := composite.New(txOracle, opsOracle)
	_, err := c.Tx(ctx, txid)
	require.ErrorIs(t, err, oracle.ErrUnavailable)
	assert.Equal(t, 0, opsOracle.Calls())
}

func TestTxNoSecondaryConfigured(t *testing.T) {
	ctx := context.Background()
	txid := storetest.NewTxID()
	txOracle := oracle.NewMock()
	txOracle.SetTx(txid, oracle.TxStatus{Known: true, Mined: true})

	c := composite.New(txOracle, nil)
	st, err := c.Tx(ctx, txid)
	require.NoError(t, err)
	assert.True(t, st.Known)
	assert.Nil(t, st.Inputs)
}
