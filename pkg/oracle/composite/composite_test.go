package composite_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
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

func TestTxNeverCallsSecondary(t *testing.T) {
	ctx := context.Background()
	txid := storetest.NewTxID()
	txOracle := oracle.NewMock()
	opsOracle := oracle.NewMock()
	txOracle.SetTx(txid, oracle.TxStatus{Known: true, Mined: true, Status: "MINED", BlockHeight: 42})
	opsOracle.SetTx(txid, oracle.TxStatus{Known: true, Inputs: []store.Outpoint{{TxID: storetest.NewTxID(), Vout: 1}}})

	c := composite.New(txOracle, opsOracle)
	st, err := c.Tx(ctx, txid)
	require.NoError(t, err)
	assert.True(t, st.Known)
	assert.True(t, st.Mined)
	assert.Equal(t, "MINED", st.Status)
	assert.Equal(t, uint32(42), st.BlockHeight)
	assert.Nil(t, st.Inputs, "Inputs come from TxInputs, not Tx")
	assert.Equal(t, 0, opsOracle.Calls(), "the watcher's Tx checks never reach the secondary")
}

func TestTxPrimaryErrorPropagates(t *testing.T) {
	ctx := context.Background()
	txOracle := oracle.NewMock()
	txOracle.SetDown(true)
	c := composite.New(txOracle, oracle.NewMock())
	_, err := c.Tx(ctx, storetest.NewTxID())
	require.ErrorIs(t, err, oracle.ErrUnavailable)
}

func TestTxInputsFromSecondaryTx(t *testing.T) {
	ctx := context.Background()
	txid := storetest.NewTxID()
	opsOracle := oracle.NewMock()
	inputs := []store.Outpoint{{TxID: storetest.NewTxID(), Vout: 1}}
	opsOracle.SetTx(txid, oracle.TxStatus{Known: true, Inputs: inputs})
	c := composite.New(oracle.NewMock(), opsOracle)

	got, err := c.TxInputs(ctx, txid)
	require.NoError(t, err)
	assert.Equal(t, inputs, got)

	got, err = c.TxInputs(ctx, storetest.NewTxID())
	require.NoError(t, err)
	assert.Nil(t, got, "a tx the secondary does not know has unknown inputs")

	opsOracle.SetDown(true)
	_, err = c.TxInputs(ctx, txid)
	require.ErrorIs(t, err, oracle.ErrUnavailable)
}

func TestTxInputsNoSecondaryIsUnknown(t *testing.T) {
	txOracle := oracle.NewMock()
	txid := storetest.NewTxID()
	txOracle.SetTx(txid, oracle.TxStatus{Known: true, Inputs: []store.Outpoint{{Vout: 3}}})
	got, err := composite.New(txOracle, nil).TxInputs(context.Background(), txid)
	require.NoError(t, err)
	assert.Nil(t, got)
}

// sharedInputs is an oracle.InputsResolver that returns (and keeps) one
// shared slice, the way a caching backend would, so aliasing is observable.
type sharedInputs struct {
	oracle.None
	ins []store.Outpoint
}

func (s *sharedInputs) TxInputs(context.Context, chainhash.Hash) ([]store.Outpoint, error) {
	return s.ins, nil
}

func TestTxInputsUsesSecondaryResolverAndDoesNotAlias(t *testing.T) {
	ctx := context.Background()
	sec := &sharedInputs{ins: []store.Outpoint{{Vout: 0}}}
	c := composite.New(oracle.NewMock(), sec)

	got, err := c.TxInputs(ctx, storetest.NewTxID())
	require.NoError(t, err)
	require.Equal(t, []store.Outpoint{{Vout: 0}}, got)
	got[0].Vout = 99
	assert.Equal(t, uint32(0), sec.ins[0].Vout, "mutating the result must not reach the backend's slice")

	sec.ins = []store.Outpoint{}
	got, err = c.TxInputs(ctx, storetest.NewTxID())
	require.NoError(t, err)
	assert.NotNil(t, got, "known, spends nothing: stays non-nil")
	assert.Empty(t, got)

	sec.ins = nil
	got, err = c.TxInputs(ctx, storetest.NewTxID())
	require.NoError(t, err)
	assert.Nil(t, got)
}

// TestNewPanicsOnNilTx covers N6: the primary tx oracle is required.
func TestNewPanicsOnNilTx(t *testing.T) {
	assert.Panics(t, func() {
		composite.New(nil, oracle.NewMock())
	})
}
