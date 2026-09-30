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

	m.SetDown(true)
	_, err = m.Outpoints(ctx, []store.Outpoint{op})
	require.ErrorIs(t, err, oracle.ErrUnavailable)
	_, err = m.Tx(ctx, spender)
	require.ErrorIs(t, err, oracle.ErrUnavailable)
	assert.Equal(t, 7, m.Calls(), "every call is counted, including failed ones")

	n := oracle.None{}
	res, err = n.Outpoints(ctx, []store.Outpoint{op})
	require.NoError(t, err)
	assert.Equal(t, oracle.StatusUnknown, res[0].Status)
	ts, err = n.Tx(ctx, spender)
	require.NoError(t, err)
	assert.False(t, ts.Known)
}
