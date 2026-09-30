package store_test

import (
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

func TestLessOutpoint(t *testing.T) {
	low := chainhash.Hash{0x01}
	high := chainhash.Hash{0x02}

	assert.True(t, store.LessOutpoint(store.Outpoint{TxID: low, Vout: 5}, store.Outpoint{TxID: high, Vout: 0}),
		"lower txid bytes sort first regardless of vout")
	assert.False(t, store.LessOutpoint(store.Outpoint{TxID: high, Vout: 0}, store.Outpoint{TxID: low, Vout: 5}))
	assert.True(t, store.LessOutpoint(store.Outpoint{TxID: low, Vout: 0}, store.Outpoint{TxID: low, Vout: 1}),
		"same txid: vout breaks the tie")
	assert.False(t, store.LessOutpoint(store.Outpoint{TxID: low, Vout: 1}, store.Outpoint{TxID: low, Vout: 0}))
	assert.False(t, store.LessOutpoint(store.Outpoint{TxID: low, Vout: 0}, store.Outpoint{TxID: low, Vout: 0}), "irreflexive")
}

func TestSortOutpoints(t *testing.T) {
	low, mid, high := chainhash.Hash{0x01}, chainhash.Hash{0x02}, chainhash.Hash{0x03}
	in := []store.Outpoint{
		{TxID: high, Vout: 1},
		{TxID: low, Vout: 9},
		{TxID: mid, Vout: 0},
		{TxID: low, Vout: 2},
	}
	orig := append([]store.Outpoint(nil), in...)

	got := store.SortOutpoints(in)
	want := []store.Outpoint{
		{TxID: low, Vout: 2},
		{TxID: low, Vout: 9},
		{TxID: mid, Vout: 0},
		{TxID: high, Vout: 1},
	}
	require.Equal(t, want, got)
	assert.Equal(t, orig, in, "SortOutpoints must not mutate its input")
}

func TestSortReserveOps(t *testing.T) {
	low, high := chainhash.Hash{0x01}, chainhash.Hash{0x02}
	tok := store.NewToken()
	in := []store.ReserveOp{
		{Outpoint: store.Outpoint{TxID: high, Vout: 0}, Token: tok},
		{Outpoint: store.Outpoint{TxID: low, Vout: 0}, Token: tok},
	}
	orig := append([]store.ReserveOp(nil), in...)

	got := store.SortReserveOps(in)
	require.Len(t, got, 2)
	assert.Equal(t, low, got[0].TxID)
	assert.Equal(t, high, got[1].TxID)
	assert.Equal(t, orig, in, "SortReserveOps must not mutate its input")
}
