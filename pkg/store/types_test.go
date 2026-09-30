package store_test

import (
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

func TestOutpointRoundTrip(t *testing.T) {
	h, err := chainhash.NewHashFromHex("6f9d4c4e0a2b1c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5")
	require.NoError(t, err)
	op := store.Outpoint{TxID: *h, Vout: 7}
	assert.Equal(t, h.String()+":7", op.String())

	parsed, err := store.ParseOutpoint(op.String())
	require.NoError(t, err)
	assert.Equal(t, op, parsed)

	// BRC-100 style "txid.vout" is accepted too.
	parsed, err = store.ParseOutpoint(h.String() + ".7")
	require.NoError(t, err)
	assert.Equal(t, op, parsed)

	_, err = store.ParseOutpoint("nope")
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = store.ParseOutpoint(h.String() + ":x")
	require.ErrorIs(t, err, store.ErrInvalidArgument)
}

func TestTokenRoundTrip(t *testing.T) {
	tok := store.NewToken()
	assert.False(t, tok.IsZero())
	assert.Len(t, tok.String(), 22)

	parsed, err := store.ParseToken(tok.String())
	require.NoError(t, err)
	assert.Equal(t, tok, parsed)

	_, err = store.ParseToken("too-short")
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	assert.True(t, store.Token{}.IsZero())
	assert.NotEqual(t, store.NewToken(), store.NewToken()) //nolint:testifylint // two independently random tokens; not a same-variable comparison
}

func TestOutpointVoutBoundaries(t *testing.T) {
	h, err := chainhash.NewHashFromHex("6f9d4c4e0a2b1c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5")
	require.NoError(t, err)

	zero := store.Outpoint{TxID: *h, Vout: 0}
	parsed, err := store.ParseOutpoint(zero.String())
	require.NoError(t, err)
	assert.Equal(t, zero, parsed)

	maxVout := store.Outpoint{TxID: *h, Vout: 4294967295}
	parsed, err = store.ParseOutpoint(maxVout.String())
	require.NoError(t, err)
	assert.Equal(t, maxVout, parsed)

	_, err = store.ParseOutpoint(h.String() + ":4294967296")
	require.ErrorIs(t, err, store.ErrInvalidArgument)
}

func TestEnumStrings(t *testing.T) {
	assert.Equal(t, "available", store.StateAvailable.String())
	assert.Equal(t, "reserved", store.StateReserved.String())
	assert.Equal(t, "quarantined", store.StateQuarantined.String())
	assert.Equal(t, "spent", store.StateSpent.String())
	assert.Equal(t, "invalid", store.StateInvalid.String())
	assert.Equal(t, "state(9)", store.State(9).String())
	assert.True(t, store.StateSpent.Terminal())
	assert.True(t, store.StateInvalid.Terminal())
	assert.False(t, store.StateQuarantined.Terminal())

	assert.Equal(t, "unmined", store.ShardUnmined.String())
	assert.Equal(t, "mined", store.ShardMined.String())
	assert.Equal(t, "retired", store.ShardRetired.String())
	assert.Equal(t, "invalid", store.ShardInvalid.String())
	assert.True(t, store.ShardInvalid.Terminal())
	assert.True(t, store.ShardRetired.Terminal())
	assert.False(t, store.ShardMined.Terminal())

	assert.Equal(t, "puzzle", store.LockPuzzle.String())
	assert.Equal(t, "p2pkh", store.LockP2PKH.String())
}
