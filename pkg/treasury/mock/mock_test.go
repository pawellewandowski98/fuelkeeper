package mock_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury/mock"
)

func TestMockBuildAndSend(t *testing.T) {
	ctx := context.Background()
	m := mock.New()

	built, err := m.Build(ctx, treasury.MintRequest{Pool: "main", Denomination: 50, Count: 4, LockMode: store.LockPuzzle})
	require.NoError(t, err)
	require.Len(t, built.Rows, 4)
	assert.Equal(t, 4, built.Shard.Size)
	assert.Equal(t, uint64(50), built.Shard.Denomination)
	assert.Equal(t, store.ShardUnmined, built.Shard.Status)
	assert.Equal(t, "main", built.Shard.Pool)
	assert.NotEmpty(t, built.Shard.BEEF)
	assert.NotEmpty(t, built.RawTx)
	for i, r := range built.Rows {
		assert.Equal(t, built.Shard.TxID, r.TxID)
		assert.Equal(t, uint32(i), r.Vout)
		assert.Equal(t, "main", r.Pool)
		assert.Equal(t, uint64(50), r.Satoshis)
		assert.True(t, fuel.VerifyPuzzle(r.LockingScript, r.UnlockingScript), "row %d unlocks its own lock", i)
	}
	require.NoError(t, store.ValidatePutShard(built.Shard, built.Rows))

	assert.False(t, m.Sent(built.Shard.TxID))
	require.NoError(t, m.Send(ctx, built.Shard.TxID))
	assert.True(t, m.Sent(built.Shard.TxID))
	require.NoError(t, m.Send(ctx, built.Shard.TxID), "send is idempotent")
	assert.Equal(t, 2, m.Sends(built.Shard.TxID))
	require.NoError(t, m.Send(ctx, built.Rows[0].TxID), "rows and shard agree on the txid")
	require.ErrorIs(t, m.Send(ctx, store.Outpoint{}.TxID), treasury.ErrUnknownTx)

	_, err = m.Build(ctx, treasury.MintRequest{Pool: "main", Denomination: 50, Count: 0})
	require.ErrorIs(t, err, treasury.ErrInvalidRequest)
	_, err = m.Build(ctx, treasury.MintRequest{Pool: "", Denomination: 50, Count: 1})
	require.ErrorIs(t, err, treasury.ErrInvalidRequest)
	_, err = m.Build(ctx, treasury.MintRequest{Pool: "main", Denomination: 50, Count: 1, LockMode: store.LockP2PKH})
	require.ErrorIs(t, err, treasury.ErrUnsupportedLockMode, "mock only mints puzzles")

	m.FailBuild(treasury.ErrInsufficientFunds)
	_, err = m.Build(ctx, treasury.MintRequest{Pool: "main", Denomination: 50, Count: 1})
	require.ErrorIs(t, err, treasury.ErrInsufficientFunds)
	m.FailBuild(nil)

	built2, err := m.Build(ctx, treasury.MintRequest{Pool: "main", Denomination: 50, Count: 1})
	require.NoError(t, err)
	m.FailSend(treasury.ErrRejected)
	require.ErrorIs(t, m.Send(ctx, built2.Shard.TxID), treasury.ErrRejected)
	assert.False(t, m.Sent(built2.Shard.TxID), "a failed send is not counted")
	m.FailSend(nil)
	assert.Equal(t, 2, m.Built(), "two successful builds")
	assert.NotEqual(t, built.Shard.TxID, built2.Shard.TxID)
}

// TestMockCrossShardIndependence checks that two builds use independent seeds:
// an unlocking script minted for one shard must not satisfy a locking script
// minted for a different shard.
func TestMockCrossShardIndependence(t *testing.T) {
	ctx := context.Background()
	m := mock.New()

	a, err := m.Build(ctx, treasury.MintRequest{Pool: "main", Denomination: 50, Count: 1})
	require.NoError(t, err)
	b, err := m.Build(ctx, treasury.MintRequest{Pool: "main", Denomination: 50, Count: 1})
	require.NoError(t, err)

	assert.False(t, fuel.VerifyPuzzle(a.Rows[0].LockingScript, b.Rows[0].UnlockingScript),
		"b's unlocking script must not satisfy a's locking script")
}
