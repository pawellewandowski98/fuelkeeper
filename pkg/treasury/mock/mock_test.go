package mock_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bsv-blockchain/go-sdk/transaction"
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
	require.NoError(t, m.Send(ctx, built.Shard.TxID, built.Shard.BEEF))
	assert.True(t, m.Sent(built.Shard.TxID))
	assert.Equal(t, built.Shard.BEEF, m.LastSendBEEF(built.Shard.TxID))
	require.NoError(t, m.Send(ctx, built.Shard.TxID, nil), "send is idempotent; beef is optional")
	assert.Equal(t, 2, m.Sends(built.Shard.TxID))
	assert.Nil(t, m.LastSendBEEF(built.Shard.TxID))
	require.NoError(t, m.Send(ctx, built.Rows[0].TxID, nil), "rows and shard agree on the txid")
	require.Error(t, m.Send(ctx, built.Shard.TxID, []byte("junk")), "a malformed beef is refused")
	assert.Equal(t, 3, m.Sends(built.Shard.TxID), "a refused send is not counted")
	require.ErrorIs(t, m.Send(ctx, store.Outpoint{}.TxID, nil), treasury.ErrUnknownTx)

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
	require.ErrorIs(t, m.Send(ctx, built2.Shard.TxID, built2.Shard.BEEF), treasury.ErrRejected)
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

// TestMockBEEFIsRealSplit checks that the mock's shard BEEF is a real,
// parseable BEEF whose transaction is the split and carries the rows' outputs,
// so client.InputBEEF and fuel.RawTxFromBEEF work without a real wallet.
func TestMockBEEFIsRealSplit(t *testing.T) {
	ctx := context.Background()
	m := mock.New()
	built, err := m.Build(ctx, treasury.MintRequest{Pool: "main", Denomination: 50, Count: 3})
	require.NoError(t, err)

	raw, err := fuel.RawTxFromBEEF(built.Shard.BEEF, built.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, built.RawTx, raw)
	tx, err := transaction.NewTransactionFromBytes(raw)
	require.NoError(t, err)
	assert.Equal(t, built.Shard.TxID, *tx.TxID())
	require.Len(t, tx.Outputs, len(built.Rows), "no change output")
	for i, r := range built.Rows {
		assert.Equal(t, r.Satoshis, tx.Outputs[i].Satoshis)
		assert.Equal(t, r.LockingScript, []byte(*tx.Outputs[i].LockingScript))
	}

	merged := transaction.NewBeefV2()
	require.NoError(t, merged.MergeBeefBytes(built.Shard.BEEF), "the BEEF merges the way client.InputBEEF merges it")
	assert.NotNil(t, merged.FindTransaction(built.Shard.TxID.String()))
}

// TestMockMinedBEEFIsRealBEEF guards the nullnet bug where a mined shard's
// stored BEEF became an unparseable "mock-mined-beef-" marker: MinedBEEF must
// delegate to fuel.MinedBEEF and return real, parseable BEEF whose BUMP
// (built by SingleTxMerklePath, the same helper cmd/fuelkeeper/serve.go's
// nullnet hook uses) proves the split tx at the reported height, with the
// rows' locking scripts untouched.
func TestMockMinedBEEFIsRealBEEF(t *testing.T) {
	ctx := context.Background()
	m := mock.New()
	built, err := m.Build(ctx, treasury.MintRequest{Pool: "main", Denomination: 50, Count: 2})
	require.NoError(t, err)
	require.NoError(t, m.Send(ctx, built.Shard.TxID, built.Shard.BEEF))

	const height = 42
	mp := mock.SingleTxMerklePath(built.Shard.TxID, height)
	minedBEEF, err := m.MinedBEEF(ctx, built.Shard.TxID, built.Shard.BEEF, mp)
	require.NoError(t, err)

	parsed, err := transaction.NewBeefFromBytes(minedBEEF)
	require.NoError(t, err, "a mined shard's BEEF must stay parseable by a real wallet")
	tx := parsed.FindTransaction(built.Shard.TxID.String())
	require.NotNil(t, tx)
	require.NotNil(t, tx.MerklePath, "mined BEEF carries the bump")
	assert.Equal(t, uint32(height), tx.MerklePath.BlockHeight)
	root, err := tx.MerklePath.ComputeRoot(&built.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, built.Shard.TxID.String(), root.String())

	assert.Equal(t, built.RawTx, tx.Bytes())
	require.Len(t, tx.Outputs, len(built.Rows), "no change output")
	for i, r := range built.Rows {
		assert.Equal(t, r.LockingScript, []byte(*tx.Outputs[i].LockingScript), "row %d locking script is intact", i)
	}

	// FailMinedBEEF still short-circuits before fuel.MinedBEEF runs.
	injected := errors.New("bad bump")
	m.FailMinedBEEF(injected)
	_, err = m.MinedBEEF(ctx, built.Shard.TxID, built.Shard.BEEF, mp)
	assert.ErrorIs(t, err, injected)
}

// TestSingleTxMerklePathBYTES verifies that SingleTxMerklePath produces
// parseable BRC-74 (BUMP) bytes that can be decoded by go-sdk and contain
// the expected merkle path structure for a single-transaction block.
func TestSingleTxMerklePathBytes(t *testing.T) {
	ctx := context.Background()
	m := mock.New()
	built, err := m.Build(ctx, treasury.MintRequest{Pool: "main", Denomination: 50, Count: 1})
	require.NoError(t, err)

	const height = 42
	bumpBytes := mock.SingleTxMerklePath(built.Shard.TxID, height)
	require.NotEmpty(t, bumpBytes)

	// Parse the BUMP bytes with go-sdk
	mp, err := transaction.NewMerklePathFromBinary(bumpBytes)
	require.NoError(t, err, "SingleTxMerklePath must produce parseable BRC-74 BUMP bytes")

	// Assert BlockHeight matches
	assert.Equal(t, uint32(height), mp.BlockHeight, "BlockHeight must match the requested height")

	// Assert exactly one level with exactly one leaf
	require.Len(t, mp.Path, 1, "single-tx block must have exactly one level")
	require.Len(t, mp.Path[0], 1, "single-tx block must have exactly one leaf at level 0")

	leaf := mp.Path[0][0]

	// Assert Offset is 0
	assert.Equal(t, uint64(0), leaf.Offset, "leaf offset must be 0")

	// Assert Hash equals txid
	require.NotNil(t, leaf.Hash, "leaf hash must be set")
	assert.Equal(t, built.Shard.TxID, *leaf.Hash, "leaf hash must be the txid")

	// Assert Txid flag is true
	require.NotNil(t, leaf.Txid, "Txid flag must be set")
	assert.True(t, *leaf.Txid, "Txid flag must be true")

	// Assert ComputeRoot(&txid) equals txid
	root, err := mp.ComputeRoot(&built.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, built.Shard.TxID, *root, "ComputeRoot must equal the txid for a single-tx block")
}
