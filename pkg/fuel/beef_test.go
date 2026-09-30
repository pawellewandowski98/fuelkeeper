package fuel_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
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

// TestMinedBEEF_RejectsWellFormedBUMPForOtherTx guards the membership check in
// forceTxidLeaf: a syntactically valid, well-formed BUMP whose leaf names some
// other transaction must be rejected outright, even though nothing about its
// binary encoding is malformed. Without an explicit hash comparison this slips
// through, because (*MerklePath).ComputeRoot's single-leaf shortcut returns
// whatever txid it is asked about as the root and never errors.
func TestMinedBEEF_RejectsWellFormedBUMPForOtherTx(t *testing.T) {
	tx := newSplitTx(t)
	txid := tx.TxID()
	unminedBytes := unminedBEEFOf(t, tx)

	other := chainhash.HashH([]byte("other"))
	mp := transaction.NewMerklePath(100, [][]*transaction.PathElement{{{Offset: 0, Hash: &other, Txid: boolPtr(true)}}})

	_, err := fuel.MinedBEEF(unminedBytes, *txid, mp.Bytes())
	require.ErrorIs(t, err, fuel.ErrTxNotInMerklePath)
}

// TestMinedBEEF_VerifiesAgainstChainTracker proves the mined BEEF actually gets
// SPV-checked against a chain tracker end to end, using a realistic two-leaf
// BUMP (a sibling hash alongside the split's txid) rather than the trivial
// single-leaf shortcut case.
func TestMinedBEEF_VerifiesAgainstChainTracker(t *testing.T) {
	tx := newSplitTx(t)
	txid := tx.TxID()
	unminedBytes := unminedBEEFOf(t, tx)
	sibling := chainhash.HashH([]byte("sibling"))
	ctx := context.Background()

	// buildMined assembles a two-leaf BUMP (txid at offset 0, sibling at offset
	// 1) with the given Txid flag on the txid leaf, mines it via MinedBEEF, and
	// returns the resulting BEEF bytes plus the root that BUMP proves.
	buildMined := func(t *testing.T, txidFlag *bool) ([]byte, *chainhash.Hash) {
		t.Helper()
		mp := transaction.NewMerklePath(100, [][]*transaction.PathElement{{
			{Offset: 0, Hash: txid, Txid: txidFlag},
			{Offset: 1, Hash: &sibling},
		}})
		root, err := mp.ComputeRoot(txid)
		require.NoError(t, err)
		mined, err := fuel.MinedBEEF(unminedBytes, *txid, mp.Bytes())
		require.NoError(t, err)
		return mined, root
	}

	t.Run("accepted by tracker", func(t *testing.T) {
		mined, root := buildMined(t, boolPtr(true))
		parsed, err := transaction.NewBeefFromBytes(mined)
		require.NoError(t, err)

		tracker := &fakeChainTracker{root: *root, height: 100, accept: true}
		ok, err := parsed.Verify(ctx, tracker, false)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, 1, tracker.calls)
	})

	t.Run("rejected by tracker", func(t *testing.T) {
		mined, _ := buildMined(t, boolPtr(true))
		parsed, err := transaction.NewBeefFromBytes(mined)
		require.NoError(t, err)

		tracker := &fakeChainTracker{height: 100, accept: false}
		ok, err := parsed.Verify(ctx, tracker, false)
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Equal(t, 1, tracker.calls)
	})

	t.Run("txid flag forced even when absent on input", func(t *testing.T) {
		// The input BUMP's matching leaf has no Txid flag set; MinedBEEF must
		// force it so Verify still folds this height's root into the set it
		// checks, instead of silently skipping the tracker call altogether.
		mined, root := buildMined(t, nil)
		parsed, err := transaction.NewBeefFromBytes(mined)
		require.NoError(t, err)

		tracker := &fakeChainTracker{root: *root, height: 100, accept: true}
		ok, err := parsed.Verify(ctx, tracker, false)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, 1, tracker.calls, "Verify must consult the tracker even though the input BUMP left Txid unset")
	})
}

// newSplitTx returns a synthetic single-output split tx (no inputs, fine for
// BEEF framing).
func newSplitTx(t *testing.T) *transaction.Transaction {
	t.Helper()
	tx := transaction.NewTransaction()
	tx.AddOutput(&transaction.TransactionOutput{
		Satoshis:      50,
		LockingScript: scriptOf(fuel.PuzzleLock(fuel.PuzzleHash(fuel.Preimage(fuel.NewSeed(), 0)))),
	})
	return tx
}

// unminedBEEFOf wraps tx's raw bytes in a bump-less BEEF v2, as the treasury
// stores it before the split is mined.
func unminedBEEFOf(t *testing.T, tx *transaction.Transaction) []byte {
	t.Helper()
	unmined := transaction.NewBeefV2()
	_, err := unmined.MergeRawTx(tx.Bytes(), nil)
	require.NoError(t, err)
	b, err := unmined.Bytes()
	require.NoError(t, err)
	return b
}

// fakeChainTracker is a minimal chaintracker.ChainTracker for tests: it
// accepts exactly the configured root at the configured height (when accept
// is true) and counts how many times it was consulted.
type fakeChainTracker struct {
	root   chainhash.Hash
	height uint32
	accept bool
	calls  int
}

func (f *fakeChainTracker) IsValidRootForHeight(_ context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	f.calls++
	if !f.accept {
		return false, nil
	}
	return height == f.height && root.Equal(f.root), nil
}

func (f *fakeChainTracker) CurrentHeight(_ context.Context) (uint32, error) {
	return f.height, nil
}

func scriptOf(b []byte) *script.Script { s := script.Script(b); return &s }
func boolPtr(b bool) *bool             { return &b }
