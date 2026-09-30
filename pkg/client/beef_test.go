package client_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"
	"github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// realShard builds a shard whose BEEF is a genuine BEEF v2 of a synthetic split tx.
func realShard(t *testing.T, st store.Store, n int) (store.Shard, []store.Row) { //nolint:unparam // rows is a general-purpose helper result; unused here, but a caller inspecting row content may want it
	t.Helper()
	tx := transaction.NewTransaction()
	seed := fuel.NewSeed()
	rows := make([]store.Row, n)
	for i := 0; i < n; i++ {
		pre := fuel.Preimage(seed, uint32(i)) //nolint:gosec // small
		lock := fuel.PuzzleLock(fuel.PuzzleHash(pre))
		s := script.Script(lock)
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: 50, LockingScript: &s})
		rows[i] = store.Row{Pool: "main", Satoshis: 50, LockingScript: lock, UnlockingScript: fuel.PuzzleUnlock(pre)}
	}
	txid := *tx.TxID()
	for i := range rows {
		rows[i].Outpoint = store.Outpoint{TxID: txid, Vout: uint32(i)} //nolint:gosec // small
	}
	beef := transaction.NewBeefV2()
	_, err := beef.MergeRawTx(tx.Bytes(), nil)
	require.NoError(t, err)
	bytes, err := beef.Bytes()
	require.NoError(t, err)
	sh := store.Shard{Pool: "main", TxID: txid, Size: n, Denomination: 50, LockMode: store.LockPuzzle, Status: store.ShardMined, BEEF: bytes}
	require.NoError(t, st.PutShard(context.Background(), sh, rows))
	return sh, rows
}

func TestInputBEEFAndCreateActionInputs(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := startAPI(t, nil)
	a, _ := realShard(t, st, 2)
	b, _ := realShard(t, st, 2)
	c, err := client.New(srv.URL)
	require.NoError(t, err)

	// Each shard has only 2 rows, and pool.Pool's synchronous adoption
	// (pool.ShardManager.EnsureRows) grabs at most one new shard per Reserve
	// call before falling back to min_count/insufficient, so a single
	// Count:4 call cannot deterministically span both shards. Two Count:2
	// calls each adopt one shard instead: the first shard's lease is still
	// held by this instance when the second call runs, so its adoption picks
	// the other, unexhausted shard. Merge the two responses for the
	// multi-shard checks below.
	res1, err := c.Reserve(ctx, "main", client.ReserveRequest{Count: 2, MinCount: 2})
	require.NoError(t, err)
	res2, err := c.Reserve(ctx, "main", client.ReserveRequest{Count: 2, MinCount: 2})
	require.NoError(t, err)
	require.NotEqual(t, res1.Shards[0].TxID, res2.Shards[0].TxID, "each call should adopt a distinct shard")
	res := &wire.ReserveResponse{
		LockMode: res1.LockMode,
		Outputs:  append(res1.Outputs, res2.Outputs...),
		Shards:   append(res1.Shards, res2.Shards...),
	}
	require.Len(t, res.Shards, 2)

	beef, err := c.InputBEEF(ctx, "main", res)
	require.NoError(t, err)
	parsed, err := transaction.NewBeefFromBytes(beef)
	require.NoError(t, err)
	assert.NotNil(t, parsed.FindTransaction(a.TxID.String()))
	assert.NotNil(t, parsed.FindTransaction(b.TxID.String()))

	inputs, err := client.CreateActionInputs(res, "fuel input")
	require.NoError(t, err)
	require.Len(t, inputs, 4)
	for i, in := range inputs {
		assert.Equal(t, res.Outputs[i].Outpoint, in.Outpoint.Txid.String()+":"+fmt.Sprint(in.Outpoint.Index))
		assert.Equal(t, "fuel input", in.InputDescription)
		want, _ := hex.DecodeString(res.Outputs[i].Unlock.Script)
		assert.Equal(t, want, in.UnlockingScript)
		assert.Equal(t, uint32(len(want)), in.UnlockingScriptLength) //nolint:gosec // small
		assert.Nil(t, in.SequenceNumber, "puzzle mode leaves the sequence at the wallet default")
	}

	// P2PKH-mode constraints pin the sequence number.
	p2 := *res
	p2.Constraints = &wire.Constraints{TxVersion: 1, LockTime: 0, Sequence: 0xffffffff}
	inputs, err = client.CreateActionInputs(&p2, "fuel input")
	require.NoError(t, err)
	require.NotNil(t, inputs[0].SequenceNumber)
	assert.Equal(t, uint32(0xffffffff), *inputs[0].SequenceNumber)

	bad := *res
	bad.Outputs = []wire.Output{{Outpoint: "nope", Unlock: wire.Unlock{Script: "00"}}}
	_, err = client.CreateActionInputs(&bad, "fuel input")
	require.Error(t, err)

	dup := *res
	dup.Outputs = []wire.Output{res.Outputs[0], res.Outputs[0]}
	_, err = client.CreateActionInputs(&dup, "fuel input")
	require.Error(t, err, "a duplicate outpoint must be rejected")

	_, err = client.CreateActionInputs(res, "fuel")
	require.Error(t, err, "a description shorter than 5 bytes must be rejected")
	_, err = client.CreateActionInputs(res, strings.Repeat("x", 2001))
	require.Error(t, err, "a description longer than 2000 bytes must be rejected")

	assert.Equal(t, uint64(50-8), client.Surplus(50, 80, 100), "fee for 80 B at 100 sat/kB is 8 sat")
	assert.Equal(t, uint64(0), client.Surplus(5, 80, 100), "never negative")
	assert.Equal(t, 74, client.PuzzleInputSize)
}

func TestApplyConstraints(t *testing.T) {
	res := &wire.ReserveResponse{}
	args := &wallet.CreateActionArgs{}
	client.ApplyConstraints(args, res)
	assert.Nil(t, args.Version, "puzzle mode (no constraints) leaves Version unset")
	assert.Nil(t, args.LockTime, "puzzle mode (no constraints) leaves LockTime unset")

	res.Constraints = &wire.Constraints{TxVersion: 1, LockTime: 0, Sequence: 0xffffffff}
	client.ApplyConstraints(args, res)
	require.NotNil(t, args.Version)
	require.NotNil(t, args.LockTime)
	assert.Equal(t, uint32(1), *args.Version)
	assert.Equal(t, uint32(0), *args.LockTime)
}

// fakeTracker is a minimal chaintracker.ChainTracker stub for Beef.Verify
// tests: it accepts exactly one (root, height) pair.
type fakeTracker struct {
	root   *chainhash.Hash
	height uint32
}

func (f fakeTracker) IsValidRootForHeight(_ context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	return height == f.height && root.Equal(*f.root), nil
}

func (f fakeTracker) CurrentHeight(_ context.Context) (uint32, error) { return f.height, nil }

var _ chaintracker.ChainTracker = fakeTracker{}

// minedShardWithBump builds (and inserts) a one-row mined shard whose BEEF
// carries a real BUMP (MerklePath) proving its single transaction at
// blockHeight, mirroring realShard but with a Merkle proof attached.
func minedShardWithBump(t *testing.T, st store.Store, blockHeight uint32) (store.Shard, *transaction.MerklePath) {
	t.Helper()
	tx := transaction.NewTransaction()
	pre := fuel.Preimage(fuel.NewSeed(), 0)
	lock := fuel.PuzzleLock(fuel.PuzzleHash(pre))
	s := script.Script(lock)
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 50, LockingScript: &s})
	txid := tx.TxID()

	isTxid := true
	mp := &transaction.MerklePath{
		BlockHeight: blockHeight,
		Path:        [][]*transaction.PathElement{{{Offset: 0, Hash: txid, Txid: &isTxid}}},
	}
	beef := transaction.NewBeefV2()
	idx := beef.MergeBump(mp)
	_, err := beef.MergeRawTx(tx.Bytes(), &idx)
	require.NoError(t, err)
	raw, err := beef.Bytes()
	require.NoError(t, err)

	sh := store.Shard{Pool: "main", TxID: *txid, Size: 1, Denomination: 50, LockMode: store.LockPuzzle, Status: store.ShardMined, BEEF: raw}
	rows := []store.Row{{
		Outpoint:        store.Outpoint{TxID: *txid, Vout: 0},
		Pool:            "main",
		Satoshis:        50,
		LockingScript:   lock,
		UnlockingScript: fuel.PuzzleUnlock(pre),
	}}
	require.NoError(t, st.PutShard(context.Background(), sh, rows))
	return sh, mp
}

func TestInputBEEFPreservesBUMPForVerify(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := startAPI(t, nil)
	sh, mp := minedShardWithBump(t, st, 100)

	c, err := client.New(srv.URL)
	require.NoError(t, err)
	res, err := c.Reserve(ctx, "main", client.ReserveRequest{Count: 1})
	require.NoError(t, err)

	beef, err := c.InputBEEF(ctx, "main", res)
	require.NoError(t, err)
	parsed, err := transaction.NewBeefFromBytes(beef)
	require.NoError(t, err)
	found := parsed.FindTransaction(sh.TxID.String())
	require.NotNil(t, found)
	require.NotNil(t, found.MerklePath, "the shard's BUMP must survive the InputBEEF merge")

	root, err := mp.ComputeRoot(&sh.TxID)
	require.NoError(t, err)
	ok, err := parsed.Verify(ctx, fakeTracker{root: root, height: 100}, false)
	require.NoError(t, err)
	assert.True(t, ok, "a mined BEEF's BUMP should verify against a chain tracker that recognizes its root")
}

func TestInputBEEFDedupesRepeatedShard(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := startAPI(t, nil)
	a, _ := realShard(t, st, 2)
	c, err := client.New(srv.URL)
	require.NoError(t, err)

	res, err := c.Reserve(ctx, "main", client.ReserveRequest{Count: 2})
	require.NoError(t, err)
	require.Len(t, res.Shards, 1)

	dup := &wire.ReserveResponse{Shards: append(append([]wire.Shard{}, res.Shards...), res.Shards...)}
	beef, err := c.InputBEEF(ctx, "main", dup)
	require.NoError(t, err)
	parsed, err := transaction.NewBeefFromBytes(beef)
	require.NoError(t, err)
	require.NotNil(t, parsed.FindTransaction(a.TxID.String()))
	assert.Len(t, parsed.Transactions, 1, "the same shard referenced twice must be merged once, not duplicated")
	assert.Equal(t, 1, c.BEEFCacheMisses(), "the repeated reference should be served from cache, not a new fetch")
}

func TestInputBEEFMergesV1Input(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := startAPI(t, nil)

	tx := transaction.NewTransaction()
	pre := fuel.Preimage(fuel.NewSeed(), 0)
	lock := fuel.PuzzleLock(fuel.PuzzleHash(pre))
	s := script.Script(lock)
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 50, LockingScript: &s})
	txid := tx.TxID()

	v1BEEF, err := tx.BEEF()
	require.NoError(t, err)

	sh := store.Shard{Pool: "main", TxID: *txid, Size: 1, Denomination: 50, LockMode: store.LockPuzzle, Status: store.ShardMined, BEEF: v1BEEF}
	rows := []store.Row{{
		Outpoint:        store.Outpoint{TxID: *txid, Vout: 0},
		Pool:            "main",
		Satoshis:        50,
		LockingScript:   lock,
		UnlockingScript: fuel.PuzzleUnlock(pre),
	}}
	require.NoError(t, st.PutShard(ctx, sh, rows))

	c, err := client.New(srv.URL)
	require.NoError(t, err)
	res, err := c.Reserve(ctx, "main", client.ReserveRequest{Count: 1})
	require.NoError(t, err)

	beef, err := c.InputBEEF(ctx, "main", res)
	require.NoError(t, err)
	parsed, err := transaction.NewBeefFromBytes(beef)
	require.NoError(t, err)
	assert.NotNil(t, parsed.FindTransaction(txid.String()), "a BEEF v1 shard (tx.BEEF()) must merge into the v2 input BEEF")
}
