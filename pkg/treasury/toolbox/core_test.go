package toolbox_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/script/interpreter"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/brc29"
	pkgerrors "github.com/bsv-blockchain/go-wallet-toolbox/pkg/errors"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/wdk/primitives"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury/toolbox"
)

const testRootKeyHex = "0000000000000000000000000000000000000000000000000000000000000001"

func rootKey(t *testing.T) *ec.PrivateKey {
	t.Helper()
	k, err := ec.PrivateKeyFromHex(testRootKeyHex)
	require.NoError(t, err)
	return k
}

// beefMode selects what the fake wallet returns as a split's BEEF.
type beefMode int

const (
	beefValid    beefMode = iota // parent with a BUMP, then the split
	beefNoParent                 // the split alone: its input does not trace to a BUMP
	beefGarbage                  // not a BEEF at all
)

// fakeWallet is a WalletAPI that builds real split transactions (funded by a
// parent with a BUMP, plus one trailing change output) and models the
// toolbox's SendWith, abort and list behaviour.
type fakeWallet struct {
	mu sync.Mutex

	creates   []sdk.CreateActionArgs
	sendWith  [][]chainhash.Hash
	aborts    []sdk.AbortActionArgs
	abortCtx  []error // ctx.Err() seen by each AbortAction
	internals []sdk.InternalizeActionArgs
	listArgs  []sdk.ListActionsArgs
	built     []chainhash.Hash // txids of splits created

	fail       error // every non-SendWith CreateAction fails with this
	onCreate   func()
	tamper     func(tx *transaction.Transaction)
	mode       beefMode
	zeroTxid   bool
	sendFail   error                                     // SendWith CreateActions fail with this
	sendStatus map[chainhash.Hash]sdk.ActionResultStatus // default unproven
	omitResult bool                                      // SendWith returns no result for the txid
	unknown    map[chainhash.Hash]bool                   // SendWith: no known-tx record
	abortFail  map[chainhash.Hash]error
	actions    []sdk.Action
	pageCap    int
	listErr    error

	internalErr error
	rejectPay   bool
	identity    *ec.PublicKey
}

func newFakeWallet(t *testing.T) *fakeWallet {
	t.Helper()
	return &fakeWallet{
		identity:   rootKey(t).PubKey(),
		sendStatus: map[chainhash.Hash]sdk.ActionResultStatus{},
		unknown:    map[chainhash.Hash]bool{},
		abortFail:  map[chainhash.Hash]error{},
	}
}

func p2pkhLikeScript(b byte) []byte {
	s := append([]byte{0x76, 0xa9, 0x14}, bytes.Repeat([]byte{b}, 20)...)
	return append(s, 0x88, 0xac)
}

func (f *fakeWallet) CreateAction(_ context.Context, args sdk.CreateActionArgs, _ string) (*sdk.CreateActionResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, args)
	if args.Options != nil && len(args.Options.SendWith) > 0 {
		return f.sendWithLocked(args.Options.SendWith)
	}
	if f.fail != nil {
		return nil, f.fail
	}
	if f.onCreate != nil {
		f.onCreate()
	}
	parent := transaction.NewTransaction()
	src := storetest.NewTxID()
	parent.AddInput(&transaction.TransactionInput{SourceTXID: &src, SequenceNumber: 0xffffffff, UnlockingScript: script.NewFromBytes([]byte{0x51})})
	parent.AddOutput(&transaction.TransactionOutput{Satoshis: 100_000_000, LockingScript: script.NewFromBytes(p2pkhLikeScript(0x33))})

	tx := transaction.NewTransaction()
	tx.AddInput(&transaction.TransactionInput{SourceTXID: parent.TxID(), SourceTxOutIndex: 0, SequenceNumber: 0xffffffff, UnlockingScript: script.NewFromBytes([]byte{0x51})})
	for _, o := range args.Outputs {
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: o.Satoshis, LockingScript: script.NewFromBytes(o.LockingScript)})
	}
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 1234, LockingScript: script.NewFromBytes(p2pkhLikeScript(0x11))})
	if f.tamper != nil {
		f.tamper(tx)
	}
	txid := *tx.TxID()
	f.built = append(f.built, txid)
	if f.zeroTxid {
		return &sdk.CreateActionResult{}, nil
	}
	var beef []byte
	switch f.mode {
	case beefGarbage:
		beef = []byte("junk")
	case beefNoParent:
		b := transaction.NewBeefV2()
		if _, err := b.MergeRawTx(tx.Bytes(), nil); err != nil {
			return nil, err
		}
		var err error
		if beef, err = b.AtomicBytes(&txid); err != nil {
			return nil, err
		}
	default:
		var err error
		if beef, err = beefWithParent(parent, tx); err != nil {
			return nil, err
		}
	}
	return &sdk.CreateActionResult{Txid: txid, Tx: beef}, nil
}

// beefWithParent returns the atomic BEEF of tx with its mined parent.
func beefWithParent(parent, tx *transaction.Transaction) ([]byte, error) {
	isTxid := true
	mp := transaction.NewMerklePath(100, [][]*transaction.PathElement{{{Offset: 0, Hash: parent.TxID(), Txid: &isTxid}}})
	b := transaction.NewBeefV2()
	idx := b.MergeBump(mp)
	if _, err := b.MergeRawTx(parent.Bytes(), &idx); err != nil {
		return nil, err
	}
	if _, err := b.MergeRawTx(tx.Bytes(), nil); err != nil {
		return nil, err
	}
	return b.AtomicBytes(tx.TxID())
}

func (f *fakeWallet) sendWithLocked(ids []chainhash.Hash) (*sdk.CreateActionResult, error) {
	f.sendWith = append(f.sendWith, append([]chainhash.Hash(nil), ids...))
	if f.sendFail != nil {
		return nil, f.sendFail
	}
	res := &sdk.CreateActionResult{}
	for _, id := range ids {
		if f.unknown[id] {
			return nil, fmt.Errorf("create action failed: failed to process created action: known tx status for txID %s not found", id)
		}
		if f.omitResult {
			continue
		}
		st, ok := f.sendStatus[id]
		if !ok {
			st = sdk.ActionResultStatusUnproven
		}
		res.SendWithResults = append(res.SendWithResults, sdk.SendWithResult{Txid: id, Status: st})
	}
	return res, nil
}

func (f *fakeWallet) AbortAction(ctx context.Context, args sdk.AbortActionArgs, _ string) (*sdk.AbortActionResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.aborts = append(f.aborts, args)
	f.abortCtx = append(f.abortCtx, ctx.Err())
	id, err := chainhash.NewHashFromHex(string(args.Reference))
	if err == nil {
		if e := f.abortFail[*id]; e != nil {
			return nil, e
		}
	}
	return &sdk.AbortActionResult{Aborted: true}, nil
}

func (f *fakeWallet) ListActions(_ context.Context, args sdk.ListActionsArgs, _ string) (*sdk.ListActionsResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listArgs = append(f.listArgs, args)
	if f.listErr != nil {
		return nil, f.listErr
	}
	off := int(*args.Offset)
	n := int(*args.Limit)
	if f.pageCap > 0 && f.pageCap < n {
		n = f.pageCap
	}
	end := min(off+n, len(f.actions))
	var page []sdk.Action
	if off < len(f.actions) {
		page = f.actions[off:end]
	}
	return &sdk.ListActionsResult{TotalActions: uint32(len(f.actions)), Actions: page}, nil //nolint:gosec // tiny
}

func (f *fakeWallet) InternalizeAction(_ context.Context, args sdk.InternalizeActionArgs, _ string) (*sdk.InternalizeActionResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.internals = append(f.internals, args)
	if f.internalErr != nil {
		return nil, f.internalErr
	}
	return &sdk.InternalizeActionResult{Accepted: !f.rejectPay}, nil
}

func (f *fakeWallet) GetPublicKey(_ context.Context, args sdk.GetPublicKeyArgs, _ string) (*sdk.GetPublicKeyResult, error) {
	if !args.IdentityKey {
		return nil, errors.New("fake: only the identity key is supported")
	}
	return &sdk.GetPublicKeyResult{PublicKey: f.identity}, nil
}

func (f *fakeWallet) sendWithCalls() [][]chainhash.Hash {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]chainhash.Hash(nil), f.sendWith...)
}

func (f *fakeWallet) abortRefs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.aborts))
	for _, a := range f.aborts {
		out = append(out, string(a.Reference))
	}
	return out
}

func (f *fakeWallet) lastBuilt() chainhash.Hash {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.built[len(f.built)-1]
}

func (f *fakeWallet) lastCreate() sdk.CreateActionArgs {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.creates[len(f.creates)-1]
}

type fakeBroadcaster struct {
	mu    sync.Mutex
	txids []string
	beefs [][]byte
	err   error
}

func (b *fakeBroadcaster) PostBEEF(_ context.Context, beef []byte, txid string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.txids = append(b.txids, txid)
	b.beefs = append(b.beefs, beef)
	return b.err
}

func (b *fakeBroadcaster) calls() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.txids...)
}

const (
	goodNonce = "bm9uY2U="
	// slashNonce uses '+' and '/', so it decodes only as standard base64.
	slashNonce = "+/+/"
)

type fakeNonces struct{ err error }

func (fakeNonces) Create(context.Context) (string, error) { return goodNonce, nil }
func (n fakeNonces) Verify(_ context.Context, s string) (bool, error) {
	if n.err != nil {
		return false, n.err
	}
	return s == goodNonce || s == slashNonce, nil
}

type fakeKeeper struct {
	calls int
	err   error
}

func (k *fakeKeeper) RunOnce(context.Context) error {
	k.calls++
	return k.err
}

func newCoreWith(t *testing.T, w toolbox.WalletAPI, bc toolbox.Broadcaster, keeper toolbox.ChunkKeeper, nonces toolbox.Nonces) *toolbox.Core {
	t.Helper()
	cfg := toolbox.DefaultConfig("test")
	cfg.RootKeyHex = testRootKeyHex
	return toolbox.NewCore(w, bc, keeper, nonces, sdk.NewKeyDeriver(rootKey(t)), cfg, nil)
}

func newCore(t *testing.T, w toolbox.WalletAPI, bc toolbox.Broadcaster) *toolbox.Core {
	t.Helper()
	return newCoreWith(t, w, bc, nil, fakeNonces{})
}

func puzzleReq(n int) treasury.MintRequest {
	return treasury.MintRequest{Pool: "main", Denomination: 50, Count: n, LockMode: store.LockPuzzle}
}

func TestBuildPuzzleSplit(t *testing.T) {
	ctx := context.Background()
	w := newFakeWallet(t)
	c := newCore(t, w, &fakeBroadcaster{})

	b, err := c.Build(ctx, puzzleReq(3))
	require.NoError(t, err)

	args := w.lastCreate()
	require.NotNil(t, args.Options)
	require.NotNil(t, args.Options.NoSend)
	assert.True(t, *args.Options.NoSend)
	require.NotNil(t, args.Options.RandomizeOutputs)
	assert.False(t, *args.Options.RandomizeOutputs)
	require.NotNil(t, args.Options.AcceptDelayedBroadcast)
	assert.False(t, *args.Options.AcceptDelayedBroadcast)
	assert.Equal(t, []string{"fuelkeeper", "fuel-split", "pool:main"}, args.Labels)
	assert.Equal(t, "fuel split main", args.Description)
	require.Len(t, args.Outputs, 3)

	assert.Equal(t, store.ShardUnmined, b.Shard.Status)
	assert.Equal(t, 3, b.Shard.Size)
	assert.Equal(t, store.LockPuzzle, b.Shard.LockMode)
	assert.Equal(t, uint64(50), b.Shard.Denomination)
	require.Len(t, b.Rows, 3)
	for i, r := range b.Rows {
		assert.Equal(t, uint32(i), r.Vout)
		assert.Equal(t, b.Shard.TxID, r.TxID)
		assert.Equal(t, "main", r.Pool)
		assert.Equal(t, uint64(50), r.Satoshis)
		assert.Equal(t, args.Outputs[i].LockingScript, r.LockingScript)
		assert.Equal(t, "fuel main", args.Outputs[i].OutputDescription)
		assert.True(t, fuel.VerifyPuzzle(r.LockingScript, r.UnlockingScript), "row %d", i)
	}
	require.NoError(t, store.ValidatePutShard(b.Shard, b.Rows))

	// The shard BEEF is plain BEEF v2 (not atomic), valid with full ancestry.
	require.GreaterOrEqual(t, len(b.Shard.BEEF), 4)
	assert.Equal(t, transaction.BEEF_V2, binary.LittleEndian.Uint32(b.Shard.BEEF[:4]), "normalised to plain BEEF v2")
	parsed, err := transaction.NewBeefFromBytes(b.Shard.BEEF)
	require.NoError(t, err)
	assert.True(t, parsed.IsValid(false))
	raw, err := fuel.RawTxFromBEEF(b.Shard.BEEF, b.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, raw, b.RawTx)
	assert.Empty(t, w.abortRefs(), "a successful build aborts nothing")
}

func TestBuildErrors(t *testing.T) {
	ctx := context.Background()
	w := newFakeWallet(t)
	c := newCore(t, w, nil)

	w.fail = wdk.ErrNotEnoughFunds
	_, err := c.Build(ctx, puzzleReq(3))
	require.ErrorIs(t, err, treasury.ErrInsufficientFunds)
	w.fail = errors.New("wdk: not enough funds")
	_, err = c.Build(ctx, puzzleReq(3))
	require.ErrorIs(t, err, treasury.ErrInsufficientFunds)
	w.fail = errors.New("boom")
	_, err = c.Build(ctx, puzzleReq(3))
	require.Error(t, err)
	require.NotErrorIs(t, err, treasury.ErrInsufficientFunds)
	w.fail = nil
	assert.Empty(t, w.abortRefs(), "nothing to abort when the wallet created nothing")

	_, err = c.Build(ctx, treasury.MintRequest{Pool: "main", Denomination: 50, Count: 0})
	require.ErrorIs(t, err, treasury.ErrInvalidRequest)
	_, err = c.Build(ctx, treasury.MintRequest{Pool: "main", Denomination: 50, Count: 1, LockMode: 7})
	require.ErrorIs(t, err, treasury.ErrUnsupportedLockMode)

	w.zeroTxid = true
	_, err = c.Build(ctx, puzzleReq(1))
	require.ErrorContains(t, err, "no transaction")
	w.zeroTxid = false
	assert.Empty(t, w.abortRefs())
}

// TestBuildFailureAborts checks that every failure after the wallet created
// the split aborts it by txid, even when the caller's context is gone.
func TestBuildFailureAborts(t *testing.T) {
	for name, tc := range map[string]struct {
		setup func(w *fakeWallet) (restore func())
		want  string
	}{
		"output value altered": {func(w *fakeWallet) func() {
			w.tamper = func(tx *transaction.Transaction) { tx.Outputs[0].Satoshis++ }
			return func() {}
		}, "do not match"},
		"outputs reordered": {func(w *fakeWallet) func() {
			w.tamper = func(tx *transaction.Transaction) { tx.Outputs[0], tx.Outputs[1] = tx.Outputs[1], tx.Outputs[0] }
			return func() {}
		}, "do not match"},
		"too few outputs": {func(w *fakeWallet) func() {
			w.tamper = func(tx *transaction.Transaction) { tx.Outputs = tx.Outputs[:1] }
			return func() {}
		}, "do not match"},
		"beef without ancestry": {func(w *fakeWallet) func() { w.mode = beefNoParent; return func() {} }, "not valid"},
		"garbage beef":          {func(w *fakeWallet) func() { w.mode = beefGarbage; return func() {} }, "split beef"},
		"shard validation": {func(*fakeWallet) func() {
			return toolbox.SetValidatePutShard(func(store.Shard, []store.Row) error { return errors.New("bad shard") })
		}, "bad shard"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newFakeWallet(t)
			restore := tc.setup(w)
			defer restore()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			w.onCreate = cancel // the caller gives up while the wallet works
			c := newCore(t, w, nil)
			_, err := c.Build(ctx, puzzleReq(3))
			require.ErrorContains(t, err, tc.want)
			require.Equal(t, []string{w.lastBuilt().String()}, w.abortRefs(), "the created split is aborted by txid")
			w.mu.Lock()
			defer w.mu.Unlock()
			require.NoError(t, w.abortCtx[0], "the abort runs on a context that outlives the caller's")
		})
	}
}

func TestVerifySplitTxidMismatch(t *testing.T) {
	tx := transaction.NewTransaction()
	src := storetest.NewTxID()
	tx.AddInput(&transaction.TransactionInput{SourceTXID: &src, SequenceNumber: 0xffffffff, UnlockingScript: script.NewFromBytes([]byte{0x51})})
	lock := fuel.PuzzleLock(fuel.PuzzleHash(fuel.Preimage(fuel.NewSeed(), 0)))
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 50, LockingScript: script.NewFromBytes(lock)})
	want := []sdk.CreateActionOutput{{LockingScript: lock, Satoshis: 50}}
	require.NoError(t, toolbox.VerifySplit(tx.Bytes(), *tx.TxID(), want))
	err := toolbox.VerifySplit(tx.Bytes(), storetest.NewTxID(), want)
	require.ErrorContains(t, err, "txid mismatch")
}

func TestBuildP2PKHSplitPresigns(t *testing.T) {
	ctx := context.Background()
	w := newFakeWallet(t)
	c := newCore(t, w, nil)
	req := treasury.MintRequest{Pool: "fast", Denomination: 60, Count: 2, LockMode: store.LockP2PKH}

	b, err := c.Build(ctx, req)
	require.NoError(t, err)
	require.NoError(t, store.ValidatePutShard(b.Shard, b.Rows))
	assert.Equal(t, store.LockP2PKH, b.Shard.LockMode)
	require.Len(t, b.Rows, 2)

	key, err := toolbox.PoolKey(sdk.NewKeyDeriver(rootKey(t)), "fast")
	require.NoError(t, err)
	wantLock, err := fuel.P2PKHLock(key.PubKey())
	require.NoError(t, err)

	for i, r := range b.Rows {
		require.Len(t, r.LockingScript, 25)
		assert.Equal(t, wantLock, r.LockingScript)

		chunks, err := script.NewFromBytes(r.UnlockingScript).Chunks()
		require.NoError(t, err)
		require.Len(t, chunks, 2)
		sig := chunks[0].Data
		assert.Equal(t, byte(0xc2), sig[len(sig)-1])
		assert.Equal(t, key.PubKey().Compressed(), chunks[1].Data)

		// A v1 / lockTime 0 / seq 0xffffffff consumer with its own inputs and outputs verifies.
		consumer := transaction.NewTransaction()
		consumer.Version, consumer.LockTime = fuel.P2PKHTxVersion, fuel.P2PKHLockTime
		other := storetest.NewTxID()
		consumer.AddInput(&transaction.TransactionInput{SourceTXID: &other, SourceTxOutIndex: 1, SequenceNumber: 0xffffffff, UnlockingScript: script.NewFromBytes([]byte{0x51})})
		txid := b.Shard.TxID
		consumer.AddInput(&transaction.TransactionInput{SourceTXID: &txid, SourceTxOutIndex: uint32(i), SequenceNumber: fuel.P2PKHSequence, UnlockingScript: script.NewFromBytes(r.UnlockingScript)})
		consumer.AddOutput(&transaction.TransactionOutput{Satoshis: 10, LockingScript: script.NewFromBytes([]byte{0x6a})})
		prev := &transaction.TransactionOutput{Satoshis: r.Satoshis, LockingScript: script.NewFromBytes(r.LockingScript)}
		require.NoError(t, interpreter.NewEngine().Execute(interpreter.WithTx(consumer, 1, prev), interpreter.WithForkID(), interpreter.WithAfterGenesis()), "row %d", i)
	}

	b2, err := c.Build(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, b.Rows[0].LockingScript, b2.Rows[0].LockingScript, "same pool, same lock")

	req.Pool = "other"
	b3, err := c.Build(ctx, req)
	require.NoError(t, err)
	assert.NotEqual(t, b.Rows[0].LockingScript, b3.Rows[0].LockingScript, "different pool, different lock")
}

func TestPoolKeyDerivation(t *testing.T) {
	root := rootKey(t)
	kd := sdk.NewKeyDeriver(root)
	a1, err := toolbox.PoolKey(kd, "main")
	require.NoError(t, err)
	a2, err := toolbox.PoolKey(sdk.NewKeyDeriver(root), "main")
	require.NoError(t, err)
	b, err := toolbox.PoolKey(kd, "b")
	require.NoError(t, err)
	assert.Equal(t, a1.Serialize(), a2.Serialize(), "deterministic")
	assert.NotEqual(t, a1.Serialize(), b.Serialize(), "differs across pools")
	assert.NotEqual(t, root.Serialize(), a1.Serialize(), "not the root key")
}

func TestSendIsWalletFirst(t *testing.T) {
	ctx := context.Background()
	w := newFakeWallet(t)
	bc := &fakeBroadcaster{}
	c := newCore(t, w, bc)
	b, err := c.Build(ctx, puzzleReq(2))
	require.NoError(t, err)
	txid := b.Shard.TxID

	// The wallet posts (or already posted) it: "unproven". The caller's BEEF
	// is re-posted, which is the watcher's rebroadcast.
	require.NoError(t, c.Send(ctx, txid, b.Shard.BEEF))
	sw := w.sendWithCalls()
	require.Len(t, sw, 1)
	assert.Equal(t, []chainhash.Hash{txid}, sw[0])
	args := w.lastCreate()
	assert.Equal(t, "send fuel split", args.Description)
	require.NotNil(t, args.Options.AcceptDelayedBroadcast)
	assert.False(t, *args.Options.AcceptDelayedBroadcast)
	assert.Equal(t, []string{txid.String()}, bc.calls())

	// Every Send asks the wallet again (no in-memory state).
	require.NoError(t, c.Send(ctx, txid, nil))
	assert.Len(t, w.sendWithCalls(), 2)
	assert.Len(t, bc.calls(), 1, "no BEEF, no re-post")

	// A restarted core (fresh state) still sends through the wallet.
	c2 := newCore(t, w, bc)
	require.NoError(t, c2.Send(ctx, txid, b.Shard.BEEF))
	assert.Len(t, w.sendWithCalls(), 3)

	// A re-post failure is transient.
	bc.err = errors.New("arcade down")
	err = c.Send(ctx, txid, b.Shard.BEEF)
	require.Error(t, err)
	require.NotErrorIs(t, err, treasury.ErrRejected)
	bc.err = nil

	// Not posted by the wallet: transient, and never posted around it.
	before := len(bc.calls())
	for _, st := range []sdk.ActionResultStatus{sdk.ActionResultStatusSending, sdk.ActionResultStatusFailed} {
		w.sendStatus[txid] = st
		err := c.Send(ctx, txid, b.Shard.BEEF)
		require.Error(t, err, st)
		require.NotErrorIs(t, err, treasury.ErrRejected)
		require.NotErrorIs(t, err, treasury.ErrUnknownTx)
	}
	delete(w.sendStatus, txid)
	w.omitResult = true
	err = c.Send(ctx, txid, b.Shard.BEEF)
	require.ErrorContains(t, err, "did not post")
	w.omitResult = false
	assert.Len(t, bc.calls(), before, "a split the wallet did not post is never posted around it")

	// A wallet with no record: post the caller's BEEF, or ErrUnknownTx without one.
	w.unknown[txid] = true
	require.NoError(t, c.Send(ctx, txid, b.Shard.BEEF))
	assert.Len(t, bc.calls(), before+1)
	require.ErrorIs(t, c.Send(ctx, txid, nil), treasury.ErrUnknownTx)
	require.ErrorIs(t, newCore(t, w, nil).Send(ctx, txid, b.Shard.BEEF), treasury.ErrUnknownTx, "no broadcaster")
}

func processErr(reviewTxid chainhash.Hash, st wdk.ReviewActionResultStatus, cause error) error {
	perr := pkgerrors.NewProcessActionError(
		[]wdk.SendWithResult{{TxID: primitives.TXIDHexString(reviewTxid.String()), Status: wdk.SendWithResultStatusFailed}},
		[]wdk.ReviewActionResult{{TxID: primitives.TXIDHexString(reviewTxid.String()), Status: st}},
	)
	if cause != nil {
		perr = perr.Wrap(cause)
	}
	return fmt.Errorf("create action failed: %w", perr)
}

func TestSendRejectedMapsToErrRejected(t *testing.T) {
	ctx := context.Background()
	w := newFakeWallet(t)
	bc := &fakeBroadcaster{}
	c := newCore(t, w, bc)

	b, err := c.Build(ctx, puzzleReq(1))
	require.NoError(t, err)
	txid := b.Shard.TxID

	for _, st := range []wdk.ReviewActionResultStatus{wdk.ReviewActionResultStatusDoubleSpend, wdk.ReviewActionResultStatusInvalidTx} {
		w.sendFail = processErr(txid, st, nil)
		require.ErrorIs(t, c.Send(ctx, txid, b.Shard.BEEF), treasury.ErrRejected, "review status %s", st)
	}

	transient := map[string]error{
		"service error":            processErr(txid, wdk.ReviewActionResultStatusServiceError, nil),
		"another txid's rejection": processErr(storetest.NewTxID(), wdk.ReviewActionResultStatusDoubleSpend, nil),
		"review without txid": fmt.Errorf("wrap: %w", pkgerrors.NewProcessActionError(nil,
			[]wdk.ReviewActionResult{{Status: wdk.ReviewActionResultStatusDoubleSpend}})),
		"connection reset":        errors.New("connection reset"),
		"arc text rejection":      errors.New("arc: transaction rejected: mandatory-script-verify-flag-failed"),
		"postgres auth":           errors.New("FATAL: connection rejected (SQLSTATE 28000)"),
		"malformed merkle path":   errors.New("failed to build valid BEEF: malformed merkle path"),
		"argument validation":     errors.New("create action failed: invalid create action args: description must be 5..2000 bytes"),
		"double spend in text":    errors.New("double spend detected"),
		"cancelled while posting": context.Canceled,
	}
	for name, e := range transient {
		w.sendFail = e
		err := c.Send(ctx, txid, b.Shard.BEEF)
		require.Error(t, err, name)
		require.NotErrorIs(t, err, treasury.ErrRejected, name)
	}

	// A caller whose context is gone never retires a shard, even on a
	// rejection-shaped error: the next Send asks the wallet again.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	w.sendFail = processErr(txid, wdk.ReviewActionResultStatusDoubleSpend, context.Canceled)
	err = c.Send(cctx, txid, b.Shard.BEEF)
	require.Error(t, err)
	require.NotErrorIs(t, err, treasury.ErrRejected)

	// None of the failures posted around the wallet; a retry asks the wallet again.
	w.sendFail = nil
	assert.Empty(t, bc.calls())
	before := len(w.sendWithCalls())
	require.NoError(t, c.Send(ctx, txid, nil))
	assert.Len(t, w.sendWithCalls(), before+1)
}

func TestReconcile(t *testing.T) {
	ctx := context.Background()
	w := newFakeWallet(t)
	c := newCore(t, w, &fakeBroadcaster{})
	knownOK, unknownOK, knownFail, unknownFail, mined := storetest.NewTxID(), storetest.NewTxID(), storetest.NewTxID(), storetest.NewTxID(), storetest.NewTxID()
	w.actions = []sdk.Action{
		{Txid: knownOK, Status: sdk.ActionStatusNoSend},
		{Txid: mined, Status: sdk.ActionStatusUnproven},
		{Txid: unknownOK, Status: sdk.ActionStatusNoSend},
		{Txid: knownFail, Status: sdk.ActionStatusNoSend},
		{Txid: unknownFail, Status: sdk.ActionStatusNoSend},
	}
	w.pageCap = 2 // the wallet pages, whatever the requested limit
	w.sendStatus[knownFail] = sdk.ActionResultStatusFailed
	w.abortFail[unknownFail] = errors.New("not abortable")
	known := map[chainhash.Hash]bool{knownOK: true, knownFail: true, mined: true}

	rep, err := c.Reconcile(ctx, func(h chainhash.Hash) bool { return known[h] })
	require.NoError(t, err)
	assert.Equal(t, []chainhash.Hash{knownOK}, rep.Resent)
	assert.Equal(t, []chainhash.Hash{unknownOK}, rep.Aborted)
	require.Len(t, rep.Errors, 2)
	assert.Contains(t, rep.Errors, knownFail)
	assert.Contains(t, rep.Errors, unknownFail)

	assert.Equal(t, [][]chainhash.Hash{{knownOK}, {knownFail}}, w.sendWithCalls(), "only known no-send splits are sent; the mined one is left alone")
	assert.Equal(t, []string{unknownOK.String(), unknownFail.String()}, w.abortRefs())
	require.Len(t, w.listArgs, 3, "paged through all five actions two at a time")
	for _, a := range w.listArgs {
		assert.Equal(t, []string{"fuel-split"}, a.Labels)
	}

	w.listErr = errors.New("storage down")
	_, err = c.Reconcile(ctx, func(chainhash.Hash) bool { return true })
	require.ErrorContains(t, err, "storage down")
}

// paymentBEEF returns an atomic BEEF of a tx whose output 0 pays someone else
// 1000 sat and output 1 pays the treasury's BRC-29 script 7000 sat.
func paymentBEEF(t *testing.T, sender *ec.PrivateKey, treasuryPub *ec.PublicKey, prefix, suffix []byte) ([]byte, *transaction.Transaction) {
	t.Helper()
	lock, err := brc29.LockForCounterparty(sender, brc29.KeyID{
		DerivationPrefix: base64.StdEncoding.EncodeToString(prefix),
		DerivationSuffix: base64.StdEncoding.EncodeToString(suffix),
	}, treasuryPub)
	require.NoError(t, err)
	tx := transaction.NewTransaction()
	src := storetest.NewTxID()
	tx.AddInput(&transaction.TransactionInput{SourceTXID: &src, SequenceNumber: 0xffffffff, UnlockingScript: script.NewFromBytes([]byte{0x51})})
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 1000, LockingScript: script.NewFromBytes(p2pkhLikeScript(0x22))})
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 7000, LockingScript: lock})
	b := transaction.NewBeefV2()
	_, err = b.MergeRawTx(tx.Bytes(), nil)
	require.NoError(t, err)
	beef, err := b.AtomicBytes(tx.TxID())
	require.NoError(t, err)
	return beef, tx
}

func TestFundingHandshake(t *testing.T) {
	ctx := context.Background()
	w := newFakeWallet(t)
	c := newCore(t, w, nil)

	info, err := c.FundingInfo(ctx)
	require.NoError(t, err)
	assert.Len(t, info.IdentityKeyHex, 66)
	assert.Equal(t, w.identity.ToDERHex(), info.IdentityKeyHex)
	assert.Equal(t, goodNonce, info.DerivationPrefixB64)
	assert.Equal(t, "test", info.Network)

	sender, err := ec.NewPrivateKey()
	require.NoError(t, err)
	suffixBytes := []byte("suffix01")
	beef, payTx := paymentBEEF(t, sender, rootKey(t).PubKey(), []byte("nonce"), suffixBytes)
	good := treasury.FundRequest{
		AtomicBEEF:           beef,
		OutputIndex:          1,
		DerivationPrefixB64:  goodNonce,
		DerivationSuffixB64:  base64.StdEncoding.EncodeToString(suffixBytes),
		SenderIdentityKeyHex: sender.PubKey().ToDERHex(),
	}

	res, err := c.Internalize(ctx, good)
	require.NoError(t, err)
	assert.Equal(t, treasury.FundResult{TxID: payTx.TxID().String(), Satoshis: 7000}, res, "the satoshis of the indexed output")
	require.Len(t, w.internals, 1)
	got := w.internals[0]
	assert.Equal(t, beef, got.Tx)
	assert.Equal(t, "fuelkeeper funding", got.Description)
	assert.Equal(t, []string{"fuelkeeper", "funding"}, got.Labels)
	require.Len(t, got.Outputs, 1)
	assert.Equal(t, uint32(1), got.Outputs[0].OutputIndex)
	assert.Equal(t, sdk.InternalizeProtocolWalletPayment, got.Outputs[0].Protocol)
	require.NotNil(t, got.Outputs[0].PaymentRemittance)
	assert.Equal(t, []byte("nonce"), got.Outputs[0].PaymentRemittance.DerivationPrefix)
	assert.Equal(t, suffixBytes, got.Outputs[0].PaymentRemittance.DerivationSuffix)
	assert.True(t, sender.PubKey().IsEqual(got.Outputs[0].PaymentRemittance.SenderIdentityKey))

	// A prefix with '+' and '/' is standard base64 only.
	slashPrefix, err := base64.StdEncoding.DecodeString(slashNonce)
	require.NoError(t, err)
	slashBEEF, _ := paymentBEEF(t, sender, rootKey(t).PubKey(), slashPrefix, suffixBytes)
	slash := good
	slash.AtomicBEEF, slash.DerivationPrefixB64 = slashBEEF, slashNonce
	_, err = c.Internalize(ctx, slash)
	require.NoError(t, err)
	assert.Equal(t, slashPrefix, w.internals[1].Outputs[0].PaymentRemittance.DerivationPrefix)

	other, err := ec.NewPrivateKey()
	require.NoError(t, err)
	bad := func(mut func(r *treasury.FundRequest)) error {
		r := good
		mut(&r)
		_, err := c.Internalize(ctx, r)
		return err
	}
	for name, mut := range map[string]func(r *treasury.FundRequest){
		"unknown nonce":        func(r *treasury.FundRequest) { r.DerivationPrefixB64 = "b3RoZXI=" },
		"url-base64 nonce":     func(r *treasury.FundRequest) { r.DerivationPrefixB64 = "-_-_" },
		"index out of range":   func(r *treasury.FundRequest) { r.OutputIndex = 2 },
		"index -1":             func(r *treasury.FundRequest) { r.OutputIndex = -1 },
		"index -2":             func(r *treasury.FundRequest) { r.OutputIndex = -2 },
		"wrong output":         func(r *treasury.FundRequest) { r.OutputIndex = 0 },
		"wrong suffix":         func(r *treasury.FundRequest) { r.DerivationSuffixB64 = "b3RoZXI=" },
		"wrong sender":         func(r *treasury.FundRequest) { r.SenderIdentityKeyHex = other.PubKey().ToDERHex() },
		"wrong prefix for pay": func(r *treasury.FundRequest) { r.DerivationPrefixB64 = slashNonce },
		"bad suffix base64":    func(r *treasury.FundRequest) { r.DerivationSuffixB64 = "!!!" },
		"empty suffix":         func(r *treasury.FundRequest) { r.DerivationSuffixB64 = "" },
		"bad sender key":       func(r *treasury.FundRequest) { r.SenderIdentityKeyHex = "zz" },
		"not atomic beef":      func(r *treasury.FundRequest) { r.AtomicBEEF = []byte("nope") },
		"oversized beef":       func(r *treasury.FundRequest) { r.AtomicBEEF = make([]byte, 4<<20+1) },
	} {
		require.ErrorIs(t, bad(mut), treasury.ErrBadFunding, name)
	}
	assert.Len(t, w.internals, 2, "bad requests never reach the wallet")

	w.internalErr = errors.New("storage down")
	err = bad(func(*treasury.FundRequest) {})
	require.Error(t, err)
	require.NotErrorIs(t, err, treasury.ErrBadFunding)
	w.internalErr = nil

	w.rejectPay = true
	err = bad(func(*treasury.FundRequest) {})
	require.Error(t, err)
	require.NotErrorIs(t, err, treasury.ErrBadFunding)
	w.rejectPay = false

	// A nonce check that could not run (wallet down) is not the payer's fault.
	down := newCoreWith(t, w, nil, nil, fakeNonces{err: errors.New("wallet unavailable")})
	_, err = down.Internalize(ctx, good)
	require.Error(t, err)
	require.NotErrorIs(t, err, treasury.ErrBadFunding)
}

func baseConfig() toolbox.Config {
	cfg := toolbox.DefaultConfig("main")
	cfg.RootKeyHex = testRootKeyHex
	cfg.ArcadeURL = "https://arcade.example"
	cfg.ChaintracksURL = "https://chaintracks.example"
	return cfg
}

func TestLeafCostAndChunkValue(t *testing.T) {
	p := pool.DefaultPoolConfig("main", 50)
	p.SplitSize = 500
	size := 8 + 1 + 3 + 148 + 500*44 + 34 // 501 outputs need a 3-byte count
	assert.Equal(t, size, toolbox.LeafSizeBytes(p))
	assert.Equal(t, uint64(500*50)+fuel.FeeSats(size, 100), toolbox.LeafCost(p, 100))
	assert.Equal(t, toolbox.LeafCost(p, 100), toolbox.LeafCost(p, 0), "0 sat/kB means 100")

	pk := p
	pk.LockMode = store.LockP2PKH
	assert.Equal(t, 8+1+3+148+500*34+34, toolbox.LeafSizeBytes(pk))

	small := p
	small.SplitSize = 251 // 252 outputs: 1-byte count
	assert.Equal(t, 8+1+1+148+251*44+34, toolbox.LeafSizeBytes(small))
	small.SplitSize = 252 // 253 outputs: 3-byte count
	assert.Equal(t, 8+1+3+148+252*44+34, toolbox.LeafSizeBytes(small))

	cfg := baseConfig()
	assert.Equal(t, toolbox.LeafCost(p, 100)+20, cfg.ChunkValue(p, 100))

	require.NoError(t, cfg.Validate([]pool.Config{p}))
	require.NoError(t, cfg.Validate([]pool.Config{p, pool.DefaultPoolConfig("twin", 50)}), "same leaf cost class")
	err := cfg.Validate([]pool.Config{p, pool.DefaultPoolConfig("big", 500)})
	require.ErrorContains(t, err, "leaf cost")
	require.ErrorContains(t, err, "main")
	require.ErrorContains(t, err, "big")
	require.ErrorContains(t, err, "run one FuelKeeper per class")

	// Equal leaf cost at 100 sat/kB only (outputs and size both differ):
	// still two classes, because the fee rate can change.
	a := pool.DefaultPoolConfig("a", 50)
	a.SplitSize = 10
	b := pool.DefaultPoolConfig("b", 51)
	b.SplitSize, b.LockMode = 10, store.LockP2PKH
	require.Equal(t, toolbox.LeafCost(a, 100), toolbox.LeafCost(b, 100))
	require.NotEqual(t, toolbox.LeafCost(a, 1000), toolbox.LeafCost(b, 1000))
	require.ErrorContains(t, cfg.Validate([]pool.Config{a, b}), "leaf cost")

	for name, tc := range map[string]struct {
		mut  func(c *toolbox.Config)
		want string
	}{
		"bad network":        {func(c *toolbox.Config) { c.Network = "regtest" }, "network"},
		"short key":          {func(c *toolbox.Config) { c.RootKeyHex = "abcd" }, "root_key_hex"},
		"non-hex key":        {func(c *toolbox.Config) { c.RootKeyHex = strings.Repeat("zz", 32) }, "root_key_hex"},
		"zero key":           {func(c *toolbox.Config) { c.RootKeyHex = strings.Repeat("00", 32) }, "root_key_hex"},
		"missing arcade":     {func(c *toolbox.Config) { c.ArcadeURL = "" }, "arcade_url"},
		"missing chaintrack": {func(c *toolbox.Config) { c.ChaintracksURL = "" }, "chaintracks_url"},
		"missing originator": {func(c *toolbox.Config) { c.Originator = "" }, "originator"},
		"bad engine":         {func(c *toolbox.Config) { c.Storage.Engine = "mysql" }, "storage.engine"},
		"sqlite no path":     {func(c *toolbox.Config) { c.Storage.SQLite = "" }, "sqlite_path"},
		"postgres no host": {func(c *toolbox.Config) {
			c.Storage.Engine = "postgres"
			c.Storage.Postgres = toolbox.PostgresConfig{DBName: "treasury"}
		}, "postgres.host"},
		"postgres no db": {func(c *toolbox.Config) {
			c.Storage.Engine = "postgres"
			c.Storage.Postgres = toolbox.PostgresConfig{Host: "db"}
		}, "postgres.dbname"},
		"baskets equal":   {func(c *toolbox.Config) { c.Chunking.ChunkBasket = c.Chunking.ReserveBasket }, "distinct"},
		"basket default":  {func(c *toolbox.Config) { c.Chunking.ChunkBasket = "default" }, "default"},
		"reserve default": {func(c *toolbox.Config) { c.Chunking.ReserveBasket = "default" }, "default"},
		"basket empty":    {func(c *toolbox.Config) { c.Chunking.ReserveBasket = "" }, "reserve_basket"},
		"low water 0":     {func(c *toolbox.Config) { c.Chunking.LowWaterPercent = 0 }, "low_water_percent"},
		"low water 101":   {func(c *toolbox.Config) { c.Chunking.LowWaterPercent = 101 }, "low_water_percent"},
		"interval 0":      {func(c *toolbox.Config) { c.Chunking.Interval = 0 }, "interval"},
		"fanout 0":        {func(c *toolbox.Config) { c.Chunking.FanoutOutputsPerTx = 0 }, "fanout_outputs_per_tx"},
		"target chunks 0": {func(c *toolbox.Config) { c.Chunking.TargetChunks = 0 }, "target_chunks"},
		"postgres bad port": {func(c *toolbox.Config) {
			c.Storage.Engine = "postgres"
			c.Storage.Postgres = toolbox.PostgresConfig{Host: "h", DBName: "d", Port: 70000}
		}, "postgres.port"},
	} {
		c := cfg
		tc.mut(&c)
		err := c.Validate([]pool.Config{p})
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), tc.want, name)
		assert.True(t, strings.HasPrefix(err.Error(), "treasury.toolbox"), "%s: %v", name, err)
	}
	err = cfg.Validate(nil)
	require.ErrorContains(t, err, "at least one pool")
}

func TestConfigRedactsSecrets(t *testing.T) {
	cfg := baseConfig()
	cfg.RootKeyHex = strings.Repeat("ab", 32)
	cfg.Storage.Postgres = toolbox.PostgresConfig{Host: "db", DBName: "treasury", User: "fk", Password: "hunter2-secret"}

	var logged bytes.Buffer
	slog.New(slog.NewJSONHandler(&logged, nil)).Info("config", "cfg", cfg, "pg", cfg.Storage.Postgres)
	for name, out := range map[string]string{
		"%v":   fmt.Sprintf("%v", cfg),
		"%+v":  fmt.Sprintf("%+v", cfg),
		"%#v":  fmt.Sprintf("%#v", cfg),
		"%s":   fmt.Sprintf("%s", cfg), //nolint:staticcheck // exercising the verb
		"pg":   fmt.Sprintf("%+v", cfg.Storage.Postgres),
		"slog": logged.String(),
	} {
		assert.NotContains(t, out, cfg.RootKeyHex, name)
		assert.NotContains(t, out, "hunter2-secret", name)
		assert.Contains(t, out, "[REDACTED]", name)
		if name != "pg" {
			assert.Contains(t, out, "arcade.example", "%s: non-secret fields still render", name)
		}
	}
	assert.Equal(t, strings.Repeat("ab", 32), cfg.RootKeyHex, "rendering does not modify the config")
}

func TestEnsureChunksDelegates(t *testing.T) {
	ctx := context.Background()
	w := newFakeWallet(t)
	require.NoError(t, newCore(t, w, nil).EnsureChunks(ctx))

	k := &fakeKeeper{}
	c := newCoreWith(t, w, nil, k, fakeNonces{})
	require.NoError(t, c.EnsureChunks(ctx))
	assert.Equal(t, 1, k.calls)
	k.err = errors.New("keeper failed")
	require.ErrorIs(t, c.EnsureChunks(ctx), k.err)
	assert.Equal(t, 2, k.calls)
}

func TestMinedBEEF(t *testing.T) {
	ctx := context.Background()
	w := newFakeWallet(t)
	c := newCore(t, w, nil)
	b, err := c.Build(ctx, puzzleReq(2))
	require.NoError(t, err)

	mp := transaction.NewMerklePath(100, [][]*transaction.PathElement{{
		{Offset: 0, Hash: &b.Shard.TxID},
	}})
	out, err := c.MinedBEEF(ctx, b.Shard.TxID, b.Shard.BEEF, mp.Bytes())
	require.NoError(t, err)
	raw, err := fuel.RawTxFromBEEF(out, b.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, b.RawTx, raw)
}
