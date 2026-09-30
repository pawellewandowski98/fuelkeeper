package toolbox_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/script/interpreter"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
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

// fakeWallet is a WalletAPI that builds real split transactions from the
// requested outputs (plus one dummy input and one trailing change output).
type fakeWallet struct {
	mu          sync.Mutex
	creates     []sdk.CreateActionArgs
	sendWith    [][]chainhash.Hash
	internals   []sdk.InternalizeActionArgs
	shapes      []wdk.ShapedChange
	fail        error // every CreateAction fails with this
	sendFail    error // only SendWith CreateActions fail with this
	internalErr error
	rejectPay   bool // InternalizeAction reports Accepted=false
	chunks      uint32
	identity    *ec.PublicKey
	tamper      func(tx *transaction.Transaction)
}

func newFakeWallet(t *testing.T) *fakeWallet {
	t.Helper()
	priv, err := ec.NewPrivateKey()
	require.NoError(t, err)
	return &fakeWallet{identity: priv.PubKey()}
}

func (f *fakeWallet) CreateAction(_ context.Context, args sdk.CreateActionArgs, _ string) (*sdk.CreateActionResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, args)
	if f.fail != nil {
		return nil, f.fail
	}
	if args.Options != nil && len(args.Options.SendWith) > 0 {
		f.sendWith = append(f.sendWith, append([]chainhash.Hash(nil), args.Options.SendWith...))
		if f.sendFail != nil {
			return nil, f.sendFail
		}
		return &sdk.CreateActionResult{}, nil
	}
	tx := transaction.NewTransaction()
	src := storetest.NewTxID()
	tx.AddInput(&transaction.TransactionInput{SourceTXID: &src, SourceTxOutIndex: 0, SequenceNumber: 0xffffffff, UnlockingScript: script.NewFromBytes([]byte{0x51})})
	for _, o := range args.Outputs {
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: o.Satoshis, LockingScript: script.NewFromBytes(o.LockingScript)})
	}
	change := append([]byte{0x76, 0xa9, 0x14}, bytes.Repeat([]byte{0x11}, 20)...)
	change = append(change, 0x88, 0xac)
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 1234, LockingScript: script.NewFromBytes(change)})
	if f.tamper != nil {
		f.tamper(tx)
	}
	beef, err := atomicBEEF(tx)
	if err != nil {
		return nil, err
	}
	return &sdk.CreateActionResult{Txid: *tx.TxID(), Tx: beef}, nil
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

func (f *fakeWallet) ListOutputs(_ context.Context, _ sdk.ListOutputsArgs, _ string) (*sdk.ListOutputsResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &sdk.ListOutputsResult{TotalOutputs: f.chunks}, nil
}

func (f *fakeWallet) GetPublicKey(_ context.Context, args sdk.GetPublicKeyArgs, _ string) (*sdk.GetPublicKeyResult, error) {
	if !args.IdentityKey {
		return nil, errors.New("fake: only the identity key is supported")
	}
	return &sdk.GetPublicKeyResult{PublicKey: f.identity}, nil
}

func (f *fakeWallet) FanOutFuel(_ context.Context, shape wdk.ShapedChange, _ string) (*sdk.CreateActionResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shapes = append(f.shapes, shape)
	return &sdk.CreateActionResult{}, nil
}

func (f *fakeWallet) sendWithCalls() [][]chainhash.Hash {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]chainhash.Hash(nil), f.sendWith...)
}

func (f *fakeWallet) lastCreate() sdk.CreateActionArgs {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.creates[len(f.creates)-1]
}

func atomicBEEF(tx *transaction.Transaction) ([]byte, error) {
	b := transaction.NewBeefV2()
	if _, err := b.MergeRawTx(tx.Bytes(), nil); err != nil {
		return nil, err
	}
	return b.AtomicBytes(tx.TxID())
}

type fakeBroadcaster struct {
	mu    sync.Mutex
	txids []string
	beefs [][]byte
}

func (b *fakeBroadcaster) PostBEEF(_ context.Context, beef []byte, txid string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.txids = append(b.txids, txid)
	b.beefs = append(b.beefs, beef)
	return nil
}

func (b *fakeBroadcaster) calls() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.txids...)
}

const goodNonce = "bm9uY2U="

type fakeNonces struct{}

func (fakeNonces) Create(context.Context) (string, error) { return goodNonce, nil }
func (fakeNonces) Verify(_ context.Context, n string) (bool, error) {
	return n == goodNonce, nil
}

type fakeKeeper struct {
	calls int
	err   error
}

func (k *fakeKeeper) RunOnce(context.Context) error {
	k.calls++
	return k.err
}

func newCore(t *testing.T, w toolbox.WalletAPI, bc toolbox.Broadcaster, keeper toolbox.ChunkKeeper) *toolbox.Core {
	t.Helper()
	root, err := ec.PrivateKeyFromHex(testRootKeyHex)
	require.NoError(t, err)
	cfg := toolbox.DefaultConfig("test")
	cfg.RootKeyHex = testRootKeyHex
	return toolbox.NewCore(w, bc, keeper, fakeNonces{}, sdk.NewKeyDeriver(root), cfg, 100, nil)
}

func puzzleReq(n int) treasury.MintRequest {
	return treasury.MintRequest{Pool: "main", Denomination: 50, Count: n, LockMode: store.LockPuzzle}
}

func TestBuildPuzzleSplitThenSend(t *testing.T) {
	ctx := context.Background()
	w := newFakeWallet(t)
	bc := &fakeBroadcaster{}
	c := newCore(t, w, bc, nil)

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
	assert.NotEmpty(t, b.Shard.BEEF)
	assert.NotEmpty(t, b.RawTx)
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
	raw, err := fuel.RawTxFromBEEF(b.Shard.BEEF, b.Shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, raw, b.RawTx)

	// First Send goes through the wallet's SendWith.
	require.NoError(t, c.Send(ctx, b.Shard.TxID))
	sw := w.sendWithCalls()
	require.Len(t, sw, 1)
	assert.Equal(t, []chainhash.Hash{b.Shard.TxID}, sw[0])
	assert.Equal(t, "send fuel split", w.lastCreate().Description)
	assert.Empty(t, bc.calls())

	// Subsequent Sends rebroadcast the stored BEEF.
	require.NoError(t, c.Send(ctx, b.Shard.TxID))
	assert.Len(t, w.sendWithCalls(), 1)
	assert.Equal(t, []string{b.Shard.TxID.String()}, bc.calls())

	require.ErrorIs(t, c.Send(ctx, storetest.NewTxID()), treasury.ErrUnknownTx)

	w.fail = wdk.ErrNotEnoughFunds
	_, err = c.Build(ctx, puzzleReq(3))
	require.ErrorIs(t, err, treasury.ErrInsufficientFunds)
	w.fail = errors.New("wdk: not enough funds")
	_, err = c.Build(ctx, puzzleReq(3))
	require.ErrorIs(t, err, treasury.ErrInsufficientFunds)
	w.fail = errors.New("boom")
	_, err = c.Build(ctx, puzzleReq(3))
	require.Error(t, err)
	require.NotErrorIs(t, err, treasury.ErrInsufficientFunds)
	w.fail = nil

	w.tamper = func(tx *transaction.Transaction) { tx.Outputs[0].Satoshis++ }
	_, err = c.Build(ctx, puzzleReq(3))
	require.ErrorContains(t, err, "do not match")
	w.tamper = func(tx *transaction.Transaction) { tx.Outputs[0], tx.Outputs[1] = tx.Outputs[1], tx.Outputs[0] }
	_, err = c.Build(ctx, puzzleReq(3))
	require.ErrorContains(t, err, "do not match", "a reordered split is refused")
	w.tamper = func(tx *transaction.Transaction) { tx.Outputs = tx.Outputs[:1] }
	_, err = c.Build(ctx, puzzleReq(3))
	require.ErrorContains(t, err, "do not match", "a split with too few outputs is refused")
	w.tamper = nil

	_, err = c.Build(ctx, treasury.MintRequest{Pool: "main", Denomination: 50, Count: 0})
	require.ErrorIs(t, err, treasury.ErrInvalidRequest)
	_, err = c.Build(ctx, treasury.MintRequest{Pool: "main", Denomination: 50, Count: 1, LockMode: 7})
	require.ErrorIs(t, err, treasury.ErrUnsupportedLockMode)
}

func TestBuildP2PKHSplitPresigns(t *testing.T) {
	ctx := context.Background()
	w := newFakeWallet(t)
	c := newCore(t, w, nil, nil)
	req := treasury.MintRequest{Pool: "fast", Denomination: 60, Count: 2, LockMode: store.LockP2PKH}

	b, err := c.Build(ctx, req)
	require.NoError(t, err)
	require.NoError(t, store.ValidatePutShard(b.Shard, b.Rows))
	assert.Equal(t, store.LockP2PKH, b.Shard.LockMode)
	require.Len(t, b.Rows, 2)

	root, err := ec.PrivateKeyFromHex(testRootKeyHex)
	require.NoError(t, err)
	key, err := toolbox.PoolKey(sdk.NewKeyDeriver(root), "fast")
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
	root, err := ec.PrivateKeyFromHex(testRootKeyHex)
	require.NoError(t, err)
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

func TestSendRejectedMapsToErrRejected(t *testing.T) {
	ctx := context.Background()
	w := newFakeWallet(t)
	bc := &fakeBroadcaster{}
	c := newCore(t, w, bc, nil)

	b, err := c.Build(ctx, puzzleReq(1))
	require.NoError(t, err)
	txid := b.Shard.TxID

	review := func(st wdk.ReviewActionResultStatus) error {
		perr := pkgerrors.NewProcessActionError(
			[]wdk.SendWithResult{{TxID: primitives.TXIDHexString(txid.String()), Status: wdk.SendWithResultStatusFailed}},
			[]wdk.ReviewActionResult{{TxID: primitives.TXIDHexString(txid.String()), Status: st}},
		)
		return fmt.Errorf("create action failed: %w", perr)
	}

	for _, st := range []wdk.ReviewActionResultStatus{wdk.ReviewActionResultStatusDoubleSpend, wdk.ReviewActionResultStatusInvalidTx} {
		w.sendFail = review(st)
		err = c.Send(ctx, txid)
		require.ErrorIs(t, err, treasury.ErrRejected, "review status %s", st)
	}

	// A service error is transient: the network did not reject the split.
	w.sendFail = review(wdk.ReviewActionResultStatusServiceError)
	err = c.Send(ctx, txid)
	require.Error(t, err)
	require.NotErrorIs(t, err, treasury.ErrRejected)

	w.sendFail = errors.New("connection reset")
	err = c.Send(ctx, txid)
	require.Error(t, err)
	require.NotErrorIs(t, err, treasury.ErrRejected)

	w.sendFail = errors.New("create action failed: invalid create action args: description must be 5..2000 bytes")
	err = c.Send(ctx, txid)
	require.Error(t, err)
	require.NotErrorIs(t, err, treasury.ErrRejected, "argument validation is not a network verdict")

	w.sendFail = errors.New("arc: transaction rejected: mandatory-script-verify-flag-failed")
	require.ErrorIs(t, c.Send(ctx, txid), treasury.ErrRejected)

	// None of the failed sends marked the split sent: a retry goes through SendWith again.
	w.sendFail = nil
	before := len(w.sendWithCalls())
	require.NoError(t, c.Send(ctx, txid))
	assert.Len(t, w.sendWithCalls(), before+1)
	assert.Empty(t, bc.calls(), "retry after a failed send must not use the broadcaster")
}

func TestFundingHandshake(t *testing.T) {
	ctx := context.Background()
	w := newFakeWallet(t)
	c := newCore(t, w, nil, nil)

	info, err := c.FundingInfo(ctx)
	require.NoError(t, err)
	assert.Len(t, info.IdentityKeyHex, 66)
	assert.Equal(t, w.identity.ToDERHex(), info.IdentityKeyHex)
	assert.Equal(t, goodNonce, info.DerivationPrefixB64)
	assert.Equal(t, "test", info.Network)

	payTx := transaction.NewTransaction()
	src := storetest.NewTxID()
	payTx.AddInput(&transaction.TransactionInput{SourceTXID: &src, SequenceNumber: 0xffffffff, UnlockingScript: script.NewFromBytes([]byte{0x51})})
	payLock := append([]byte{0x76, 0xa9, 0x14}, bytes.Repeat([]byte{0x22}, 20)...)
	payLock = append(payLock, 0x88, 0xac)
	payTx.AddOutput(&transaction.TransactionOutput{Satoshis: 5000, LockingScript: script.NewFromBytes(payLock)})
	beef, err := atomicBEEF(payTx)
	require.NoError(t, err)

	sender, err := ec.NewPrivateKey()
	require.NoError(t, err)
	suffix := base64.StdEncoding.EncodeToString([]byte("suffix01"))
	good := treasury.FundRequest{
		AtomicBEEF:           beef,
		OutputIndex:          0,
		DerivationPrefixB64:  goodNonce,
		DerivationSuffixB64:  suffix,
		SenderIdentityKeyHex: sender.PubKey().ToDERHex(),
	}

	res, err := c.Internalize(ctx, good)
	require.NoError(t, err)
	assert.Equal(t, treasury.FundResult{TxID: payTx.TxID().String(), Satoshis: 5000}, res)
	require.Len(t, w.internals, 1)
	got := w.internals[0]
	assert.Equal(t, beef, got.Tx)
	assert.Equal(t, "fuelkeeper funding", got.Description)
	assert.Equal(t, []string{"fuelkeeper", "funding"}, got.Labels)
	require.Len(t, got.Outputs, 1)
	assert.Equal(t, uint32(0), got.Outputs[0].OutputIndex)
	assert.Equal(t, sdk.InternalizeProtocolWalletPayment, got.Outputs[0].Protocol)
	require.NotNil(t, got.Outputs[0].PaymentRemittance)
	assert.Equal(t, []byte("nonce"), got.Outputs[0].PaymentRemittance.DerivationPrefix)
	assert.Equal(t, []byte("suffix01"), got.Outputs[0].PaymentRemittance.DerivationSuffix)
	assert.True(t, sender.PubKey().IsEqual(got.Outputs[0].PaymentRemittance.SenderIdentityKey))

	bad := func(mut func(r *treasury.FundRequest)) error {
		r := good
		mut(&r)
		_, err := c.Internalize(ctx, r)
		return err
	}
	for name, mut := range map[string]func(r *treasury.FundRequest){
		"unknown nonce":      func(r *treasury.FundRequest) { r.DerivationPrefixB64 = "b3RoZXI=" },
		"index out of range": func(r *treasury.FundRequest) { r.OutputIndex = 1 },
		"index -1":           func(r *treasury.FundRequest) { r.OutputIndex = -1 },
		"index -2":           func(r *treasury.FundRequest) { r.OutputIndex = -2 },
		"bad suffix base64":  func(r *treasury.FundRequest) { r.DerivationSuffixB64 = "!!!" },
		"empty suffix":       func(r *treasury.FundRequest) { r.DerivationSuffixB64 = "" },
		"bad sender key":     func(r *treasury.FundRequest) { r.SenderIdentityKeyHex = "zz" },
		"not atomic beef":    func(r *treasury.FundRequest) { r.AtomicBEEF = []byte("nope") },
		"oversized beef":     func(r *treasury.FundRequest) { r.AtomicBEEF = make([]byte, 4<<20+1) },
	} {
		require.ErrorIs(t, bad(mut), treasury.ErrBadFunding, name)
	}
	assert.Len(t, w.internals, 1, "bad requests never reach the wallet")

	w.internalErr = errors.New("storage down")
	err = bad(func(*treasury.FundRequest) {})
	require.Error(t, err)
	require.NotErrorIs(t, err, treasury.ErrBadFunding)
	w.internalErr = nil

	w.rejectPay = true
	err = bad(func(*treasury.FundRequest) {})
	require.Error(t, err)
	require.NotErrorIs(t, err, treasury.ErrBadFunding)
}

func TestLeafCostAndChunkValue(t *testing.T) {
	p := pool.DefaultPoolConfig("main", 50)
	p.SplitSize = 500
	size := 10 + 148 + 500*44 + 34
	assert.Equal(t, size, toolbox.LeafSizeBytes(p))
	assert.Equal(t, uint64(500*50)+fuel.FeeSats(size, 100), toolbox.LeafCost(p, 100))
	assert.Equal(t, toolbox.LeafCost(p, 100), toolbox.LeafCost(p, 0), "0 sat/kB means 100")

	pk := p
	pk.LockMode = store.LockP2PKH
	assert.Equal(t, 10+148+500*34+34, toolbox.LeafSizeBytes(pk))

	cfg := toolbox.DefaultConfig("main")
	cfg.RootKeyHex = testRootKeyHex
	cfg.ArcadeURL = "https://arcade.example"
	cfg.ChaintracksURL = "https://chaintracks.example"
	assert.Equal(t, toolbox.LeafCost(p, 100)+20, cfg.ChunkValue(p, 100))

	require.NoError(t, cfg.Validate([]pool.Config{p}))
	require.NoError(t, cfg.Validate([]pool.Config{p, pool.DefaultPoolConfig("twin", 50)}), "same leaf cost class")
	err := cfg.Validate([]pool.Config{p, pool.DefaultPoolConfig("big", 500)})
	require.ErrorContains(t, err, "leaf cost")
	require.ErrorContains(t, err, "main")
	require.ErrorContains(t, err, "big")
	require.ErrorContains(t, err, "run one FuelKeeper per class")

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

func TestBuiltMapIsBounded(t *testing.T) {
	ctx := context.Background()
	w := newFakeWallet(t)
	c := newCore(t, w, &fakeBroadcaster{}, nil)
	now := time.Unix(1_700_000_000, 0)
	c.SetClock(func() time.Time { return now })

	// TTL path: sent splits older than BuiltTTL are forgotten.
	old := make([]chainhash.Hash, 0, 4)
	for range 4 {
		b, err := c.Build(ctx, puzzleReq(1))
		require.NoError(t, err)
		require.NoError(t, c.Send(ctx, b.Shard.TxID))
		old = append(old, b.Shard.TxID)
	}
	unsent, err := c.Build(ctx, puzzleReq(1))
	require.NoError(t, err)
	assert.Equal(t, 5, c.BuiltLen())
	now = now.Add(toolbox.BuiltTTL + time.Second)
	_, err = c.Build(ctx, puzzleReq(1))
	require.NoError(t, err)
	assert.Equal(t, 2, c.BuiltLen(), "old sent splits evicted, the unsent one kept")
	require.ErrorIs(t, c.Send(ctx, old[0]), treasury.ErrUnknownTx, "an evicted split is unknown; the watcher falls back to the stored BEEF")
	require.NoError(t, c.Send(ctx, unsent.Shard.TxID), "an unsent split survives the TTL sweep")

	// Cap path: oldest sent first, then oldest built.
	c2 := newCore(t, w, &fakeBroadcaster{}, nil)
	c2.SetMaxBuilt(5)
	ids := make([]chainhash.Hash, 0, 8)
	for i := range 8 {
		b, err := c2.Build(ctx, puzzleReq(1))
		require.NoError(t, err)
		ids = append(ids, b.Shard.TxID)
		if i < 3 {
			require.NoError(t, c2.Send(ctx, b.Shard.TxID))
		}
		assert.LessOrEqual(t, c2.BuiltLen(), 5)
	}
	for _, id := range ids[:3] {
		require.ErrorIs(t, c2.Send(ctx, id), treasury.ErrUnknownTx, "sent splits evicted first")
	}
	_, err = c2.Build(ctx, puzzleReq(1))
	require.NoError(t, err)
	assert.Equal(t, 5, c2.BuiltLen())
	require.ErrorIs(t, c2.Send(ctx, ids[3]), treasury.ErrUnknownTx, "then the oldest unsent split")
	require.NoError(t, c2.Send(ctx, ids[4]))
	assert.Equal(t, 10_000, toolbox.MaxBuilt)
	assert.Equal(t, time.Hour, toolbox.BuiltTTL)
}

func TestEnsureChunksDelegates(t *testing.T) {
	ctx := context.Background()
	w := newFakeWallet(t)
	require.NoError(t, newCore(t, w, nil, nil).EnsureChunks(ctx))

	k := &fakeKeeper{}
	c := newCore(t, w, nil, k)
	require.NoError(t, c.EnsureChunks(ctx))
	assert.Equal(t, 1, k.calls)
	k.err = errors.New("keeper failed")
	require.ErrorIs(t, c.EnsureChunks(ctx), k.err)
	assert.Equal(t, 2, k.calls)
}

func TestMinedBEEF(t *testing.T) {
	ctx := context.Background()
	w := newFakeWallet(t)
	c := newCore(t, w, nil, nil)
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
