package toolbox

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/wdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
)

type fakePoster struct {
	res   wdk.PostFromBeefResult
	err   error
	calls int
	txids []string
	beef  *transaction.Beef
}

func (f *fakePoster) PostFromBEEF(_ context.Context, b *transaction.Beef, txids []string) (wdk.PostFromBeefResult, error) {
	f.calls++
	f.beef, f.txids = b, txids
	return f.res, f.err
}

// testBEEF returns a one-transaction BEEF and its txid.
func testBEEF(t *testing.T) ([]byte, string) {
	t.Helper()
	tx := transaction.NewTransaction()
	s := script.Script([]byte{0x6a})
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 0, LockingScript: &s})
	b := transaction.NewBeefV2()
	_, err := b.MergeTransaction(tx)
	require.NoError(t, err)
	raw, err := b.Bytes()
	require.NoError(t, err)
	return raw, tx.TxID().String()
}

func posted(txid string, r wdk.PostedTxIDResultStatus, double bool) *wdk.PostFromBEEFServiceResult {
	return &wdk.PostFromBEEFServiceResult{Name: "Arcade", PostedBEEFResult: &wdk.PostedBEEF{
		TxIDResults: []wdk.PostedTxID{{TxID: txid, Result: r, DoubleSpend: double}},
	}}
}

func TestBroadcasterSuccess(t *testing.T) {
	beef, txid := testBEEF(t)
	for _, r := range []wdk.PostedTxIDResultStatus{wdk.PostedTxIDResultSuccess, wdk.PostedTxIDResultAlreadyKnown} {
		p := &fakePoster{res: wdk.PostFromBeefResult{posted(txid, r, false)}}
		require.NoError(t, broadcaster{svc: p}.PostBEEF(context.Background(), beef, txid), r)
		assert.Equal(t, []string{txid}, p.txids)
		require.NotNil(t, p.beef.FindTransaction(txid))
	}
}

func TestBroadcasterFailuresAreNeverRejections(t *testing.T) {
	beef, txid := testBEEF(t)
	cases := map[string]*fakePoster{
		"double spend":   {res: wdk.PostFromBeefResult{posted(txid, wdk.PostedTxIDResultDoubleSpend, true)}},
		"invalid":        {res: wdk.PostFromBeefResult{posted(txid, wdk.PostedTxIDResultError, false)}},
		"service error":  {res: wdk.PostFromBeefResult{{Name: "Arcade", Error: errors.New("503")}}},
		"no result":      {res: nil},
		"other txid":     {res: wdk.PostFromBeefResult{posted("00", wdk.PostedTxIDResultSuccess, false)}},
		"go error":       {err: treasury.ErrRejected}, // even a (misbehaving) poster returning it
		"missing inputs": {res: wdk.PostFromBeefResult{posted(txid, wdk.PostedTxIDResultMissingInputs, false)}},
	}
	for name, p := range cases {
		t.Run(name, func(t *testing.T) {
			err := broadcaster{svc: p}.PostBEEF(context.Background(), beef, txid)
			require.ErrorIs(t, err, errPostFailed)
			require.NotErrorIs(t, err, treasury.ErrRejected)
			assert.Equal(t, 1, p.calls)
		})
	}
}

func TestBroadcasterBadBEEF(t *testing.T) {
	p := &fakePoster{}
	err := broadcaster{svc: p}.PostBEEF(context.Background(), []byte{1, 2, 3}, "ab")
	require.ErrorIs(t, err, errPostFailed)
	require.NotErrorIs(t, err, treasury.ErrRejected)
	assert.Zero(t, p.calls)
}

func protoWallet(t *testing.T) *sdk.ProtoWallet {
	t.Helper()
	k, err := ec.NewPrivateKey()
	require.NoError(t, err)
	w, err := sdk.NewProtoWallet(sdk.ProtoWalletArgs{Type: sdk.ProtoWalletArgsTypePrivateKey, PrivateKey: k})
	require.NoError(t, err)
	return w
}

func TestNoncesRoundTrip(t *testing.T) {
	ctx := context.Background()
	n := nonces{w: protoWallet(t)}
	s, err := n.Create(ctx)
	require.NoError(t, err)
	ok, err := n.Verify(ctx, s)
	require.NoError(t, err)
	assert.True(t, ok)

	other := nonces{w: protoWallet(t)}
	ok, err = other.Verify(ctx, s)
	require.NoError(t, err)
	assert.False(t, ok, "a nonce from another wallet does not verify")
}

func TestNoncesMalformed(t *testing.T) {
	ctx := context.Background()
	n := nonces{w: protoWallet(t)}
	for _, s := range []string{
		"", "!!!not base64", base64.StdEncoding.EncodeToString(make([]byte, 16)),
		base64.StdEncoding.EncodeToString(make([]byte, 48)), // right length, wrong HMAC
	} {
		ok, err := n.Verify(ctx, s)
		require.NoError(t, err, s)
		assert.False(t, ok, s)
	}
}

// failingHMAC is a wallet whose HMAC operations fail.
type failingHMAC struct{ sdk.KeyOperations }

var errHMAC = errors.New("wallet down")

func (failingHMAC) VerifyHMAC(context.Context, sdk.VerifyHMACArgs, string) (*sdk.VerifyHMACResult, error) {
	return nil, errHMAC
}

func (failingHMAC) CreateHMAC(context.Context, sdk.CreateHMACArgs, string) (*sdk.CreateHMACResult, error) {
	return nil, errHMAC
}

func TestNoncesWalletFailureIsAnError(t *testing.T) {
	ctx := context.Background()
	good, err := nonces{w: protoWallet(t)}.Create(ctx)
	require.NoError(t, err)
	n := nonces{w: failingHMAC{}}
	ok, err := n.Verify(ctx, good)
	require.ErrorIs(t, err, errHMAC)
	assert.False(t, ok)
	_, err = n.Create(ctx)
	require.ErrorIs(t, err, errHMAC)
}

type fakeRunOnce struct {
	err   error
	calls int
}

func (f *fakeRunOnce) RunOnce(context.Context) error { f.calls++; return f.err }

func TestKeeperAdapter(t *testing.T) {
	f := &fakeRunOnce{}
	require.NoError(t, keeper{k: f}.RunOnce(context.Background()))
	f.err = errors.New("no funds")
	require.ErrorIs(t, keeper{k: f}.RunOnce(context.Background()), f.err)
	assert.Equal(t, 2, f.calls)
}
