package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
)

// fakeFunder is a treasury.Funder test double that records the FundRequest it
// received and returns a configurable result/error.
type fakeFunder struct {
	info      treasury.FundingInfo
	result    treasury.FundResult
	resultErr error
	gotReq    treasury.FundRequest
}

func (f *fakeFunder) FundingInfo(context.Context) (treasury.FundingInfo, error) {
	return f.info, nil
}

func (f *fakeFunder) Internalize(_ context.Context, req treasury.FundRequest) (treasury.FundResult, error) {
	f.gotReq = req
	return f.result, f.resultErr
}

var _ treasury.Funder = (*fakeFunder)(nil)

// fakePayer is a payer test double standing in for the operator's local
// BRC-100 wallet: it returns fixed identity/derived keys and a synthetic
// signed transaction, recording every GetPublicKey call it saw.
type fakePayer struct {
	identity *ec.PublicKey
	derived  *ec.PublicKey
	tx       []byte
	txid     chainhash.Hash

	createErr error
	calls     []wallet.GetPublicKeyArgs
}

func (f *fakePayer) GetPublicKey(_ context.Context, args wallet.GetPublicKeyArgs) (*wallet.GetPublicKeyResult, error) {
	f.calls = append(f.calls, args)
	if args.IdentityKey {
		return &wallet.GetPublicKeyResult{PublicKey: f.identity}, nil
	}
	return &wallet.GetPublicKeyResult{PublicKey: f.derived}, nil
}

func (f *fakePayer) CreateAction(_ context.Context, _ wallet.CreateActionArgs) (*wallet.CreateActionResult, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &wallet.CreateActionResult{Tx: f.tx, Txid: f.txid}, nil
}

var _ payer = (*fakePayer)(nil)

// newFakePayer builds a fakePayer with two distinct, valid compressed public
// keys (identity and derived) and a synthetic, non-empty "signed" tx.
func newFakePayer(t *testing.T) *fakePayer {
	t.Helper()
	id, err := ec.NewPrivateKey()
	require.NoError(t, err)
	derived, err := ec.NewPrivateKey()
	require.NoError(t, err)
	return &fakePayer{
		identity: id.PubKey(),
		derived:  derived.PubKey(),
		tx:       []byte("synthetic-atomic-beef-bytes"),
		txid:     chainhash.Hash{0xab, 0xcd, 0xef},
	}
}

// mustPubKeyHex returns a fresh, syntactically valid compressed public key as
// DER hex, for use as a fake treasury identity key.
func mustPubKeyHex(t *testing.T) string {
	t.Helper()
	k, err := ec.NewPrivateKey()
	require.NoError(t, err)
	return k.PubKey().ToDERHex()
}

// newFundTestServer starts a real pkg/api handler (api.NewHandler) with
// funder wired in as an admin-tier-gated treasury, and returns its base URL.
func newFundTestServer(t *testing.T, funder *fakeFunder) string {
	t.Helper()
	h := api.NewHandler(api.Deps{
		Store:     memory.New(),
		Funder:    funder,
		AdminKeys: []string{"admin-key"},
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

// TestRunFundCmdSuccess drives the full BRC-29 flow against a real pkg/api
// handler and a fake wallet, checking that: the funder saw the derivation
// prefix handed out by FundingInfo, an 8-byte random suffix, the payer's
// identity key as sender, output index 0 (the default), the wallet's tx
// bytes as the AtomicBEEF, and that fundCmd prints the expected line.
func TestRunFundCmdSuccess(t *testing.T) {
	funder := &fakeFunder{
		info: treasury.FundingInfo{
			IdentityKeyHex:      mustPubKeyHex(t),
			DerivationPrefixB64: base64.StdEncoding.EncodeToString([]byte("prefix-nonce")),
			Network:             "test",
		},
		result: treasury.FundResult{TxID: "final-txid", Satoshis: 4200},
	}
	baseURL := newFundTestServer(t, funder)
	fp := newFakePayer(t)

	var stdout bytes.Buffer
	err := runFundCmd(context.Background(),
		[]string{"--sats", "4200", "--url", baseURL, "--key", "admin-key"},
		&stdout,
		func(string) payer { return fp })
	require.NoError(t, err)

	assert.Equal(t, "funded 4200 sats in final-txid\n", stdout.String())

	assert.Equal(t, funder.info.DerivationPrefixB64, funder.gotReq.DerivationPrefixB64)
	suffix, err := base64.StdEncoding.DecodeString(funder.gotReq.DerivationSuffixB64)
	require.NoError(t, err)
	assert.Len(t, suffix, derivationSuffixBytes, "derivation suffix must be 8 random bytes")
	assert.Equal(t, fp.identity.ToDERHex(), funder.gotReq.SenderIdentityKeyHex)
	assert.Equal(t, 0, funder.gotReq.OutputIndex, "default output index is 0")
	assert.Equal(t, fp.tx, funder.gotReq.AtomicBEEF, "the wallet's created tx must reach the server unmodified")

	// The derived-key request must carry the BRC-29 protocol, the
	// prefix+suffix keyID and the treasury as counterparty.
	var derivedCall *wallet.GetPublicKeyArgs
	for i := range fp.calls {
		if !fp.calls[i].IdentityKey {
			derivedCall = &fp.calls[i]
		}
	}
	require.NotNil(t, derivedCall, "the wallet must be asked to derive a BRC-29 payment key")
	assert.Equal(t, brc29Protocol, derivedCall.ProtocolID.Protocol)
	assert.Equal(t, wallet.SecurityLevelEveryAppAndCounterparty, derivedCall.ProtocolID.SecurityLevel)
	assert.Equal(t, funder.info.DerivationPrefixB64+" "+funder.gotReq.DerivationSuffixB64, derivedCall.KeyID)
	assert.Equal(t, wallet.CounterpartyTypeOther, derivedCall.Counterparty.Type)
	require.NotNil(t, derivedCall.Counterparty.Counterparty)
	assert.Equal(t, funder.info.IdentityKeyHex, derivedCall.Counterparty.Counterparty.ToDERHex())
}

// TestRunFundCmdFundFailurePreservesRecoveryInfo checks that when the wallet
// has already broadcast the payment but POST /v1/funding then fails, the
// returned error names the already-broadcast txid and the exact
// prefix/suffix pair the operator must retry with -- that information must
// never be lost, since it is the only way to recover the payment.
func TestRunFundCmdFundFailurePreservesRecoveryInfo(t *testing.T) {
	funder := &fakeFunder{
		info: treasury.FundingInfo{
			IdentityKeyHex:      mustPubKeyHex(t),
			DerivationPrefixB64: base64.StdEncoding.EncodeToString([]byte("prefix-nonce")),
			Network:             "test",
		},
		resultErr: errors.New("toolbox: mid-broadcast"),
	}
	baseURL := newFundTestServer(t, funder)
	fp := newFakePayer(t)

	var stdout bytes.Buffer
	err := runFundCmd(context.Background(),
		[]string{"--sats", "1000", "--url", baseURL, "--key", "admin-key"},
		&stdout,
		func(string) payer { return fp })
	require.Error(t, err)
	assert.Contains(t, err.Error(), fp.txid.String(), "the wallet's already-broadcast txid must never be lost")
	assert.Contains(t, err.Error(), funder.info.DerivationPrefixB64)
	assert.Contains(t, err.Error(), funder.gotReq.DerivationSuffixB64,
		"the same suffix used for the failed attempt must be surfaced for retry")
	assert.Empty(t, stdout.String(), "nothing should be printed on failure")
}

// TestFundCmdRequiresSats checks --sats is required and must be > 0, as a
// usageError (exit code 2), before any network call is attempted.
func TestFundCmdRequiresSats(t *testing.T) {
	var stdout bytes.Buffer
	err := runFundCmd(context.Background(), []string{"--url", "http://127.0.0.1:1"}, &stdout, func(string) payer {
		t.Fatal("the wallet must not be touched when --sats is missing")
		return nil
	})
	require.Error(t, err)
	var uerr *usageError
	require.ErrorAs(t, err, &uerr)

	err = runFundCmd(context.Background(), []string{"--sats", "0", "--url", "http://127.0.0.1:1"}, &stdout, nil)
	require.Error(t, err)
	require.ErrorAs(t, err, &uerr)
}

// TestFundCmdBadFlagIsUsageError mirrors TestAdminCmdBadFlagIsUsageError for fund.
func TestFundCmdBadFlagIsUsageError(t *testing.T) {
	var stdout bytes.Buffer
	err := runFundCmd(context.Background(), []string{"--not-a-real-flag"}, &stdout, nil)
	require.Error(t, err)
	var uerr *usageError
	require.ErrorAs(t, err, &uerr, "expected a usageError, got %T: %v", err, err)
}

// TestFundCmdBadURLIsUsageError checks that a malformed --url (client.New's
// only failure mode) is a usageError (exit 2), not a bare runtime error (exit
// 1): the operator mistyped a flag, the wallet was never touched, and no
// network call was attempted.
func TestFundCmdBadURLIsUsageError(t *testing.T) {
	var stdout bytes.Buffer
	err := runFundCmd(context.Background(), []string{"--sats", "100", "--url", "://bad"}, &stdout, func(string) payer {
		t.Fatal("the wallet must not be touched when --url is malformed")
		return nil
	})
	require.Error(t, err)
	var uerr *usageError
	require.ErrorAs(t, err, &uerr, "expected a usageError, got %T: %v", err, err)
	assert.Empty(t, stdout.String())
}

// TestRunFundRejectsUnknownProtocol checks that a server advertising a
// different BRC-29 protocol is refused before the wallet derives or pays
// anything.
func TestRunFundRejectsUnknownProtocol(t *testing.T) {
	idKey := mustPubKeyHex(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/funding" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"identity_key":%q,"derivation_prefix":"cHJlZml4","network":"ttn","protocol":"ffff","instructions":""}`, idKey)
	}))
	defer srv.Close()
	p := newFakePayer(t)
	var stdout bytes.Buffer
	err := runFundCmd(context.Background(), []string{"--sats", "100", "--url", srv.URL}, &stdout, func(string) payer { return p })
	require.ErrorContains(t, err, `protocol "ffff"`)
	assert.Equal(t, 1, exitCode(err))
	assert.Empty(t, p.calls, "the wallet is never asked for a key")
	assert.Empty(t, stdout.String())
}

// TestHelpExitsZero checks that -h/--help on every command prints usage
// (to the flag set's output) and maps to exit code 0, while a bad flag
// stays a usage error (2).
func TestHelpExitsZero(t *testing.T) {
	ctx := context.Background()
	for _, h := range []string{"-h", "--help"} {
		assert.Equal(t, 0, exitCode(runFundCmd(ctx, []string{h}, io.Discard, nil)), "fund "+h)
		assert.Equal(t, 0, exitCode(serveCmd(ctx, []string{h})), "serve "+h)
		assert.Equal(t, 0, exitCode(adminCmd(ctx, "pools", []string{h})), "pools "+h)
	}
	assert.Equal(t, 2, exitCode(runFundCmd(ctx, []string{"--nope"}, io.Discard, nil)))
	assert.Equal(t, 1, exitCode(errors.New("boom")))
	assert.Equal(t, 0, exitCode(nil))
}
