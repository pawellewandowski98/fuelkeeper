package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/bsv-blockchain/go-sdk/wallet/substrates"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
)

// fundOriginator identifies fuelkeeper's CLI to the local BRC-100 wallet.
const fundOriginator = "fuelkeeper.cli"

// brc29Protocol is FuelKeeper's treasury's fixed BRC-29 protocol ID (security
// level 2, "every app and counterparty"); see pkg/api's GET /v1/funding and
// wire.FundingInfo.Protocol, which should echo this same value.
const brc29Protocol = "3241645161d8"

// derivationSuffixBytes is the length of the random BRC-29 derivation suffix
// fund generates for each payment.
const derivationSuffixBytes = 8

// payer is the subset of a BRC-100 wallet's interface (wallet.Interface) fund
// drives: deriving public keys and creating the funding transaction. Naming
// it separately (rather than depending on the full wallet.Interface) lets
// tests substitute a fake wallet for the real HTTP-JSON substrate
// (wallet/substrates.HTTPWalletJSON), which satisfies it as-is.
type payer interface {
	GetPublicKey(ctx context.Context, args wallet.GetPublicKeyArgs) (*wallet.GetPublicKeyResult, error)
	CreateAction(ctx context.Context, args wallet.CreateActionArgs) (*wallet.CreateActionResult, error)
}

// fundCmd runs "fuelkeeper fund" against the real world: os.Stdout and a
// substrates.NewHTTPWalletJSON pointed at --wallet.
func fundCmd(ctx context.Context, args []string) error {
	return runFundCmd(ctx, args, os.Stdout, nil)
}

// runFundCmd is fundCmd's testable core. newPayer, when non-nil, replaces the
// default wallet client (a substrates.NewHTTPWalletJSON pointed at --wallet)
// with a fake, so tests never make a real HTTP call to a wallet.
func runFundCmd(ctx context.Context, args []string, stdout io.Writer, newPayer func(walletURL string) payer) error {
	fs := flag.NewFlagSet("fund", flag.ContinueOnError)
	sats := fs.Uint64("sats", 0, "amount to fund, in satoshis (required, > 0)")
	url := fs.String("url", "http://127.0.0.1:8080", "FuelKeeper base URL")
	key := fs.String("key", os.Getenv("FUELKEEPER_API_KEY"),
		"API key (or $FUELKEEPER_API_KEY); must be an admin key when the server has admin keys configured")
	walletURL := fs.String("wallet", "http://localhost:3321", "local BRC-100 wallet base URL")
	outputIndex := fs.Int("output-index", 0,
		"index of the funding output within the wallet's created transaction (default 0; -1/auto-detect is not yet supported and is rejected as bad_funding)")
	if err := fs.Parse(args); err != nil {
		return wrapUsageError(err)
	}
	if *sats == 0 {
		return usageErrorf("fund: --sats is required and must be > 0")
	}

	fk, err := client.New(*url, client.WithAPIKey(*key))
	if err != nil {
		// client.New only ever fails on a malformed --url; that is a usage
		// mistake (exit 2), not a runtime failure.
		return wrapUsageError(err)
	}

	var w payer
	if newPayer != nil {
		w = newPayer(*walletURL)
	} else {
		w = substrates.NewHTTPWalletJSON(fundOriginator, *walletURL, http.DefaultClient)
	}

	txid, satsFunded, err := runFund(ctx, fk, w, *sats, *outputIndex)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "funded %d sats in %s\n", satsFunded, txid)
	return err
}

// runFund drives the BRC-29 payment flow end to end: fetch the treasury's
// funding instructions, derive a one-time payment key from w, pay it via a
// P2PKH output in a transaction w creates, then post that payment to fk. It
// returns the txid and satoshis FuelKeeper reports having internalized.
func runFund(ctx context.Context, fk *client.Client, w payer, sats uint64, outputIndex int) (string, uint64, error) {
	info, err := fk.FundingInfo(ctx)
	if err != nil {
		return "", 0, fmt.Errorf("fund: get funding info: %w", err)
	}
	if info.Protocol != brc29Protocol {
		// Deriving under another protocol would pay a key the treasury
		// cannot find: refuse before the wallet spends anything.
		return "", 0, fmt.Errorf("fund: server reports BRC-29 protocol %q, this client derives under %q; upgrade one of them", info.Protocol, brc29Protocol)
	}
	fkKey, err := ec.PublicKeyFromString(info.IdentityKey)
	if err != nil {
		return "", 0, fmt.Errorf("fund: parse treasury identity key: %w", err)
	}

	suffix := make([]byte, derivationSuffixBytes)
	if _, err := rand.Read(suffix); err != nil {
		return "", 0, fmt.Errorf("fund: generate derivation suffix: %w", err)
	}
	suffixB64 := base64.StdEncoding.EncodeToString(suffix)

	me, err := w.GetPublicKey(ctx, wallet.GetPublicKeyArgs{IdentityKey: true})
	if err != nil {
		return "", 0, fmt.Errorf("fund: get wallet identity key: %w", err)
	}
	derived, err := w.GetPublicKey(ctx, wallet.GetPublicKeyArgs{
		EncryptionArgs: wallet.EncryptionArgs{
			ProtocolID: wallet.Protocol{
				SecurityLevel: wallet.SecurityLevelEveryAppAndCounterparty,
				Protocol:      brc29Protocol,
			},
			KeyID:        info.DerivationPrefix + " " + suffixB64,
			Counterparty: wallet.Counterparty{Type: wallet.CounterpartyTypeOther, Counterparty: fkKey},
		},
	})
	if err != nil {
		return "", 0, fmt.Errorf("fund: derive payment key: %w", err)
	}

	addr, err := script.NewAddressFromPublicKey(derived.PublicKey, info.Network == "main")
	if err != nil {
		return "", 0, fmt.Errorf("fund: derive payment address: %w", err)
	}
	lock, err := p2pkh.Lock(addr)
	if err != nil {
		return "", 0, fmt.Errorf("fund: build locking script: %w", err)
	}

	randomizeOutputs := false
	res, err := w.CreateAction(ctx, wallet.CreateActionArgs{
		Description: "Fund FuelKeeper",
		Outputs: []wallet.CreateActionOutput{{
			LockingScript:     *lock,
			Satoshis:          sats,
			OutputDescription: "FuelKeeper treasury funding",
		}},
		Options: &wallet.CreateActionOptions{RandomizeOutputs: &randomizeOutputs},
	})
	if err != nil {
		return "", 0, fmt.Errorf("fund: create funding transaction: %w", err)
	}
	if len(res.Tx) == 0 {
		return "", 0, errors.New("fund: wallet returned no transaction (is signAndProcess enabled?)")
	}

	idx := outputIndex
	result, err := fk.Fund(ctx, wire.FundRequest{
		AtomicBEEF:        base64.StdEncoding.EncodeToString(res.Tx),
		OutputIndex:       &idx,
		DerivationPrefix:  info.DerivationPrefix,
		DerivationSuffix:  suffixB64,
		SenderIdentityKey: me.PublicKey.ToDERHex(),
	})
	if err != nil {
		// The wallet has already broadcast the payment even though FuelKeeper
		// failed to internalize it: never lose the txid or the derivation
		// prefix/suffix needed to retry, since the BRC-29 nonce is HMAC-based
		// and stays valid.
		return "", 0, fmt.Errorf(
			"fund: wallet already broadcast tx %s but POST /v1/funding failed: %w -- "+
				"retry POST /v1/funding with derivation_prefix=%s derivation_suffix=%s",
			res.Txid.String(), err, info.DerivationPrefix, suffixB64)
	}
	return result.TxID, result.Satoshis, nil
}
