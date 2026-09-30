package toolbox

import (
	"context"

	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/wallet"
)

// WalletAPI is the slice of the toolbox wallet the treasury core uses.
// *wallet.Wallet satisfies it (go-wallet-toolbox v0.184.16
// pkg/wallet/wallet.go: GetPublicKey :293, CreateAction :370, AbortAction
// :465, ListActions :492, InternalizeAction :583).
type WalletAPI interface {
	CreateAction(ctx context.Context, args sdk.CreateActionArgs, originator string) (*sdk.CreateActionResult, error)
	AbortAction(ctx context.Context, args sdk.AbortActionArgs, originator string) (*sdk.AbortActionResult, error)
	ListActions(ctx context.Context, args sdk.ListActionsArgs, originator string) (*sdk.ListActionsResult, error)
	InternalizeAction(ctx context.Context, args sdk.InternalizeActionArgs, originator string) (*sdk.InternalizeActionResult, error)
	GetPublicKey(ctx context.Context, args sdk.GetPublicKeyArgs, originator string) (*sdk.GetPublicKeyResult, error)
}

// A toolbox signature drift is a compile error here rather than a wiring
// surprise later.
var _ WalletAPI = (*wallet.Wallet)(nil)

// Broadcaster re-posts an already-built transaction.
type Broadcaster interface {
	PostBEEF(ctx context.Context, beef []byte, txid string) error
}

// ChunkKeeper keeps leaf-sized chunks ready (the toolbox's
// pkg/wallet/fuelkeeper.Keeper).
type ChunkKeeper interface {
	RunOnce(ctx context.Context) error
}

// Nonces creates and verifies BRC-29 derivation-prefix nonces.
//
// Verify must return (false, nil) for a nonce that is malformed or was not
// issued by this treasury, and an error only when it could not check (the
// wallet is unavailable): the core maps false to treasury.ErrBadFunding and an
// error to a transient failure. The go-sdk nonces are HMAC-based and not
// one-shot: a nonce stays valid after use, and replaying one only lets a
// payer fund the treasury again.
type Nonces interface {
	Create(ctx context.Context) (string, error)
	Verify(ctx context.Context, nonce string) (bool, error)
}
