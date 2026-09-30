package toolbox

import (
	"context"

	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/wallet"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/wdk"
)

// WalletAPI is the slice of the toolbox wallet the treasury core uses.
// *wallet.Wallet satisfies it.
type WalletAPI interface {
	CreateAction(ctx context.Context, args sdk.CreateActionArgs, originator string) (*sdk.CreateActionResult, error)
	InternalizeAction(ctx context.Context, args sdk.InternalizeActionArgs, originator string) (*sdk.InternalizeActionResult, error)
	ListOutputs(ctx context.Context, args sdk.ListOutputsArgs, originator string) (*sdk.ListOutputsResult, error)
	GetPublicKey(ctx context.Context, args sdk.GetPublicKeyArgs, originator string) (*sdk.GetPublicKeyResult, error)
	FanOutFuel(ctx context.Context, shape wdk.ShapedChange, originator string) (*sdk.CreateActionResult, error)
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
type Nonces interface {
	Create(ctx context.Context) (string, error)
	Verify(ctx context.Context, nonce string) (bool, error)
}
