package toolbox

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/bsv-blockchain/go-sdk/auth/utils"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/wdk"
)

// beefPoster is the slice of the toolbox services the broadcaster uses
// (go-wallet-toolbox v0.184.16 pkg/services/services.go:661 PostFromBEEF).
type beefPoster interface {
	PostFromBEEF(ctx context.Context, beef *transaction.Beef, txIDs []string) (wdk.PostFromBeefResult, error)
}

// broadcaster re-posts a BEEF through the toolbox services (arcade first,
// with the services' own circuit-breaker failover).
type broadcaster struct {
	svc beefPoster
}

var _ Broadcaster = broadcaster{}

// errPostFailed is the (never treasury.ErrRejected) error a failed re-post
// returns: Core only logs it, and the oracle, not a re-post, decides whether
// a split was rejected.
var errPostFailed = errors.New("treasury: re-post failed")

// PostBEEF posts beef and reports whether the services accepted txid. A
// double spend or invalid verdict from a broadcaster is reported as a plain
// error, never as treasury.ErrRejected (see Broadcaster).
func (b broadcaster) PostBEEF(ctx context.Context, beef []byte, txid string) error {
	parsed, err := transaction.NewBeefFromBytes(beef)
	if err != nil {
		return fmt.Errorf("%w: parse beef: %v", errPostFailed, err) //nolint:errorlint // deliberately not wrapped: only errPostFailed is exposed
	}
	res, err := b.svc.PostFromBEEF(ctx, parsed, []string{txid})
	if err != nil {
		return fmt.Errorf("%w: %v", errPostFailed, err) //nolint:errorlint // deliberately not wrapped: only errPostFailed is exposed
	}
	agg := res.Aggregated([]string{txid})[txid]
	if agg != nil && agg.Status == wdk.AggregatedPostedTxIDSuccess {
		return nil
	}
	status := wdk.AggregatedPostedTxIDServiceError
	if agg != nil {
		status = agg.Status
	}
	return fmt.Errorf("%w: %s %s%s", errPostFailed, txid, status, serviceErrors(res))
}

// serviceErrors renders the per-service errors of a post, sorted by service.
func serviceErrors(res wdk.PostFromBeefResult) string {
	errs := res.ServiceErrors()
	if len(errs) == 0 {
		return ""
	}
	names := make([]string, 0, len(errs))
	for n := range errs {
		names = append(names, n)
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = n + ": " + errs[n].Error()
	}
	return " (" + strings.Join(parts, "; ") + ")"
}

// nonceBytes is the decoded length of a go-sdk nonce: 16 random bytes and a
// 32-byte HMAC (auth/utils/create_nonce.go:15-45). go-sdk's VerifyNonce
// copies the tail into a 32-byte array, so a longer string with extra bytes
// appended would still verify; only the exact length is accepted.
const nonceBytes = 16 + 32

// nonces issues and checks BRC-29 derivation prefixes with go-sdk's HMAC
// nonces (auth/utils/create_nonce.go:15, verify_nonce.go:14), keyed to the
// treasury wallet with counterparty self.
type nonces struct {
	w sdk.KeyOperations
}

var _ Nonces = nonces{}

var selfCounterparty = sdk.Counterparty{Type: sdk.CounterpartyTypeSelf}

// Create returns a fresh nonce.
func (n nonces) Create(ctx context.Context) (string, error) {
	s, err := utils.CreateNonce(ctx, n.w, selfCounterparty)
	if err != nil {
		return "", fmt.Errorf("treasury: create nonce: %w", err)
	}
	return s, nil
}

// Verify reports whether nonce was issued by this wallet. A malformed nonce
// is (false, nil), which go-sdk would report as an error; only a failed HMAC
// check is an error.
func (n nonces) Verify(ctx context.Context, nonce string) (bool, error) {
	if !wellFormedNonce(nonce) {
		return false, nil
	}
	ok, err := utils.VerifyNonce(ctx, nonce, n.w, selfCounterparty)
	if err != nil {
		return false, fmt.Errorf("treasury: verify nonce: %w", err)
	}
	return ok, nil
}

// wellFormedNonce reports whether nonce is standard base64 of exactly
// nonceBytes bytes.
func wellFormedNonce(nonce string) bool {
	raw, err := base64.StdEncoding.DecodeString(nonce)
	return err == nil && len(raw) == nonceBytes
}

// runOncer is the toolbox chunk keeper (pkg/wallet/fuelkeeper/keeper.go:287
// RunOnce).
type runOncer interface {
	RunOnce(ctx context.Context) error
}

// keeper adapts the toolbox chunk keeper to ChunkKeeper.
type keeper struct {
	k runOncer
}

var _ ChunkKeeper = keeper{}

// RunOnce runs one keeper round.
func (k keeper) RunOnce(ctx context.Context) error {
	if err := k.k.RunOnce(ctx); err != nil {
		return fmt.Errorf("treasury: chunk keeper: %w", err)
	}
	return nil
}
