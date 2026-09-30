package toolbox

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	pkgerrors "github.com/bsv-blockchain/go-wallet-toolbox/pkg/errors"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/wdk"

	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
)

// Bounded-memory policy for the built-split map (see Core.prune).
const (
	// builtTTL is how long a sent split stays known for idempotent rebroadcast.
	builtTTL = time.Hour
	// maxBuilt caps the map; beyond it the oldest sent splits go first, then
	// the oldest unsent ones.
	maxBuilt = 10_000
)

// maxFundingBEEF caps the atomic BEEF a funding request may carry.
const maxFundingBEEF = 4 << 20

// errSplitMismatch is returned when the wallet's split does not carry exactly
// the requested fuel outputs, in order, at its leading vouts.
var errSplitMismatch = errors.New("treasury: split outputs do not match request")

// builtInfo is what Send needs to know about a split this core built.
type builtInfo struct {
	pool    string
	beef    []byte
	builtAt time.Time
	sent    bool
	sentAt  time.Time
}

// Core is the network-free part of the toolbox treasury. It is safe for
// concurrent use.
//
// Core remembers the splits it built so Send can tell a first send (through
// the wallet, which owns the no-send transaction) from a rebroadcast (through
// the Broadcaster). That memory is bounded: a sent split is forgotten
// builtTTL after its first successful send, and the map never holds more than
// maxBuilt entries (oldest sent evicted first, then oldest unsent). Send for a
// forgotten split returns treasury.ErrUnknownTx; the watcher then falls back
// to rebroadcasting from the shard BEEF held in the store.
type Core struct {
	w      WalletAPI
	bc     Broadcaster
	keeper ChunkKeeper // may be nil
	nonces Nonces
	kd     *sdk.KeyDeriver
	cfg    Config
	feeKB  uint64
	log    *slog.Logger

	mu       sync.Mutex
	built    map[chainhash.Hash]*builtInfo
	now      func() time.Time
	maxBuilt int
}

var (
	_ treasury.Treasury = (*Core)(nil)
	_ treasury.Funder   = (*Core)(nil)
)

// NewCore returns a treasury core over w. bc (rebroadcasts) and keeper (chunk
// maintenance) may be nil; log nil discards.
func NewCore(w WalletAPI, bc Broadcaster, keeper ChunkKeeper, nonces Nonces, kd *sdk.KeyDeriver, cfg Config, feeSatPerKB uint64, log *slog.Logger) *Core {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Core{
		w: w, bc: bc, keeper: keeper, nonces: nonces, kd: kd, cfg: cfg, feeKB: feeSatPerKB, log: log,
		built:    map[chainhash.Hash]*builtInfo{},
		now:      time.Now,
		maxBuilt: maxBuilt,
	}
}

// Build creates and signs a split with the wallet (NoSend) and returns its
// shard and rows. The rows' unlocking scripts are bearer secrets and are never
// logged.
func (c *Core) Build(ctx context.Context, req treasury.MintRequest) (*treasury.BuiltShard, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	var (
		seed fuel.Seed
		key  *ec.PrivateKey
		lock []byte
	)
	switch req.LockMode {
	case store.LockPuzzle:
		seed = fuel.NewSeed()
	case store.LockP2PKH:
		var err error
		if key, err = poolKey(c.kd, req.Pool); err != nil {
			return nil, err
		}
		if lock, err = fuel.P2PKHLock(key.PubKey()); err != nil {
			return nil, fmt.Errorf("treasury: pool lock: %w", err)
		}
	default:
		return nil, treasury.ErrUnsupportedLockMode
	}

	outputs := make([]sdk.CreateActionOutput, req.Count)
	for i := range outputs {
		l := lock
		if req.LockMode == store.LockPuzzle {
			l = fuel.PuzzleLock(fuel.PuzzleHash(fuel.Preimage(seed, uint32(i)))) //nolint:gosec // i < Count, a small int
		}
		outputs[i] = sdk.CreateActionOutput{LockingScript: l, Satoshis: req.Denomination, OutputDescription: "fuel " + req.Pool}
	}
	no, yes := false, true
	res, err := c.w.CreateAction(ctx, sdk.CreateActionArgs{
		Description: "fuel split " + req.Pool,
		Outputs:     outputs,
		Labels:      []string{"fuelkeeper", "fuel-split", "pool:" + req.Pool},
		Options:     &sdk.CreateActionOptions{NoSend: &yes, RandomizeOutputs: &no, AcceptDelayedBroadcast: &no},
	}, c.cfg.Originator)
	if err != nil {
		if isInsufficientFunds(err) {
			return nil, fmt.Errorf("%w: %w", treasury.ErrInsufficientFunds, err)
		}
		return nil, fmt.Errorf("treasury: create split: %w", err)
	}
	if res == nil || res.Txid == (chainhash.Hash{}) || len(res.Tx) == 0 {
		return nil, errors.New("treasury: create split: wallet returned no transaction")
	}
	txid := res.Txid
	raw, err := fuel.RawTxFromBEEF(res.Tx, txid)
	if err != nil {
		return nil, fmt.Errorf("treasury: split beef: %w", err)
	}
	if err := verifySplit(raw, txid, outputs); err != nil {
		return nil, err
	}

	rows := make([]store.Row, req.Count)
	for i := range rows {
		vout := uint32(i) //nolint:gosec // i < Count, a small int
		var unlock []byte
		if req.LockMode == store.LockPuzzle {
			unlock = fuel.PuzzleUnlock(fuel.Preimage(seed, vout))
		} else if unlock, err = fuel.P2PKHPresign(key, txid, vout, lock, req.Denomination); err != nil {
			return nil, fmt.Errorf("treasury: presign vout %d: %w", vout, err)
		}
		rows[i] = store.Row{
			Outpoint:        store.Outpoint{TxID: txid, Vout: vout},
			Pool:            req.Pool,
			Satoshis:        req.Denomination,
			LockingScript:   outputs[i].LockingScript,
			UnlockingScript: unlock,
			State:           store.StateAvailable,
		}
	}
	shard := store.Shard{
		Pool:         req.Pool,
		TxID:         txid,
		Size:         req.Count,
		Denomination: req.Denomination,
		LockMode:     req.LockMode,
		Status:       store.ShardUnmined,
		BEEF:         res.Tx,
	}
	if err := store.ValidatePutShard(shard, rows); err != nil {
		return nil, fmt.Errorf("treasury: built shard: %w", err)
	}

	c.mu.Lock()
	c.built[txid] = &builtInfo{pool: req.Pool, beef: res.Tx, builtAt: c.now()}
	c.pruneLocked()
	c.mu.Unlock()
	c.log.Info("fuel split built", "pool", req.Pool, "txid", txid.String(), "count", req.Count, "lock_mode", req.LockMode.String())
	return &treasury.BuiltShard{Shard: shard, Rows: rows, RawTx: raw}, nil
}

// verifySplit checks that raw is txid and its leading len(want) outputs are
// exactly the requested fuel outputs, in order. Trailing outputs (the
// wallet's change) are allowed. It guards the fuel rows against a wallet that
// reorders or alters outputs.
func verifySplit(raw []byte, txid chainhash.Hash, want []sdk.CreateActionOutput) error {
	tx, err := transaction.NewTransactionFromBytes(raw)
	if err != nil {
		return fmt.Errorf("treasury: parse split: %w", err)
	}
	if !tx.TxID().IsEqual(&txid) {
		return fmt.Errorf("%w: txid mismatch", errSplitMismatch)
	}
	if len(tx.Outputs) < len(want) {
		return fmt.Errorf("%w: %d outputs, want at least %d", errSplitMismatch, len(tx.Outputs), len(want))
	}
	for i, w := range want {
		o := tx.Outputs[i]
		if o == nil || o.LockingScript == nil || o.Satoshis != w.Satoshis || !bytes.Equal(*o.LockingScript, w.LockingScript) {
			return fmt.Errorf("%w: vout %d", errSplitMismatch, i)
		}
	}
	return nil
}

// isInsufficientFunds reports whether a createAction error means the wallet
// cannot fund the split right now.
func isInsufficientFunds(err error) bool {
	if errors.Is(err, wdk.ErrNotEnoughFunds) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not enough funds") || strings.Contains(msg, "insufficient") || strings.Contains(msg, "contention")
}

// Send broadcasts a split this core built. The first successful call goes
// through the wallet (SendWith), which owns the no-send transaction; later
// calls rebroadcast the stored BEEF through the Broadcaster (nil: no-op). A
// failed first send is not recorded, so a retry goes through the wallet
// again. A Broadcaster that knows a rebroadcast was terminally rejected
// should return an error wrapping treasury.ErrRejected.
func (c *Core) Send(ctx context.Context, txid chainhash.Hash) error {
	c.mu.Lock()
	info, ok := c.built[txid]
	if !ok {
		c.mu.Unlock()
		return treasury.ErrUnknownTx
	}
	sent, beef, poolName := info.sent, info.beef, info.pool
	c.mu.Unlock()

	if sent {
		if c.bc == nil {
			return nil
		}
		if err := c.bc.PostBEEF(ctx, beef, txid.String()); err != nil {
			return fmt.Errorf("treasury: rebroadcast split: %w", err)
		}
		return nil
	}

	no := false
	_, err := c.w.CreateAction(ctx, sdk.CreateActionArgs{
		Description: "send fuel split",
		Options:     &sdk.CreateActionOptions{SendWith: []chainhash.Hash{txid}, AcceptDelayedBroadcast: &no},
	}, c.cfg.Originator)
	if err != nil {
		return classifySendError(ctx, err, txid)
	}
	c.mu.Lock()
	if info, ok := c.built[txid]; ok && !info.sent {
		info.sent = true
		info.sentAt = c.now()
	}
	c.pruneLocked()
	c.mu.Unlock()
	c.log.Info("fuel split sent", "pool", poolName, "txid", txid.String())
	return nil
}

// classifySendError maps a SendWith failure: a terminal network rejection of
// txid wraps treasury.ErrRejected (the minter and watcher then retire the
// shard); anything else is transient.
//
// The toolbox reports every non-successful undelayed broadcast as a
// *ProcessActionError, including transient service errors. Only its review
// statuses doubleSpend and invalidTx are terminal; a serviceError (or no
// review at all) must not retire a shard whose split may still be mined.
func classifySendError(ctx context.Context, err error, txid chainhash.Hash) error {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("treasury: send split: %w", err)
	}
	var perr *pkgerrors.ProcessActionError
	if errors.As(err, &perr) {
		id := txid.String()
		for _, r := range perr.ReviewResults {
			if r.TxID != "" && string(r.TxID) != id {
				continue
			}
			if r.Status == wdk.ReviewActionResultStatusDoubleSpend || r.Status == wdk.ReviewActionResultStatusInvalidTx {
				return fmt.Errorf("%w: %w", treasury.ErrRejected, err)
			}
		}
		return fmt.Errorf("treasury: send split: %w", err)
	}
	msg := strings.ToLower(err.Error())
	for _, s := range rejectionPhrases {
		if strings.Contains(msg, s) {
			return fmt.Errorf("%w: %w", treasury.ErrRejected, err)
		}
	}
	return fmt.Errorf("treasury: send split: %w", err)
}

// rejectionPhrases identify a terminal rejection in an unstructured error.
// "invalid" alone is deliberately absent: the toolbox also says "invalid" for
// argument validation, which is not a network verdict on the split.
var rejectionPhrases = []string{
	"double spend", "double-spend", "doublespend",
	"rejected", "malformed",
	"invalid tx", "invalid transaction", "invalidtx",
}

// pruneLocked applies the bounded-memory policy. c.mu must be held.
func (c *Core) pruneLocked() {
	now := c.now()
	for id, b := range c.built {
		if b.sent && now.Sub(b.sentAt) > builtTTL {
			delete(c.built, id)
		}
	}
	for len(c.built) > c.maxBuilt {
		var (
			victim    chainhash.Hash
			found     bool
			vSent     bool
			vAt       time.Time
			candidate = func(id chainhash.Hash, sent bool, at time.Time) {
				switch {
				case !found, sent && !vSent, sent == vSent && at.Before(vAt):
					victim, found, vSent, vAt = id, true, sent, at
				}
			}
		)
		for id, b := range c.built {
			if b.sent {
				candidate(id, true, b.sentAt)
			} else {
				candidate(id, false, b.builtAt)
			}
		}
		delete(c.built, victim)
	}
}

// builtLen reports how many splits the core remembers.
func (c *Core) builtLen() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.built)
}

// MinedBEEF converts a shard's unmined BEEF and the oracle's BUMP into the
// compact mined BEEF.
func (c *Core) MinedBEEF(_ context.Context, txid chainhash.Hash, unminedBEEF, merklePath []byte) ([]byte, error) {
	return fuel.MinedBEEF(unminedBEEF, txid, merklePath)
}

// EnsureChunks runs one pass of the chunk keeper; a no-op without one.
func (c *Core) EnsureChunks(ctx context.Context) error {
	if c.keeper == nil {
		return nil
	}
	return c.keeper.RunOnce(ctx)
}

// FundingInfo returns the treasury identity key and a fresh derivation prefix.
func (c *Core) FundingInfo(ctx context.Context) (treasury.FundingInfo, error) {
	res, err := c.w.GetPublicKey(ctx, sdk.GetPublicKeyArgs{IdentityKey: true}, c.cfg.Originator)
	if err != nil {
		return treasury.FundingInfo{}, fmt.Errorf("treasury: identity key: %w", err)
	}
	if res == nil || res.PublicKey == nil {
		return treasury.FundingInfo{}, errors.New("treasury: identity key: wallet returned none")
	}
	nonce, err := c.nonces.Create(ctx)
	if err != nil {
		return treasury.FundingInfo{}, fmt.Errorf("treasury: derivation prefix: %w", err)
	}
	return treasury.FundingInfo{IdentityKeyHex: res.PublicKey.ToDERHex(), DerivationPrefixB64: nonce, Network: c.cfg.Network}, nil
}

// badFunding wraps treasury.ErrBadFunding with a reason.
func badFunding(format string, args ...any) error {
	return fmt.Errorf("%w: %s", treasury.ErrBadFunding, fmt.Sprintf(format, args...))
}

// Internalize accepts a BRC-29 payment into the treasury wallet. Malformed or
// unverifiable requests return treasury.ErrBadFunding; a wallet failure does
// not (it is the server's problem, not the payer's).
func (c *Core) Internalize(ctx context.Context, req treasury.FundRequest) (treasury.FundResult, error) {
	if len(req.AtomicBEEF) > maxFundingBEEF {
		return treasury.FundResult{}, badFunding("atomic BEEF is %d bytes, max %d", len(req.AtomicBEEF), maxFundingBEEF)
	}
	if req.OutputIndex == -1 {
		return treasury.FundResult{}, badFunding("output index auto-detection is not available")
	}
	if req.OutputIndex < 0 {
		return treasury.FundResult{}, badFunding("output index %d is negative", req.OutputIndex)
	}
	ok, err := c.nonces.Verify(ctx, req.DerivationPrefixB64)
	if err != nil {
		return treasury.FundResult{}, fmt.Errorf("%w: verify derivation prefix: %w", treasury.ErrBadFunding, err)
	}
	if !ok {
		return treasury.FundResult{}, badFunding("unknown derivation prefix")
	}
	prefix, err := base64.StdEncoding.DecodeString(req.DerivationPrefixB64)
	if err != nil || len(prefix) == 0 {
		return treasury.FundResult{}, badFunding("derivation prefix is not base64")
	}
	suffix, err := base64.StdEncoding.DecodeString(req.DerivationSuffixB64)
	if err != nil || len(suffix) == 0 {
		return treasury.FundResult{}, badFunding("derivation suffix must be non-empty base64")
	}
	sender, err := ec.PublicKeyFromString(req.SenderIdentityKeyHex)
	if err != nil {
		return treasury.FundResult{}, badFunding("sender identity key: %v", err)
	}
	_, txid, err := transaction.NewBeefFromAtomicBytes(req.AtomicBEEF)
	if err != nil {
		return treasury.FundResult{}, badFunding("atomic BEEF: %v", err)
	}
	raw, err := fuel.RawTxFromBEEF(req.AtomicBEEF, *txid)
	if err != nil {
		return treasury.FundResult{}, badFunding("atomic BEEF subject: %v", err)
	}
	tx, err := transaction.NewTransactionFromBytes(raw)
	if err != nil {
		return treasury.FundResult{}, badFunding("payment tx: %v", err)
	}
	if req.OutputIndex >= len(tx.Outputs) {
		return treasury.FundResult{}, badFunding("output index %d out of range (tx has %d outputs)", req.OutputIndex, len(tx.Outputs))
	}
	res, err := c.w.InternalizeAction(ctx, sdk.InternalizeActionArgs{
		Tx:          req.AtomicBEEF,
		Description: "fuelkeeper funding",
		Labels:      []string{"fuelkeeper", "funding"},
		Outputs: []sdk.InternalizeOutput{{
			OutputIndex:       uint32(req.OutputIndex), //nolint:gosec // bounded by len(tx.Outputs) above
			Protocol:          sdk.InternalizeProtocolWalletPayment,
			PaymentRemittance: &sdk.Payment{DerivationPrefix: prefix, DerivationSuffix: suffix, SenderIdentityKey: sender},
		}},
	}, c.cfg.Originator)
	if err != nil {
		return treasury.FundResult{}, fmt.Errorf("treasury: internalize: %w", err)
	}
	if res == nil || !res.Accepted {
		return treasury.FundResult{}, errors.New("treasury: internalize: wallet did not accept the payment")
	}
	sats := tx.Outputs[req.OutputIndex].Satoshis
	c.log.Info("treasury funded", "txid", txid.String(), "vout", req.OutputIndex, "satoshis", sats)
	return treasury.FundResult{TxID: txid.String(), Satoshis: sats}, nil
}
