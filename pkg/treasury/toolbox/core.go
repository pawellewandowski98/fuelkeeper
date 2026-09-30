package toolbox

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/brc29"
	pkgerrors "github.com/bsv-blockchain/go-wallet-toolbox/pkg/errors"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/wdk"
	"golang.org/x/sync/singleflight"

	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
)

// maxFundingBEEF caps the atomic BEEF a funding request may carry.
const maxFundingBEEF = 4 << 20

// abortTimeout bounds a best-effort abort that runs after the caller's
// context may already be done.
const abortTimeout = 30 * time.Second

// splitLabel marks every split transaction in the wallet; Reconcile lists by it.
const splitLabel = "fuel-split"

// reconcilePageSize is the ListActions page size Reconcile uses (the toolbox
// caps it at 10000).
const reconcilePageSize = 1000

// errSplitMismatch is returned when the wallet's split does not carry exactly
// the requested fuel outputs, in order, at its leading vouts.
var errSplitMismatch = errors.New("treasury: split outputs do not match request")

// errWalletDidNotPost is returned by Send when the wallet reports it did not
// post the split (it could not claim it, or reports it failed). It is
// transient: the split must not be posted around the wallet, whose claim gate
// may be refusing because it released the split's inputs.
var errWalletDidNotPost = errors.New("treasury: wallet did not post the split")

// Core is the network-free part of the toolbox treasury. It is safe for
// concurrent use and keeps no per-split state: the wallet remembers every
// split it built (as a no-send action) across restarts, so Send asks the
// wallet first every time.
//
// After a restart, Send for a split recorded in the store goes to the wallet
// as usual: a no-send split is claimed and posted, a split the wallet already
// posted comes back "unproven" and is re-posted from the caller's BEEF. A
// split the store never recorded (a crash between Build and PutShard) is
// found and aborted by Reconcile, which must run before the minter starts.
type Core struct {
	w      WalletAPI
	bc     Broadcaster
	keeper ChunkKeeper // may be nil
	nonces Nonces
	kd     *sdk.KeyDeriver
	cfg    Config
	log    *slog.Logger
	sleep  func(ctx context.Context, d time.Duration) error

	// sends collapses concurrent Sends of one txid (the minter and the
	// watcher can race) into one wallet call; every caller gets its result.
	sends singleflight.Group
}

var (
	_ treasury.Treasury = (*Core)(nil)
	_ treasury.Funder   = (*Core)(nil)
)

// NewCore returns a treasury core over w. bc (re-posts) and keeper (chunk
// maintenance) may be nil; log nil discards. kd must derive from the wallet's
// root key: it presigns P2PKH fuel and checks BRC-29 payments.
func NewCore(w WalletAPI, bc Broadcaster, keeper ChunkKeeper, nonces Nonces, kd *sdk.KeyDeriver, cfg Config, log *slog.Logger) *Core {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Core{w: w, bc: bc, keeper: keeper, nonces: nonces, kd: kd, cfg: cfg, log: log, sleep: sleepCtx}
}

// Build creates and signs a split with the wallet (NoSend) and returns its
// shard and rows. Any failure after the wallet created the split aborts it,
// so its inputs return to the wallet and it can never be broadcast. The rows'
// unlocking scripts are bearer secrets and are never logged.
//
// The toolbox trims and lowercases labels
// (wallet/internal/mapping/mapping_create_action_args.go:32 via
// mapping_internalize_action.go:24 and wdk/primitives/strings.go:81-82), so
// pools whose names differ only in case ("Main", "main") share the
// "pool:main" label. The label is informational only; nothing (Reconcile
// included) selects splits by it.
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
		Labels:      []string{"fuelkeeper", splitLabel, "pool:" + req.Pool},
		Options:     &sdk.CreateActionOptions{NoSend: &yes, RandomizeOutputs: &no, AcceptDelayedBroadcast: &no},
	}, c.cfg.Originator)
	if err != nil {
		if isInsufficientFunds(err) {
			return nil, fmt.Errorf("%w: %w", treasury.ErrInsufficientFunds, err)
		}
		return nil, fmt.Errorf("treasury: create split: %w", err)
	}
	if res == nil || res.Txid == (chainhash.Hash{}) {
		return nil, errors.New("treasury: create split: wallet returned no transaction")
	}
	txid := res.Txid
	fail := func(err error) (*treasury.BuiltShard, error) {
		c.abort(ctx, txid, err)
		return nil, err
	}
	if len(res.Tx) == 0 {
		return fail(errors.New("treasury: create split: wallet returned no BEEF"))
	}
	beef, raw, err := splitBEEF(res.Tx, txid)
	if err != nil {
		return fail(err)
	}
	if err := verifySplit(raw, txid, outputs); err != nil {
		return fail(err)
	}

	rows := make([]store.Row, req.Count)
	for i := range rows {
		vout := uint32(i) //nolint:gosec // i < Count, a small int
		var unlock []byte
		if req.LockMode == store.LockPuzzle {
			unlock = fuel.PuzzleUnlock(fuel.Preimage(seed, vout))
		} else if unlock, err = fuel.P2PKHPresign(key, txid, vout, lock, req.Denomination); err != nil {
			return fail(fmt.Errorf("treasury: presign vout %d: %w", vout, err))
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
		BEEF:         beef,
	}
	if err := validatePutShard(shard, rows); err != nil {
		return fail(fmt.Errorf("treasury: built shard: %w", err))
	}
	c.log.Info("fuel split built", "pool", req.Pool, "txid", txid.String(), "count", req.Count, "lock_mode", req.LockMode.String())
	return &treasury.BuiltShard{Shard: shard, Rows: rows, RawTx: raw}, nil
}

// validatePutShard is store.ValidatePutShard, replaceable by tests.
var validatePutShard = store.ValidatePutShard

// splitBEEF parses the wallet's (atomic) BEEF, requires it to be valid with
// full ancestry (go-sdk v1.4.1 transaction/beef.go:967 IsValid(false) rejects
// txid-only entries and inputs that do not trace to a BUMP, :1177-1189), and
// returns it re-serialised as plain BEEF v2 (the format fuel.MinedBEEF emits,
// so unmined and mined shards share one wire format) plus txid's raw bytes.
func splitBEEF(walletBEEF []byte, txid chainhash.Hash) (beef, raw []byte, err error) {
	b, err := transaction.NewBeefFromBytes(walletBEEF)
	if err != nil {
		return nil, nil, fmt.Errorf("treasury: split beef: %w", err)
	}
	if !b.IsValid(false) {
		return nil, nil, errors.New("treasury: split beef is not valid (missing or txid-only ancestry)")
	}
	btx := b.FindTransaction(txid.String())
	if btx == nil {
		return nil, nil, fmt.Errorf("treasury: split beef: %w", fuel.ErrTxNotInBEEF)
	}
	b.Version = transaction.BEEF_V2
	if beef, err = b.Bytes(); err != nil {
		return nil, nil, fmt.Errorf("treasury: serialise split beef: %w", err)
	}
	return beef, btx.Bytes(), nil
}

// abort releases a split the wallet created but Build could not complete,
// best-effort, even if ctx is already done: a split without a shard record
// must never reach the chain (its puzzle seed lives only in memory), and its
// inputs should go back to the wallet. The toolbox accepts the hex txid as
// the reference (storage/internal/actions/abort.go:63-73).
func (c *Core) abort(ctx context.Context, txid chainhash.Hash, cause error) {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), abortTimeout)
	defer cancel()
	res, err := c.w.AbortAction(actx, sdk.AbortActionArgs{Reference: []byte(txid.String())}, c.cfg.Originator)
	switch {
	case err != nil:
		c.log.Error("abort failed split", "txid", txid.String(), "cause", cause, "err", err)
	case res == nil || !res.Aborted:
		c.log.Error("abort failed split: wallet did not abort", "txid", txid.String(), "cause", cause)
	default:
		c.log.Warn("aborted failed split", "txid", txid.String(), "cause", cause)
	}
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

// Send broadcasts a split. It always asks the wallet first (createAction
// with SendWith = [txid], not delayed), because the wallet owns the split's
// broadcast state (go-wallet-toolbox v0.184.16
// storage/internal/actions/process.go):
//
//   - a no-send (or previously failed-to-post) split is claimed and posted
//     (:547-602); the wallet reports "unproven" once the network took it;
//   - a split the wallet already posted comes back "unproven" without a post
//     (:469-477, :654-693); Send then re-posts beef through the Broadcaster,
//     which is the watcher's rebroadcast (a re-post failure is logged, and
//     Send still succeeds: the split is accepted);
//   - "sending", "failed" or no result for txid is transient, and the split
//     is never posted around the wallet: the claim gate can legitimately
//     refuse (:558-573), e.g. after an abort released the inputs, and a post
//     then would be a double spend (internal/storage/repo/known_tx.go:140-148);
//   - a wallet with no record of txid (:1090, :665) gets beef posted directly
//     when the caller supplied it, else treasury.ErrUnknownTx.
//
// Only a *ProcessActionError whose review for txid is doubleSpend or
// invalidTx returns treasury.ErrRejected. With a nil beef, Send skips every
// direct post. Concurrent Sends of one txid share one wallet call (and the
// first caller's ctx).
func (c *Core) Send(ctx context.Context, txid chainhash.Hash, beef []byte) error {
	_, err, _ := c.sends.Do(txid.String(), func() (any, error) {
		return nil, c.send(ctx, txid, beef)
	})
	return err
}

func (c *Core) send(ctx context.Context, txid chainhash.Hash, beef []byte) error {
	no := false
	res, err := c.w.CreateAction(ctx, sdk.CreateActionArgs{
		Description: "send fuel split",
		Options:     &sdk.CreateActionOptions{SendWith: []chainhash.Hash{txid}, AcceptDelayedBroadcast: &no},
	}, c.cfg.Originator)
	if err != nil {
		if unknownToWallet(err, txid) {
			return c.postUnknown(ctx, txid, beef, err)
		}
		return classifySendError(ctx, err, txid)
	}
	status, found := sendWithStatus(res, txid)
	switch {
	case found && status == sdk.ActionResultStatusUnproven:
		// The network accepted the split: that is final for Send. A failed
		// re-post is only logged, whatever it wraps, so it can neither retire
		// an accepted split nor count as a failed send.
		if len(beef) > 0 && c.bc != nil {
			if err := c.bc.PostBEEF(ctx, beef, txid.String()); err != nil {
				c.log.Warn("re-post of accepted split failed", "txid", txid.String(), "err", err)
			}
		}
		c.log.Info("fuel split sent", "txid", txid.String())
		return nil
	case found && status == sdk.ActionResultStatusSending:
		return fmt.Errorf("treasury: send split: wallet still sending %s", txid)
	case found:
		return fmt.Errorf("%w: %s reported %q", errWalletDidNotPost, txid, status)
	default:
		return fmt.Errorf("%w: no result for %s", errWalletDidNotPost, txid)
	}
}

// postUnknown posts beef for a split the wallet has no record of.
func (c *Core) postUnknown(ctx context.Context, txid chainhash.Hash, beef []byte, walletErr error) error {
	if len(beef) == 0 || c.bc == nil {
		return fmt.Errorf("%w: wallet has no record of %s: %w", treasury.ErrUnknownTx, txid, walletErr)
	}
	if err := c.bc.PostBEEF(ctx, beef, txid.String()); err != nil {
		return fmt.Errorf("treasury: post split unknown to wallet: %w", err)
	}
	c.log.Warn("posted split the wallet has no record of", "txid", txid.String())
	return nil
}

// sendWithStatus finds txid's status in a SendWith result.
func sendWithStatus(res *sdk.CreateActionResult, txid chainhash.Hash) (sdk.ActionResultStatus, bool) {
	if res == nil {
		return "", false
	}
	for _, r := range res.SendWithResults {
		if r.Txid == txid {
			return r.Status, true
		}
	}
	return "", false
}

// unknownToWallet reports whether a SendWith error says the wallet has no
// broadcast record of txid. The toolbox reports it only as text:
// "known tx status for txID <txid> not found" (process.go:1090) and
// "transaction status not found for txID <txid>" (process.go:665).
func unknownToWallet(err error, txid chainhash.Hash) bool {
	msg := strings.ToLower(err.Error())
	id := txid.String()
	return strings.Contains(msg, "known tx status for txid "+id+" not found") ||
		strings.Contains(msg, "transaction status not found for txid "+id)
}

// classifySendError maps a SendWith failure: only a *ProcessActionError
// whose review of txid is doubleSpend or invalidTx is a terminal network
// rejection (treasury.ErrRejected: the minter and watcher retire the shard).
// Everything else - service errors, reviews of other transactions,
// unstructured errors - is transient, because retiring a shard whose split
// may still be mined strands its fuel.
func classifySendError(ctx context.Context, err error, txid chainhash.Hash) error {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("treasury: send split: %w", err)
	}
	var perr *pkgerrors.ProcessActionError
	if errors.As(err, &perr) {
		id := txid.String()
		for _, r := range perr.ReviewResults {
			if string(r.TxID) != id {
				continue
			}
			if r.Status == wdk.ReviewActionResultStatusDoubleSpend || r.Status == wdk.ReviewActionResultStatusInvalidTx {
				return fmt.Errorf("%w: %w", treasury.ErrRejected, err)
			}
		}
	}
	return fmt.Errorf("treasury: send split: %w", err)
}

// ReconcileReport is what Reconcile did.
type ReconcileReport struct {
	// Resent are no-send splits the store knows, handed to Send.
	Resent []chainhash.Hash
	// Aborted are no-send splits the store did not know in either pass.
	Aborted []chainhash.Hash
	// Deferred are unknown no-send splits seen only in the second listing:
	// possibly a Build whose PutShard is still in flight, so left alone.
	Deferred []chainhash.Hash
	// Errors holds the per-split failures (the split is in no other list),
	// including a failed store lookup, which never leads to an abort.
	Errors map[chainhash.Hash]error
}

// Reconcile finds splits the wallet holds as no-send (built but never sent:
// a crash between Build, PutShard and Send) and settles each one. It lists
// them twice, Config.ReconcileGrace apart (longer than any Build to PutShard
// window), and then asks known(ctx, txid) afresh for each split of the
// second listing:
//
//   - known: sent with Send(ctx, txid, nil). A no-send split is claimed and
//     posted by the wallet itself, so no BEEF is needed.
//   - unknown and seen in both listings: aborted. Its puzzle seed lived only
//     in the crashed process's memory, so it must never reach the chain.
//     Abort parks it as invalidTx in the wallet
//     (internal/storage/repo/known_tx.go:81-88), which is irreversible.
//   - unknown and seen only in the second listing: Deferred, not touched.
//   - known returned an error: recorded in Errors and never aborted, since a
//     transient store error must not destroy a recorded split.
//
// The grace covers a minter in another instance (overlapping runs are
// allowed during a lease hand-over) that built a split and has not yet
// recorded it. Task 5 should run Reconcile as the first step inside the
// minter's lease, or at startup, and always with the grace. Only a listing
// failure (or ctx ending during the grace) returns an error.
func (c *Core) Reconcile(ctx context.Context, known func(context.Context, chainhash.Hash) (bool, error)) (ReconcileReport, error) {
	rep := ReconcileReport{Errors: map[chainhash.Hash]error{}}
	first, err := c.noSendSplits(ctx)
	if err != nil {
		return rep, err
	}
	if len(first) == 0 {
		return rep, nil
	}
	seen := make(map[chainhash.Hash]bool, len(first))
	for _, txid := range first {
		seen[txid] = true
	}
	if err := c.sleep(ctx, c.cfg.ReconcileGrace); err != nil {
		return rep, fmt.Errorf("treasury: reconcile grace: %w", err)
	}
	second, err := c.noSendSplits(ctx)
	if err != nil {
		return rep, err
	}
	for _, txid := range second {
		isKnown, err := known(ctx, txid)
		switch {
		case err != nil:
			rep.Errors[txid] = fmt.Errorf("treasury: look up split: %w", err)
		case isKnown:
			if err := c.Send(ctx, txid, nil); err != nil {
				rep.Errors[txid] = err
				continue
			}
			rep.Resent = append(rep.Resent, txid)
		case !seen[txid]:
			rep.Deferred = append(rep.Deferred, txid)
		default:
			c.abortUnrecorded(ctx, txid, &rep)
		}
	}
	c.log.Info("treasury reconciled", "resent", len(rep.Resent), "aborted", len(rep.Aborted),
		"deferred", len(rep.Deferred), "errors", len(rep.Errors))
	return rep, nil
}

// abortUnrecorded aborts a split no store record will ever point at.
func (c *Core) abortUnrecorded(ctx context.Context, txid chainhash.Hash, rep *ReconcileReport) {
	res, err := c.w.AbortAction(ctx, sdk.AbortActionArgs{Reference: []byte(txid.String())}, c.cfg.Originator)
	switch {
	case err != nil:
		rep.Errors[txid] = fmt.Errorf("treasury: abort unrecorded split: %w", err)
	case res == nil || !res.Aborted:
		rep.Errors[txid] = errors.New("treasury: abort unrecorded split: wallet did not abort")
	default:
		rep.Aborted = append(rep.Aborted, txid)
	}
}

// sleepCtx waits d or until ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// noSendSplits lists every split action in status nosend. The whole listing
// is collected before anything is acted on, so aborts cannot shift the pages.
// The toolbox's ListActions includes nosend actions
// (storage/internal/actions/list_actions_mapping.go:42-45).
func (c *Core) noSendSplits(ctx context.Context) ([]chainhash.Hash, error) {
	var out []chainhash.Hash
	limit := uint32(reconcilePageSize)
	for offset := uint32(0); ; {
		off := offset
		res, err := c.w.ListActions(ctx, sdk.ListActionsArgs{
			Labels:         []string{splitLabel},
			LabelQueryMode: sdk.QueryModeAny,
			Limit:          &limit,
			Offset:         &off,
		}, c.cfg.Originator)
		if err != nil {
			return nil, fmt.Errorf("treasury: list splits: %w", err)
		}
		if res == nil {
			return nil, errors.New("treasury: list splits: wallet returned nothing")
		}
		for _, a := range res.Actions {
			if a.Status == sdk.ActionStatusNoSend {
				out = append(out, a.Txid)
			}
		}
		offset += uint32(len(res.Actions)) //nolint:gosec // a page is at most limit entries
		if len(res.Actions) == 0 || offset >= res.TotalActions {
			return out, nil
		}
	}
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

// Internalize accepts a BRC-29 payment into the treasury wallet. Everything
// the payer controls is checked first and returns treasury.ErrBadFunding
// (including an output that does not pay the treasury's BRC-29 script for the
// given prefix, suffix and sender); a wallet failure or an unavailable nonce
// check does not (it is the server's problem, not the payer's).
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
	prefix, err := base64.StdEncoding.DecodeString(req.DerivationPrefixB64)
	if err != nil || len(prefix) == 0 {
		return treasury.FundResult{}, badFunding("derivation prefix must be non-empty standard base64")
	}
	ok, err := c.nonces.Verify(ctx, req.DerivationPrefixB64)
	if err != nil {
		return treasury.FundResult{}, fmt.Errorf("treasury: verify derivation prefix: %w", err)
	}
	if !ok {
		return treasury.FundResult{}, badFunding("unknown derivation prefix")
	}
	suffix, err := base64.StdEncoding.DecodeString(req.DerivationSuffixB64)
	if err != nil || len(suffix) == 0 {
		return treasury.FundResult{}, badFunding("derivation suffix must be non-empty standard base64")
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
	out := tx.Outputs[req.OutputIndex]
	// The same check the wallet makes (go-wallet-toolbox v0.184.16
	// pkg/internal/validate/validate_internalize_action_args.go:76-93), with
	// the same key id: the std-base64 prefix and suffix
	// (pkg/wallet/internal/mapping/mapping_internalize_action.go:54,60-63).
	want, err := brc29.LockForSelf(sender, brc29.KeyID{
		DerivationPrefix: base64.StdEncoding.EncodeToString(prefix),
		DerivationSuffix: base64.StdEncoding.EncodeToString(suffix),
	}, c.kd)
	if err != nil {
		return treasury.FundResult{}, fmt.Errorf("treasury: expected payment script: %w", err)
	}
	if out.LockingScript == nil || !bytes.Equal(*out.LockingScript, *want) {
		return treasury.FundResult{}, badFunding("output %d does not pay the treasury's BRC-29 script for this prefix, suffix and sender", req.OutputIndex)
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
	c.log.Info("treasury funded", "txid", txid.String(), "vout", req.OutputIndex, "satoshis", out.Satoshis)
	return treasury.FundResult{TxID: txid.String(), Satoshis: out.Satoshis}, nil
}
