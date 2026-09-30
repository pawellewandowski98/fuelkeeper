package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"

	"golang.org/x/time/rate"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/defs"
	pkgerrors "github.com/bsv-blockchain/go-wallet-toolbox/pkg/errors"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/services"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/storage"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/wallet"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/wdk"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
)

// toolboxStorageName is the name the wallet's sqlite storage is migrated
// under.
const toolboxStorageName = "fueldemo"

// toolboxOriginator is the BRC-100 originator every createAction call names.
const toolboxOriginator = "fueldemo"

// toolboxInputDescription describes every fuel input of a createAction call.
// It must be 5..2000 bytes (client.CreateActionInputs and the toolbox both
// enforce it).
const toolboxInputDescription = "fuel input"

// probeTimeout bounds the start-up reachability probe of chaintracks.
const probeTimeout = 10 * time.Second

// actionTimeout bounds one createAction call. The call runs on a
// context.WithoutCancel-derived deadline rather than the worker's own ctx:
// once the wallet has signed and handed a transaction to storage it may
// already be on its way to arcade, and aborting halfway would leave the fuel
// output's fate unknown (neither reportably spent nor safely releasable).
// The run's end therefore waits for in-flight actions, up to this bound.
const actionTimeout = 30 * time.Second

// actionCreator is what toolbox mode needs from a BRC-100 wallet: go-sdk's
// wallet.Interface CreateAction. It is satisfied by the go-wallet-toolbox
// *wallet.Wallet and, in tests, by a fake.
type actionCreator interface {
	CreateAction(ctx context.Context, args sdk.CreateActionArgs, originator string) (*sdk.CreateActionResult, error)
}

// toolboxRunner drives toolbox mode's workers: each round, reserve a batch
// of fuel from fk, then spend every output through its own createAction call
// on a go-wallet-toolbox wallet that holds no funds of its own. The action's
// only input is the fuel output (its unlocking script supplied, its BEEF the
// reservation's merged shard BEEF) and its only output an OP_RETURN; the
// wallet computes the fee. A surplus (denomination minus fee) below the
// funder's dust floor -- twice the fee of a minimal 1-in/1-out P2PKH spend,
// 40 sat at 100 sat/kB (TB pkg/internal/storage/funder/sql.go:505-512) -- is
// left to miners as extra fee, so small fuel (e.g. 50 sat) creates no change
// output; a surplus above fee plus that floor does become a change output in
// the wallet's default basket. The wallet broadcasts through arcade itself
// (non-delayed), so a successful createAction already carries arcade's
// verdict.
type toolboxRunner struct {
	fk      *client.Client
	w       actionCreator
	pool    string
	batch   int
	rep     *report
	limiter *rate.Limiter // nil = unbounded
	memo    []byte        // OP_RETURN locking script

	underfundedOnce sync.Once
	warn            func(format string, args ...any)

	// Owned toolbox stack; nil in unit tests that inject w directly.
	svc      *services.WalletServices
	provider *storage.Provider
	wallet   *wallet.Wallet
	cancel   context.CancelFunc
}

// newToolboxRunner runs toolbox mode for o.duration (or until ctx ends):
// it opens the toolbox stack (see openToolbox), drives o.workers workers
// against fk, tallies into rep, and closes the stack. A configuration or
// start-up failure is returned before any worker starts; nothing is left
// running and, when the failure precedes storage creation (every flag and
// reachability check does), no sqlite file is created.
func newToolboxRunner(ctx context.Context, o options, fk *client.Client, rep *report) error {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	t, err := openToolbox(ctx, o, log)
	if err != nil {
		return err
	}
	defer t.Close()

	t.fk = fk
	t.rep = rep
	if o.rateLim > 0 {
		t.limiter = rate.NewLimiter(rate.Limit(o.rateLim), 1)
	}

	runCtx, cancel := context.WithTimeout(ctx, o.duration)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(o.workers)
	for i := 0; i < o.workers; i++ {
		go func(id int) {
			defer wg.Done()
			t.worker(runCtx, id)
		}(i)
	}
	wg.Wait()
	return nil
}

// openToolbox validates the toolbox-mode flags and builds the wallet stack,
// in the order of Plan 5's treasury wiring (pkg/treasury/toolbox/wiring.go)
// minus the monitor, chunk keeper, throughput UTXO management and funding:
// services (arcade + chaintracks only), a chaintracks reachability probe,
// the GORM sqlite storage provider (migrated), and the wallet. The wallet
// key is o.walletKey, or a fresh random key printed to stderr (so the run
// can be repeated against the same storage) when empty; a supplied key is
// never printed or echoed in an error.
func openToolbox(ctx context.Context, o options, log *slog.Logger) (_ *toolboxRunner, err error) {
	if o.arcadeURL == "" {
		return nil, usageErrorf("fueldemo: toolbox mode requires --arcade (or $ARCADE_URL)")
	}
	if o.chaintracks == "" {
		return nil, usageErrorf("fueldemo: toolbox mode requires --chaintracks (or $CHAINTRACKS_URL): the wallet verifies every input BEEF against chaintracks")
	}
	if o.storage == "" {
		return nil, usageErrorf("fueldemo: toolbox mode requires --storage")
	}
	if o.feeKB == 0 {
		return nil, usageErrorf("fueldemo: toolbox mode requires --fee-kb > 0")
	}
	network, err := defs.ParseBSVNetworkStr(o.network)
	if err != nil {
		return nil, usageErrorf("fueldemo: --network: %w", err)
	}

	key, err := walletKey(o.walletKey)
	if err != nil {
		return nil, err
	}
	identityKey := key.PubKey().ToDERHex()

	svcCfg, err := servicesConfig(o, network, identityKey)
	if err != nil {
		return nil, err
	}
	fee := defs.FeeModel{Type: defs.SatPerKB, Value: int64(o.feeKB)} //nolint:gosec // a fee rate, far below MaxInt64

	memo, err := transaction.CreateOpReturnOutput([][]byte{[]byte("fueldemo")})
	if err != nil {
		return nil, fmt.Errorf("fueldemo: build OP_RETURN output: %w", err)
	}

	bgCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	t := &toolboxRunner{
		pool:   o.pool,
		batch:  o.batch,
		memo:   *memo.LockingScript,
		cancel: cancel,
		warn:   func(format string, args ...any) { fmt.Fprintf(os.Stderr, format, args...) },
	}
	defer func() {
		if err != nil {
			t.Close()
		}
	}()

	t.svc = services.New(log, svcCfg)
	probeCtx, probeCancel := context.WithTimeout(ctx, probeTimeout)
	_, perr := t.svc.FindChainTipHeader(probeCtx)
	probeCancel()
	if perr != nil {
		return nil, fmt.Errorf("fueldemo: chaintracks at %s unreachable (needed to verify input BEEF): %w", o.chaintracks, perr)
	}
	if err := t.svc.StartBackgroundServices(bgCtx); err != nil {
		return nil, fmt.Errorf("fueldemo: start toolbox services: %w", err)
	}

	db := defs.DefaultDBConfig()
	db.Engine = defs.DBTypeSQLite
	db.SQLite.ConnectionString = o.storage
	t.provider, err = storage.NewGORMProvider(network, t.svc,
		storage.WithDBConfig(db),
		storage.WithFeeModel(fee),
		storage.WithLogger(log),
		storage.WithBackgroundBroadcasterContext(bgCtx),
	)
	if err != nil {
		return nil, fmt.Errorf("fueldemo: toolbox storage: %w", err)
	}
	if _, err := t.provider.Migrate(ctx, toolboxStorageName, identityKey); err != nil {
		return nil, fmt.Errorf("fueldemo: migrate toolbox storage: %w", err)
	}

	// AutoKnownTxids off: storage answers with full ancestry rather than
	// bare txids the wallet knows, for the output BEEF it returns. Harmless
	// here either way (ReturnTXIDOnly: that BEEF is never used), and it
	// keeps the wallet out of the known-txid advertise/resolve path.
	// TrustSelf "known" (also the toolbox default) lets the wallet accept
	// txid-only inputBEEF entries for splits it has already seen.
	t.wallet, err = wallet.New(network, key, t.provider,
		wallet.WithServices(t.svc),
		wallet.WithLogger(log),
		wallet.WithAutoKnownTxids(false),
		wallet.WithTrustSelf(sdk.TrustSelfKnown),
	)
	if err != nil {
		return nil, fmt.Errorf("fueldemo: toolbox wallet: %w", err)
	}
	t.w = t.wallet
	return t, nil
}

// servicesConfig is the toolbox's network defaults (TB
// pkg/defs/services.go:126) with the command line's arcade (callback token
// derived from the wallet identity key, TB pkg/wdk/arcade_token.go:12) and
// remote chaintracks, and every other broadcaster and header source turned
// off: no failover broadcast to public ARC/GorillaPool/WhatsOnChain/Bitails
// endpoints, which would silently move a benchmark off the system under
// test.
func servicesConfig(o options, network defs.BSVNetwork, identityKey string) (defs.WalletServices, error) {
	s := defs.DefaultServicesConfig(network)
	s.Arcade.Enabled = true
	s.Arcade.URL = o.arcadeURL
	s.Arcade.EventsURL = o.arcadeURL
	s.Arcade.CallbackToken = wdk.DeriveArcadeCallbackToken(identityKey)
	s.ChaintracksClient.Enabled = true
	s.ChaintracksClient.Mode = defs.ChaintracksClientModeRemote
	s.ChaintracksClient.RemoteURL = o.chaintracks
	s.ArcConfig.Enabled = false
	s.ArcGorillaPoolConfig.Enabled = false
	s.WhatsOnChain.Enabled = false
	s.Bitails.Enabled = false
	if err := s.Validate(); err != nil {
		return defs.WalletServices{}, fmt.Errorf("fueldemo: toolbox services config: %w", err)
	}
	return s, nil
}

// walletKey parses hexKey, or generates a fresh key (printed to stderr so
// the run can be repeated) when hexKey is empty. A supplied key is never
// echoed, not even in an error.
func walletKey(hexKey string) (*ec.PrivateKey, error) {
	if hexKey != "" {
		k, err := ec.PrivateKeyFromHex(hexKey)
		if err != nil {
			return nil, usageErrorf("fueldemo: --wallet-key: not a valid hex private key")
		}
		return k, nil
	}
	k, err := ec.NewPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("fueldemo: generate wallet key: %w", err)
	}
	fmt.Fprintf(os.Stderr, "fueldemo: generated wallet key %s (pass --wallet-key to reuse it with the same --storage)\n", k.Hex())
	return k, nil
}

// Close stops the background services and broadcaster, closes the wallet
// and the storage database. It is safe on a partially opened runner.
func (t *toolboxRunner) Close() {
	if t.cancel != nil {
		t.cancel()
	}
	if t.wallet != nil {
		t.wallet.Close()
	}
	if t.provider != nil {
		t.provider.Stop() // stops the background broadcaster, closes the database
	}
}

// classifyCreateError maps a createAction failure to the code toolbox mode
// tallies it under and the client.Settlement to report for the fuel output
// it spent -- see (*toolboxRunner).spendBatch, which routes every one of
// these through fk.Settle rather than deciding spend-vs-release itself:
//
//   - a *pkgerrors.ProcessActionError carrying a TxID -- the wallet built,
//     signed and posted the transaction -- is classified from its
//     ReviewResults entry for that TxID, when the wallet returned one:
//     "reject:double_spend" (client.OutcomeDoubleSpend, hinting the
//     competitor that sorts first -- see below) for status doubleSpend;
//     "reject:invalid" (client.OutcomeNotBroadcast -- no competing
//     transaction exists, so it's safe to release) for status invalidTx;
//     "reject:service" (client.OutcomeUnknown -- every enabled broadcaster
//     failed to answer, the toolbox may still post the transaction later)
//     for status serviceError.
//
//     This verdict is set synchronously, at post time, not by polling
//     arcade later: go-wallet-toolbox's storage layer aggregates every
//     enabled broadcaster's result for the txid (TB
//     pkg/storage/internal/actions/process.go's singleTxBroadcastResult)
//     and, when at least one reported a double-spend, re-verifies it (TB
//     pkg/storage/internal/actions/process_confirm_double_spends.go's
//     confirmDoubleSpends -- a single broadcaster can report a false
//     positive, e.g. WhatsOnChain answering "missing inputs" for a
//     freshly-chained tx whose parent hasn't propagated yet) before letting
//     it stay terminal, so a doubleSpend review result here can be trusted
//     immediately, unlike a bare RejectError/SubmitResponse straight off
//     arcade's own synchronous HTTP response (see raw.go's worker doc
//     comment). That said, against the toolbox's own configuration
//     (servicesConfig enables only arcade), this path realistically never
//     fires against real arcade: the aggregation only records a
//     double-spend when a broadcaster's result says so, and arcade's own
//     synchronous POST /tx response never names a competing transaction, at
//     submission or otherwise (see Client.Settle's "When a double-spend is
//     actually knowable"). It stays wired up for other configured gateways
//     and for arcade's own future evolution. The review's CompetingTxs is
//     collected from a Go map's keys (unspecified order), so this function
//     reports a sorted copy: Settle only ever hints the first entry, and
//     sorting first makes that choice deterministic rather than dependent
//     on map iteration order.
//
//   - a TxID with no matching review result -- e.g. storage failing to
//     record a transaction it has already broadcast (TB
//     pkg/storage/internal/actions/process.go:771-775) -- is
//     "reject:unknown" (client.OutcomeUnknown): reporting it Spend on sight
//     regardless of review, as toolbox mode once did, would burn the row
//     under spend_policy: trust on a mere bookkeeping failure, so its fate
//     is instead left to the reservation's own expiry.
//
//   - "timeout" (client.OutcomeUnknown): the action's own deadline
//     (actionTimeout) expired; the transaction may or may not have been
//     posted.
//
//   - "underfunded" (client.OutcomeNotBroadcast): wdk.ErrNotEnoughFunds --
//     the fuel output alone cannot pay the OP_RETURN output plus the fee at
//     the wallet's fee model (raise the pool denomination or lower
//     --fee-kb).
//
//   - "create" (client.OutcomeNotBroadcast): anything else (input BEEF
//     verification, a malformed input, storage), all before any broadcast.
func classifyCreateError(err error) (code string, s client.Settlement) {
	var perr *pkgerrors.ProcessActionError
	if errors.As(err, &perr) {
		if perr.TxID != "" {
			if rr, ok := reviewResultFor(perr.ReviewResults, perr.TxID); ok {
				switch rr.Status {
				case wdk.ReviewActionResultStatusDoubleSpend:
					competitors := slices.Clone(rr.CompetingTxs)
					slices.Sort(competitors) // CompetingTxs comes from a Go map's keys; sort for a deterministic hint
					return "reject:double_spend", client.Settlement{
						Outcome: client.OutcomeDoubleSpend, TxID: perr.TxID, CompetingTxIDs: competitors,
					}
				case wdk.ReviewActionResultStatusInvalidTx:
					return "reject:invalid", client.Settlement{Outcome: client.OutcomeNotBroadcast, TxID: perr.TxID}
				case wdk.ReviewActionResultStatusServiceError:
					return "reject:service", client.Settlement{Outcome: client.OutcomeUnknown, TxID: perr.TxID}
				}
			}
		}
		return "reject:unknown", client.Settlement{Outcome: client.OutcomeUnknown, TxID: perr.TxID}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout", client.Settlement{Outcome: client.OutcomeUnknown}
	}
	if errors.Is(err, wdk.ErrNotEnoughFunds) {
		return "underfunded", client.Settlement{Outcome: client.OutcomeNotBroadcast}
	}
	return "create", client.Settlement{Outcome: client.OutcomeNotBroadcast}
}

// reviewResultFor returns results' entry for txid, when one is present.
func reviewResultFor(results []wdk.ReviewActionResult, txid string) (wdk.ReviewActionResult, bool) {
	for _, rr := range results {
		if rr.TxID.String() == txid {
			return rr, true
		}
	}
	return wdk.ReviewActionResult{}, false
}

// actionArgs is the createAction call spending one fuel input into the
// runner's OP_RETURN output, with beef (the reservation's merged shard
// BEEF) as inputBEEF and res's P2PKH constraints applied.
func (t *toolboxRunner) actionArgs(res *wire.ReserveResponse, beef []byte, in sdk.CreateActionInput) sdk.CreateActionArgs {
	no, yes := false, true
	args := sdk.CreateActionArgs{
		Description: "fueldemo",
		InputBEEF:   beef,
		Inputs:      []sdk.CreateActionInput{in},
		Outputs: []sdk.CreateActionOutput{{
			LockingScript:     t.memo,
			Satoshis:          0,
			OutputDescription: "fueldemo memo", // BRC-100: 5..2000 bytes
		}},
		Options: &sdk.CreateActionOptions{
			AcceptDelayedBroadcast: &no,
			RandomizeOutputs:       &no,
			ReturnTXIDOnly:         &yes,
			TrustSelf:              sdk.TrustSelfKnown,
		},
	}
	client.ApplyConstraints(&args, res) // tx version 1 / lockTime 0 for P2PKH pools; no-op for puzzle pools
	return args
}

// reportSettle reports s for outpoint under token on a detached deadline of
// its own (see reportTimeout) and tallies "settle:<code>" on failure.
func (t *toolboxRunner) reportSettle(ctx context.Context, token, outpoint string, s client.Settlement) {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reportTimeout)
	defer cancel()
	if _, err := t.fk.Settle(settleCtx, t.pool, token, []string{outpoint}, s); err != nil {
		t.rep.fail("settle:" + apiCode(err))
	}
}

// release settles outpoints as OutcomeNotBroadcast (never reached a
// gateway, or classified so by classifyCreateError) on a detached deadline
// of its own (see reportTimeout), best effort. Batching every batch's
// leftover outpoints into one Settle call here, rather than reporting each
// individually, is what "release-of-unused" means throughout this file.
func (t *toolboxRunner) release(ctx context.Context, token string, outpoints []string) {
	if len(outpoints) == 0 {
		return
	}
	relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reportTimeout)
	defer cancel()
	_, _ = t.fk.Settle(relCtx, t.pool, token, outpoints, client.Settlement{Outcome: client.OutcomeNotBroadcast})
}

// worker runs one toolbox-mode loop until ctx is done: reserve a batch of
// fuel, then spend it (see spendBatch). Reserve failures are tallied and
// retried after a short jittered pause, exactly as in raw mode.
func (t *toolboxRunner) worker(ctx context.Context, id int) {
	pause := reserveRetryPause + time.Duration(id%5)*time.Millisecond
	for {
		if ctx.Err() != nil {
			return
		}
		res, err := t.fk.Reserve(ctx, t.pool, client.ReserveRequest{Count: t.batch, MinCount: 1})
		if err != nil {
			if ctx.Err() != nil {
				return // shutdown mid-call; not a real failure
			}
			t.rep.fail("reserve:" + apiCode(err))
			sleepBounded(ctx, pause)
			continue
		}
		t.rep.reserved()
		if t.spendBatch(ctx, res) == 0 {
			// Nothing went through: pause before reserving again, so a
			// persistent failure does not hot-loop reserve/release.
			sleepBounded(ctx, pause)
		}
	}
}

// spendBatch spends every output of res through its own createAction call
// (the reservation's shard BEEF is fetched and merged once), reports each
// outcome to FuelKeeper as classifyCreateError directs, and returns how many
// actions succeeded. Outputs left unspent before any broadcast -- cut short
// by ctx or the rate limiter, underfunded, or failed before broadcast -- are
// released at the end of the batch; an output whose broadcast fate is
// unknown is held (neither spent nor released) until its reservation
// expires.
func (t *toolboxRunner) spendBatch(ctx context.Context, res *wire.ReserveResponse) (ok int) {
	unused := make([]string, 0, len(res.Outputs))
	defer func() { t.release(ctx, res.Token, unused) }()
	all := func() {
		for _, o := range res.Outputs {
			unused = append(unused, o.Outpoint)
		}
	}

	inputs, err := client.CreateActionInputs(res, toolboxInputDescription)
	if err != nil {
		t.rep.fail("inputs")
		all()
		return 0
	}
	beef, err := t.fk.InputBEEF(ctx, t.pool, res)
	if err != nil {
		if ctx.Err() == nil {
			t.rep.fail("beef:" + apiCode(err))
		}
		all()
		return 0
	}

	for i, o := range res.Outputs {
		if ctx.Err() != nil {
			unused = append(unused, o.Outpoint)
			continue
		}
		if t.limiter != nil {
			if werr := t.limiter.Wait(ctx); werr != nil {
				unused = append(unused, o.Outpoint)
				continue
			}
		}

		args := t.actionArgs(res, beef, inputs[i])
		start := time.Now()
		actCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), actionTimeout)
		r, cerr := t.w.CreateAction(actCtx, args, toolboxOriginator)
		cancel()
		if cerr != nil {
			code, s := classifyCreateError(cerr)
			t.rep.fail(code)
			if code == "underfunded" {
				t.underfundedOnce.Do(func() {
					t.warn("fueldemo: the toolbox wallet cannot pay the fee from one fuel output alone at --fee-kb; raise the pool denomination or lower --fee-kb (%v)\n", cerr)
				})
			}
			switch s.Outcome {
			case client.OutcomeNotBroadcast:
				unused = append(unused, o.Outpoint) // batched into one Settle call at the end; see release
			case client.OutcomeDoubleSpend:
				// Never released: see classifyCreateError and Settle's doc
				// comments. Reported immediately, spent by the competitor.
				t.reportSettle(ctx, res.Token, o.Outpoint, s)
			default: // client.OutcomeUnknown (and, defensively, anything else)
				// Left reserved: see classifyCreateError's "timeout",
				// "reject:unknown" and "reject:service" cases. The
				// reservation expires and the reaper's quarantine + oracle
				// path settles it.
			}
			continue
		}

		t.rep.ok(time.Since(start))
		t.reportSettle(ctx, res.Token, o.Outpoint, client.Settlement{Outcome: client.OutcomeBroadcast, TxID: r.Txid.String()})
		ok++
	}
	return ok
}
