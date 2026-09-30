package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"golang.org/x/time/rate"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/arcadeclient"
	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
)

// reserveRetryPause is how long a worker waits, after a failed Reserve call,
// before trying again; it is jittered a little per worker (see worker) so
// many workers hitting the same transient failure do not retry in lockstep.
const reserveRetryPause = 50 * time.Millisecond

// reportTimeout bounds every best-effort report-back call a worker makes to
// FuelKeeper once the underlying action (a real arcade submission, or a
// forced release) has already happened: Spend, after a successful or
// rejected PostTx, and Release for a batch's leftover outputs. Each such
// call runs on a context.WithoutCancel-derived deadline of its own rather
// than the worker's own (possibly already-expiring) ctx, so a run ending
// right as a transaction lands can never cause a real broadcast to go
// unreported and its row to be silently abandoned as merely "reserved".
const reportTimeout = 2 * time.Second

// Submitter is what raw mode needs from arcade: submitting one transaction
// (raw or extended format) for validation and propagation. It is satisfied
// by *arcadeclient.Client and, in tests, by a fake.
type Submitter interface {
	PostTx(ctx context.Context, tx []byte) (*arcadeclient.SubmitResponse, error)
}

// rawRunner drives raw mode's workers: each round, reserve a batch of fuel
// from fk, spend every output into its own OP_RETURN transaction submitted
// to arc in extended format, and report the outcome (spend, or release for
// anything left unspent) back to fk.
type rawRunner struct {
	fk      *client.Client
	arc     Submitter
	pool    string
	batch   int
	rep     *report
	limiter *rate.Limiter // nil = unbounded

	memo     []byte // OP_RETURN payload
	satPerKB uint64 // fee rate for buildFuelTx's change decision
	changeTo []byte // locking script for an optional change output; nil = none

	// afterCycle, when set, is called once at the end of every
	// reserve/process/release cycle (after any leftover outputs have been
	// released, before the loop checks ctx again). It is nil in production.
	// Tests use it to stop a worker deterministically between cycles rather
	// than racing ctx's cancellation against an in-flight Reserve call: a
	// Reserve the server commits just as the client's ctx expires mid-flight
	// is reported to the caller as a context error (see worker's
	// ctx.Err()-after-Reserve-error guard) with no token to release those
	// rows by, leaving them reserved until the pool's own TTL reclaims them
	// -- the intended self-healing behaviour of a lease-based pool, but not
	// something a wall-clock-timeout test can assert around reliably.
	afterCycle func()
}

// buildFuelTx spends one fuel output o into an OP_RETURN carrying memo. When
// constraints is non-nil (presigned P2PKH mode), the transaction's version,
// lockTime and the input's sequence number are pinned to it, matching what
// the output's presigned SIGHASH_NONE|ANYONECANPAY signature commits to;
// otherwise (puzzle mode) version 1, lockTime 0 and sequence 0xffffffff are
// used.
//
// When changeTo is non-empty, a change output paying it is added, sized at
// o.Satoshis minus the fee estimated (via fuel.FeeSats) at satPerKB for the
// transaction's size including that change output. If that would leave less
// than 1 satoshi of change, the change output is dropped instead: the whole
// denomination is left to the miner as fee, which is the deliberate
// no-change behaviour for a fee-only OP_RETURN spend.
func buildFuelTx(o wire.Output, constraints *wire.Constraints, memo, changeTo []byte, satPerKB uint64) (*transaction.Transaction, error) {
	srcTxID, err := chainhash.NewHashFromHex(o.TxID)
	if err != nil {
		return nil, fmt.Errorf("fueldemo: output %s: parse txid: %w", o.Outpoint, err)
	}
	lockingScript, err := hex.DecodeString(o.LockingScript)
	if err != nil {
		return nil, fmt.Errorf("fueldemo: output %s: decode locking script: %w", o.Outpoint, err)
	}
	unlockingScript, err := hex.DecodeString(o.Unlock.Script)
	if err != nil {
		return nil, fmt.Errorf("fueldemo: output %s: decode unlocking script: %w", o.Outpoint, err)
	}

	version, lockTime, sequence := uint32(1), uint32(0), uint32(0xffffffff)
	if constraints != nil {
		version, lockTime, sequence = constraints.TxVersion, constraints.LockTime, constraints.Sequence
	}

	tx := &transaction.Transaction{Version: version, LockTime: lockTime}
	tx.AddInputWithOutput(
		&transaction.TransactionInput{
			SourceTXID:       srcTxID,
			SourceTxOutIndex: o.Vout,
			SequenceNumber:   sequence,
			UnlockingScript:  script.NewFromBytes(unlockingScript),
		},
		&transaction.TransactionOutput{
			Satoshis:      o.Satoshis,
			LockingScript: script.NewFromBytes(lockingScript),
		},
	)

	opReturn, err := transaction.CreateOpReturnOutput([][]byte{memo})
	if err != nil {
		return nil, fmt.Errorf("fueldemo: output %s: build OP_RETURN output: %w", o.Outpoint, err)
	}
	tx.AddOutput(opReturn)

	if len(changeTo) > 0 {
		change := &transaction.TransactionOutput{LockingScript: script.NewFromBytes(changeTo)}
		tx.AddOutput(change)
		fee := fuel.FeeSats(tx.Size(), satPerKB)
		// Require at least 1 satoshi of change: o.Satoshis > fee already
		// guarantees that for these uint64 values, but the explicit >= 1
		// keeps the dust/no-zero-value-output threshold visible here rather
		// than relying on unsigned-subtraction semantics to imply it.
		if o.Satoshis > fee && o.Satoshis-fee >= 1 {
			change.Satoshis = o.Satoshis - fee
		} else {
			tx.Outputs = tx.Outputs[:len(tx.Outputs)-1]
		}
	}

	return tx, nil
}

// apiCode maps err to the short code raw mode tallies a failure under: a
// *client.APIError's own Code (a FuelKeeper reserve/spend/release failure),
// an *arcadeclient.RejectError's numeric ARC status, "backpressure" for an
// *arcadeclient.BackpressureError, or "transport" for anything else (a
// dial/timeout failure or the caller's own context ending outside of a
// typed error).
func apiCode(err error) string {
	var apiErr *client.APIError
	if errors.As(err, &apiErr) {
		return apiErr.Code
	}
	var reject *arcadeclient.RejectError
	if errors.As(err, &reject) {
		return strconv.Itoa(reject.Status)
	}
	var backpressure *arcadeclient.BackpressureError
	if errors.As(err, &backpressure) {
		return "backpressure"
	}
	return "transport"
}

// sleepBounded blocks for d, or until ctx is done, whichever comes first. A
// non-positive d returns immediately.
func sleepBounded(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// reportSettle reports s for outpoint under token, on a
// context.WithoutCancel-derived deadline of its own (see reportTimeout)
// rather than ctx, and tallies "settle:<code>" on failure.
func (r *rawRunner) reportSettle(ctx context.Context, token, outpoint string, s client.Settlement) {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reportTimeout)
	defer cancel()
	if _, serr := r.fk.Settle(settleCtx, r.pool, token, []string{outpoint}, s); serr != nil {
		r.rep.fail("settle:" + apiCode(serr))
	}
}

// worker runs one raw-mode round-trip loop until ctx is done: reserve a
// batch of fuel, spend every output into its own OP_RETURN transaction
// submitted to arcade in extended format, and report the outcome to fk via
// Settle. Outputs that could not be spent at all (rate limiting cut short by
// ctx, a transport failure, arcade backpressure, or a rejection that names
// no competing transaction) are released back to the pool at the end of the
// batch; a rejection identifying a double-spend is reported spent by the
// competitor instead -- never released, since a spent output released back
// to the pool would be handed to every later consumer, each failing the
// same way in turn (see the RejectError and 2xx-with-a-verdict cases below).
//
// Against arcade specifically, neither of those double-spend cases fires
// synchronously today: POST /tx's 4xx body is {error, txid, reason, status,
// title, detail, type} with no competingTxs, its 202 body is only {txid,
// status, txStatus}, and a conflict is assigned purely asynchronously (ARC
// 466, visible later via GET /tx as REJECTED with status 466 and
// competingTxs) -- arcade has no synchronous DOUBLE_SPEND_ATTEMPTED
// producer. A synchronous rejection from arcade is therefore always
// classified invalid (released) here; the correct handling of an
// arcade-originated double-spend is that Settle(OutcomeBroadcast) already
// ran at submission time (so the row is spent, or -- under spend_policy:
// verify -- quarantined pending the oracle), and it is the reaper/oracle
// that later resolves the 466 conflict, not this worker. The
// RejectError/SubmitResponse DoubleSpend() branches below exist for
// forward/other-gateway compatibility (an ARC-compatible gateway other than
// arcade, or a future arcade version, that does assign a conflict
// synchronously) and are exercised by TestWorkerRejectDoubleSpendReportsSpend
// and TestWorkerDoubleSpendReportsSpend against a fake arcade, not live
// arcade behaviour.
func (r *rawRunner) worker(ctx context.Context, id int) {
	// A small per-worker jitter on the reserve retry pause keeps many
	// workers that hit the same transient failure from retrying in lockstep.
	pause := reserveRetryPause + time.Duration(id%5)*time.Millisecond

	for {
		if ctx.Err() != nil {
			return
		}

		res, err := r.fk.Reserve(ctx, r.pool, client.ReserveRequest{Count: r.batch, MinCount: 1})
		if err != nil {
			if ctx.Err() != nil {
				return // shutdown mid-call; not a real failure
			}
			r.rep.fail("reserve:" + apiCode(err))
			sleepBounded(ctx, pause)
			continue
		}
		r.rep.reserved()

		unused := make([]string, 0, len(res.Outputs))
		for _, o := range res.Outputs {
			if ctx.Err() != nil {
				unused = append(unused, o.Outpoint)
				continue
			}
			if r.limiter != nil {
				if werr := r.limiter.Wait(ctx); werr != nil {
					unused = append(unused, o.Outpoint)
					continue
				}
			}

			start := time.Now()
			tx, berr := buildFuelTx(o, res.Constraints, r.memo, r.changeTo, r.satPerKB)
			if berr != nil {
				r.rep.fail("build:error")
				unused = append(unused, o.Outpoint)
				continue
			}
			ef, eerr := tx.EF()
			if eerr != nil {
				r.rep.fail("build:ef")
				unused = append(unused, o.Outpoint)
				continue
			}

			resp, perr := r.arc.PostTx(ctx, ef)
			if perr != nil {
				r.rep.fail("arcade:" + apiCode(perr))

				var backpressure *arcadeclient.BackpressureError
				var reject *arcadeclient.RejectError
				switch {
				case errors.As(perr, &backpressure):
					unused = append(unused, o.Outpoint)
					sleepBounded(ctx, time.Duration(backpressure.RetryAfter)*time.Second)
				case errors.As(perr, &reject) && reject.DoubleSpend():
					// Forward/other-gateway compatibility: arcade's own 4xx
					// body never carries competingTxs today (see worker's
					// doc comment), so this does not fire against arcade,
					// but another ARC-compatible gateway might name a
					// competitor synchronously. Report it spent by the
					// competitor, never released.
					r.reportSettle(ctx, res.Token, o.Outpoint, client.Settlement{
						Outcome: client.OutcomeDoubleSpend, TxID: tx.TxID().String(), CompetingTxIDs: reject.CompetingTxs,
					})
				default:
					// Rejected as invalid with no competing transaction (or
					// any other submission failure): safe to release.
					unused = append(unused, o.Outpoint)
				}
				continue
			}

			switch {
			case resp.DoubleSpend():
				// Forward/other-gateway compatibility: arcade's 202 body is
				// only {txid, status, txStatus} today, with no
				// DOUBLE_SPEND_ATTEMPTED producer and no competingTxs (see
				// worker's doc comment), so this branch does not fire
				// against arcade as it stands.
				r.rep.fail("arcade:double_spend")
				r.reportSettle(ctx, res.Token, o.Outpoint, client.Settlement{
					Outcome: client.OutcomeDoubleSpend, TxID: tx.TxID().String(), CompetingTxIDs: resp.CompetingTxs,
				})
			case resp.TxStatus == "REJECTED":
				// A 2xx response can still carry a terminal REJECTED verdict
				// inline; with no competing transaction named, this is safe
				// to release.
				r.rep.fail("arcade:rejected")
				unused = append(unused, o.Outpoint)
			default:
				r.rep.ok(time.Since(start))
				r.reportSettle(ctx, res.Token, o.Outpoint, client.Settlement{Outcome: client.OutcomeBroadcast, TxID: tx.TxID().String()})
			}
		}

		if len(unused) > 0 {
			relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reportTimeout)
			_, _ = r.fk.Settle(relCtx, r.pool, res.Token, unused, client.Settlement{Outcome: client.OutcomeNotBroadcast})
			cancel()
		}

		if r.afterCycle != nil {
			r.afterCycle()
		}
	}
}
