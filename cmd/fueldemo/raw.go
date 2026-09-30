package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"syscall"
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

// isDialFailure reports whether err is a connection-establishment failure:
// a dial-phase *net.OpError, or the connection-refused syscall error it
// commonly wraps. Either means the request body was never written, so the
// transaction provably never reached arcade -- unlike a failure past the
// dial phase (the connection was established, so the request, or part of
// it, may already be on the wire), whose fate worker treats as genuinely
// uncertain; see worker's doc comment.
func isDialFailure(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED)
}

// outpointsOf extracts outputs' outpoints, for releasing a batch's untried
// tail in one Settle call (see worker).
func outpointsOf(outputs []wire.Output) []string {
	ops := make([]string, len(outputs))
	for i, o := range outputs {
		ops[i] = o.Outpoint
	}
	return ops
}

// outageBackoffMax caps the exponential backoff worker applies after
// abandoning a batch early for an outage-shaped PostTx failure (a
// dial-phase failure, or one whose cause leaves arcade's receipt of the
// transaction genuinely uncertain): each such failure with no successful
// send in between doubles the pause before the next Reserve, up to this
// bound, so a sustained arcade outage cannot make a worker spin through
// Reserve/PostTx/Release at an unbounded rate. See worker's doc comment.
const outageBackoffMax = 2 * time.Second

// nextOutageBackoff doubles d, capped at outageBackoffMax.
func nextOutageBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > outageBackoffMax {
		return outageBackoffMax
	}
	return d
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
// Settle.
//
// After a failed PostTx, whether to release an output, hold it
// (OutcomeUnknown -- left reserved, for the reservation's own expiry to
// resolve), and whether to abandon the rest of the batch early, turns on
// what the failure actually tells us:
//
//   - a dial-phase failure (isDialFailure: a *net.OpError with Op ==
//     "dial", or the ECONNREFUSED it commonly wraps) means the request
//     body was never written -- the transaction provably never reached
//     arcade -- so it is safe to release. But arcade being unreachable
//     this way is evidence the rest of the batch will fail exactly the
//     same way: trying each remaining output anyway would only burn time
//     rediscovering that individually, so the batch is abandoned early
//     (its untried outputs released alongside the one actually attempted)
//     and the worker backs off (see outageBackoffMax) before the next
//     Reserve.
//   - *arcadeclient.BackpressureError means arcade's broker is shedding
//     load, not that the transaction never reached arcade: arcade inserts
//     the RECEIVED row (carrying the raw tx) before the Kafka publish that
//     can answer this 503 (arcade services/api_server/handlers.go's
//     txsHandler, ~895-966), and arcade's propagation reaper rebroadcasts a
//     stale RECEIVED row after an hour (services/propagation/reaper.go:59,
//     264) -- so, unlike a dial failure, this is not provably "never
//     sent". The attempted output is held, not released; only the batch's
//     untried outputs are released, the batch is abandoned early the same
//     way, and the worker waits the server-given Retry-After before the
//     next Reserve.
//   - a *arcadeclient.RejectError naming no competitor is a well-formed
//     rejection: arcade parsed the request and said no before ever
//     accepting it, so it's safe to release. This is a verdict on one
//     transaction, not evidence the gateway itself is unavailable, so --
//     unlike the two cases above -- it does not abandon the rest of the
//     batch.
//   - everything else -- a transport failure past the dial phase (so the
//     request, or part of it, may already be on the wire), ctx ending
//     mid-POST, an unexpected HTTP status, or a 2xx response this client
//     failed to decode -- leaves whether arcade ever received (and
//     accepted) the transaction unknown: it may already be on the network.
//     Releasing it regardless would hand an already-broadcast output to a
//     later consumer, whose own attempt to spend it then fails as a
//     double-spend. So this output is held (tallied "arcade:unknown",
//     skipped when ctx is already done -- shutdown, not a real failure,
//     exactly as the Reserve path above treats it), the rest of the batch
//     is abandoned and released the same way as the two outage cases
//     above, and the worker backs off.
//
// Abandoning a batch early on any of those three outage-shaped failures,
// and backing off before the next Reserve, is what keeps a genuine arcade
// outage from draining the whole pool: without it, the per-output loop --
// tried against every reservable row as fast as Reserve would hand them
// out, and with every failure classified as "hold" before this fix existed
// -- once measured holding 200 rows in under 500ms against a refused
// connection, each one then stuck reserved until its own expiry plus
// unknown_requeue_age before the reaper's requeue rule could return it to
// the pool.
//
// A rejection (or 2xx verdict) identifying a double-spend is reported spent
// by the competitor instead -- never released, for the same reason as
// backpressure and the uncertain case above (see the RejectError and
// 2xx-with-a-verdict cases below). Likewise, a 2xx response carrying a
// terminal REJECTED verdict inline is held, not released: a 2xx means
// arcade already accepted the submission, so (exactly as with
// backpressure) a REJECTED lifecycle status alone does not prove the
// transaction never reached the network.
//
// Against arcade specifically, none of those double-spend cases fire
// synchronously today: POST /tx's 4xx body is {error, txid, reason, status,
// title, detail, type} with no competingTxs, its 202 body is only {txid,
// status, txStatus}, and arcade has no synchronous DOUBLE_SPEND_ATTEMPTED
// producer. A conflict is assigned purely asynchronously (ARC error 466),
// and even then arcade never populates competingTxs or a
// DOUBLE_SPEND_ATTEMPTED status: what a later GET /tx shows is a REJECTED
// verdict with status 466 and the teranode rejection reason as a line of
// free text in ExtraInfo, not a named competitor. A synchronous rejection
// from arcade is therefore always classified invalid (released, or -- on
// backpressure or a REJECTED 2xx verdict -- held) here; the correct
// handling of an arcade-originated double-spend is that
// Settle(OutcomeBroadcast) already ran at submission time (so the row is
// spent, or -- under spend_policy: verify -- quarantined pending the
// oracle), and it is the reaper/oracle that later resolves the 466 verdict,
// not this worker. The RejectError/SubmitResponse DoubleSpend() branches
// below exist for forward/other-gateway compatibility (an ARC-compatible
// gateway other than arcade, or a future arcade version, that does assign a
// conflict synchronously) and are exercised by
// TestWorkerRejectDoubleSpendReportsSpend and TestWorkerDoubleSpendReportsSpend
// against a fake arcade, not live arcade behaviour.
func (r *rawRunner) worker(ctx context.Context, id int) {
	// A small per-worker jitter on the reserve retry pause keeps many
	// workers that hit the same transient failure from retrying in lockstep.
	pause := reserveRetryPause + time.Duration(id%5)*time.Millisecond
	// outageBackoff is the pause used after a batch is abandoned early for
	// an outage-shaped PostTx failure (a dial-phase failure, or one whose
	// cause leaves arcade's receipt of the transaction genuinely
	// uncertain): it starts at pause and doubles (capped at
	// outageBackoffMax) each time that happens again with no successful
	// send in between, resetting to pause the moment a send succeeds.
	outageBackoff := pause

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
	outputLoop:
		for i, o := range res.Outputs {
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
				var backpressure *arcadeclient.BackpressureError
				var reject *arcadeclient.RejectError
				switch {
				case errors.As(perr, &backpressure):
					// Not provably "never sent" (see worker's doc comment):
					// hold the attempted output, release only the rest of
					// this batch (untried), abandon it, and wait the
					// server-given Retry-After.
					r.rep.fail("arcade:" + apiCode(perr))
					unused = append(unused, outpointsOf(res.Outputs[i+1:])...)
					sleepBounded(ctx, time.Duration(backpressure.RetryAfter)*time.Second)
					break outputLoop
				case errors.As(perr, &reject) && reject.DoubleSpend():
					// Forward/other-gateway compatibility: arcade's own 4xx
					// body never carries competingTxs today (see worker's
					// doc comment), so this does not fire against arcade,
					// but another ARC-compatible gateway might name a
					// competitor synchronously. Report it spent by the
					// competitor, never released.
					r.rep.fail("arcade:" + apiCode(perr))
					r.reportSettle(ctx, res.Token, o.Outpoint, client.Settlement{
						Outcome: client.OutcomeDoubleSpend, TxID: tx.TxID().String(), CompetingTxIDs: reject.CompetingTxs,
					})
				case errors.As(perr, &reject):
					// A well-formed rejection: arcade parsed the request
					// and said no before ever accepting it, naming no
					// competitor, so it provably never reached the
					// network. Safe to release; a verdict on one
					// transaction, not evidence arcade itself is down, so
					// the rest of the batch is still worth trying.
					r.rep.fail("arcade:" + apiCode(perr))
					unused = append(unused, o.Outpoint)
				case isDialFailure(perr):
					// The request body was never written: provably never
					// reached arcade, so safe to release -- but arcade
					// being unreachable this way means the rest of the
					// batch will fail the exact same way, so abandon it
					// (release its untried outputs too) and back off.
					r.rep.fail("arcade:" + apiCode(perr))
					unused = append(unused, o.Outpoint)
					unused = append(unused, outpointsOf(res.Outputs[i+1:])...)
					sleepBounded(ctx, outageBackoff)
					outageBackoff = nextOutageBackoff(outageBackoff)
					break outputLoop
				default:
					// Everything else: a transport failure past the dial
					// phase (so the request, or part of it, may already be
					// on the wire), ctx ending mid-POST, an unexpected
					// HTTP status, or a 2xx response this client failed to
					// decode. Whether arcade ever received (and accepted)
					// the transaction is unknown -- it may already be on
					// the network -- so hold rather than release it (see
					// worker's doc comment), abandon the rest of the batch
					// (release its untried outputs), and back off. The
					// tally is skipped when ctx is already done: that is
					// shutdown, not a real failure, exactly as the Reserve
					// path above treats it.
					if ctx.Err() == nil {
						r.rep.fail("arcade:unknown")
					}
					unused = append(unused, outpointsOf(res.Outputs[i+1:])...)
					sleepBounded(ctx, outageBackoff)
					outageBackoff = nextOutageBackoff(outageBackoff)
					break outputLoop
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
				// A 2xx means arcade already accepted the submission, so
				// (exactly as with backpressure) this verdict alone does
				// not prove the transaction never reached the network:
				// hold, don't release.
				r.rep.fail("arcade:rejected")
			default:
				r.rep.ok(time.Since(start))
				outageBackoff = pause // a real send went through: stop backing off
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
