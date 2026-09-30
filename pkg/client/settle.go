package client

import (
	"context"
	"errors"
	"fmt"
)

// Outcome is what happened when a consumer tried to broadcast the
// transaction spending a reservation's outpoints. It is the input to
// Settle, which maps it to the one call the FuelKeeper API contract wants
// for that outcome.
type Outcome int

// Outcome values. See Settle for the call each one resolves to, and why.
const (
	// OutcomeBroadcast means the gateway accepted the transaction:
	// Settle calls Spend(TxID).
	OutcomeBroadcast Outcome = iota
	// OutcomeNotBroadcast means the transaction provably never reached a
	// gateway (it was never sent, or a gateway said so definitively -- a
	// well-formed rejection with no competing transaction named, sent
	// before the gateway ever accepted the request), so it is safe to
	// release: Settle releases the outpoints. If instead the request may
	// have reached the gateway and simply left its fate unclear -- a
	// transport error after the request was already sent, a timeout, a
	// 5xx, or the gateway's own backpressure/shedding response -- use
	// OutcomeUnknown, not this one: releasing an output that was in fact
	// already broadcast (or merely accepted and queued) would hand it to
	// the next consumer to reserve, whose own attempt to spend it then
	// fails as a double-spend. Backpressure in particular is not provably
	// "never accepted" for every gateway: arcade, for example, persists the
	// submission (inserting a RECEIVED row carrying the raw tx) before the
	// downstream publish that can answer 503, and arcade's own propagation
	// reaper rebroadcasts a stale RECEIVED row an hour later -- so a caller
	// reporting a gateway's backpressure response should default to
	// OutcomeUnknown unless it knows that particular gateway rejects before
	// persisting anything.
	OutcomeNotBroadcast
	// OutcomeDoubleSpend means the broadcast was rejected because an input
	// was already spent, or because a competing transaction exists: Settle
	// reports the outpoints spent, hinting the first well-formed competitor
	// (or, absent one, the consumer's own TxID).
	OutcomeDoubleSpend
	// OutcomeUnknown means the broadcast was attempted but its fate is not
	// known (a timeout, or a gateway service error): Settle makes no call.
	OutcomeUnknown
)

// String implements fmt.Stringer.
func (o Outcome) String() string {
	switch o {
	case OutcomeBroadcast:
		return "broadcast"
	case OutcomeNotBroadcast:
		return "not_broadcast"
	case OutcomeDoubleSpend:
		return "double_spend"
	case OutcomeUnknown:
		return "unknown"
	default:
		return fmt.Sprintf("client.Outcome(%d)", int(o))
	}
}

// Settlement is a consumer's report of what happened to its own broadcast
// attempt, for Settle.
type Settlement struct {
	// Outcome says what happened; see the Outcome constants.
	Outcome Outcome
	// TxID is the consumer's own transaction id (64 lowercase hex). Required
	// for OutcomeBroadcast, and for OutcomeDoubleSpend when CompetingTxIDs
	// is empty; ignored otherwise.
	TxID string
	// CompetingTxIDs lists transactions known to double-spend TxID, most
	// preferred first (e.g. the one a gateway itself named). Only consulted
	// for OutcomeDoubleSpend, where the first well-formed entry is reported
	// instead of TxID.
	CompetingTxIDs []string
}

// ErrSettleTxIDRequired is returned by Settle when the Settlement needs a
// txid (OutcomeBroadcast, or OutcomeDoubleSpend with no CompetingTxIDs) and
// none was given, or the one given is not 64 lowercase hex characters.
var ErrSettleTxIDRequired = errors.New("client: settlement needs a txid")

// txIDHexLen is the length, in hex characters, of a display-form txid.
const txIDHexLen = 64

// isHexTxID reports whether s is exactly 64 lowercase hex characters
// (chainhash.Hash's display form), mirroring the server's own check (see
// pkg/api's parseTxID) so a malformed txid is rejected here rather than
// round-tripping to the server for the same error.
func isHexTxID(s string) bool {
	if len(s) != txIDHexLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Settle reports the outcome of a consumer's broadcast for outpoints (all
// belonging to one reservation identified by token) of pool, and returns the
// count the server reported changed (spent for a Spend, released for a
// Release).
//
// This is the entry point a consuming wallet should use once it knows its
// broadcast's fate, rather than calling Spend or Release directly, because
// getting the mapping backwards under pressure is easy and the two
// invariants it protects have asymmetric, compounding consequences:
//
//   - never Release on a double-spend (OutcomeDoubleSpend): a spent output
//     released back to the pool would be reserved by the next consumer,
//     whose own broadcast then fails the exact same way -- and the one
//     after that, and the one after that. This holds even when outpoints
//     spans several fuel inputs of one transaction and only one of them was
//     the actual conflict: under spend_policy: trust, Settle's one Spend
//     call still ends every outpoint in the call terminally (spent),
//     including fuel whose own input was never itself double-spent --
//     burning it rather than returning it to the pool for reuse. That is
//     still the right call: the transaction failed to confirm regardless of
//     which input caused it, so every one of its fuel inputs is gone from
//     circulation either way, and this Settlement carries no way to tell,
//     among several outpoints, which input was the innocent one.
//   - never Spend on a rejection that names no competing transaction
//     (OutcomeNotBroadcast): under spend_policy: trust a Spend is terminal
//     the moment the server accepts it -- no oracle ever re-examines it --
//     so an invalid-but-not-double-spent output would be burned rather than
//     returned to the pool for reuse.
//
// OutcomeUnknown makes no HTTP call and returns (0, nil): neither "spent"
// nor "available" is known to be true yet, so the reservation is left in
// place to expire on its own and be quarantined. With an outpoint oracle
// configured, the reaper's requeue rule settles it once that oracle proves
// it unspent. With arcade alone -- no outpoint oracle -- the row's fate is
// never actually learned by FuelKeeper: the reaper's unknown_requeue_age
// (10 minutes, by default) instead requeues it blindly once the row has
// been quarantined that long, trusting that a transaction still
// unconfirmed after that much time is not coming.
//
// An Outcome outside the Outcome constants is an error. A txid Settle needs
// but does not have, or that is not 64 lowercase hex characters, is
// ErrSettleTxIDRequired (the server would reject it too; Settle catches it
// first so the caller is not left wondering whether the outpoints were
// touched).
//
// # When a double-spend is actually knowable
//
// Gateways differ in whether a double-spend is knowable at submission time.
// Against arcade as it stands, it is not: POST /tx's synchronous responses
// (both the 4xx rejection body and the 202 accepted body) never carry a
// competing txid, and a conflict is assigned purely asynchronously (ARC
// error 466). Even then, arcade never names the competitor: a later GET /tx
// shows a REJECTED verdict with status 466 and the teranode rejection
// reason as a line of free text in ExtraInfo, not a populated competingTxs.
// A consumer submitting through arcade therefore reports OutcomeBroadcast
// (Spend) the moment arcade accepts the transaction, exactly as it would
// for any other accepted submission; if a competing transaction turns out
// to exist, that is discovered afterwards by whoever polls the
// transaction's fate next (e.g. FuelKeeper's own reaper/oracle, under
// spend_policy: verify), not by the original consumer calling Settle a
// second time. OutcomeDoubleSpend exists for gateways (or gateway versions)
// that do name a competitor synchronously, and for callers -- like a
// go-wallet-toolbox-backed consumer -- whose own review process learns of
// one after the fact but before it reports back through Settle.
func (c *Client) Settle(ctx context.Context, pool, token string, outpoints []string, s Settlement) (int, error) {
	switch s.Outcome {
	case OutcomeBroadcast:
		if !isHexTxID(s.TxID) {
			return 0, ErrSettleTxIDRequired
		}
		return c.Spend(ctx, pool, token, outpoints, s.TxID)
	case OutcomeDoubleSpend:
		hint, ok := firstValidHint(s.CompetingTxIDs, s.TxID)
		if !ok {
			return 0, ErrSettleTxIDRequired
		}
		return c.Spend(ctx, pool, token, outpoints, hint)
	case OutcomeNotBroadcast:
		return c.Release(ctx, pool, token, outpoints)
	case OutcomeUnknown:
		return 0, nil
	default:
		return 0, fmt.Errorf("client: invalid settlement outcome %v", s.Outcome)
	}
}

// firstValidHint returns the first well-formed txid among candidates, in
// order, falling back to fallback when none of candidates is. A malformed
// entry earlier in candidates therefore never masks a well-formed one later
// in the list, or in fallback.
func firstValidHint(candidates []string, fallback string) (string, bool) {
	for _, c := range candidates {
		if isHexTxID(c) {
			return c, true
		}
	}
	if isHexTxID(fallback) {
		return fallback, true
	}
	return "", false
}
