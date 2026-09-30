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
	// Settle(TxID).
	OutcomeBroadcast Outcome = iota
	// OutcomeNotBroadcast means the transaction never reached a gateway, or
	// was rejected as invalid with no competing transaction: Settle
	// releases the outpoints.
	OutcomeNotBroadcast
	// OutcomeDoubleSpend means the broadcast was rejected because an input
	// was already spent, or because a competing transaction exists: Settle
	// reports the outpoints spent, hinting the first known competitor (or,
	// absent one, the consumer's own TxID).
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
	// for OutcomeDoubleSpend, where CompetingTxIDs[0] is reported instead of
	// TxID when present.
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
//     after that, and the one after that.
//   - never Spend on a rejection that names no competing transaction
//     (OutcomeNotBroadcast): under spend_policy: trust a Spend is terminal
//     the moment the server accepts it -- no oracle ever re-examines it --
//     so an invalid-but-not-double-spent output would be burned rather than
//     returned to the pool for reuse.
//
// OutcomeUnknown makes no HTTP call and returns (0, nil): neither "spent"
// nor "available" is known to be true yet, so the reservation is left in
// place to expire on its own, letting the reaper's requeue rule settle it
// once its fate becomes clear (or clearly never will).
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
// error 466), surfaced later only via a follow-up GET /tx. A consumer
// submitting through arcade therefore reports OutcomeBroadcast (Spend) the
// moment arcade accepts the transaction, exactly as it would for any other
// accepted submission; if a competing transaction turns out to exist, that
// is discovered afterwards by whoever polls the transaction's fate next
// (e.g. FuelKeeper's own reaper/oracle, under spend_policy: verify), not by
// the original consumer calling Settle a second time. OutcomeDoubleSpend
// exists for gateways (or gateway versions) that do name a competitor
// synchronously, and for callers -- like a go-wallet-toolbox-backed
// consumer -- whose own review process learns of one after the fact but
// before it reports back through Settle.
func (c *Client) Settle(ctx context.Context, pool, token string, outpoints []string, s Settlement) (int, error) {
	switch s.Outcome {
	case OutcomeBroadcast:
		if !isHexTxID(s.TxID) {
			return 0, ErrSettleTxIDRequired
		}
		return c.Spend(ctx, pool, token, outpoints, s.TxID)
	case OutcomeDoubleSpend:
		hint := s.TxID
		if len(s.CompetingTxIDs) > 0 {
			hint = s.CompetingTxIDs[0]
		}
		if !isHexTxID(hint) {
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
