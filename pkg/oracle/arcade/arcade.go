// Package arcade adapts pkg/arcadeclient's arcade broadcast-gateway client
// to the oracle.Oracle interface.
//
// arcade tracks transaction lifecycle (submitted/mined/rejected/competing)
// but has no UTXO index, so it cannot answer "is this outpoint spent"; its
// Outpoints always reports StatusUnknown. It also never supplies Inputs on
// Tx, since it does not serve raw transactions. Pair this oracle with
// pkg/oracle/composite and a teranode oracle to get outpoint verdicts and
// input details alongside arcade's lifecycle verdict. Used alone, or paired
// via composite with a nil outpoints oracle, no oracle ever confirms an
// outpoint spent, so the reaper never requeues a row through that path and
// quarantine grows without bound; only the mined-hint path can still move a
// row forward.
package arcade

import (
	"context"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/arcadeclient"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// Oracle adapts an arcadeclient.Client to oracle.Oracle.
type Oracle struct {
	c *arcadeclient.Client
}

var _ oracle.Oracle = (*Oracle)(nil)

// New returns an Oracle backed by c.
func New(c *arcadeclient.Client) *Oracle {
	return &Oracle{c: c}
}

// Tx returns arcade's verdict for txid. A 404 (arcade has never seen this
// txid) is reported as the zero TxStatus with a nil error, per the oracle
// contract: unknown is not an error. Any other failure (transport, 5xx, an
// undecodable body) is wrapped in oracle.ErrUnavailable.
func (o *Oracle) Tx(ctx context.Context, txid chainhash.Hash) (oracle.TxStatus, error) {
	st, err := o.c.GetTx(ctx, txid.String())
	if err != nil {
		if errors.Is(err, arcadeclient.ErrNotFound) {
			return oracle.TxStatus{}, nil
		}
		return oracle.TxStatus{}, fmt.Errorf("%w: arcade GetTx %s: %w", oracle.ErrUnavailable, txid.String(), err)
	}
	// RejectReason is gated on Rejected: st.Status is only ever populated by
	// arcade alongside a REJECTED verdict (arcade.openapi.yaml:1221, "recorded
	// with a REJECTED verdict"), but nothing stops another txStatus from
	// carrying a stray nonzero numeric code, and rejectReasonForCode's table
	// is meaningless outside a REJECTED verdict — most sharply for 476
	// StatusNotFinal, which really is used on a live, still-pending
	// transaction's line in other contexts (see its table entry below).
	rr := oracle.RejectReasonUnknown
	if st.Rejected() {
		rr = rejectReasonForCode(st.Status)
	}
	return oracle.TxStatus{
		Known:        true,
		Mined:        st.Mined(),
		Rejected:     st.Rejected(),
		RejectReason: rr,
		Status:       st.TxStatus,
		BlockHeight:  st.BlockHeight,
		MerklePath:   st.MerklePathBytes(),
		CompetingTxs: st.CompetingTxs,
		Inputs:       nil,
	}, nil
}

// rejectReasonForCode maps arcadeclient.TxStatus.Status — the numeric ARC
// error code (460-476 taxonomy) arcade records with a REJECTED verdict when
// the failure is classifiable — to an oracle.RejectReason. It is meaningful
// only when the verdict is Rejected; DOUBLE_SPEND_ATTEMPTED is a distinct,
// non-Rejected txStatus. Tx still populates CompetingTxs for it, but
// Status/RejectReason carry no meaning there; the reaper is expected to
// check CompetingTxs first.
//
// The table (and the titles/details quoted below) is arcade's own taxonomy:
// /git/arcade/errors/errors.go NewErrorFields, cross-checked against how
// arcade's validator and propagator actually assign each code.
//
//	460 StatusTxFormat            "Not extended format" / "Missing input
//	                              scripts: Transaction could not be
//	                              transformed to extended format" — a
//	                              format problem with the bytes, not the
//	                              inputs' spend state.            → TxInvalid
//	461 StatusUnlockingScripts    "Malformed transaction" / "Transaction
//	                              unlocking scripts are invalid" — script
//	                              failure.                        → TxInvalid
//	462 StatusInputs              Title/detail read "Invalid inputs" /
//	                              "non-existent or spent", which looks like
//	                              the missing/already-spent-input case, but
//	                              arcade's own local validator — the only
//	                              producer of this code — never has a UTXO
//	                              index to check spent-ness against.
//	                              validator/errmap.go:56-58 assigns 462
//	                              only for two message substrings, both
//	                              about the submitted bytes: "coinbase"
//	                              (teranode's TxValidator.go:139, tx.
//	                              IsCoinbase() — the tx itself is
//	                              coinbase-shaped) and
//	                              "bad-txns-inputs-too-large"
//	                              (TxValidator.go:443-451, checkInputs's
//	                              accumulated-previous-script-size DoS
//	                              policy cap — a size limit on the inputs
//	                              as submitted, not a spent-utxo check). No
//	                              other site in arcade assigns StatusInputs.
//	                              The inputs named are therefore untouched.
//	                                                               → TxInvalid
//	463 StatusMalformed           "Malformed transaction" / "Transaction is
//	                              malformed and cannot be processed".
//	                                                               → TxInvalid
//	464 StatusOutputs             "Invalid outputs" / "Transaction is
//	                              invalid because the outputs are
//	                              non-existent or invalid" — the outputs,
//	                              not the spent-ness of the inputs.
//	                                                               → TxInvalid
//	465 StatusFees                "Fee too low" / "Transaction fee is too
//	                              low".                            → TxInvalid
//	466 StatusConflict            "Conflicting tx found" / "Transaction is
//	                              valid, but there is a conflicting tx in
//	                              the block template" — a double-spend: the
//	                              input is owned by a competing/confirmed
//	                              tx. propagator.go:1841-1846 assigns this
//	                              code for TX_CONFLICTING, UTXO_SPENT,
//	                              TX_INVALID_DOUBLE_SPEND and TX_LOCKED —
//	                              every one of them an already-decided
//	                              ownership of the input, never a property
//	                              of the submitted bytes. This is the one
//	                              code this table maps to InputsSpent.
//	                                                               → InputsSpent
//	467 StatusGeneric             "Generic error" / "Transaction could not
//	                              be processed" — propagator.go:1818
//	                              documents this as TX_INVALID wrapping
//	                              fee/script/policy failures (the name
//	                              alone can't recover which), never a
//	                              spent-input verdict.             → TxInvalid
//	468 StatusBeefInvalid         "Invalid BEEF" / "BEEF validation failed:
//	                              BEEF invalid" — envelope format.
//	                                                               → TxInvalid
//	469 StatusMerkleRoots         "Merkle Roots validation failed" / "BEEF
//	                              validation failed: couldn't verify Merkle
//	                              Roots" — envelope format.        → TxInvalid
//	470 (unassigned)              Not in arcade's StatusCode const block
//	                              (errors.go jumps 469 -> 471); no meaning
//	                              to classify.                     → Unknown
//	471 StatusFrozenPolicy        "Input Frozen" / "Input Frozen (blacklist
//	                              manager policy blacklisted)" — the output
//	                              itself is untouched (not spent), merely
//	                              refused by this instance's policy. Unlike
//	                              476's non-finality, a freeze does not
//	                              expire: because the output stays frozen,
//	                              resubmitting this exact spend — or any
//	                              other spend of the same outpoint — will
//	                              draw the same TxInvalid rejection from
//	                              every future consumer, not just this one.
//	                                                               → TxInvalid
//	472 StatusFrozenConsensus     "Input Frozen" / "Input Frozen (blacklist
//	                              manager consensus blacklisted)" — same
//	                              reasoning as 471, including the
//	                              permanent-rejection note.        → TxInvalid
//	473 StatusCumulativeFees      "Cumulative fee validation failed".
//	                                                               → TxInvalid
//	474 StatusTxSize              "Transaction size validation failed".
//	                                                               → TxInvalid
//	475 StatusMinedAncestorsNotInBUMP "Mined ancestors not found in BUMPs"
//	                              / "BEEF validation failed: couldn't find
//	                              mined ancestor of the transaction in
//	                              provided BUMPs" — envelope format.
//	                                                               → TxInvalid
//	476 StatusNotFinal            "Transaction not final" / "Transaction
//	                              nLockTime is in the future and input
//	                              sequence numbers are not final". (POST
//	                              /tx itself does not persist a verdict for
//	                              this case — arcade.openapi.yaml:279 —
//	                              but GET /tx/{txid} still carries the code
//	                              when a REJECTED row does report it.)
//	                              Deliberately NOT TxInvalid: the inputs
//	                              are untouched today, but the transaction
//	                              becomes valid the instant its locktime
//	                              passes, and anyone still holding the
//	                              signed bytes — not only this submitter —
//	                              can rebroadcast it then. Reporting
//	                              TxInvalid would tell the reaper the fuel
//	                              output is safe to requeue now, which
//	                              races that later rebroadcast: if it wins,
//	                              the row's outpoint was spent all along.
//	                              Unknown holds the row instead — the only
//	                              answer this verdict actually supports.
//	                                                               → Unknown
//
// Any code this table does not list, including 0 ("zero/unclassified" per
// arcade.openapi.yaml:1221), also falls to Unknown — never TxInvalid: an
// unrecognized code is not evidence the inputs are untouched.
func rejectReasonForCode(code int) oracle.RejectReason {
	switch code {
	case 466:
		return oracle.RejectReasonInputsSpent
	case 460, 461, 462, 463, 464, 465, 467, 468, 469, 471, 472, 473, 474, 475:
		return oracle.RejectReasonTxInvalid
	case 476:
		// Deliberately Unknown, not TxInvalid: see the 476 table entry
		// above (requeueing would race a later rebroadcast once the
		// locktime passes).
		return oracle.RejectReasonUnknown
	default:
		return oracle.RejectReasonUnknown
	}
}

// Outpoints reports StatusUnknown for every outpoint: arcade has no UTXO
// index to consult. See the package comment for the recommended pairing
// with a teranode oracle via pkg/oracle/composite.
func (o *Oracle) Outpoints(_ context.Context, ops []store.Outpoint) ([]oracle.Result, error) {
	out := make([]oracle.Result, len(ops))
	for i, op := range ops {
		out[i] = oracle.Result{Outpoint: op, Status: oracle.StatusUnknown}
	}
	return out, nil
}
