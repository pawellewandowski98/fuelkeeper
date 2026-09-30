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
	"regexp"
	"strings"

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
	// RejectReason (and, for 466, SpentInputs / the named spender) is gated
	// on Rejected: st.Status/st.ExtraInfo are only ever populated by arcade
	// alongside a REJECTED verdict (arcade.openapi.yaml:1221, "recorded with
	// a REJECTED verdict"), but nothing stops another txStatus from carrying
	// a stray nonzero numeric code, and this classification is meaningless
	// outside a REJECTED verdict — most sharply for 476 StatusNotFinal,
	// which really is used on a live, still-pending transaction's line in
	// other contexts (see its table entry below).
	rr := oracle.RejectReasonUnknown
	competingTxs := st.CompetingTxs
	var spentInputs []store.Outpoint
	if st.Rejected() {
		rr = rejectReasonForCode(st.Status, st.ExtraInfo)
		if st.Status == 466 {
			var spender string
			if spentInputs, spender = spentInputsFromExtraInfo(st.ExtraInfo); spender != "" {
				competingTxs = append(append([]string(nil), st.CompetingTxs...), spender)
			}
		}
	}
	return oracle.TxStatus{
		Known:        true,
		Mined:        st.Mined(),
		Rejected:     st.Rejected(),
		RejectReason: rr,
		Status:       st.TxStatus,
		BlockHeight:  st.BlockHeight,
		MerklePath:   st.MerklePathBytes(),
		CompetingTxs: competingTxs,
		SpentInputs:  spentInputs,
		Inputs:       nil,
	}, nil
}

// utxoSpentLinePattern matches a UTXO_SPENT extraInfo line's fixed message
// shape. The "<NAME> (<code>): <message>" envelope (errCodeMsgFmt,
// /git/teranode/errors/errors.go) is universal to every teranode *Error;
// the message for UTXO_SPENT specifically is produced by exactly one
// constructor used identically by every call site (NewUtxoSpentError,
// /git/teranode/errors/error_data_utxo_spent.go:75 — format
// "%s (%d): %s:%d utxo already spent by tx %v", where %v is
// SpendingData.String() = "%s[%d]",
// /git/teranode/stores/utxo/spend/spending_data.go:76-82). Teranode
// sometimes wraps the inner error's UserMessage, doubling the
// "UTXO_SPENT (n): " prefix — arcade's
// services/propagation/classify_test.go documents this as an "Exact
// doubled-prefix shape observed on mainnet 409 responses" — so the prefix
// group matches one or more repeats.
var utxoSpentLinePattern = regexp.MustCompile(
	`^(?:UTXO_SPENT \(\d+\): )+([0-9a-f]{64}):(\d+) utxo already spent by tx ([0-9a-f]{64})\[\d+\]`,
)

// spentInputsFromExtraInfo best-effort parses a REJECTED/466 verdict's
// ExtraInfo (arcade's verbatim teranode line) for the spent outpoint and,
// when named, the competing transaction that spent it. It returns
// (nil, "") — never a partial result — for every shape other than
// UTXO_SPENT's, including the TX_CONFLICTING and TX_INVALID_DOUBLE_SPEND
// lines that also classify InputsSpent (see rejectReasonFor466):
//
//   - TX_INVALID_DOUBLE_SPEND's only observed message
//     (arcade's services/propagation/classify_test.go:65,
//     "[ProcessTransaction][<txid>] tx invalid double spend") never names
//     an outpoint or a spender.
//   - TX_CONFLICTING's message varies by call site, not one format:
//     /git/teranode/stores/utxo/sql/sql.go:2439/2943 name an outpoint
//     ("[Spend] tx is conflicting for %s:%d"), but
//     /git/teranode/services/validator/Validator.go:1202/1216 name only
//     the submitted txid with no vout ("[Validate][%s] tx is
//     conflicting"), and /git/teranode/stores/utxo/aerospike/spend.go:1051
//     interposes a block height where a vout might otherwise be expected
//     ("[SPEND_BATCH_LUA][%s] transaction is conflicting, blockHeight %d -
//     %s"). None of these name a spender either way. Guessing which shape
//     produced a given line risks mistaking a block height for a vout or
//     the submitted txid for the spent outpoint's, so TX_CONFLICTING lines
//     are classified InputsSpent without SpentInputs/CompetingTxs detail.
//
// UTXO_SPENT alone has one constructor used identically everywhere (see
// utxoSpentLinePattern's doc), so it alone is safe to parse.
func spentInputsFromExtraInfo(extraInfo string) (spent []store.Outpoint, spenderTxID string) {
	m := utxoSpentLinePattern.FindStringSubmatch(extraInfo)
	if m == nil {
		return nil, ""
	}
	op, err := store.ParseOutpoint(m[1] + ":" + m[2])
	if err != nil {
		return nil, ""
	}
	return []store.Outpoint{op}, m[3]
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
//	                              the block template". propagator.go:
//	                              1841-1846 assigns this single ARC code to
//	                              FOUR distinct teranode names, and they do
//	                              not all mean the same thing — see
//	                              rejectReasonFor466, which parses
//	                              ExtraInfo's leading "<NAME> (<code>):" to
//	                              tell them apart:
//	                                UTXO_SPENT, TX_CONFLICTING,
//	                                TX_INVALID_DOUBLE_SPEND: an
//	                                already-decided ownership of the
//	                                input.                    → InputsSpent
//	                                TX_LOCKED: the parent transaction's own
//	                                UTXO record is still finishing its
//	                                two-phase commit inside teranode's
//	                                store — transient, not a decided
//	                                ownership (/git/teranode/stores/utxo/
//	                                sql/sql.go:2443-2447, "the parent tx's
//	                                own in-flight commit", distinct from
//	                                the alert system's height-gated freeze
//	                                hold, which is UTXO_FROZEN/471
//	                                instead). Classifying it InputsSpent
//	                                would retire an unspent fuel output
//	                                permanently at exactly the moment a
//	                                consumer spends it from a freshly
//	                                broadcast parent.            → Unknown
//	                              An ExtraInfo that is empty or not the
//	                              "NAME (n): ..." envelope also → Unknown:
//	                              466 with no parseable detail is not
//	                              evidence either way.        → (see above)
//	467 StatusGeneric             "Generic error" / "Transaction could not
//	                              be processed". propagator.go:1847-1848
//	                              assigns this for TX_INVALID, wrapping
//	                              fee/script/policy failures the name alone
//	                              can't recover — plausibly TxInvalid. But
//	                              arcade's own local validator ALSO assigns
//	                              467 for teranode's internal ERR_PROCESSING
//	                              (validator/errmap.go:30-33, "Internal
//	                              BDK/cgo failure... not attributable to
//	                              the submitted transaction" — i.e. an
//	                              arcade/BDK-side fault, not a verdict on
//	                              the bytes at all, and plausibly transient
//	                              on retry). 467 therefore has two
//	                              producers whose implications disagree,
//	                              and ExtraInfo carries no equivalent of
//	                              466's leading name to tell them apart —
//	                              unlike 466, there is nothing here to
//	                              parse.                        → Unknown
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
//	                              refused by this instance's policy. This
//	                              is not always as durable as 476's
//	                              non-finality is transient, though: a
//	                              UTXO_FROZEN condition covers both a
//	                              standing blacklist entry AND the alert
//	                              system's height-gated hold, which clears
//	                              on its own once the chain reaches a
//	                              target height (/git/teranode/stores/utxo/
//	                              sql/sql.go:2451, NewUtxoFrozenError("...
//	                              not spendable until %d", ..., *r.
//	                              spendableIn)) — arcade's own producer of
//	                              471 (propagator.go's PROCESSING+HTTP-403
//	                              catch-all) cannot tell the two apart, so
//	                              neither can this table. TxInvalid is
//	                              still the conservative answer either way
//	                              — the output is untouched now — but a
//	                              resubmission is not guaranteed hopeless.
//	                                                               → TxInvalid
//	472 StatusFrozenConsensus     "Input Frozen" / "Input Frozen (blacklist
//	                              manager consensus blacklisted)" — same
//	                              reasoning as 471. No producer of this
//	                              code was found in arcade's current
//	                              services; kept TxInvalid for whenever one
//	                              is added, on the same footing as 471.
//	                                                               → TxInvalid
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
func rejectReasonForCode(code int, extraInfo string) oracle.RejectReason {
	switch code {
	case 466:
		// 466 alone needs extraInfo: it is arcade's catch-all for four
		// distinct teranode verdicts (see the 466 table entry above), only
		// three of which are actually InputsSpent.
		return rejectReasonFor466(extraInfo)
	case 460, 461, 462, 463, 464, 465, 468, 469, 471, 472, 473, 474, 475:
		return oracle.RejectReasonTxInvalid
	case 467, 476:
		// Deliberately Unknown, not TxInvalid: see the 467 and 476 table
		// entries above (467 can be an internal arcade/BDK fault rather
		// than a verdict on the bytes; 476 requeueing would race a later
		// rebroadcast once the locktime passes).
		return oracle.RejectReasonUnknown
	default:
		return oracle.RejectReasonUnknown
	}
}

// rejectReasonFor466 classifies a 466 StatusConflict verdict by parsing the
// leading "<NAME> (<code>):" of extraInfo — arcade's ExtraInfo carries the
// teranode line verbatim for this family (propagator.go's
// classifyFailureLine returns the line unmodified into ExtraInfo for all
// four names below). See the 466 table entry for the full citations and
// reasoning; this only implements it.
func rejectReasonFor466(extraInfo string) oracle.RejectReason {
	name, _, found := strings.Cut(extraInfo, " (")
	if !found {
		return oracle.RejectReasonUnknown
	}
	switch name {
	case "UTXO_SPENT", "TX_CONFLICTING", "TX_INVALID_DOUBLE_SPEND":
		return oracle.RejectReasonInputsSpent
	default: // "TX_LOCKED" and anything unrecognized
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
