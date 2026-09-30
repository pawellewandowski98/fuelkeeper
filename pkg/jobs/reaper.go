package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

var (
	reaperQuarantined = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_reaper_quarantined_total", Help: "Rows moved to quarantine by expiry.",
	}, []string{"pool"})
	reaperResolved = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_reaper_resolved_total", Help: "Quarantined rows resolved, by target state.",
	}, []string{"pool", "to"})
	quarantineOldest = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fuelkeeper_quarantine_oldest_age_seconds", Help: "Age of the oldest quarantined row seen this tick.",
	}, []string{"pool"})
	oracleRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_oracle_requests_total", Help: "Oracle calls by outcome.",
	}, []string{"kind", "result"})
	reaperInputMismatch = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_reaper_input_mismatch_total",
		Help: "Mined spender txids whose Inputs did not include the outpoint being resolved, by source (oracle's own SpendingTxID vs a client's SpentHint).",
	}, []string{"pool", "source"})
	reaperRequeued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_reaper_requeued_total",
		Help: "Quarantined rows returned to available, by reason (unspent: an Unspent verdict; unknown_aged: an unhinted row with no verdict after unknown_requeue_age; rejected: a hint rejected as invalid, or for spent inputs that exclude the row's outpoint, after hint_patience; hint_expired: a hint never mined or classified, after hint_max_wait).",
	}, []string{"pool", "reason"})
	reaperSpent = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_reaper_spent_total",
		Help: "Quarantined rows resolved to spent, by source (oracle: the outpoint oracle's mined SpendingTxID; hint: a client's mined SpentHint; competitor: a mined competitor of the hint; inputs_spent: the hint was rejected for a spent or conflicting input, named by the gateway as the row's outpoint or unnamed; SpentBy is the hint).",
	}, []string{"pool", "source"})
	reaperHeld = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fuelkeeper_reaper_held",
		Help: "Quarantined rows the reaper examined and left quarantined on its last tick, by reason (oracle_down, unknown_fresh, expiry_hold, unspent_fresh, spent_no_txid, spender_pending, hint_pending, hint_unseen, competitor_pending, rejected_fresh, rejected_unknown, conflicting, tx_down).",
	}, []string{"pool", "reason"})
)

// Input-mismatch sources for reaperInputMismatch and the mismatch log lines:
// "oracle" is the outpoint oracle's own SpendingTxID, "hint" is a client's
// SpentHint (used either as a fallback when the outpoint oracle names no
// spender, or directly when the outpoint oracle's verdict is Unknown).
const (
	inputMismatchSourceOracle = "oracle"
	inputMismatchSourceHint   = "hint"
)

// Spent sources for reaperSpent: what proved the row spent. A mined spender
// found through resolveViaTx is counted under its inputMismatchSource*
// value ("oracle" or "hint").
const (
	// spentSourceHint: the client's SpentHint, mined.
	spentSourceHint = inputMismatchSourceHint
	// spentSourceCompetitor: a mined transaction among the hint's
	// CompetingTxs that spends the outpoint (or whose inputs are unknown).
	spentSourceCompetitor = "competitor"
	// spentSourceInputsSpent: the hint was rejected because an input was
	// already spent; SpentBy is the attempt (the hint), not the spender.
	spentSourceInputsSpent = "inputs_spent"
)

// Requeue reasons for reaperRequeued: why a quarantined row was returned to
// available.
const (
	// requeueUnspent: the outpoint oracle answered Unspent for at least
	// MinUnspentAge (HintPatience for a hinted row).
	requeueUnspent = "unspent"
	// requeueUnknownAged: an unhinted row with no oracle verdict in a pool
	// with expiry_policy requeue, quarantined for at least UnknownRequeueAge.
	requeueUnknownAged = "unknown_aged"
	// requeueRejected: a hinted row whose transaction was rejected as
	// invalid, or for gateway-named spent inputs excluding the row's
	// outpoint (the fuel output untouched), after HintPatience.
	requeueRejected = "rejected"
	// requeueHintExpired: a hinted row whose transaction was never mined or
	// classified, after HintMaxWait.
	requeueHintExpired = "hint_expired"
)

// Held reasons for reaperHeld: why a quarantined row the reaper listed this
// tick was left quarantined. Every listed row that is not resolved is
// counted under exactly one reason.
const (
	// heldOracleDown: the outpoint oracle failed (or answered malformed)
	// before this row was classified.
	heldOracleDown = "oracle_down"
	// heldUnknownFresh: unhinted, no verdict, quarantined for less than
	// UnknownRequeueAge under expiry_policy requeue.
	heldUnknownFresh = "unknown_fresh"
	// heldExpiryHold: unhinted, no verdict, and the pool's expiry_policy is
	// hold (or UnknownRequeueAge is 0, which disables the requeue).
	heldExpiryHold = "expiry_hold"
	// heldUnspentFresh: an Unspent verdict still waiting out MinUnspentAge
	// (HintPatience for a hinted row).
	heldUnspentFresh = "unspent_fresh"
	// heldSpentNoTxID: a Spent verdict naming no txid, on an unhinted row.
	heldSpentNoTxID = "spent_no_txid"
	// heldSpenderPending: the outpoint oracle's own SpendingTxID is not (yet)
	// provably the mined spender: unseen, unmined, rejected, or its inputs
	// exclude the outpoint.
	heldSpenderPending = "spender_pending"
	// heldHintPending: hinted, and Tx(hint) is known but not mined and not
	// rejected, or mined (or inputs-spent rejected) without spending the
	// outpoint.
	heldHintPending = "hint_pending"
	// heldHintUnseen: hinted, and Tx(hint) is not Known.
	heldHintUnseen = "hint_unseen"
	// heldCompetitorPending: hinted, the hint has competing transactions,
	// and none of those looked up is mined.
	heldCompetitorPending = "competitor_pending"
	// heldRejectedFresh: hinted, the hint was rejected as invalid (or for
	// gateway-named spent inputs that exclude the outpoint), and the row
	// has not yet waited HintPatience.
	heldRejectedFresh = "rejected_fresh"
	// heldRejectedUnknown: hinted, the hint was rejected for an
	// unclassified reason.
	heldRejectedUnknown = "rejected_unknown"
	// heldConflicting: the outpoint oracle answered Conflicting.
	heldConflicting = "conflicting"
	// heldTxDown: a Tx lookup the row depends on failed this tick.
	heldTxDown = "tx_down"
)

// heldReasons lists every held reason, so each tick can reset all of them.
var heldReasons = []string{
	heldOracleDown, heldUnknownFresh, heldExpiryHold, heldUnspentFresh, heldSpentNoTxID, heldSpenderPending,
	heldHintPending, heldHintUnseen, heldCompetitorPending, heldRejectedFresh, heldRejectedUnknown,
	heldConflicting, heldTxDown,
}

// ReaperConfig tunes the expiry/quarantine/oracle flow (spec "Background jobs").
type ReaperConfig struct {
	// Interval is how often the reaper ticks; see Job.Interval.
	Interval time.Duration `yaml:"interval"`
	// Grace is a clock-skew guard before quarantining an expired reservation.
	Grace time.Duration `yaml:"grace"`
	// QuarantineMinAge is how long a row sits quarantined before the first
	// oracle check.
	QuarantineMinAge time.Duration `yaml:"quarantine_min_age"`
	// MinUnspentAge is how long an unhinted quarantined row must be Unspent
	// before it is requeued.
	MinUnspentAge time.Duration `yaml:"min_unspent_age"`
	// HintPatience is how long a hinted quarantined row must be Unspent before
	// it is requeued (at least MinUnspentAge: the client's own claim never
	// gets less patience than an unhinted row).
	HintPatience time.Duration `yaml:"hint_patience"`
	// Batch is the ListQuarantined page size.
	Batch int `yaml:"batch"`
	// MaxPages caps how many ListQuarantined pages are fetched per tick.
	MaxPages int `yaml:"max_pages"`
	// QuarantineBatch is the limit passed to Store.Quarantine per tick.
	QuarantineBatch int `yaml:"quarantine_batch"`
	// OracleBatch is the maximum number of outpoints per Oracle.Outpoints call.
	OracleBatch int `yaml:"oracle_batch"`
	// UnknownRequeueAge is how long an unhinted quarantined row with no
	// oracle verdict waits before it is requeued under `expiry_policy:
	// requeue`; 0 disables the requeue. When set it must be at least
	// MinUnspentAge: a row with no evidence is never requeued faster than one
	// with an Unspent verdict.
	UnknownRequeueAge time.Duration `yaml:"unknown_requeue_age"`
	// HintMaxWait: after this long a hinted row whose transaction arcade
	// never mined or classified is requeued; 0 disables. When set it must be
	// at least HintPatience.
	HintMaxWait time.Duration `yaml:"hint_max_wait"`
	// TxRecheckInterval is the minimum interval between Tx lookups for the
	// same hinted transaction.
	TxRecheckInterval time.Duration `yaml:"tx_recheck_interval"`
}

// DefaultReaperConfig returns the spec defaults.
func DefaultReaperConfig() ReaperConfig {
	return ReaperConfig{
		Interval: time.Second, Grace: 10 * time.Second, QuarantineMinAge: 30 * time.Second,
		MinUnspentAge: 30 * time.Second, HintPatience: 10 * time.Minute,
		Batch: 500, MaxPages: 10, QuarantineBatch: 1000, OracleBatch: 200,
		UnknownRequeueAge: 10 * time.Minute, HintMaxWait: 24 * time.Hour, TxRecheckInterval: 30 * time.Second,
	}
}

// Validate reports whether every field is in its allowed range: Interval and
// every count must be positive, Grace/QuarantineMinAge/MinUnspentAge must be
// non-negative, HintPatience must be at least MinUnspentAge,
// UnknownRequeueAge and HintMaxWait must be 0 (disabled) or at least
// MinUnspentAge and HintPatience respectively, and TxRecheckInterval must be
// positive.
func (c ReaperConfig) Validate() error {
	switch {
	case c.Interval <= 0:
		return fmt.Errorf("jobs: reaper interval must be > 0, got %s", c.Interval)
	case c.Grace < 0:
		return fmt.Errorf("jobs: reaper grace must be >= 0, got %s", c.Grace)
	case c.QuarantineMinAge < 0:
		return fmt.Errorf("jobs: reaper quarantine_min_age must be >= 0, got %s", c.QuarantineMinAge)
	case c.MinUnspentAge < 0:
		return fmt.Errorf("jobs: reaper min_unspent_age must be >= 0, got %s", c.MinUnspentAge)
	case c.HintPatience < c.MinUnspentAge:
		return fmt.Errorf("jobs: reaper hint_patience (%s) must be >= min_unspent_age (%s)", c.HintPatience, c.MinUnspentAge)
	case c.Batch <= 0:
		return fmt.Errorf("jobs: reaper batch must be > 0, got %d", c.Batch)
	case c.MaxPages <= 0:
		return fmt.Errorf("jobs: reaper max_pages must be > 0, got %d", c.MaxPages)
	case c.QuarantineBatch <= 0:
		return fmt.Errorf("jobs: reaper quarantine_batch must be > 0, got %d", c.QuarantineBatch)
	case c.OracleBatch <= 0:
		return fmt.Errorf("jobs: reaper oracle_batch must be > 0, got %d", c.OracleBatch)
	case c.UnknownRequeueAge < 0:
		return fmt.Errorf("jobs: reaper unknown_requeue_age must be >= 0, got %s", c.UnknownRequeueAge)
	case c.UnknownRequeueAge > 0 && c.UnknownRequeueAge < c.MinUnspentAge:
		return fmt.Errorf("jobs: reaper unknown_requeue_age (%s) must be 0 or >= min_unspent_age (%s)", c.UnknownRequeueAge, c.MinUnspentAge)
	case c.HintMaxWait < 0:
		return fmt.Errorf("jobs: reaper hint_max_wait must be >= 0, got %s", c.HintMaxWait)
	case c.HintMaxWait > 0 && c.HintMaxWait < c.HintPatience:
		return fmt.Errorf("jobs: reaper hint_max_wait (%s) must be 0 or >= hint_patience (%s)", c.HintMaxWait, c.HintPatience)
	case c.TxRecheckInterval <= 0:
		return fmt.Errorf("jobs: reaper tx_recheck_interval must be > 0, got %s", c.TxRecheckInterval)
	default:
		return nil
	}
}

// Reaper is the background job that quarantines expired reservations and asks
// the oracle whether to requeue them (available) or retire them (spent). A
// Conflicting answer, an oracle error, and an oracle "spent" verdict with no
// spending txid and no client hint all leave the row quarantined; hinted
// rows follow the rules below.
//
// Quarantine policy for unhinted rows (an expired reservation, or a
// release_policy verify release, which stores no hint): an Unspent verdict
// requeues the row once it has been quarantined for MinUnspentAge. An
// Unknown verdict — the only answer an outpoint oracle that cannot see
// outpoints (e.g. arcade alone) ever gives — requeues it (reason
// "unknown_aged") once now - QuarantinedAt >= UnknownRequeueAge, provided
// the pool's expiry_policy is requeue and UnknownRequeueAge > 0; under
// expiry_policy hold, or with UnknownRequeueAge 0, it stays quarantined for
// an operator. An unhinted row's capacity is therefore not stranded by
// default (under expiry_policy: requeue). This says nothing about rows that
// never reach quarantine: under spend_policy: trust (the default with an
// arcade-only oracle) a client's spend retires the row at once, unverified;
// see "The spend_policy: trust blind spot with arcade-only" in
// docs/OPERATIONS.md. The age is
// measured from QuarantinedAt alone, and the rule is applied only to a row
// the outpoint oracle actually answered Unknown for this tick: an outpoint
// oracle error (or malformed answer) leaves every remaining row untouched,
// so an outage never requeues anything by itself; the rows are requeued on
// the first tick after it recovers. This is a blind requeue: the consuming
// wallet is responsible for releasing on failure and for reporting a
// double-spend as a spend, so a rare stale hand-out (a row that was in fact
// spent) surfaces as a double-spend rejection at its next consumer. G1 (no
// outpoint in two live reservations) still rests on the store's CAS, and
// rows are conserved (G2): a requeue only moves a row from quarantined to
// available.
//
// Hinted rows (a client's spend, under spend_policy verify) whose outpoint
// verdict is Unknown — every hinted row when arcade is the only chain
// source — are decided from Tx(hint) (classifyHinted), checked in this
// order: 1, 3a and 3b (only for an inputs-spent rejection whose gateway
// named the spent inputs), 2, 3a/3c/3d, 4, 5.
//
//  1. Hint Known, Mined, not Rejected: spent by the hint (source "hint"), if
//     its inputs include the outpoint or are unknown; otherwise held
//     (hint_pending, input mismatch counted) as in rule 5.
//  2. Hint has CompetingTxs (rejected, or a double-spend attempt): up to
//     maxCompetitorsPerHint competitors are looked up (cached per tick,
//     with fillInputs). The first mined competitor whose inputs include the
//     outpoint, or are unknown (trusted, logged at Warn once per hint per
//     tick), is the spender (source "competitor"); a mined competitor whose
//     known inputs exclude the outpoint is ignored. No mined competitor:
//     held (competitor_pending) as in rule 5. Only non-spending mined
//     competitors: fall through to rules 3-5.
//  3. Rejected, RejectReasonInputsSpent: the hint was rejected because one
//     of its inputs is spent or conflicting.
//     a. The hint's own known Inputs exclude the outpoint: held
//     (hint_pending, as in rule 5; input mismatch counted).
//     b. The gateway named the spent inputs (TxStatus.SpentInputs
//     non-nil): decided by that list alone, BEFORE rule 2. Including the
//     outpoint, the row is retired with SpentBy = the hint (source
//     "inputs_spent"): the attempt, not the spender (arcade appends the
//     named spender to CompetingTxs, but that list may also hold other
//     competitors, so the spender is not recorded). Excluding it, or
//     empty, the transaction died on another input and the fuel output is
//     unspent: treated as rule 4 (requeue "rejected" after HintPatience,
//     rejected_fresh before). Running rule 2 first would be wrong for the
//     shape arcade returns for a UTXO_SPENT line (SpentInputs plus the
//     spender in CompetingTxs, never Inputs): a mined spender would be
//     trusted as spending the fuel output even when the list excludes it,
//     and an unknown or unmined spender would hold the row as
//     competitor_pending instead of deciding it now.
//     c. (SpentInputs nil, after rule 2.) At least one mined competitor,
//     and every mined competitor's known inputs exclude the outpoint: held
//     (hint_pending, as in rule 5).
//     d. (SpentInputs nil.) Otherwise the fuel output is assumed gone and
//     the row is retired with SpentBy = the hint (source "inputs_spent").
//     The assumption is bounded to the client's own row: an honest wallet
//     whose transaction failed on another input, with a gateway that does
//     not name it, loses that fuel output, counted under
//     spent{source="inputs_spent"}.
//  4. Rejected, RejectReasonTxInvalid: the fuel output is untouched; the
//     row is requeued (reason "rejected") once quarantined for HintPatience,
//     held (rejected_fresh) before that.
//  5. Rejected with RejectReasonUnknown (rejected_unknown), Known but not
//     mined (hint_pending), or not Known (hint_unseen): held; with
//     HintMaxWait > 0, requeued (reason "hint_expired", Warn once per hint
//     per tick) once now - QuarantinedAt >= HintMaxWait.
//
// Every one of these transitions, including the hint_expired requeue,
// needs a successful Tx(hint) answer (and, for rule 2, successful
// competitor answers) in the same tick: a Tx outage holds the row (tx_down)
// however old it is. Tx(hint) is asked at most once per TxRecheckInterval
// per txid across ticks (hintThrottle, bounded to maxHintChecks txids);
// in between, rows naming it are held with the reason of the last answer,
// so a transition can lag its age gate by up to one interval. An Unspent
// verdict requeues a hinted row after HintPatience; a Spent or Conflicting
// verdict takes precedence over the hint as for any row.
//
// A hostile client can at worst retire its own reserved fuel output early
// (rule 3 by naming an unrelated inputs-spent rejection whose inputs arcade
// does not report); it cannot make an output be handed out twice.
//
// Terminal-state policy: only an oracle-proven, MINED spend may retire a row,
// with one exception: hinted rule 3 above (the hint's rejection says the
// fuel output's input is already spent).
// Every candidate spending txid — the outpoint oracle's own SpendingTxID, and
// a client's SpentHint used as a fallback when the outpoint oracle names none
// — is confirmed via Tx and requires Known && Mined && !Rejected before the
// row is resolved to spent. A mempool-only spend is not enough on its own,
// even one the outpoint oracle itself reports: it could still be evicted or
// lose a double-spend race before being mined. An unconfirmed, merely-known,
// or rejected txid leaves the row quarantined for a later tick to resolve.
//
// Input verification: when TxStatus.Inputs is non-nil, every candidate
// spending txid — the outpoint oracle's own SpendingTxID and a client's
// SpentHint alike — is also required to actually spend the row's outpoint,
// i.e. the outpoint must be a member of Inputs, before the row is resolved to
// spent (see TestReaperHintPathRejectsSpenderThatDoesNotSpendTheOutpoint,
// TestReaperHintPathAcceptsSpenderWhoseInputsIncludeTheOutpoint and
// TestReaperOutpointOracleSpenderMustSpendTheOutpoint). This closes what was
// formerly a gap: without it, a hostile client under spend_policy=verify
// could name any mined, unrelated txid as its hint and have the row
// incorrectly resolved to spent.
//
// When a mined verdict's Inputs is nil and the oracle implements
// oracle.InputsResolver (a composite of arcade and teranode), the reaper asks
// it for the inputs, once per txid per tick (fillInputs); the watcher never
// pays for that lookup.
//
// When TxStatus.Inputs is still nil — an oracle that cannot serve the raw
// transaction (e.g. a gateway that only tracks broadcast status) reports
// unknown inputs, never "no inputs" — this check cannot be performed and the
// previous trust model applies: a Spent verdict or a client's hint is trusted
// to name a txid that actually spends the outpoint in question, once that
// txid is mined (see TestReaperSpenderWithNilInputsIsTrustedWhenMined). In
// that configuration the outpoint oracle remains the deployment's real safety
// boundary: it must answer Spent/Unspent correctly for on-chain-confirmed
// spends, and the Tx(hint) fallback (used when the outpoint oracle answers
// Unknown or reports Spent without naming a txid) is only as safe as the
// outpoint oracle is authoritative for mempool-only spends too.
//
// A mismatch on the outpoint oracle's own SpendingTxID does NOT fall back to
// checking a client's SpentHint instead: the oracle contradicted itself, so
// the row simply stays quarantined for a later tick, the same safe direction
// as the outpoint oracle overriding a hint when both are present (see
// TestReaperOutpointOracleOverridesHint). This is logged at Error (source
// "oracle") rather than Warn (source "hint"), since the outpoint oracle is
// the deployment's trusted safety boundary and this indicates it misbehaved;
// see TestReaperOutpointOracleSpenderMustSpendTheOutpoint and
// reaperInputMismatch.
type Reaper struct {
	st  store.Store
	orc oracle.Oracle
	// names lists the pools in configuration order; RunOnce visits them in
	// this order.
	names []string
	// expiry is each pool's ExpiryPolicy (see classifyOne).
	expiry map[string]pool.ExpiryPolicy
	cfg    ReaperConfig
	clock  pool.Clock
	log    *slog.Logger
	// hints throttles Tx(hint) lookups across ticks (TxRecheckInterval); the
	// same single-caller argument as next applies.
	hints *hintThrottle
	// next is the per-pool ListQuarantined resume cursor (see listQuarantined).
	// It is safe without a lock because Job.RunOnce is never called
	// concurrently with itself for the same job instance (the Runner
	// sequences a job's ticks).
	next map[string]*store.RowCursor
}

// NewReaper creates a reaper for pools (visited in the given order), or an
// error if cfg is invalid. Only each pool's Name and ExpiryPolicy are used.
func NewReaper(st store.Store, orc oracle.Oracle, pools []pool.Config, cfg ReaperConfig, clock pool.Clock, log *slog.Logger) (*Reaper, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(pools))
	expiry := make(map[string]pool.ExpiryPolicy, len(pools))
	for _, pc := range pools {
		names = append(names, pc.Name)
		expiry[pc.Name] = pc.ExpiryPolicy
	}
	return &Reaper{
		st: st, orc: orc, names: names, expiry: expiry, cfg: cfg, clock: clock,
		log: log.With("job", "reaper"), next: map[string]*store.RowCursor{}, hints: newHintThrottle(),
	}, nil
}

// Name implements Job.
func (r *Reaper) Name() string { return "reaper" }

// Interval implements Job.
func (r *Reaper) Interval() time.Duration { return r.cfg.Interval }

// RunOnce processes every pool in turn, joining store errors so that one
// pool's failure does not block the rest; a cancelled context stops
// immediately instead of working through the remaining pools. Oracle outages
// are logged, not returned: they are not a job failure, and rows they leave
// quarantined are picked up again on the next tick.
func (r *Reaper) RunOnce(ctx context.Context) error {
	r.hints.prune(r.clock.Now())
	var errs []error
	for _, p := range r.names {
		if err := r.reapPool(ctx, p); err != nil {
			errs = append(errs, err)
			if ctx.Err() != nil {
				break
			}
		}
	}
	return errors.Join(errs...)
}

func hasHint(row store.Row) bool { return row.SpentHint != nil }

// reapPool moves due reservations to quarantined, then asks the oracle about
// quarantined rows old enough to check and resolves what it can prove or
// what the quarantine policy requeues (see Reaper). reaperHeld is reset for
// this pool at the start of every tick and then set from the tick's
// classification.
func (r *Reaper) reapPool(ctx context.Context, p string) error {
	now := r.clock.Now()
	n, err := r.st.Quarantine(ctx, p, now.Add(-r.cfg.Grace), r.cfg.QuarantineBatch)
	if err != nil {
		return err
	}
	if n > 0 {
		reaperQuarantined.WithLabelValues(p).Add(float64(n))
	}
	r.updateOldestGauge(ctx, p, now)
	for _, reason := range heldReasons {
		reaperHeld.WithLabelValues(p, reason).Set(0)
	}

	rows, err := r.listQuarantined(ctx, p, now)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	v, err := r.classify(ctx, p, now, rows)
	for reason, n := range v.held {
		reaperHeld.WithLabelValues(p, reason).Set(float64(n))
	}
	if err != nil {
		return err
	}
	if n := len(v.toAvailable[requeueUnknownAged]); n > 0 {
		r.log.Info("requeued unhinted quarantined rows after unknown_requeue_age",
			"pool", p, "count", n, "unknown_requeue_age", r.cfg.UnknownRequeueAge)
	}
	return r.resolve(ctx, p, v)
}

// updateOldestGauge reports the age of the oldest quarantined row, independent
// of the resume cursor used by listQuarantined: since a tick's window can
// start mid-queue, the first row of that window is not necessarily the oldest
// row overall. This probe always starts from the head and, unlike
// listQuarantined, is not limited to rows older than QuarantineMinAge (before
// is now+1s), so a row too fresh to be checked yet is still reflected here.
func (r *Reaper) updateOldestGauge(ctx context.Context, p string, now time.Time) {
	oldest, err := r.st.ListQuarantined(ctx, p, now.Add(time.Second), nil, 1)
	if err != nil || len(oldest) == 0 {
		quarantineOldest.WithLabelValues(p).Set(0)
		return
	}
	quarantineOldest.WithLabelValues(p).Set(now.Sub(oldest[0].QuarantinedAt).Seconds())
}

// listQuarantined pages ListQuarantined starting from this pool's resume
// cursor, up to Batch×MaxPages rows, and saves where it left off so the next
// tick continues from there instead of restarting at the head. Without this,
// rows the oracle cannot resolve yet (Unknown with no hint under
// expiry_policy hold or younger than UnknownRequeueAge, Conflicting, a
// hinted row still waiting out HintPatience) would occupy the whole window on
// every tick and rows behind them would never reach the oracle.
func (r *Reaper) listQuarantined(ctx context.Context, p string, now time.Time) ([]store.Row, error) {
	before := now.Add(-r.cfg.QuarantineMinAge)
	rows, next, err := r.pageQuarantined(ctx, p, before, r.next[p])
	if err != nil {
		return nil, err
	}
	if r.next[p] != nil && len(rows) == 0 {
		// The saved cursor may now be past everything eligible (e.g. those
		// rows resolved since); retry once from the head rather than waste
		// the tick.
		rows, next, err = r.pageQuarantined(ctx, p, before, nil)
		if err != nil {
			return nil, err
		}
	}
	r.next[p] = next
	return rows, nil
}

// pageQuarantined fetches up to MaxPages pages of Batch rows starting at
// start. The returned cursor is nil when the queue was exhausted (a short
// page was seen, so the next call should wrap around to the head) or the
// position to resume from otherwise.
func (r *Reaper) pageQuarantined(ctx context.Context, p string, before time.Time, start *store.RowCursor) ([]store.Row, *store.RowCursor, error) {
	var rows []store.Row
	cursor := start
	for page := 0; page < r.cfg.MaxPages; page++ {
		batch, err := r.st.ListQuarantined(ctx, p, before, cursor, r.cfg.Batch)
		if err != nil {
			return nil, nil, err
		}
		rows = append(rows, batch...)
		if len(batch) < r.cfg.Batch {
			return rows, nil, nil
		}
		c := batch[len(batch)-1].QuarantineCursor()
		cursor = &c
	}
	return rows, cursor, nil
}

// spentKey groups rows proven spent by one txid, per source of that txid
// (inputMismatchSourceOracle or inputMismatchSourceHint).
type spentKey struct {
	txid   chainhash.Hash
	source string
}

// verdicts is one tick's classification of a pool's quarantined rows.
type verdicts struct {
	// toSpent holds rows proven spent, by spending txid and source.
	toSpent map[spentKey][]store.Outpoint
	// toAvailable holds rows to requeue, by requeue reason.
	toAvailable map[string][]store.Outpoint
	// held counts rows left quarantined, by held reason.
	held map[string]int
}

func newVerdicts() verdicts {
	return verdicts{
		toSpent:     map[spentKey][]store.Outpoint{},
		toAvailable: map[string][]store.Outpoint{},
		held:        map[string]int{},
	}
}

func (v verdicts) spend(txid chainhash.Hash, source string, op store.Outpoint) {
	k := spentKey{txid: txid, source: source}
	v.toSpent[k] = append(v.toSpent[k], op)
}

func (v verdicts) requeue(reason string, op store.Outpoint) {
	v.toAvailable[reason] = append(v.toAvailable[reason], op)
}

// classify asks the oracle about rows and sorts them into rows to resolve to
// spent (grouped by the proven spending txid and its source), rows to
// requeue (grouped by reason), and counts of rows held (by reason). It
// resolves as much as it can: if the oracle fails or misbehaves partway
// through, it stops and returns everything classified so far rather than
// discarding it — the remaining rows stay quarantined for the next tick,
// whatever their age. The returned error is non-nil only for context
// cancellation.
func (r *Reaper) classify(ctx context.Context, p string, now time.Time, rows []store.Row) (verdicts, error) {
	v := newVerdicts()
	t := newTick(now)
	defer func() {
		for txid := range t.resolvedHints {
			r.hints.forget(txid)
		}
	}()

	for start := 0; start < len(rows); start += r.cfg.OracleBatch {
		if err := ctx.Err(); err != nil {
			return v, err
		}
		end := min(start+r.cfg.OracleBatch, len(rows))
		batch := rows[start:end]
		ops := make([]store.Outpoint, len(batch))
		for i, row := range batch {
			ops[i] = row.Outpoint
		}
		results, oerr := r.orc.Outpoints(ctx, ops)
		if oerr != nil {
			oracleRequests.WithLabelValues("outpoints", "error").Inc()
			r.log.Warn("oracle unavailable; leaving remaining rows quarantined this tick", "pool", p, "err", oerr)
			v.held[heldOracleDown] += len(rows) - start
			return v, nil
		}
		if !sameOutpoints(batch, results) {
			oracleRequests.WithLabelValues("outpoints", "error").Inc()
			r.log.Error("oracle returned mismatched results; leaving remaining rows quarantined this tick",
				"pool", p, "want", len(batch), "got", len(results))
			v.held[heldOracleDown] += len(rows) - start
			return v, nil
		}
		oracleRequests.WithLabelValues("outpoints", "ok").Inc()

		for i, row := range batch {
			if cerr := r.classifyOne(ctx, p, t, row, results[i], v); cerr != nil {
				return v, cerr
			}
		}
	}
	return v, nil
}

// sameOutpoints reports whether results is a well-formed, in-order answer to
// batch: same length, and each result names the outpoint it was asked about.
func sameOutpoints(batch []store.Row, results []oracle.Result) bool {
	if len(batch) != len(results) {
		return false
	}
	for i, row := range batch {
		if results[i].Outpoint != row.Outpoint {
			return false
		}
	}
	return true
}

// classifyOne applies one oracle verdict to one row, recording the outcome
// in v. The returned error is non-nil only for context cancellation
// encountered while resolving a spender.
func (r *Reaper) classifyOne(ctx context.Context, p string, t *tick, row store.Row, res oracle.Result, v verdicts) error {
	switch res.Status {
	case oracle.StatusSpent:
		spender := res.SpendingTxID
		source := inputMismatchSourceOracle
		if spender == nil && hasHint(row) {
			spender = row.SpentHint
			source = inputMismatchSourceHint
		}
		if spender == nil {
			// Proven spent, but neither the oracle nor the client tells us by
			// what: SpentBy must be an oracle-proven fact, so we do not
			// fabricate one. Leave the row quarantined.
			r.log.Warn("oracle reports spent with no spending txid and no hint; leaving quarantined",
				"pool", p, "outpoint", row.Outpoint)
			v.held[heldSpentNoTxID]++
			return nil
		}
		// Even the outpoint oracle's own SpendingTxID is only mempool
		// evidence until Tx confirms it is mined: see the terminal-state
		// policy in the Reaper doc comment.
		out, err := r.resolveViaTx(ctx, p, t, row.Outpoint, *spender, source, v)
		if err != nil {
			return err
		}
		countTxOutcome(v, out, source)
	case oracle.StatusUnspent:
		wait := r.cfg.MinUnspentAge
		if hasHint(row) {
			wait = r.cfg.HintPatience
		}
		if t.now.Sub(row.QuarantinedAt) >= wait {
			v.requeue(requeueUnspent, row.Outpoint)
		} else {
			v.held[heldUnspentFresh]++
		}
	case oracle.StatusUnknown:
		if hasHint(row) {
			return r.classifyHinted(ctx, p, t, row, v)
		}
		// Unhinted, no verdict (an expired reservation or a verified
		// release): see the quarantine policy in the Reaper doc comment.
		switch {
		case r.expiry[p] != pool.ExpiryRequeue || r.cfg.UnknownRequeueAge <= 0:
			v.held[heldExpiryHold]++
		case t.now.Sub(row.QuarantinedAt) >= r.cfg.UnknownRequeueAge:
			v.requeue(requeueUnknownAged, row.Outpoint)
		default:
			v.held[heldUnknownFresh]++
		}
	case oracle.StatusConflicting:
		// Leave quarantined; an operator decides.
		v.held[heldConflicting]++
	}
	return nil
}

// countTxOutcome records why a row that went through resolveViaTx was held,
// if it was: hint_* for a client's hint, spender_pending for the outpoint
// oracle's own SpendingTxID.
func countTxOutcome(v verdicts, out txOutcome, source string) {
	switch out {
	case txProven:
	case txLookupDown:
		v.held[heldTxDown]++
	case txUnseen, txPending:
		switch {
		case source == inputMismatchSourceOracle:
			v.held[heldSpenderPending]++
		case out == txUnseen:
			v.held[heldHintUnseen]++
		default:
			v.held[heldHintPending]++
		}
	}
}

// txCacheEntry caches one tick's Tx(txid) verdict alongside a membership set
// built once from its Inputs, so resolveViaTx's per-row input check is O(1)
// instead of an O(len(Inputs)) scan per row sharing the txid. inputs is nil
// exactly when TxStatus.Inputs is nil (unknown inputs, trust the mined
// verdict as-is); it is a non-nil, possibly-empty set otherwise. warnedOracle
// and warnedHint record whether the input-mismatch log line has already been
// emitted for this txid this tick, one flag per source since the two log at
// different levels (see resolveViaTx), so 1000 rows sharing one bogus hint
// produce at most one log line, not 1000. reaperInputMismatch, unlike the
// log, is incremented once per mismatched row: it is cheap and an operator
// may want the true count of affected rows, not just that it happened.
type txCacheEntry struct {
	ts           *oracle.TxStatus
	inputs       map[store.Outpoint]struct{}
	warnedOracle bool
	warnedHint   bool
}

// txOutcome is what resolveViaTx concluded about one row.
type txOutcome int

const (
	// txProven: the txid is mined and spends the outpoint; the row was
	// added to the spent set.
	txProven txOutcome = iota
	// txLookupDown: the Tx lookup failed (this row's or an earlier one's
	// this tick).
	txLookupDown
	// txUnseen: the oracle does not know the txid.
	txUnseen
	// txPending: the txid is known but unmined, rejected, or does not spend
	// the outpoint.
	txPending
)

// resolveViaTx looks up a claimed spending txid — the outpoint oracle's own
// SpendingTxID, or a client's SpentHint, named by source — through the tick's
// cache, and counts it as proven only once it is mined (see the Reaper doc
// comment's terminal-state policy). On an oracle failure lookupTx sets
// t.txDown and logs once, so 1000 rows sharing one txid cost at most one failed Tx call
// and one warning per tick, not one per row. When the verdict's Inputs is
// non-nil, outpoint must also be a member of it (see the Reaper doc comment's
// input verification paragraph); a mismatch is logged at most once per txid
// per source per tick (Error for source "oracle", since the outpoint oracle
// contradicting itself is the deployment's trusted safety boundary
// misbehaving; Warn for source "hint", an unproven client claim) and counted
// in reaperInputMismatch every time, not just on the row that triggers the
// log line. It reports what it concluded (see txOutcome); the returned error
// is non-nil only for context cancellation.
func (r *Reaper) resolveViaTx(
	ctx context.Context, p string, t *tick, outpoint store.Outpoint, txid chainhash.Hash, source string, v verdicts,
) (txOutcome, error) {
	entry, err := r.lookupTx(ctx, p, t, txid)
	if err != nil {
		return txLookupDown, err
	}
	if entry == nil {
		return txLookupDown, nil
	}
	ts := entry.ts
	if !ts.Known {
		return txUnseen, nil
	}
	if !ts.Mined || ts.Rejected {
		return txPending, nil
	}
	if !entry.spends(outpoint) {
		r.inputMismatch(p, txid, source, entry)
		return txPending, nil
	}
	v.spend(txid, source, outpoint)
	return txProven, nil
}

// inputMismatch counts a mined (or inputs-spent rejected) txid whose known
// inputs exclude the row's outpoint and logs it once per txid per source per
// tick: Error for source "oracle" (the trusted outpoint oracle contradicted
// itself), Warn for source "hint" (an unproven client claim).
func (r *Reaper) inputMismatch(p string, txid chainhash.Hash, source string, entry *txCacheEntry) {
	reaperInputMismatch.WithLabelValues(p, source).Inc()
	switch source {
	case inputMismatchSourceOracle:
		if !entry.warnedOracle {
			r.log.Error("outpoint oracle's own spending txid does not spend this outpoint; leaving quarantined, not falling back to a client hint",
				"pool", p, "txid", txid, "source", source)
			entry.warnedOracle = true
		}
	default: // inputMismatchSourceHint
		if !entry.warnedHint {
			r.log.Warn("mined spender's inputs do not include this outpoint; leaving quarantined",
				"pool", p, "txid", txid, "source", source)
			entry.warnedHint = true
		}
	}
}

// fillInputs asks an oracle.InputsResolver for the inputs of a mined spender
// whose verdict carries none (a tx-only gateway such as arcade, behind a
// composite oracle with teranode). It runs once per txid per tick (the
// result is cached with the verdict). A failure leaves Inputs nil, i.e.
// unknown: the mined verdict is then trusted as documented on Reaper.
func (r *Reaper) fillInputs(ctx context.Context, p string, txid chainhash.Hash, ts *oracle.TxStatus) {
	if ts.Inputs != nil || !ts.Known || !ts.Mined || ts.Rejected {
		return
	}
	ir, ok := r.orc.(oracle.InputsResolver)
	if !ok {
		return
	}
	ins, err := ir.TxInputs(ctx, txid)
	if err != nil {
		oracleRequests.WithLabelValues("tx_inputs", "error").Inc()
		r.log.Warn("oracle TxInputs unavailable; trusting the mined spender without an input check", "pool", p, "txid", txid, "err", err)
		return
	}
	oracleRequests.WithLabelValues("tx_inputs", "ok").Inc()
	ts.Inputs = ins
}

// resolve applies the classified outcomes, chunking to the store's MaxBatch,
// and counts what the store actually applied: reaperResolved by target
// state, plus reaperSpent by txid source and reaperRequeued by reason.
func (r *Reaper) resolve(ctx context.Context, p string, v verdicts) error {
	maxBatch := r.st.Capabilities().MaxBatch
	for key, ops := range v.toSpent {
		sp := key.txid
		for _, chunk := range chunks(ops, maxBatch) {
			k, err := r.st.Resolve(ctx, p, chunk, store.StateSpent, &sp)
			if err != nil {
				return err
			}
			reaperResolved.WithLabelValues(p, "spent").Add(float64(k))
			reaperSpent.WithLabelValues(p, key.source).Add(float64(k))
		}
	}
	for reason, ops := range v.toAvailable {
		for _, chunk := range chunks(ops, maxBatch) {
			k, err := r.st.Resolve(ctx, p, chunk, store.StateAvailable, nil)
			if err != nil {
				return err
			}
			reaperResolved.WithLabelValues(p, "available").Add(float64(k))
			reaperRequeued.WithLabelValues(p, reason).Add(float64(k))
		}
	}
	return nil
}

// chunks splits ops into slices of at most n (n <= 0 means unbounded).
func chunks(ops []store.Outpoint, n int) [][]store.Outpoint {
	if len(ops) == 0 {
		return nil
	}
	if n <= 0 || len(ops) <= n {
		return [][]store.Outpoint{ops}
	}
	out := make([][]store.Outpoint, 0, (len(ops)+n-1)/n)
	for start := 0; start < len(ops); start += n {
		out = append(out, ops[start:min(start+n, len(ops))])
	}
	return out
}
