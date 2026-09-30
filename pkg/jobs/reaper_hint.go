package jobs

import (
	"context"
	"slices"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// maxCompetitorsPerHint caps how many of a hint's CompetingTxs are looked up
// (per hint txid per tick; the lookups are cached and shared by every row
// naming that hint).
const maxCompetitorsPerHint = 8

// maxHintChecks bounds the reaper's cross-tick hint recheck map; the oldest
// entry is evicted beyond it.
const maxHintChecks = 100_000

// tick is the per-pool, per-tick state shared by every row classified in one
// reapPool call: the Tx cache, whether Tx is down, one-per-tick log guards,
// and the hint txids that resolved at least one row.
type tick struct {
	now     time.Time
	txCache map[chainhash.Hash]*txCacheEntry
	txDown  bool
	// warnedCompetitor and warnedExpired are keyed by hint txid.
	warnedCompetitor map[chainhash.Hash]bool
	warnedExpired    map[chainhash.Hash]bool
	// resolvedHints are hint txids whose fresh answer this tick moved at
	// least one row; their recheck entries are dropped at the end of the tick.
	resolvedHints map[chainhash.Hash]bool
}

func newTick(now time.Time) *tick {
	return &tick{
		now:              now,
		txCache:          map[chainhash.Hash]*txCacheEntry{},
		warnedCompetitor: map[chainhash.Hash]bool{},
		warnedExpired:    map[chainhash.Hash]bool{},
		resolvedHints:    map[chainhash.Hash]bool{},
	}
}

// lookupTx returns txid's Tx verdict through the tick's cache. A nil entry
// with a nil error means the Tx oracle is down this tick (the first failure
// is logged and sets t.txDown; later uncached lookups are skipped). The
// returned error is non-nil only for context cancellation.
func (r *Reaper) lookupTx(ctx context.Context, p string, t *tick, txid chainhash.Hash) (*txCacheEntry, error) {
	if entry, ok := t.txCache[txid]; ok {
		return entry, nil
	}
	if t.txDown {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	got, err := r.orc.Tx(ctx, txid)
	if err != nil {
		oracleRequests.WithLabelValues("tx", "error").Inc()
		t.txDown = true
		r.log.Warn("oracle Tx lookup unavailable; leaving quarantined rows waiting on it this tick", "pool", p, "err", err)
		return nil, nil
	}
	oracleRequests.WithLabelValues("tx", "ok").Inc()
	r.fillInputs(ctx, p, txid, &got)
	entry := &txCacheEntry{ts: &got}
	if got.Inputs != nil {
		entry.inputs = make(map[store.Outpoint]struct{}, len(got.Inputs))
		for _, in := range got.Inputs {
			entry.inputs[in] = struct{}{}
		}
	}
	t.txCache[txid] = entry
	return entry, nil
}

// spends reports whether the cached verdict's inputs include op; unknown
// inputs (nil) count as yes (see the Reaper doc comment's trust model).
func (e *txCacheEntry) spends(op store.Outpoint) bool {
	if e.inputs == nil {
		return true
	}
	_, ok := e.inputs[op]
	return ok
}

func isMinedSpender(ts *oracle.TxStatus) bool { return ts.Known && ts.Mined && !ts.Rejected }

// classifyHinted applies the hinted-row rules to a row whose outpoint
// verdict is Unknown (see the Reaper doc comment's "Hinted rows" policy).
// Every transition needs a successful Tx(hint) answer this tick; within
// TxRecheckInterval of the last answer the row is held with the last reason
// instead of asking again. The returned error is non-nil only for context
// cancellation.
func (r *Reaper) classifyHinted(ctx context.Context, p string, t *tick, row store.Row, v verdicts) error {
	hint := *row.SpentHint
	entry, fresh := t.txCache[hint]
	if !fresh {
		if reason, ok := r.hints.within(hint, t.now); ok {
			v.held[reason]++
			return nil
		}
		var err error
		if entry, err = r.lookupTx(ctx, p, t, hint); err != nil {
			return err
		}
		if entry == nil {
			v.held[heldTxDown]++
			return nil
		}
	}
	reason, err := r.applyHintRules(ctx, p, t, row, hint, entry, v)
	if err != nil {
		return err
	}
	switch reason {
	case "":
		t.resolvedHints[hint] = true
	case heldTxDown:
		// A competitor lookup failed: no answer to throttle on.
		v.held[reason]++
	default:
		v.held[reason]++
		r.hints.note(hint, t.now, t.now.Add(r.cfg.TxRecheckInterval), reason)
	}
	return nil
}

// applyHintRules decides one hinted row from a fresh Tx(hint) answer. It
// returns "" when the row was added to v's spent or requeue set, or the held
// reason otherwise.
func (r *Reaper) applyHintRules(
	ctx context.Context, p string, t *tick, row store.Row, hint chainhash.Hash, entry *txCacheEntry, v verdicts,
) (string, error) {
	ts := entry.ts
	op := row.Outpoint

	// 1. The hint is mined: it is the spender, if it spends the outpoint.
	if isMinedSpender(ts) {
		if entry.spends(op) {
			v.spend(hint, spentSourceHint, op)
			return "", nil
		}
		r.inputMismatch(p, hint, inputMismatchSourceHint, entry)
		return r.hintWait(p, t, row, hint, heldHintPending, v), nil
	}

	// 3a/3b, before rule 2: an inputs-spent rejection whose gateway named the
	// spent inputs is decided by that list alone. arcade appends the named
	// spender to CompetingTxs and never reports its Inputs, so rule 2 would
	// trust any mined spender as spending the fuel output even when the
	// gateway said the conflict was on another input, and would hold an
	// unknown spender as competitor_pending where the list already decides.
	inputsSpent := ts.Rejected && ts.RejectReason == oracle.RejectReasonInputsSpent
	if inputsSpent && ts.SpentInputs != nil {
		if !entry.spends(op) {
			r.inputMismatch(p, hint, inputMismatchSourceHint, entry)
			return r.hintWait(p, t, row, hint, heldHintPending, v), nil
		}
		if slices.Contains(ts.SpentInputs, op) {
			v.spend(hint, spentSourceInputsSpent, op)
			return "", nil
		}
		return r.untouchedAfterPatience(t, row, v), nil
	}

	// 2. Competitors (a rejected or double-spend-attempted hint): a mined
	// competitor that spends the outpoint (or whose inputs are unknown) is
	// the spender.
	// otherInputsSpent: at least one competitor is mined and every mined
	// competitor's known inputs exclude the fuel output, i.e. the conflict
	// was on another input and the output is positively unspent by them.
	otherInputsSpent := false
	if comps := competitors(ts, hint); len(comps) > 0 {
		minedAny := false
		for _, c := range comps {
			ce, err := r.lookupTx(ctx, p, t, c)
			if err != nil {
				return "", err
			}
			if ce == nil {
				return heldTxDown, nil
			}
			if !isMinedSpender(ce.ts) {
				continue
			}
			minedAny = true
			if ce.inputs == nil {
				if !t.warnedCompetitor[hint] {
					r.log.Warn("trusting a mined competitor without an input check", "pool", p, "hint", hint, "competitor", c)
					t.warnedCompetitor[hint] = true
				}
				v.spend(c, spentSourceCompetitor, op)
				return "", nil
			}
			if ce.spends(op) {
				v.spend(c, spentSourceCompetitor, op)
				return "", nil
			}
			// A mined competitor that does not spend this outpoint says
			// nothing about it: ignore it.
		}
		if !minedAny {
			return r.hintWait(p, t, row, hint, heldCompetitorPending, v), nil
		}
		// Only mined competitors that do not spend the outpoint: fall
		// through to the reject rules.
		otherInputsSpent = true
	}

	if ts.Rejected {
		switch ts.RejectReason {
		case oracle.RejectReasonInputsSpent:
			// 3. The hint was rejected because one of its inputs is spent
			// or conflicting, and the gateway did not name which (3b, with
			// SpentInputs, was decided above, before rule 2). In order:
			//  a. the hint's own known inputs exclude the fuel output: the
			//     hint says nothing about it, hold (hint_pending);
			//  c. mined competitors provably spend only other inputs: hold
			//     (hint_pending);
			//  d. otherwise the fuel output is assumed gone: SpentBy is the
			//     attempt (the hint), not the spender.
			if !entry.spends(op) {
				r.inputMismatch(p, hint, inputMismatchSourceHint, entry)
				return r.hintWait(p, t, row, hint, heldHintPending, v), nil
			}
			if otherInputsSpent {
				return r.hintWait(p, t, row, hint, heldHintPending, v), nil
			}
			v.spend(hint, spentSourceInputsSpent, op)
			return "", nil
		case oracle.RejectReasonTxInvalid:
			// 4. The hint never touched its inputs: requeue after
			// HintPatience.
			return r.untouchedAfterPatience(t, row, v), nil
		default:
			// 5. Unclassified rejection.
			return r.hintWait(p, t, row, hint, heldRejectedUnknown, v), nil
		}
	}
	if ts.Known {
		return r.hintWait(p, t, row, hint, heldHintPending, v), nil
	}
	return r.hintWait(p, t, row, hint, heldHintUnseen, v), nil
}

// untouchedAfterPatience handles a hinted row whose rejected transaction
// provably left the fuel output unspent (rule 4, and rule 3 when the gateway
// names only other spent inputs): requeue (reason "rejected") once
// quarantined for HintPatience, held as rejected_fresh before that.
func (r *Reaper) untouchedAfterPatience(t *tick, row store.Row, v verdicts) string {
	if t.now.Sub(row.QuarantinedAt) >= r.cfg.HintPatience {
		v.requeue(requeueRejected, row.Outpoint)
		return ""
	}
	return heldRejectedFresh
}

// hintWait holds a hinted row with reason until HintMaxWait (when > 0) has
// passed since it was quarantined, then requeues it (hint_expired). It is
// only reached with a fresh Tx(hint) answer this tick.
func (r *Reaper) hintWait(p string, t *tick, row store.Row, hint chainhash.Hash, reason string, v verdicts) string {
	if r.cfg.HintMaxWait <= 0 || t.now.Sub(row.QuarantinedAt) < r.cfg.HintMaxWait {
		return reason
	}
	if !t.warnedExpired[hint] {
		r.log.Warn("requeuing hinted rows after hint_max_wait: the hinted transaction was never mined or classified",
			"pool", p, "hint", hint, "last_reason", reason, "hint_max_wait", r.cfg.HintMaxWait)
		t.warnedExpired[hint] = true
	}
	v.requeue(requeueHintExpired, row.Outpoint)
	return ""
}

// competitors parses ts.CompetingTxs, skipping malformed entries,
// duplicates and hint itself, capped at maxCompetitorsPerHint.
func competitors(ts *oracle.TxStatus, hint chainhash.Hash) []chainhash.Hash {
	if len(ts.CompetingTxs) == 0 {
		return nil
	}
	out := make([]chainhash.Hash, 0, min(len(ts.CompetingTxs), maxCompetitorsPerHint))
	seen := map[chainhash.Hash]bool{hint: true}
	for _, s := range ts.CompetingTxs {
		if len(out) == maxCompetitorsPerHint {
			break
		}
		h, err := chainhash.NewHashFromHex(s)
		if err != nil || seen[*h] {
			continue
		}
		seen[*h] = true
		out = append(out, *h)
	}
	return out
}

// hintThrottle is the reaper's cross-tick record of when each hint txid was
// last answered, so a txid is asked at most once per TxRecheckInterval. It
// keeps the held reason of that answer for rows held in between. Entries
// leave when due (prune), when a fresh answer resolves a row (forget), or,
// beyond cap, oldest first.
type hintThrottle struct {
	cap   int
	m     map[chainhash.Hash]*hintCheck
	order []hintOrder // FIFO by insertion; stale items are skipped
	head  int
	seq   uint64
}

type hintCheck struct {
	next   time.Time
	reason string
	seq    uint64
}

type hintOrder struct {
	txid chainhash.Hash
	seq  uint64
}

func newHintThrottle() *hintThrottle {
	return &hintThrottle{cap: maxHintChecks, m: map[chainhash.Hash]*hintCheck{}}
}

// within returns the last held reason for txid if its next check is still
// in the future.
func (h *hintThrottle) within(txid chainhash.Hash, now time.Time) (string, bool) {
	c, ok := h.m[txid]
	if !ok || !now.Before(c.next) {
		return "", false
	}
	return c.reason, true
}

// note records a fresh answer for txid: a new entry due at next, or, for an
// entry still pending from earlier this tick, just the latest reason.
func (h *hintThrottle) note(txid chainhash.Hash, now, next time.Time, reason string) {
	if c, ok := h.m[txid]; ok && now.Before(c.next) {
		c.reason = reason
		return
	}
	h.seq++
	h.m[txid] = &hintCheck{next: next, reason: reason, seq: h.seq}
	h.order = append(h.order, hintOrder{txid: txid, seq: h.seq})
	for len(h.m) > h.cap {
		h.popFront()
	}
	h.compact()
}

func (h *hintThrottle) forget(txid chainhash.Hash) { delete(h.m, txid) }

// prune drops entries due at or before now. The interval is fixed, so FIFO
// order is due order.
func (h *hintThrottle) prune(now time.Time) {
	for h.head < len(h.order) {
		it := h.order[h.head]
		c, ok := h.m[it.txid]
		if ok && c.seq == it.seq && now.Before(c.next) {
			break
		}
		h.popFront()
	}
	h.compact()
}

// popFront removes the oldest queue item and, if it is current, its entry.
func (h *hintThrottle) popFront() {
	it := h.order[h.head]
	h.head++
	if c, ok := h.m[it.txid]; ok && c.seq == it.seq {
		delete(h.m, it.txid)
	}
}

func (h *hintThrottle) compact() {
	if h.head > 0 && h.head >= len(h.order)/2 {
		h.order = append(h.order[:0], h.order[h.head:]...)
		h.head = 0
	}
}
