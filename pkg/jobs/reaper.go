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
)

// Input-mismatch sources for reaperInputMismatch and the mismatch log lines:
// "oracle" is the outpoint oracle's own SpendingTxID, "hint" is a client's
// SpentHint (used either as a fallback when the outpoint oracle names no
// spender, or directly when the outpoint oracle's verdict is Unknown).
const (
	inputMismatchSourceOracle = "oracle"
	inputMismatchSourceHint   = "hint"
)

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
}

// DefaultReaperConfig returns the spec defaults.
func DefaultReaperConfig() ReaperConfig {
	return ReaperConfig{
		Interval: time.Second, Grace: 10 * time.Second, QuarantineMinAge: 30 * time.Second,
		MinUnspentAge: 30 * time.Second, HintPatience: 10 * time.Minute,
		Batch: 500, MaxPages: 10, QuarantineBatch: 1000, OracleBatch: 200,
	}
}

// Validate reports whether every field is in its allowed range: Interval and
// every count must be positive, Grace/QuarantineMinAge/MinUnspentAge must be
// non-negative, and HintPatience must be at least MinUnspentAge.
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
	default:
		return nil
	}
}

// Reaper is the background job that quarantines expired reservations and asks
// the oracle whether to requeue them (available) or retire them (spent). It
// never guesses: an Unknown answer, a Conflicting answer, an oracle error, or
// an oracle "spent" verdict with no spending txid and no client hint all leave
// the row quarantined.
//
// Terminal-state policy: only an oracle-proven, MINED spend may retire a row.
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
	st    store.Store
	orc   oracle.Oracle
	pools []string
	cfg   ReaperConfig
	clock pool.Clock
	log   *slog.Logger
	// next is the per-pool ListQuarantined resume cursor (see listQuarantined).
	// It is safe without a lock because Job.RunOnce is never called
	// concurrently with itself for the same job instance (the Runner
	// sequences a job's ticks).
	next map[string]*store.RowCursor
}

// NewReaper creates a reaper for the named pools, or an error if cfg is invalid.
func NewReaper(st store.Store, orc oracle.Oracle, pools []string, cfg ReaperConfig, clock pool.Clock, log *slog.Logger) (*Reaper, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Reaper{
		st: st, orc: orc, pools: pools, cfg: cfg, clock: clock,
		log: log.With("job", "reaper"), next: map[string]*store.RowCursor{},
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
	var errs []error
	for _, p := range r.pools {
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
// quarantined rows old enough to check and resolves what it can prove.
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

	rows, err := r.listQuarantined(ctx, p, now)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	toSpent, toAvailable, err := r.classify(ctx, p, now, rows)
	if err != nil {
		return err
	}
	return r.resolve(ctx, p, toSpent, toAvailable)
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
// rows the oracle can never resolve (Unknown with no hint, Conflicting, a
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

// classify asks the oracle about rows and sorts them into rows to resolve to
// spent (grouped by the proven spending txid) or to available. It resolves as
// much as it can: if the oracle fails or misbehaves partway through, it stops
// and returns everything classified so far rather than discarding it — the
// remaining rows stay quarantined for the next tick. The returned error is
// non-nil only for context cancellation.
func (r *Reaper) classify(ctx context.Context, p string, now time.Time, rows []store.Row) (toSpent map[chainhash.Hash][]store.Outpoint, toAvailable []store.Outpoint, err error) {
	toSpent = map[chainhash.Hash][]store.Outpoint{}
	txCache := map[chainhash.Hash]*txCacheEntry{}
	txDown := false

	for start := 0; start < len(rows); start += r.cfg.OracleBatch {
		if err := ctx.Err(); err != nil {
			return toSpent, toAvailable, err
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
			return toSpent, toAvailable, nil
		}
		if !sameOutpoints(batch, results) {
			oracleRequests.WithLabelValues("outpoints", "error").Inc()
			r.log.Error("oracle returned mismatched results; leaving remaining rows quarantined this tick",
				"pool", p, "want", len(batch), "got", len(results))
			return toSpent, toAvailable, nil
		}
		oracleRequests.WithLabelValues("outpoints", "ok").Inc()

		for i, row := range batch {
			if cerr := r.classifyOne(ctx, p, now, row, results[i], toSpent, &toAvailable, txCache, &txDown); cerr != nil {
				return toSpent, toAvailable, cerr
			}
		}
	}
	return toSpent, toAvailable, nil
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

// classifyOne applies one oracle verdict to one row. The returned error is
// non-nil only for context cancellation encountered while resolving a hint.
func (r *Reaper) classifyOne(
	ctx context.Context, p string, now time.Time, row store.Row, res oracle.Result,
	toSpent map[chainhash.Hash][]store.Outpoint, toAvailable *[]store.Outpoint,
	txCache map[chainhash.Hash]*txCacheEntry, txDown *bool,
) error {
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
			return nil
		}
		// Even the outpoint oracle's own SpendingTxID is only mempool
		// evidence until Tx confirms it is mined: see the terminal-state
		// policy in the Reaper doc comment.
		return r.resolveViaTx(ctx, p, row.Outpoint, *spender, source, toSpent, txCache, txDown)
	case oracle.StatusUnspent:
		wait := r.cfg.MinUnspentAge
		if hasHint(row) {
			wait = r.cfg.HintPatience
		}
		if now.Sub(row.QuarantinedAt) >= wait {
			*toAvailable = append(*toAvailable, row.Outpoint)
		}
	case oracle.StatusUnknown:
		if hasHint(row) {
			return r.resolveViaTx(ctx, p, row.Outpoint, *row.SpentHint, inputMismatchSourceHint, toSpent, txCache, txDown)
		}
	case oracle.StatusConflicting:
		// Leave quarantined; an operator decides.
	}
	return nil
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

// resolveViaTx looks up a claimed spending txid — the outpoint oracle's own
// SpendingTxID, or a client's SpentHint, named by source — through the tick's
// cache, and counts it as proven only once it is mined (see the Reaper doc
// comment's terminal-state policy). On an oracle failure it sets *txDown and
// logs once, so 1000 rows sharing one txid cost at most one failed Tx call
// and one warning per tick, not one per row. When the verdict's Inputs is
// non-nil, outpoint must also be a member of it (see the Reaper doc comment's
// input verification paragraph); a mismatch is logged at most once per txid
// per source per tick (Error for source "oracle", since the outpoint oracle
// contradicting itself is the deployment's trusted safety boundary
// misbehaving; Warn for source "hint", an unproven client claim) and counted
// in reaperInputMismatch every time, not just on the row that triggers the
// log line. The returned error is non-nil only for context cancellation.
func (r *Reaper) resolveViaTx(
	ctx context.Context, p string, outpoint store.Outpoint, txid chainhash.Hash, source string,
	toSpent map[chainhash.Hash][]store.Outpoint, txCache map[chainhash.Hash]*txCacheEntry, txDown *bool,
) error {
	entry, cached := txCache[txid]
	if !cached {
		if *txDown {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		got, err := r.orc.Tx(ctx, txid)
		if err != nil {
			oracleRequests.WithLabelValues("tx", "error").Inc()
			*txDown = true
			r.log.Warn("oracle Tx lookup unavailable; leaving quarantined rows waiting on it this tick", "pool", p, "err", err)
			return nil
		}
		oracleRequests.WithLabelValues("tx", "ok").Inc()
		r.fillInputs(ctx, p, txid, &got)
		entry = &txCacheEntry{ts: &got}
		if got.Inputs != nil {
			entry.inputs = make(map[store.Outpoint]struct{}, len(got.Inputs))
			for _, in := range got.Inputs {
				entry.inputs[in] = struct{}{}
			}
		}
		txCache[txid] = entry
	}
	ts := entry.ts
	if !ts.Known || !ts.Mined || ts.Rejected {
		return nil
	}
	if entry.inputs != nil {
		if _, spendsOutpoint := entry.inputs[outpoint]; !spendsOutpoint {
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
			return nil
		}
	}
	toSpent[txid] = append(toSpent[txid], outpoint)
	return nil
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

// resolve applies the classified outcomes, chunking to the store's MaxBatch.
func (r *Reaper) resolve(ctx context.Context, p string, toSpent map[chainhash.Hash][]store.Outpoint, toAvailable []store.Outpoint) error {
	maxBatch := r.st.Capabilities().MaxBatch
	for spender, ops := range toSpent {
		sp := spender
		for _, chunk := range chunks(ops, maxBatch) {
			k, err := r.st.Resolve(ctx, p, chunk, store.StateSpent, &sp)
			if err != nil {
				return err
			}
			reaperResolved.WithLabelValues(p, "spent").Add(float64(k))
		}
	}
	for _, chunk := range chunks(toAvailable, maxBatch) {
		k, err := r.st.Resolve(ctx, p, chunk, store.StateAvailable, nil)
		if err != nil {
			return err
		}
		reaperResolved.WithLabelValues(p, "available").Add(float64(k))
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
