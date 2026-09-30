package jobs

import (
	"context"
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
	// it is requeued (longer than MinUnspentAge: the client's own claim gets
	// more time to be proven true).
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

// Reaper is the background job that quarantines expired reservations and asks
// the oracle whether to requeue them (available) or retire them (spent). It
// never guesses: an Unknown answer, a Conflicting answer, an oracle error, or
// an oracle "spent" verdict with no spending txid and no client hint all leave
// the row quarantined.
type Reaper struct {
	st    store.Store
	orc   oracle.Oracle
	pools []string
	cfg   ReaperConfig
	clock pool.Clock
	log   *slog.Logger
}

// NewReaper creates a reaper for the named pools.
func NewReaper(st store.Store, orc oracle.Oracle, pools []string, cfg ReaperConfig, clock pool.Clock, log *slog.Logger) *Reaper {
	return &Reaper{st: st, orc: orc, pools: pools, cfg: cfg, clock: clock, log: log.With("job", "reaper")}
}

// Name implements Job.
func (r *Reaper) Name() string { return "reaper" }

// Interval implements Job.
func (r *Reaper) Interval() time.Duration { return r.cfg.Interval }

// RunOnce processes every pool in turn. Oracle outages are logged, not
// returned: they are not a job failure, and rows they leave quarantined are
// picked up again on the next tick.
func (r *Reaper) RunOnce(ctx context.Context) error {
	for _, p := range r.pools {
		if err := r.reapPool(ctx, p); err != nil {
			return err
		}
	}
	return nil
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

	rows, err := r.listQuarantined(ctx, p, now)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		quarantineOldest.WithLabelValues(p).Set(0)
		return nil
	}
	quarantineOldest.WithLabelValues(p).Set(now.Sub(rows[0].QuarantinedAt).Seconds())

	toSpent, toAvailable, ok := r.classify(ctx, p, now, rows)
	if !ok {
		// Oracle unavailable (or misbehaving) partway through; leave every row
		// quarantined and let the next tick retry.
		return nil
	}

	return r.resolve(ctx, p, toSpent, toAvailable)
}

// listQuarantined pages ListQuarantined with the row cursor, up to
// Batch×MaxPages rows per tick, so a row the oracle can never resolve never
// starves the rest of the queue.
func (r *Reaper) listQuarantined(ctx context.Context, p string, now time.Time) ([]store.Row, error) {
	var rows []store.Row
	var cursor *store.RowCursor
	for page := 0; page < r.cfg.MaxPages; page++ {
		batch, err := r.st.ListQuarantined(ctx, p, now.Add(-r.cfg.QuarantineMinAge), cursor, r.cfg.Batch)
		if err != nil {
			return nil, err
		}
		rows = append(rows, batch...)
		if len(batch) < r.cfg.Batch {
			break
		}
		c := batch[len(batch)-1].QuarantineCursor()
		cursor = &c
	}
	return rows, nil
}

// classify asks the oracle about rows and sorts them into rows to resolve to
// spent (grouped by the proven spending txid) or to available. ok is false
// when the oracle could not be consulted for some rows this tick; the caller
// must then leave everything quarantined rather than act on a partial view.
func (r *Reaper) classify(ctx context.Context, p string, now time.Time, rows []store.Row) (toSpent map[chainhash.Hash][]store.Outpoint, toAvailable []store.Outpoint, ok bool) {
	toSpent = map[chainhash.Hash][]store.Outpoint{}
	txCache := map[chainhash.Hash]*oracle.TxStatus{}

	for start := 0; start < len(rows); start += r.cfg.OracleBatch {
		end := min(start+r.cfg.OracleBatch, len(rows))
		batch := rows[start:end]
		ops := make([]store.Outpoint, len(batch))
		for i, row := range batch {
			ops[i] = row.Outpoint
		}
		results, err := r.orc.Outpoints(ctx, ops)
		if err != nil {
			oracleRequests.WithLabelValues("outpoints", "error").Inc()
			r.log.Warn("oracle unavailable; leaving rows quarantined", "pool", p, "err", err)
			return nil, nil, false
		}
		if len(results) != len(batch) {
			oracleRequests.WithLabelValues("outpoints", "error").Inc()
			r.log.Error("oracle returned a mismatched result count; leaving rows quarantined",
				"pool", p, "want", len(batch), "got", len(results))
			return nil, nil, false
		}
		oracleRequests.WithLabelValues("outpoints", "ok").Inc()

		for i, row := range batch {
			r.classifyOne(ctx, p, now, row, results[i], toSpent, &toAvailable, txCache)
		}
	}
	return toSpent, toAvailable, true
}

// classifyOne applies one oracle verdict to one row.
func (r *Reaper) classifyOne(
	ctx context.Context, p string, now time.Time, row store.Row, res oracle.Result,
	toSpent map[chainhash.Hash][]store.Outpoint, toAvailable *[]store.Outpoint, txCache map[chainhash.Hash]*oracle.TxStatus,
) {
	age := now.Sub(row.QuarantinedAt)
	switch res.Status {
	case oracle.StatusSpent:
		var spender *chainhash.Hash
		switch {
		case res.SpendingTxID != nil:
			spender = res.SpendingTxID
		case hasHint(row):
			spender = row.SpentHint
		}
		if spender == nil {
			// Proven spent, but neither the oracle nor the client tells us by
			// what: SpentBy must be an oracle-proven fact (see store.go), so
			// we do not fabricate one. Leave the row quarantined.
			r.log.Warn("oracle reports spent with no spending txid and no hint; leaving quarantined",
				"pool", p, "outpoint", row.Outpoint)
			return
		}
		toSpent[*spender] = append(toSpent[*spender], row.Outpoint)
	case oracle.StatusUnspent:
		wait := r.cfg.MinUnspentAge
		if hasHint(row) {
			wait = r.cfg.HintPatience
		}
		if age >= wait {
			*toAvailable = append(*toAvailable, row.Outpoint)
		}
	case oracle.StatusUnknown:
		if !hasHint(row) {
			return
		}
		ts, ok := txCache[*row.SpentHint]
		if !ok {
			got, err := r.orc.Tx(ctx, *row.SpentHint)
			if err != nil {
				oracleRequests.WithLabelValues("tx", "error").Inc()
				r.log.Warn("oracle Tx lookup failed; leaving row quarantined", "pool", p, "outpoint", row.Outpoint, "err", err)
				return
			}
			oracleRequests.WithLabelValues("tx", "ok").Inc()
			ts = &got
			txCache[*row.SpentHint] = ts
		}
		if ts.Known && !ts.Rejected {
			toSpent[*row.SpentHint] = append(toSpent[*row.SpentHint], row.Outpoint)
		}
	case oracle.StatusConflicting:
		// Leave quarantined; an operator decides.
	}
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
