package jobs

import (
	"bytes"
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
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
)

var (
	watcherUnmined = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fuelkeeper_watcher_unmined_shards", Help: "Shards awaiting a merkle proof.",
	}, []string{"pool"})
	watcherOldestUnmined = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fuelkeeper_watcher_oldest_unmined_age_seconds", Help: "Age of the oldest unmined shard.",
	}, []string{"pool"})
	watcherTransitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_watcher_transitions_total", Help: "Shard transitions by outcome.",
	}, []string{"pool", "outcome"})
)

// WatcherConfig tunes proof polling.
type WatcherConfig struct {
	// Interval is how often the watcher ticks; see Job.Interval.
	Interval time.Duration `yaml:"interval"`
	// RebroadcastAfter is how long a shard's split tx may be unknown to the
	// oracle before the watcher sends it again.
	RebroadcastAfter time.Duration `yaml:"rebroadcast_after"`
	// AlertAfter is how long a shard may stay unmined before the watcher logs
	// an error for it. Must be >= RebroadcastAfter.
	AlertAfter time.Duration `yaml:"alert_after"`
	// Limit is the maximum number of unmined shards fetched per pool per tick,
	// oldest first (ListShards has no server-side cursor). It must exceed the
	// minter's max_pending_shards: that setting bounds how large the real
	// backlog can ever get, so a Limit above it guarantees every unmined
	// shard is fetched every tick and none is permanently excluded from the
	// fetch window. A Limit smaller than the minter's cap would let the
	// oldest Limit shards occupy the window forever if they never resolve,
	// starving newer ones outright.
	Limit int `yaml:"limit"`
	// MaxChecksPerTick caps how many of the fetched shards get an oracle
	// check in a single tick. The rest wait for a later tick: the watcher
	// rotates its starting point per pool (see next/checkpoint) so every
	// fetched shard is checked at least once within ceil(n/MaxChecksPerTick)
	// ticks, where n is the number of shards fetched that tick, instead of
	// the same leading MaxChecksPerTick shards being checked every time.
	MaxChecksPerTick int `yaml:"max_checks_per_tick"`
}

// DefaultWatcherConfig returns the spec defaults.
func DefaultWatcherConfig() WatcherConfig {
	return WatcherConfig{
		Interval: 10 * time.Second, RebroadcastAfter: 5 * time.Minute, AlertAfter: 6 * time.Hour,
		Limit: 5000, MaxChecksPerTick: 500,
	}
}

// Validate reports whether every field is in its allowed range: Interval,
// RebroadcastAfter, Limit and MaxChecksPerTick must be positive, and
// AlertAfter must be at least RebroadcastAfter (an unmined shard is always
// eligible for rebroadcast before it is ever alerted on).
func (c WatcherConfig) Validate() error {
	switch {
	case c.Interval <= 0:
		return fmt.Errorf("jobs: watcher interval must be > 0, got %s", c.Interval)
	case c.RebroadcastAfter <= 0:
		return fmt.Errorf("jobs: watcher rebroadcast_after must be > 0, got %s", c.RebroadcastAfter)
	case c.AlertAfter < c.RebroadcastAfter:
		return fmt.Errorf("jobs: watcher alert_after (%s) must be >= rebroadcast_after (%s)", c.AlertAfter, c.RebroadcastAfter)
	case c.Limit <= 0:
		return fmt.Errorf("jobs: watcher limit must be > 0, got %d", c.Limit)
	case c.MaxChecksPerTick <= 0:
		return fmt.Errorf("jobs: watcher max_checks_per_tick must be > 0, got %d", c.MaxChecksPerTick)
	default:
		return nil
	}
}

// checkpoint is a pool's oracle-check rotation position: the sort key
// (created_at, txid) of the last shard checked. A raw slice index would not
// survive a shrinking or growing fetch (ListShards has no server-side
// cursor, and a shard's index shifts whenever an earlier one resolves and
// drops out of the unmined set), so the position is stored as a sort key and
// re-located in each tick's fresh fetch instead of reused directly as an
// index.
type checkpoint struct {
	createdAt time.Time
	txid      chainhash.Hash
	set       bool
}

// Watcher is the proof job: it polls the oracle for every unmined shard,
// records the mined BEEF once a merkle proof is available, retires shards
// whose split was rejected, and re-sends splits the network has not seen
// after RebroadcastAfter.
type Watcher struct {
	st    store.Store
	orc   oracle.Oracle
	tr    treasury.Treasury
	pools []string
	cfg   WatcherConfig
	clock pool.Clock
	log   *slog.Logger
	// lastTx tracks, per txid, the last time this watcher either (re)sent the
	// split or logged a mined-BEEF build failure for it — whichever
	// applies, since a given unmined shard is only ever in one of those two
	// situations at a time. Shard txids are globally unique across pools
	// (see store.PutShard), so a single map needs no pool key. It is pruned
	// once per RunOnce from the union of every pool's fetched shards (see
	// pruneLastTx): pruning per pool from only that pool's shards would
	// wipe every other pool's entries on the same tick.
	lastTx map[chainhash.Hash]time.Time
	// next is each pool's oracle-check rotation checkpoint; see checkpoint.
	next map[string]checkpoint
}

// NewWatcher creates a watcher for the named pools, or an error if cfg is invalid.
func NewWatcher(st store.Store, orc oracle.Oracle, tr treasury.Treasury, pools []string, cfg WatcherConfig, clock pool.Clock, log *slog.Logger) (*Watcher, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Watcher{
		st: st, orc: orc, tr: tr, pools: pools, cfg: cfg, clock: clock,
		log: log.With("job", "watcher"), lastTx: map[chainhash.Hash]time.Time{}, next: map[string]checkpoint{},
	}, nil
}

// Name implements Job.
func (w *Watcher) Name() string { return "watcher" }

// Interval implements Job.
func (w *Watcher) Interval() time.Duration { return w.cfg.Interval }

// RunOnce checks unmined shards in every configured pool, continuing with
// the remaining pools when one fails so a single bad pool cannot block the
// rest; a cancelled context stops immediately instead of working through
// the remaining pools. lastTx is pruned exactly once, from the union of
// every pool's fetched shards, and only when every pool succeeded — a
// partial view must never be used to prune, or a pool that failed (or that
// RunOnce never reached) would have its still-live entries wiped.
func (w *Watcher) RunOnce(ctx context.Context) error {
	var errs []error
	live := map[chainhash.Hash]struct{}{}
	for _, p := range w.pools {
		if err := w.watchPool(ctx, p, live); err != nil {
			errs = append(errs, err)
			if ctx.Err() != nil {
				break
			}
		}
	}
	if len(errs) == 0 {
		w.pruneLastTx(live)
	}
	return errors.Join(errs...)
}

// watchPool fetches this pool's unmined shards, checks up to
// Config.MaxChecksPerTick of them against the oracle (rotating the starting
// point across ticks), and adds every fetched shard's txid to live so
// RunOnce's single end-of-tick prune does not mistake "not checked this
// tick" for "no longer unmined".
func (w *Watcher) watchPool(ctx context.Context, p string, live map[chainhash.Hash]struct{}) error {
	now := w.clock.Now()
	shards, err := w.st.ListShards(ctx, p, store.ShardFilter{Statuses: []store.ShardStatus{store.ShardUnmined}, Limit: w.cfg.Limit})
	if err != nil {
		return err
	}
	w.updateUnminedGauge(ctx, p)
	if len(shards) == 0 {
		watcherOldestUnmined.WithLabelValues(p).Set(0)
		return nil
	}
	watcherOldestUnmined.WithLabelValues(p).Set(now.Sub(shards[0].CreatedAt).Seconds())
	for _, sh := range shards {
		live[sh.TxID] = struct{}{}
	}

	n := len(shards)
	start := w.resumeIndex(p, shards)
	count := min(w.cfg.MaxChecksPerTick, n)
	for i := range count {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		sh := shards[(start+i)%n]
		ts, err := w.orc.Tx(ctx, sh.TxID)
		if err != nil {
			oracleRequests.WithLabelValues("tx", "error").Inc()
			w.log.Warn("oracle unavailable", "pool", p, "err", err)
			// Oracle outage is not a job failure: leave the checkpoint where
			// it was so this same window is retried next tick, and leave
			// every remaining shard unmined.
			return nil
		}
		oracleRequests.WithLabelValues("tx", "ok").Inc()
		if err := w.handleShard(ctx, p, now, sh, ts); err != nil {
			return err
		}
	}
	last := shards[(start+count-1)%n]
	w.next[p] = checkpoint{createdAt: last.CreatedAt, txid: last.TxID, set: true}
	return nil
}

// updateUnminedGauge reports the pool's true unmined shard count from
// Stats, not len(shards): a fetch is capped at Config.Limit, but the gauge
// must reflect the real backlog even when it exceeds Limit.
func (w *Watcher) updateUnminedGauge(ctx context.Context, p string) {
	stats, err := w.st.Stats(ctx, p)
	if err != nil {
		w.log.Warn("stats", "pool", p, "err", err)
		return
	}
	watcherUnmined.WithLabelValues(p).Set(float64(stats.ShardsUnmined))
}

// resumeIndex returns the index in shards (sorted oldest first) to resume
// oracle checks from: the first shard sorting strictly after this pool's
// checkpoint, or 0 when there is none yet, or none sorts after it (the
// checkpoint was the fetch's last shard, or has since dropped out of the
// unmined set entirely).
func (w *Watcher) resumeIndex(p string, shards []store.Shard) int {
	cp, ok := w.next[p]
	if !ok || !cp.set {
		return 0
	}
	for i, sh := range shards {
		if shardAfterCheckpoint(sh, cp) {
			return i
		}
	}
	return 0
}

// shardAfterCheckpoint reports whether sh sorts strictly after cp under the
// same (created_at, txid-as-32-internal-bytes) order ListShards uses.
func shardAfterCheckpoint(sh store.Shard, cp checkpoint) bool {
	if sh.CreatedAt.After(cp.createdAt) {
		return true
	}
	if sh.CreatedAt.Before(cp.createdAt) {
		return false
	}
	return bytes.Compare(sh.TxID[:], cp.txid[:]) > 0
}

// handleShard applies one oracle verdict to one unmined shard.
func (w *Watcher) handleShard(ctx context.Context, p string, now time.Time, sh store.Shard, ts oracle.TxStatus) error {
	age := now.Sub(sh.CreatedAt)
	switch {
	case ts.Rejected:
		return w.retire(ctx, p, sh, "rejected", ts.Status, ts.CompetingTxs)
	case ts.Mined && len(ts.MerklePath) > 0:
		return w.mine(ctx, p, now, sh, ts)
	case !ts.Known:
		w.maybeRebroadcast(ctx, p, now, sh)
		w.maybeAlert(p, sh, age)
	default:
		// Known but not yet mined, or mined without a proof yet: wait.
		w.maybeAlert(p, sh, age)
	}
	return nil
}

// mine builds the compact mined BEEF and records the shard as mined. A build
// failure leaves the shard unmined for the next tick (the oracle will report
// the same proof again) and is logged at most once per RebroadcastAfter
// window per shard, using the same per-txid map maybeRebroadcast uses: a
// shard that repeatedly fails to build its BEEF would otherwise log an error
// on every tick forever.
func (w *Watcher) mine(ctx context.Context, p string, now time.Time, sh store.Shard, ts oracle.TxStatus) error {
	full, err := w.st.GetShard(ctx, p, sh.TxID)
	if err != nil {
		return err
	}
	beef, err := w.tr.MinedBEEF(ctx, sh.TxID, full.BEEF, ts.MerklePath)
	if err != nil {
		watcherTransitions.WithLabelValues(p, "beef_failed").Inc()
		if last, seen := w.lastTx[sh.TxID]; !seen || now.Sub(last) > w.cfg.RebroadcastAfter {
			w.log.Error("build mined beef", "pool", p, "txid", sh.TxID.String(), "err", err)
			w.lastTx[sh.TxID] = now
		}
		return nil
	}
	if err := w.st.UpdateShard(ctx, p, sh.TxID, store.ShardMined, beef); err != nil {
		return err
	}
	watcherTransitions.WithLabelValues(p, "mined").Inc()
	w.log.Info("shard mined", "pool", p, "txid", sh.TxID.String(), "height", ts.BlockHeight)
	delete(w.lastTx, sh.TxID)
	return nil
}

// retire moves a shard (and its rows) to invalid, for a split the network
// terminally rejected — whether the oracle reported it directly or Send
// discovered it while rebroadcasting.
func (w *Watcher) retire(ctx context.Context, p string, sh store.Shard, reason, status string, competing []string) error {
	n, err := w.st.RetireShard(ctx, p, sh.TxID)
	if err != nil {
		return err
	}
	watcherTransitions.WithLabelValues(p, "rejected").Inc()
	w.log.Error("split rejected; shard retired", "pool", p, "txid", sh.TxID.String(),
		"reason", reason, "status", status, "competing", competing, "rows_invalidated", n)
	delete(w.lastTx, sh.TxID)
	return nil
}

// maybeRebroadcast re-sends a split the oracle has not seen, at most once per
// RebroadcastAfter interval. A terminal rejection surfaced by Send itself
// (treasury.ErrRejected) retires the shard exactly as an oracle Rejected
// verdict would; any other Send error leaves the shard unmined for a later
// retry.
func (w *Watcher) maybeRebroadcast(ctx context.Context, p string, now time.Time, sh store.Shard) {
	last, seen := w.lastTx[sh.TxID]
	if !seen {
		last = sh.CreatedAt
	}
	if now.Sub(last) <= w.cfg.RebroadcastAfter {
		return
	}
	w.lastTx[sh.TxID] = now
	// ListShards omits BEEF, so fetch the stored split for the treasury's
	// rebroadcast path. Without it Send still asks its wallet to send; only a
	// direct re-post of the BEEF is skipped.
	var beef []byte
	if full, err := w.st.GetShard(ctx, p, sh.TxID); err != nil {
		w.log.Warn("rebroadcast: load shard beef", "pool", p, "txid", sh.TxID.String(), "err", err)
	} else {
		beef = full.BEEF
	}
	if err := w.tr.Send(ctx, sh.TxID, beef); err != nil {
		if errors.Is(err, treasury.ErrRejected) {
			if rerr := w.retire(ctx, p, sh, "rebroadcast_rejected", "", nil); rerr != nil {
				w.log.Error("retire after rejected rebroadcast", "pool", p, "txid", sh.TxID.String(), "err", rerr)
			}
			return
		}
		watcherTransitions.WithLabelValues(p, "rebroadcast_failed").Inc()
		w.log.Warn("rebroadcast", "pool", p, "txid", sh.TxID.String(), "err", err)
		return
	}
	watcherTransitions.WithLabelValues(p, "rebroadcast").Inc()
}

// maybeAlert logs an error once a shard has been unmined for too long. It is
// re-logged on every tick past the threshold; the watcher keeps no
// per-shard "already alerted" state.
func (w *Watcher) maybeAlert(p string, sh store.Shard, age time.Duration) {
	if age > w.cfg.AlertAfter {
		w.log.Error("shard unmined for too long", "pool", p, "txid", sh.TxID.String(), "age", age.String())
	}
}

// pruneLastTx drops bookkeeping for txids that are no longer unmined in any
// pool, so lastTx cannot grow without bound. It must only be called with the
// union of every pool's fetched shards for this tick (see RunOnce); lastTx
// is per-process, so after a failover the new instance has no memory of
// prior rebroadcasts or beef_failed logs and falls back to CreatedAt /
// logging immediately, which is acceptable.
func (w *Watcher) pruneLastTx(live map[chainhash.Hash]struct{}) {
	for txid := range w.lastTx {
		if _, ok := live[txid]; !ok {
			delete(w.lastTx, txid)
		}
	}
}
