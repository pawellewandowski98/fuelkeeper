package jobs

import (
	"context"
	"errors"
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
	// an error for it.
	AlertAfter time.Duration `yaml:"alert_after"`
	// Limit is the maximum number of unmined shards fetched per pool per tick.
	Limit int `yaml:"limit"`
}

// DefaultWatcherConfig returns the spec defaults.
func DefaultWatcherConfig() WatcherConfig {
	return WatcherConfig{Interval: 10 * time.Second, RebroadcastAfter: 5 * time.Minute, AlertAfter: 6 * time.Hour, Limit: 500}
}

// Watcher is the proof job: it polls the oracle for every unmined shard,
// records the mined BEEF once a merkle proof is available, retires shards
// whose split was rejected, and re-sends splits the network has not seen
// after RebroadcastAfter.
type Watcher struct {
	st     store.Store
	orc    oracle.Oracle
	tr     treasury.Treasury
	pools  []string
	cfg    WatcherConfig
	clock  pool.Clock
	log    *slog.Logger
	lastTx map[chainhash.Hash]time.Time // txid -> last (re)send time seen by this watcher
}

// NewWatcher creates a watcher for the named pools.
func NewWatcher(st store.Store, orc oracle.Oracle, tr treasury.Treasury, pools []string, cfg WatcherConfig, clock pool.Clock, log *slog.Logger) *Watcher {
	return &Watcher{st: st, orc: orc, tr: tr, pools: pools, cfg: cfg, clock: clock, log: log.With("job", "watcher"), lastTx: map[chainhash.Hash]time.Time{}}
}

// Name implements Job.
func (w *Watcher) Name() string { return "watcher" }

// Interval implements Job.
func (w *Watcher) Interval() time.Duration { return w.cfg.Interval }

// RunOnce checks every unmined shard, in every configured pool, once. A
// backlog larger than Config.Limit is processed across ticks: ListShards
// returns at most Limit shards, oldest first, so the oldest shards are
// always serviced first and nothing starves.
func (w *Watcher) RunOnce(ctx context.Context) error {
	for _, p := range w.pools {
		if err := w.watchPool(ctx, p); err != nil {
			return err
		}
	}
	return nil
}

func (w *Watcher) watchPool(ctx context.Context, p string) error {
	now := w.clock.Now()
	shards, err := w.st.ListShards(ctx, p, store.ShardFilter{Statuses: []store.ShardStatus{store.ShardUnmined}, Limit: w.cfg.Limit})
	if err != nil {
		return err
	}
	watcherUnmined.WithLabelValues(p).Set(float64(len(shards)))
	if len(shards) == 0 {
		watcherOldestUnmined.WithLabelValues(p).Set(0)
		w.pruneLastTx(nil)
		return nil
	}
	watcherOldestUnmined.WithLabelValues(p).Set(now.Sub(shards[0].CreatedAt).Seconds())
	live := make(map[chainhash.Hash]struct{}, len(shards))

	for _, sh := range shards {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		live[sh.TxID] = struct{}{}
		ts, err := w.orc.Tx(ctx, sh.TxID)
		if err != nil {
			oracleRequests.WithLabelValues("tx", "error").Inc()
			w.log.Warn("oracle unavailable", "pool", p, "err", err)
			// Oracle outage is not a job failure: leave every remaining shard
			// unmined and let the next tick retry.
			return nil
		}
		oracleRequests.WithLabelValues("tx", "ok").Inc()
		if err := w.handleShard(ctx, p, now, sh, ts); err != nil {
			return err
		}
	}
	w.pruneLastTx(live)
	return nil
}

// handleShard applies one oracle verdict to one unmined shard.
func (w *Watcher) handleShard(ctx context.Context, p string, now time.Time, sh store.Shard, ts oracle.TxStatus) error {
	age := now.Sub(sh.CreatedAt)
	switch {
	case ts.Rejected:
		return w.retire(ctx, p, sh, "rejected", ts.Status, ts.CompetingTxs)
	case ts.Mined && len(ts.MerklePath) > 0:
		return w.mine(ctx, p, sh, ts)
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
// failure leaves the shard unmined for the next tick: the oracle will report
// the same proof again.
func (w *Watcher) mine(ctx context.Context, p string, sh store.Shard, ts oracle.TxStatus) error {
	full, err := w.st.GetShard(ctx, p, sh.TxID)
	if err != nil {
		return err
	}
	beef, err := w.tr.MinedBEEF(ctx, sh.TxID, full.BEEF, ts.MerklePath)
	if err != nil {
		watcherTransitions.WithLabelValues(p, "beef_failed").Inc()
		w.log.Error("build mined beef", "pool", p, "txid", sh.TxID.String(), "err", err)
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
// verdict would.
func (w *Watcher) maybeRebroadcast(ctx context.Context, p string, now time.Time, sh store.Shard) {
	last, seen := w.lastTx[sh.TxID]
	if !seen {
		last = sh.CreatedAt
	}
	if now.Sub(last) <= w.cfg.RebroadcastAfter {
		return
	}
	w.lastTx[sh.TxID] = now
	if err := w.tr.Send(ctx, sh.TxID); err != nil {
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

// pruneLastTx drops rebroadcast bookkeeping for shards that are no longer
// unmined, so lastTx cannot grow without bound. lastTx is per-process: after
// a failover, a new watcher instance has no memory of prior rebroadcasts and
// falls back to CreatedAt, which is acceptable (it may rebroadcast slightly
// earlier or later than a long-running instance would have).
func (w *Watcher) pruneLastTx(live map[chainhash.Hash]struct{}) {
	for txid := range w.lastTx {
		if _, ok := live[txid]; !ok {
			delete(w.lastTx, txid)
		}
	}
}
