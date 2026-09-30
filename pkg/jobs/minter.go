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

	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
)

var (
	minterSplits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fuelkeeper_minter_splits_total", Help: "Split attempts by outcome.",
	}, []string{"pool", "result"})
	minterLastSuccess = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fuelkeeper_minter_last_success_timestamp", Help: "Unix time of the last successful split.",
	}, []string{"pool"})
	poolRows = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fuelkeeper_pool_rows", Help: "Rows by state (sampled by the minter).",
	}, []string{"pool", "state"})
	poolShards = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fuelkeeper_pool_shards", Help: "Shards by status (sampled by the minter).",
	}, []string{"pool", "status"})
)

// MinterConfig tunes replenishment. Per-pool refill hysteresis
// (pool.Config.LowWater / TargetAvailable) is tracked only in this
// process's memory, not here or in the store; see the Minter doc comment
// for the exact rule and what a restart does with it.
type MinterConfig struct {
	// Interval is how often the minter ticks; see Job.Interval.
	Interval time.Duration `yaml:"interval"`
	// MaxPendingShards caps how many unmined shards may exist at once.
	MaxPendingShards int `yaml:"max_pending_shards"`
	// MaxPerRound caps how many splits are attempted per pool per RunOnce.
	MaxPerRound int `yaml:"max_per_round"`
	// BuildTimeout bounds one split's Build plus PutShard. A treasury that
	// reconciles unrecorded splits (treasury.Reconciler) must wait longer than
	// this before aborting one; config.Validate enforces it for the toolbox.
	BuildTimeout time.Duration `yaml:"build_timeout"`
}

// DefaultMinterConfig returns the spec defaults.
func DefaultMinterConfig() MinterConfig {
	return MinterConfig{Interval: 30 * time.Second, MaxPendingShards: 50, MaxPerRound: 20, BuildTimeout: time.Minute}
}

// Validate checks the config.
func (c MinterConfig) Validate() error {
	if c.Interval <= 0 {
		return fmt.Errorf("jobs: minter interval must be > 0")
	}
	if c.MaxPendingShards <= 0 || c.MaxPerRound <= 0 {
		return fmt.Errorf("jobs: minter max_pending_shards and max_per_round must be > 0")
	}
	if c.BuildTimeout <= 0 {
		return fmt.Errorf("jobs: minter build_timeout must be > 0")
	}
	return nil
}

// Minter is the replenishment job: it keeps each pool's available rows near
// its target by building, recording and sending split transactions ("shards").
//
// Order is load-bearing: Treasury.Build (signed, not broadcast) →
// store.PutShard (record shard + rows, status unmined) → Treasury.Send. A
// terminal Send rejection (treasury.ErrRejected) retires the shard (its rows
// become invalid); a transient send failure leaves the shard unmined for the
// proof watcher to re-send; treasury.ErrInsufficientFunds is logged, not
// fatal. MaxPendingShards bounds exposure to unmined ("pending") shards, and
// MaxPerRound bounds work per pool per tick.
//
// Refilling is hysteresis, not a live setpoint: a pool starts refilling only
// once its available rows (rows already sitting in unmined shards count too,
// see the deficit comment in mintPool) drop below pool.Config.LowWater
// (LowWater == 0 means "always refill below target"). Once started,
// refilling is sticky across rounds: mintPool keeps minting -- bounded by
// MaxPerRound and MaxPendingShards each round, same as always -- even after
// available climbs back to or past LowWater, until a round's projected
// supply reaches TargetAvailable; only then does refilling stop, and it does
// not start again until available truly drops below LowWater. While
// available sits anywhere in [LowWater, TargetAvailable) and no refill is in
// progress, the minter does nothing, so a pool does not re-split for every
// single row consumed. See refillTrigger and the refilling field.
//
// refilling lives only in this process's memory: it starts false (not
// refilling) for every pool, so a process that (re)starts with a pool
// sitting between LowWater and TargetAvailable does not assume a refill was
// under way -- it waits for a genuine dip below LowWater rather than mint on
// every restart. See TestMinterRefillSurvivesRestart.
//
// Treasuries with optional capabilities get two more steps at the start of a
// round: a treasury.Reconciler is reconciled (see startRound), and a
// treasury.ChunkEnsurer tops up its funding chunks. Reconciliation never
// blocks a round for its grace: one round takes the snapshot, and the first
// round at least ReconcileGrace later finishes it, each phase bounded by
// ReconcileBudget. Build plus PutShard of one split is bounded by
// MinterConfig.BuildTimeout, the window a Reconciler's grace must exceed.
type Minter struct {
	st    store.Store
	tr    treasury.Treasury
	pools []pool.Config
	cfg   MinterConfig
	clock pool.Clock
	log   *slog.Logger

	// lastRound is when the previous RunOnce ended (zero before the first).
	lastRound time.Time
	// reconcilePending asks the next round to take a reconcile snapshot. It
	// is set on the first round of a lease, after a split may have been left
	// built but unrecorded (a failed PutShard, a timed-out Build), and after
	// a finish that deferred or failed to settle a split; it is cleared once
	// a snapshot is taken.
	reconcilePending bool
	// snapshot is a taken but unfinished reconcile, or nil.
	snapshot *reconcileSnapshot
	// refilling tracks, per pool name, whether that pool is mid-refill (see
	// the Minter doc comment). It is set once a round finds available below
	// refillTrigger(pc), and cleared once a round's projected supply reaches
	// TargetAvailable. Absent/false means "not refilling", so every pool
	// starts not-refilling when a new Minter is created.
	refilling map[string]bool
}

// reconcileSnapshot is the first listing of a two-phase reconcile.
type reconcileSnapshot struct {
	txids []chainhash.Hash
	due   time.Time // ReconcileFinish may run from here on
}

// ReconcileBudget bounds each reconcile phase (a wallet listing, store
// lookups and sends). A round runs at most one finish and one start, so it
// adds at most twice this to a round; config.Validate requires the job
// lease to outlast build_timeout plus that.
const ReconcileBudget = 10 * time.Second

// NewMinter creates a minter for the configured pools, or an error if cfg is
// invalid.
func NewMinter(st store.Store, tr treasury.Treasury, pools []pool.Config, cfg MinterConfig, clock pool.Clock, log *slog.Logger) (*Minter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Minter{st: st, tr: tr, pools: pools, cfg: cfg, clock: clock, log: log.With("job", "minter"), refilling: make(map[string]bool)}, nil
}

// Name implements Job.
func (m *Minter) Name() string { return "minter" }

// Interval implements Job.
func (m *Minter) Interval() time.Duration { return m.cfg.Interval }

// RunOnce tops up every pool; one pool's store error does not skip the
// others, but a cancelled context stops immediately instead of working
// through the remaining pools.
func (m *Minter) RunOnce(ctx context.Context) error {
	defer func() { m.lastRound = m.clock.Now() }()
	m.startRound(ctx)
	var errs []error
	for _, pc := range m.pools {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := m.mintPool(ctx, pc); err != nil {
			errs = append(errs, fmt.Errorf("pool %s: %w", pc.Name, err))
		}
	}
	return errors.Join(errs...)
}

// newLease reports whether this round starts a lease. The runner calls
// RunOnce only while it holds the minter's lease, one Interval after the
// previous round ended, so a gap of more than 1.5 intervals means at least
// one tick found the lease held elsewhere and this instance has just
// (re)acquired it. The first round of the process is a new lease too. A
// false positive (a slow lease renewal) only costs one extra wallet listing.
func (m *Minter) newLease(now time.Time) bool {
	return m.lastRound.IsZero() || now.Sub(m.lastRound) > m.cfg.Interval+m.cfg.Interval/2
}

// startRound runs the treasury's optional per-round steps: finish a due
// reconcile snapshot, take a new one when one is pending, then
// EnsureChunks. No step waits for the grace, and no failure stops the round.
func (m *Minter) startRound(ctx context.Context) {
	now := m.clock.Now()
	if m.newLease(now) {
		m.reconcilePending = true
	}
	if rec, ok := m.tr.(treasury.Reconciler); ok {
		if m.snapshot != nil && !now.Before(m.snapshot.due) {
			m.finishReconcile(ctx, rec)
		}
		if m.reconcilePending && m.snapshot == nil {
			m.startReconcile(ctx, rec)
		}
	}
	if ce, ok := m.tr.(treasury.ChunkEnsurer); ok {
		if err := ce.EnsureChunks(ctx); err != nil && ctx.Err() == nil {
			m.log.Warn("treasury could not ensure funding chunks", "err", err)
		}
	}
}

// startReconcile takes the first listing and sends the splits the store
// already records; the rest wait for finishReconcile.
func (m *Minter) startReconcile(ctx context.Context, rec treasury.Reconciler) {
	rctx, cancel := context.WithTimeout(ctx, ReconcileBudget)
	defer cancel()
	txids, rep, err := rec.ReconcileStart(rctx, m.knownSplit)
	if err != nil {
		if ctx.Err() == nil {
			m.log.Error("treasury reconcile snapshot failed; retrying next round", "err", err)
		}
		return
	}
	m.reconcilePending = false
	if len(txids) > 0 {
		// The grace runs from the end of the first listing, not from the
		// round's start: a finish earlier in this round may have taken up to
		// ReconcileBudget, and the two listings must be a full grace apart.
		m.snapshot = &reconcileSnapshot{txids: txids, due: m.clock.Now().Add(rec.ReconcileGrace())}
	}
	m.log.Info("treasury reconcile snapshot", "unsent", len(txids), "resent", len(rep.Resent), "errors", len(rep.Errors))
	m.log.Debug("treasury reconcile snapshot txids", "unsent", hashStrings(txids), "resent", hashStrings(rep.Resent))
	for txid, err := range rep.Errors {
		m.log.Warn("treasury reconcile could not settle a split", "txid", txid.String(), "err", err)
	}
}

// finishReconcile lists again and settles the snapshot. A failure keeps the
// snapshot for the next round; deferred or failed splits ask for a new one.
func (m *Minter) finishReconcile(ctx context.Context, rec treasury.Reconciler) {
	rctx, cancel := context.WithTimeout(ctx, ReconcileBudget)
	defer cancel()
	rep, err := rec.ReconcileFinish(rctx, m.snapshot.txids, m.knownSplit)
	if err != nil {
		if ctx.Err() == nil {
			m.log.Error("treasury reconcile failed; retrying next round", "err", err)
		}
		return
	}
	m.snapshot = nil
	if len(rep.Deferred) > 0 || len(rep.Errors) > 0 {
		m.reconcilePending = true
	}
	lvl := slog.LevelInfo
	if len(rep.Aborted) > 0 || len(rep.Errors) > 0 {
		lvl = slog.LevelWarn
	}
	m.log.Log(ctx, lvl, "treasury reconciled", "resent", len(rep.Resent), "aborted", len(rep.Aborted),
		"deferred", len(rep.Deferred), "errors", len(rep.Errors))
	m.log.Debug("treasury reconcile txids", "resent", hashStrings(rep.Resent), "aborted", hashStrings(rep.Aborted),
		"deferred", hashStrings(rep.Deferred))
	for txid, err := range rep.Errors {
		m.log.Warn("treasury reconcile could not settle a split", "txid", txid.String(), "err", err)
	}
}

// knownSplit reports whether any configured pool records a shard for txid.
// A store failure is an error, so Reconcile never aborts a split it could
// not look up.
func (m *Minter) knownSplit(ctx context.Context, txid chainhash.Hash) (bool, error) {
	for _, pc := range m.pools {
		_, err := m.st.GetShard(ctx, pc.Name, txid)
		switch {
		case err == nil:
			return true, nil
		case errors.Is(err, store.ErrNotFound):
		default:
			return false, fmt.Errorf("pool %s: %w", pc.Name, err)
		}
	}
	return false, nil
}

func hashStrings(hs []chainhash.Hash) []string {
	out := make([]string, len(hs))
	for i, h := range hs {
		out[i] = h.String()
	}
	return out
}

func publishStats(p string, st store.Stats) {
	poolRows.WithLabelValues(p, "available").Set(float64(st.Available))
	poolRows.WithLabelValues(p, "reserved").Set(float64(st.Reserved))
	poolRows.WithLabelValues(p, "quarantined").Set(float64(st.Quarantined))
	poolRows.WithLabelValues(p, "spent").Set(float64(st.Spent))
	poolRows.WithLabelValues(p, "invalid").Set(float64(st.Invalid))
	poolShards.WithLabelValues(p, "unmined").Set(float64(st.ShardsUnmined))
	poolShards.WithLabelValues(p, "mined").Set(float64(st.ShardsMined))
	poolShards.WithLabelValues(p, "retired").Set(float64(st.ShardsRetired))
	poolShards.WithLabelValues(p, "invalid").Set(float64(st.ShardsInvalid))
}

// refillTrigger returns the available-rows threshold that starts a refill
// for pc: LowWater when set, otherwise TargetAvailable. LowWater == 0
// disables hysteresis (the pool starts refilling whenever it is below
// TargetAvailable at all), matching pool.Config.LowWater's zero value.
func refillTrigger(pc pool.Config) int64 {
	if pc.LowWater > 0 {
		return pc.LowWater
	}
	return pc.TargetAvailable
}

// mintPool tops up one pool toward its target, subject to LowWater hysteresis
// (see refillTrigger, the Minter.refilling field and the Minter doc
// comment), MaxPendingShards and MaxPerRound.
func (m *Minter) mintPool(ctx context.Context, pc pool.Config) error {
	stats, err := m.st.Stats(ctx, pc.Name)
	if err != nil {
		return err
	}
	publishStats(pc.Name, stats)

	// Available dropping below the trigger starts refilling; once started it
	// is sticky (see the Minter doc comment) until this round's projected
	// supply reaches TargetAvailable, checked after the loop below.
	if stats.Available < refillTrigger(pc) {
		m.refilling[pc.Name] = true
	}
	if !m.refilling[pc.Name] {
		return nil
	}
	// Rows in unmined shards are already counted in stats.Available (they are
	// available rows that mined-only instances cannot serve yet), so the
	// deficit is simply target - available, i.e. target - projected as the
	// loop below mints.
	deficit := pc.TargetAvailable - stats.Available
	pending := int(stats.ShardsUnmined)
	for built := 0; deficit > 0 && pending < m.cfg.MaxPendingShards && built < m.cfg.MaxPerRound; built++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Build and PutShard share one deadline (see MinterConfig.BuildTimeout).
		bctx, cancel := context.WithTimeout(ctx, m.cfg.BuildTimeout)
		b, err := m.tr.Build(bctx, treasury.MintRequest{Pool: pc.Name, Denomination: pc.Denomination, Count: pc.SplitSize, LockMode: pc.LockMode})
		if err != nil {
			cancel()
			minterSplits.WithLabelValues(pc.Name, "build_failed").Inc()
			if errors.Is(err, treasury.ErrInsufficientFunds) {
				m.log.Warn("treasury cannot fund a split", "pool", pc.Name, "deficit", deficit)
				return nil
			}
			// A Build cut off by its deadline may have left a split in the
			// treasury's wallet that nothing records: reconcile it.
			if errors.Is(err, context.DeadlineExceeded) {
				m.reconcilePending = true
			}
			// An unexpected build failure is not a known, transient condition
			// like ErrInsufficientFunds: propagate it so the runner logs a job
			// failure instead of silently doing nothing this tick.
			m.log.Error("build split", "pool", pc.Name, "err", err)
			return err
		}
		err = m.st.PutShard(bctx, b.Shard, b.Rows)
		cancel()
		if err != nil {
			minterSplits.WithLabelValues(pc.Name, "record_failed").Inc()
			// A signed split now exists in the treasury that was never
			// recorded; log at Error with the txid. The next round takes a
			// reconcile snapshot, so a treasury.Reconciler aborts it a grace
			// later even if this instance keeps the lease.
			m.reconcilePending = true
			m.log.Error("record split; a signed split may now be unrecorded", "pool", pc.Name, "txid", b.Shard.TxID.String(), "err", err)
			return err // never send what we could not record
		}
		// PutShard puts the rows in the available state immediately, so this
		// round's accounting advances here regardless of how Send turns out
		// below: a rejection retires the shard again (moving its rows to
		// invalid), and a transient send failure leaves them available for
		// the watcher to resend. Either way, once a round has attempted
		// enough splits to cover the deficit it must stop rather than hammer
		// a persistently failing treasury or network up to MaxPerRound times
		// in a single tick.
		deficit -= int64(pc.SplitSize)
		pending++
		// This is always the first send of a split the treasury just built, so
		// no BEEF: the treasury's wallet posts it, and a BEEF would only make
		// the treasury re-post what its wallet just posted.
		if err := m.tr.Send(ctx, b.Shard.TxID, nil); err != nil {
			if errors.Is(err, treasury.ErrRejected) {
				minterSplits.WithLabelValues(pc.Name, "rejected").Inc()
				if _, rerr := m.st.RetireShard(ctx, pc.Name, b.Shard.TxID); rerr != nil {
					// The split was rejected AND we failed to retire it: the
					// shard is left unmined with rows an operator must find
					// and reconcile by hand, so log everything needed to do
					// that before propagating the error.
					m.log.Error("retire rejected split", "pool", pc.Name, "txid", b.Shard.TxID.String(), "send_err", err, "retire_err", rerr)
					return rerr
				}
				m.log.Error("split rejected on send", "pool", pc.Name, "txid", b.Shard.TxID.String(), "err", err)
				continue
			}
			minterSplits.WithLabelValues(pc.Name, "send_failed").Inc()
			m.log.Warn("send split failed; watcher will retry", "pool", pc.Name, "txid", b.Shard.TxID.String(), "err", err)
			continue
		}
		minterSplits.WithLabelValues(pc.Name, "ok").Inc()
		minterLastSuccess.WithLabelValues(pc.Name).Set(float64(m.clock.Now().Unix()))
	}
	// The loop above only exits normally (not via an early return on error)
	// when the deficit and pending/per-round caps say so; only a deficit at
	// or below zero means projected supply actually reached TargetAvailable,
	// so only then does refilling stop. Stopping early for a cap instead
	// leaves refilling set, which is exactly what carries it sticky into the
	// next round.
	if deficit <= 0 {
		m.refilling[pc.Name] = false
	}
	return nil
}
