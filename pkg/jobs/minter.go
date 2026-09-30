package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

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

// MinterConfig tunes replenishment.
type MinterConfig struct {
	// Interval is how often the minter ticks; see Job.Interval.
	Interval time.Duration `yaml:"interval"`
	// MaxPendingShards caps how many unmined shards may exist at once.
	MaxPendingShards int `yaml:"max_pending_shards"`
	// MaxPerRound caps how many splits are attempted per pool per RunOnce.
	MaxPerRound int `yaml:"max_per_round"`
}

// DefaultMinterConfig returns the spec defaults.
func DefaultMinterConfig() MinterConfig {
	return MinterConfig{Interval: 30 * time.Second, MaxPendingShards: 50, MaxPerRound: 20}
}

// Validate checks the config.
func (c MinterConfig) Validate() error {
	if c.Interval <= 0 {
		return fmt.Errorf("jobs: minter interval must be > 0")
	}
	if c.MaxPendingShards <= 0 || c.MaxPerRound <= 0 {
		return fmt.Errorf("jobs: minter max_pending_shards and max_per_round must be > 0")
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
// Refilling is hysteresis, not a live setpoint: a pool is topped up only once
// its available rows drop below pool.Config.LowWater (LowWater == 0 means
// "always refill below target"), and once triggered it mints back up to
// TargetAvailable rather than just past LowWater. While available sits
// anywhere in [LowWater, TargetAvailable) the minter does nothing, so a pool
// does not re-split for every single row consumed. See needsRefill.
type Minter struct {
	st    store.Store
	tr    treasury.Treasury
	pools []pool.Config
	cfg   MinterConfig
	clock pool.Clock
	log   *slog.Logger
}

// NewMinter creates a minter for the configured pools, or an error if cfg is
// invalid.
func NewMinter(st store.Store, tr treasury.Treasury, pools []pool.Config, cfg MinterConfig, clock pool.Clock, log *slog.Logger) (*Minter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Minter{st: st, tr: tr, pools: pools, cfg: cfg, clock: clock, log: log.With("job", "minter")}, nil
}

// Name implements Job.
func (m *Minter) Name() string { return "minter" }

// Interval implements Job.
func (m *Minter) Interval() time.Duration { return m.cfg.Interval }

// RunOnce tops up every pool; one pool's store error does not skip the
// others, but a cancelled context stops immediately instead of working
// through the remaining pools.
func (m *Minter) RunOnce(ctx context.Context) error {
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

// needsRefill reports whether pc should be topped up given its currently
// available rows: LowWater is a hysteresis threshold, not a second target.
// Once available falls below LowWater the pool refills (mintPool then fills
// all the way up to TargetAvailable, not just past LowWater); while available
// sits anywhere in [LowWater, TargetAvailable) it is left alone. LowWater == 0
// disables hysteresis: the pool refills whenever it is below TargetAvailable
// at all, matching pool.Config.LowWater's zero value.
func needsRefill(pc pool.Config, available int64) bool {
	if pc.LowWater > 0 {
		return available < pc.LowWater
	}
	return available < pc.TargetAvailable
}

// mintPool tops up one pool toward its target, subject to LowWater hysteresis
// (see needsRefill), MaxPendingShards and MaxPerRound.
func (m *Minter) mintPool(ctx context.Context, pc pool.Config) error {
	stats, err := m.st.Stats(ctx, pc.Name)
	if err != nil {
		return err
	}
	publishStats(pc.Name, stats)
	if !needsRefill(pc, stats.Available) {
		return nil
	}
	// Rows in unmined shards are already counted in stats.Available (they are
	// available rows that mined-only instances cannot serve yet), so the
	// deficit is simply target - available.
	deficit := pc.TargetAvailable - stats.Available
	pending := int(stats.ShardsUnmined)
	for built := 0; deficit > 0 && pending < m.cfg.MaxPendingShards && built < m.cfg.MaxPerRound; built++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := m.tr.Build(ctx, treasury.MintRequest{Pool: pc.Name, Denomination: pc.Denomination, Count: pc.SplitSize, LockMode: pc.LockMode})
		if err != nil {
			minterSplits.WithLabelValues(pc.Name, "build_failed").Inc()
			if errors.Is(err, treasury.ErrInsufficientFunds) {
				m.log.Warn("treasury cannot fund a split", "pool", pc.Name, "deficit", deficit)
				return nil
			}
			// An unexpected build failure is not a known, transient condition
			// like ErrInsufficientFunds: propagate it so the runner logs a job
			// failure instead of silently doing nothing this tick.
			m.log.Error("build split", "pool", pc.Name, "err", err)
			return err
		}
		if err := m.st.PutShard(ctx, b.Shard, b.Rows); err != nil {
			minterSplits.WithLabelValues(pc.Name, "record_failed").Inc()
			// A signed split now exists in the treasury that was never
			// recorded; log at Error with the txid so Plan 5's startup
			// reconciliation of unrecorded splits can find it.
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
	return nil
}
