package toolbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"

	"github.com/bsv-blockchain/go-chaintracks/chaintracks"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/monitor"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/services"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/storage"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/wallet"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/wallet/fuelkeeper"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/wdk"

	"github.com/bsv-blockchain/fuelkeeper/pkg/arcadeclient"
	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
)

// storageName is the name the toolbox storage is migrated under.
const storageName = "fuelkeeper-treasury"

// fanoutMaxTxsPerRound caps the keeper's leaf fan-outs per round.
const fanoutMaxTxsPerRound = 100

// chaintracksEventBuffer sizes the reorg and tip channels fed to the monitor
// (the toolbox's own server uses 10, pkg/infra/server.go:153,166).
const chaintracksEventBuffer = 10

// Treasury is the toolbox treasury: a Core over a live go-wallet-toolbox
// wallet, plus everything that wallet needs (services, GORM storage, the
// monitor daemon and the chunk keeper). Close releases them.
type Treasury struct {
	*Core

	svc      *services.WalletServices
	provider *storage.Provider
	wallet   *wallet.Wallet
	daemon   *monitor.Daemon
	keeper   *fuelkeeper.Keeper
	feeKB    uint64
	chunk    uint64

	cancel    context.CancelFunc
	cleanups  []func()
	closeOnce sync.Once
	closeErr  error
}

var (
	_ treasury.Treasury     = (*Treasury)(nil)
	_ treasury.Funder       = (*Treasury)(nil)
	_ treasury.Reconciler   = (*Treasury)(nil)
	_ treasury.ChunkEnsurer = (*Treasury)(nil)
)

// Open validates cfg against pools and then builds the toolbox stack, in
// the order of the toolbox's own server (pkg/infra/server.go:95-200):
// services, storage provider (migrated), wallet, monitor daemon, chunk
// keeper. Background work (the arcade health probe, chaintracks, the
// background broadcaster and the monitor) runs until Close, not until ctx
// ends; ctx bounds only the start-up calls. A config error returns before
// any network or disk access.
func Open(ctx context.Context, cfg Config, pools []pool.Config, log *slog.Logger) (_ *Treasury, err error) {
	if err := cfg.Validate(pools); err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	network, err := defs.ParseBSVNetworkStr(cfg.Network)
	if err != nil {
		return nil, fmt.Errorf("treasury: network: %w", err)
	}
	identityKey, err := wdk.IdentityKey(cfg.RootKeyHex)
	if err != nil {
		return nil, errors.New("treasury: root key: cannot derive identity key") // never echo the key
	}
	root, err := ec.PrivateKeyFromHex(cfg.RootKeyHex)
	if err != nil {
		return nil, errors.New("treasury: root key: invalid")
	}
	kd := sdk.NewKeyDeriver(root)

	svcCfg, err := servicesConfig(cfg, network, identityKey)
	if err != nil {
		return nil, err
	}
	feeKB, err := resolveFee(ctx, cfg)
	if err != nil {
		return nil, err
	}
	fee := defs.FeeModel{Type: defs.SatPerKB, Value: int64(feeKB)} //nolint:gosec // an operator fee rate, far below MaxInt64
	chunkValue := cfg.ChunkValue(pools[0], feeKB)
	um, err := utxoManagement(cfg, chunkValue, fee)
	if err != nil {
		return nil, err
	}
	kcfg := keeperConfig(cfg, um, chunkValue, feeKB)

	bgCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	t := &Treasury{feeKB: feeKB, chunk: chunkValue, cancel: cancel}
	defer func() {
		if err != nil {
			_ = t.Close()
		}
	}()

	t.svc = services.New(log, svcCfg)
	if err := t.svc.StartBackgroundServices(bgCtx); err != nil {
		return nil, fmt.Errorf("treasury: start services: %w", err)
	}

	t.provider, err = storage.NewGORMProvider(network, t.svc,
		storage.WithDBConfig(dbConfig(cfg.Storage)),
		storage.WithFeeModel(fee),
		storage.WithChangeBasket(defs.DefaultChangeBasket()),
		storage.WithUTXOManagement(um),
		storage.WithLogger(log),
		storage.WithBackgroundBroadcasterContext(bgCtx),
	)
	if err != nil {
		return nil, fmt.Errorf("treasury: storage: %w", err)
	}
	if _, err := t.provider.Migrate(ctx, storageName, identityKey); err != nil {
		return nil, fmt.Errorf("treasury: migrate storage: %w", err)
	}

	// AutoKnownTxids off: Core requires every split BEEF to carry full
	// ancestry (splitBEEF), which bare known txids would break.
	t.wallet, err = wallet.New(network, kd, t.provider,
		wallet.WithServices(t.svc), wallet.WithLogger(log), wallet.WithAutoKnownTxids(false))
	if err != nil {
		return nil, fmt.Errorf("treasury: wallet: %w", err)
	}

	monOpts := append(t.chaintracksEvents(), monitor.WithBroadcastEventStream(t.svc))
	t.daemon, err = monitor.NewDaemonWithGORMLocker(bgCtx, log, t.provider, t.provider.Database.DB, monOpts...)
	if err != nil {
		return nil, fmt.Errorf("treasury: monitor: %w", err)
	}
	monCfg := defs.DefaultMonitorConfig()
	if err := t.daemon.Start(bgCtx, monCfg.Tasks.EnabledTasks()); err != nil {
		return nil, fmt.Errorf("treasury: start monitor: %w", err)
	}

	t.keeper, err = fuelkeeper.New(t.wallet, kcfg, log)
	if err != nil {
		return nil, fmt.Errorf("treasury: chunk keeper: %w", err)
	}

	t.Core = NewCore(t.wallet, broadcaster{svc: t.svc}, keeper{k: t.keeper}, nonces{w: t.wallet}, kd, cfg, log)
	log.Info("toolbox treasury open", "network", cfg.Network, "identity_key", identityKey,
		"fee_sat_per_kb", feeKB, "chunk_value", chunkValue, "target_chunks", kcfg.TargetPoolSize,
		"storage", cfg.Storage.Engine)
	return t, nil
}

// chaintracksEvents subscribes the monitor to chaintracks reorgs and tips,
// as the toolbox's server does (pkg/infra/server.go:152-176); a nil
// unsubscribe means chaintracks is off and the channel is dropped.
func (t *Treasury) chaintracksEvents() []monitor.DaemonEventOption {
	var opts []monitor.DaemonEventOption
	reorgs := make(chan *chaintracks.ReorgEvent, chaintracksEventBuffer)
	if unsub := t.svc.SubscribeReorgs(reorgs); unsub != nil {
		opts = append(opts, monitor.WithReorgChannel(reorgs))
		t.cleanups = append(t.cleanups, func() { unsub(); close(reorgs) })
	}
	tips := make(chan *chaintracks.BlockHeader, chaintracksEventBuffer)
	if unsub := t.svc.SubscribeTips(tips); unsub != nil {
		opts = append(opts, monitor.WithTipChannel(tips))
		t.cleanups = append(t.cleanups, func() { unsub(); close(tips) })
	}
	return opts
}

// Close stops the monitor, the background services and broadcaster, closes
// the wallet and the storage database. It is idempotent.
func (t *Treasury) Close() error {
	t.closeOnce.Do(func() {
		if t.daemon != nil {
			if err := t.daemon.Stop(); err != nil {
				t.closeErr = fmt.Errorf("treasury: stop monitor: %w", err)
			}
		}
		if t.cancel != nil {
			t.cancel()
		}
		for i := len(t.cleanups) - 1; i >= 0; i-- {
			t.cleanups[i]()
		}
		if t.wallet != nil {
			t.wallet.Close()
		}
		if t.provider != nil {
			t.provider.Stop() // stops the background broadcaster, closes the database
		}
	})
	return t.closeErr
}

// Reconcile is Core.Reconcile (blocking for the grace) with the report in
// the treasury package's type. The minter uses the non-blocking
// ReconcileStart/ReconcileFinish instead.
func (t *Treasury) Reconcile(ctx context.Context, known func(context.Context, chainhash.Hash) (bool, error)) (treasury.ReconcileReport, error) {
	rep, err := t.Core.Reconcile(ctx, known)
	return treasury.ReconcileReport(rep), err
}

// ReconcileStart is Core.ReconcileStart in the treasury package's types.
func (t *Treasury) ReconcileStart(ctx context.Context, known func(context.Context, chainhash.Hash) (bool, error)) ([]chainhash.Hash, treasury.ReconcileReport, error) {
	snap, rep, err := t.Core.ReconcileStart(ctx, known)
	return snap, treasury.ReconcileReport(rep), err
}

// ReconcileFinish is Core.ReconcileFinish in the treasury package's types.
func (t *Treasury) ReconcileFinish(ctx context.Context, snapshot []chainhash.Hash, known func(context.Context, chainhash.Hash) (bool, error)) (treasury.ReconcileReport, error) {
	rep, err := t.Core.ReconcileFinish(ctx, snapshot, known)
	return treasury.ReconcileReport(rep), err
}

// FeeSatPerKB is the fee rate the treasury resolved at Open.
func (t *Treasury) FeeSatPerKB() uint64 { return t.feeKB }

// ChunkValue is the exact-denomination chunk value the treasury keeps ready.
func (t *Treasury) ChunkValue() uint64 { return t.chunk }

// servicesConfig is the toolbox's network defaults (defs/services.go:126)
// with the operator's arcade and chaintracks endpoints, both enabled, and the
// arcade callback token derived from the storage identity key
// (wdk/arcade_token.go:12), which scopes arcade's SSE status stream to this
// wallet.
func servicesConfig(cfg Config, network defs.BSVNetwork, identityKey string) (defs.WalletServices, error) {
	s := defs.DefaultServicesConfig(network)
	s.Arcade.Enabled = true
	s.Arcade.URL = cfg.ArcadeURL
	s.Arcade.EventsURL = cfg.ArcadeEvents
	if s.Arcade.EventsURL == "" {
		s.Arcade.EventsURL = cfg.ArcadeURL
	}
	s.Arcade.CallbackToken = wdk.DeriveArcadeCallbackToken(identityKey)
	s.ChaintracksClient.Enabled = true
	s.ChaintracksClient.RemoteURL = cfg.ChaintracksURL
	if err := s.Validate(); err != nil {
		return defs.WalletServices{}, fmt.Errorf("treasury: services config: %w", err)
	}
	return s, nil
}

// policyFee reads arcade's fee policy; replaceable by tests.
var policyFee = func(ctx context.Context, arcadeURL string) (uint64, error) {
	c, err := arcadeclient.New(arcadeURL)
	if err != nil {
		return 0, err
	}
	p, err := c.Policy(ctx)
	if err != nil {
		return 0, err
	}
	return p.SatPerKB(), nil
}

// resolveFee is cfg.FeeSatPerKB, or arcade's policy rate (at least 1) when 0.
func resolveFee(ctx context.Context, cfg Config) (uint64, error) {
	if cfg.FeeSatPerKB > 0 {
		return cfg.FeeSatPerKB, nil
	}
	fee, err := policyFee(ctx, cfg.ArcadeURL)
	if err != nil {
		return 0, fmt.Errorf("treasury: fee rate from arcade policy (set treasury.toolbox.fee_sat_per_kb to skip): %w", err)
	}
	return max(fee, 1), nil
}

// dbConfig maps the storage config onto the toolbox defaults
// (defs/dbs.go:94), keeping the defaults' pool sizes, time zone, and the
// postgres port and sslmode when unset.
func dbConfig(s StorageConfig) defs.Database {
	db := defs.DefaultDBConfig()
	switch s.Engine {
	case "postgres":
		db.Engine = defs.DBTypePostgres
		pg := &db.PostgreSQL
		pg.Host, pg.DBName, pg.User, pg.Password = s.Postgres.Host, s.Postgres.DBName, s.Postgres.User, s.Postgres.Password
		if s.Postgres.Port != 0 {
			pg.Port = strconv.Itoa(s.Postgres.Port)
		}
		if s.Postgres.SSLMode != "" {
			pg.SslMode = s.Postgres.SSLMode
		}
	default:
		db.Engine = defs.DBTypeSQLite
		db.SQLite.ConnectionString = s.SQLite
	}
	return db
}

// utxoManagement selects the toolbox's throughput strategy
// (defs/utxo_management.go:63-147) with one exact denomination, the leaf
// chunk value, and runs the toolbox's own validator. The provider's
// server-side top-up stays off: the chunk keeper, driven by the minter, is
// the only minting path.
func utxoManagement(cfg Config, chunkValue uint64, fee defs.FeeModel) (defs.UTXOManagement, error) {
	k := cfg.Chunking
	um := defs.DefaultUTXOManagement()
	um.Strategy = defs.StrategyThroughput
	t := &um.Throughput
	t.DenominationSatoshis = chunkValue
	t.PoolBasket = k.ChunkBasket
	t.ReserveBasket = k.ReserveBasket
	t.FanoutOutputsPerTx = uint64(k.FanoutOutputsPerTx) //nolint:gosec // validated > 0
	t.FanoutMaxTxsPerRound = fanoutMaxTxsPerRound
	t.SpendPolicy = defs.SpendPolicyPreferMined
	t.TargetTPS = 1
	t.TargetPoolSize = uint64(k.TargetChunks)     //nolint:gosec // validated > 0
	t.LowWaterPercent = uint64(k.LowWaterPercent) //nolint:gosec // validated 1..100
	t.HighWaterPercent = 100
	t.TopUp.Enabled = false
	t.TopUp.IntervalSeconds = uint(max(k.Interval.Seconds(), 1))
	if err := um.Validate(fee, defs.DefaultCommission()); err != nil {
		return defs.UTXOManagement{}, fmt.Errorf("treasury: utxo management: %w", err)
	}
	if got, _ := t.Denomination(fee, defs.DefaultCommission()); got != chunkValue {
		return defs.UTXOManagement{}, fmt.Errorf("treasury: utxo management resolves denomination %d, want chunk value %d", got, chunkValue)
	}
	return um, nil
}

// keeperConfig derives the chunk keeper's config with
// fuelkeeper.FromThroughput (wallet/fuelkeeper/keeper.go:107), which
// guarantees the keeper and the storage agree on the denomination, target,
// water marks, fan-out shape and baskets (a mismatch disables minting). Only
// three fields are then overridden: the interval and originator come from
// the treasury config, and ChunkFeeHeadroom, the per-reserve-chunk fee
// allowance for one leaf fan-out (M chunk outputs; not Chunking.HeadroomSats,
// which is the per-chunk allowance for one fuel split), is the larger of
// FromThroughput's max(1000, 8 × denomination) and twice the fan-out's fee
// at the resolved rate, so a high fee rate cannot starve the fan-out. Any
// excess returns to the wallet as change.
func keeperConfig(cfg Config, um defs.UTXOManagement, chunkValue, feeKB uint64) fuelkeeper.Config {
	k := fuelkeeper.FromThroughput(um.Throughput, chunkValue)
	k.Interval = cfg.Chunking.Interval
	k.Originator = cfg.Originator
	k.ChunkFeeHeadroom = max(k.ChunkFeeHeadroom, 2*fuel.FeeSats(fanoutSizeBytes(k.FanoutOutputsPerTx), feeKB))
	return k
}

// fanoutSizeBytes estimates a leaf fan-out: one P2PKH input (a reserve
// chunk), m P2PKH chunk outputs and one change output, plus the fixed
// fields and CompactSize counts.
func fanoutSizeBytes(m uint64) int {
	return txFixedSize + varIntSize(1) + varIntSize(m+1) + fuel.P2PKHInputSize +
		int(m)*p2pkhOutputSize + changeOutputSize //nolint:gosec // m is a small config value
}
