package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/openapi"
	"github.com/bsv-blockchain/fuelkeeper/pkg/api"
	"github.com/bsv-blockchain/fuelkeeper/pkg/config"
	"github.com/bsv-blockchain/fuelkeeper/pkg/jobs"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/factory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
	tmock "github.com/bsv-blockchain/fuelkeeper/pkg/treasury/mock"
)

// shutdownTimeout bounds how long graceful HTTP shutdown waits for
// outstanding requests to finish before runServe moves on to tearing down
// pools, jobs and the store.
const shutdownTimeout = 10 * time.Second

// nullnetHintThreshold is how long a watcher/minter interval must be, with a
// mock treasury, before runServeConfig logs a development hint: past this,
// "first fuel" can take long enough that a developer polling readyz/reserve
// might think the server is stuck.
const nullnetHintThreshold = 5 * time.Second

// serveCmd parses the "serve" subcommand's flags and runs the service until
// ctx is cancelled.
func serveCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	cfgPath := fs.String("config", "fuelkeeper.yaml", "path to the YAML config")
	if err := fs.Parse(args); err != nil {
		return wrapUsageError(err)
	}
	return runServe(ctx, *cfgPath, nil)
}

// newLogger builds a slog.Logger from the configured level and format,
// writing to stderr so stdout stays free for the admin CLI's JSON output.
func newLogger(c config.LogConfig) *slog.Logger {
	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(c.Level)) // config.Validate already checked c.Level
	opts := &slog.HandlerOptions{Level: lvl}
	if c.Format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}

// buildOracle returns the configured oracle. Only "none" and "mock" are
// available in this build; teranode/arcade/composite arrive in Plan 5. The
// second return value is non-nil only for "mock", so callers can drive it
// further (SetAssumeUnspent, or wiring it to the mock treasury's send hook).
func buildOracle(c config.OracleConfig) (oracle.Oracle, *oracle.Mock, error) {
	switch c.Kind {
	case "none":
		return oracle.None{}, nil, nil
	case "mock":
		m := oracle.NewMock()
		m.SetAssumeUnspent(c.Mock.AssumeUnspent)
		return m, m, nil
	default:
		return nil, nil, fmt.Errorf("oracle.kind %q is not available in this build", c.Kind)
	}
}

// stoppable is implemented by a treasury.Treasury that owns background
// timers which must be halted before the store closes (see
// sendConfirmer/buildTreasury). runServeConfig type-asserts for it during
// teardown; a treasury that owns no such timers need not implement it.
type stoppable interface {
	stopPendingTimers()
}

// sendConfirmer schedules delayed callbacks (the nullnet mock's simulated
// mining confirmation) and guarantees none of them run once Stop has been
// called, whether they are still pending or racing to fire: a callback can
// never touch the mock oracle after the store has been closed.
type sendConfirmer struct {
	mu     sync.Mutex
	closed bool
	timers map[*time.Timer]struct{}
}

func newSendConfirmer() *sendConfirmer {
	return &sendConfirmer{timers: make(map[*time.Timer]struct{})}
}

// schedule runs fn after delay unless Stop has already been called, or is
// called before the timer fires.
func (c *sendConfirmer) schedule(delay time.Duration, fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	var t *time.Timer
	t = time.AfterFunc(delay, func() {
		c.mu.Lock()
		closed := c.closed
		if !closed {
			delete(c.timers, t)
		}
		c.mu.Unlock()
		if !closed {
			fn()
		}
	})
	c.timers[t] = struct{}{}
}

// Stop halts every pending timer and marks the confirmer closed, so a timer
// racing with Stop cannot invoke its callback afterwards.
func (c *sendConfirmer) Stop() {
	c.mu.Lock()
	c.closed = true
	timers := c.timers
	c.timers = nil
	c.mu.Unlock()
	for t := range timers {
		t.Stop()
	}
}

// confirmingTreasury wraps a treasury.Treasury with the sendConfirmer that
// backs its WithOnSend callback, so runServeConfig can stop pending timers on
// teardown via the stoppable interface.
type confirmingTreasury struct {
	treasury.Treasury
	confirmer *sendConfirmer
}

func (t *confirmingTreasury) stopPendingTimers() { t.confirmer.Stop() }

var _ stoppable = (*confirmingTreasury)(nil)

// buildTreasury returns the configured treasury. Only "mock" is available in
// this build; the toolbox treasury arrives in Plan 5. For "mock", it wires
// WithOnSend so that, ConfirmAfter after a split is sent, mockOracle reports
// it mined -- the "nullnet" pipeline that lets the watcher and minter run
// against no real chain. mockOracle may be nil (oracle.kind: none), in which
// case sends are never confirmed. This never touches the store, so callers
// can (and should) call it before opening one: an unsupported oracle/treasury
// kind then fails fast without ever creating a sqlite file or postgres pool.
func buildTreasury(c config.TreasuryConfig, mockOracle *oracle.Mock) (treasury.Treasury, error) {
	switch c.Kind {
	case "mock":
		var opts []tmock.Option
		confirmer := newSendConfirmer()
		if mockOracle != nil {
			delay := c.Mock.ConfirmAfter
			opts = append(opts, tmock.WithOnSend(func(txid chainhash.Hash) {
				confirmer.schedule(delay, func() {
					mockOracle.SetTx(txid, oracle.TxStatus{Known: true, Mined: true, BlockHeight: 1, MerklePath: []byte{0}})
				})
			}))
		}
		return &confirmingTreasury{Treasury: tmock.New(opts...), confirmer: confirmer}, nil
	default:
		return nil, fmt.Errorf("treasury.kind %q is not available in this build", c.Kind)
	}
}

// logNullnetHint logs a one-line development nudge when a mock treasury is
// paired with slow watcher/minter intervals: fuel can then take long enough
// to mint-then-mine that a developer polling readyz/reserve might think the
// server is stuck.
func logNullnetHint(log *slog.Logger, cfg *config.Config) {
	if cfg.Treasury.Kind != "mock" || !cfg.Jobs.Enabled {
		return
	}
	if cfg.Jobs.Watcher.Interval <= nullnetHintThreshold && cfg.Jobs.Minter.Interval <= nullnetHintThreshold {
		return
	}
	eta := (cfg.Treasury.Mock.ConfirmAfter + cfg.Jobs.Watcher.Interval).Round(time.Second)
	log.Info(fmt.Sprintf("nullnet: first fuel in ~%ds; lower jobs.watcher.interval/minter.interval for development",
		int(eta.Seconds())))
}

// warnInsecureListener logs a Warn when the API has no api_keys configured:
// a reminder that is loud on loopback (harmless for local development, but
// easy to forget about) and louder still for a non-loopback listener, which
// only reaches this point at all because the operator explicitly set
// api.insecure: true (config.Load refuses to start otherwise).
func warnInsecureListener(log *slog.Logger, cfg *config.Config) {
	if len(cfg.API.APIKeys) > 0 {
		return
	}
	host, _, err := net.SplitHostPort(cfg.API.Listen)
	if err != nil { // unreachable: config.Load already validated api.listen
		return
	}
	if config.IsLoopbackHost(host) {
		log.Warn("api: no api_keys configured; open access on a loopback listener is fine for local development",
			"listen", cfg.API.Listen)
		return
	}
	log.Warn("api: running with no api_keys on a non-loopback listener because api.insecure is true; "+
		"make sure that is intentional", "listen", cfg.API.Listen)
}

// serveHooks lets tests observe or intercept points runServeConfig would
// otherwise only expose via logs, real network I/O or a real store backend.
type serveHooks struct {
	// onListen, if non-nil, is called once with the bound HTTP address
	// ("host:port") right after the listener opens -- tests use it to
	// discover the ephemeral port from "127.0.0.1:0".
	onListen func(string)
	// newStore overrides how the store is constructed; nil means
	// factory.New. Tests use it to inject a wrapped store (one that pauses a
	// call, or fails outright) without needing a config-level knob for
	// something that is purely a test seam.
	newStore func(ctx context.Context, cfg factory.Config, now func() time.Time) (store.Store, error)
}

// runServe loads cfgPath and runs the service until ctx is cancelled or the
// server fails. onListen, if non-nil, is called once with the bound address.
func runServe(ctx context.Context, cfgPath string, onListen func(string)) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	return runServeConfig(ctx, cfg, serveHooks{onListen: onListen})
}

// runServeConfig wires cfg -> oracle/treasury -> store -> pools -> jobs ->
// API and serves until ctx is cancelled or the server fails. It is runServe
// with config loading factored out, so tests can exercise it against a
// hand-built *config.Config (e.g. one with a deliberately invalid Jobs
// config) or with hooks.newStore standing in for a real backend.
//
// Order is deliberate: the oracle and treasury are built, and their "kind"
// validated, before the store is opened -- an unsupported kind then fails
// fast without ever touching disk or a database. Shutdown order is the
// mirror image and is the load-bearing part of this function: stop accepting
// HTTP (srv.Shutdown, which drains in-flight requests) *before* cancelling
// the pools and jobs that those in-flight requests still need, then close
// the store only once everything above it has stopped touching it.
//
// poolCtx and jobCtx deliberately derive from context.WithoutCancel(ctx),
// not ctx itself: ctx is the process's signal context, cancelled the instant
// SIGTERM arrives, at exactly the same moment srv.Shutdown begins draining
// in-flight requests. If the pools/jobs contexts were children of ctx, that
// signal would cancel them immediately too -- releasing shard leases and
// tearing down the coalescer out from under a request Shutdown is still
// trying to let finish. Instead, cancelPools/cancelJobs are called
// explicitly, in order, only after srv.Shutdown has returned.
//
// Every stage logs once it completes, at Info: "shutting down" (the decision
// to stop has been made), "http drained" (Shutdown returned), "pools
// stopped", "jobs stopped", "store closed" (or a store-close error, at
// Error). Pools, jobs and any pending mock-treasury timers are torn down by
// one deferred function, registered right after they start, so a listen
// failure or a startJobs error also reaches it -- there is exactly one
// teardown path, not one for the happy path and another for early errors.
func runServeConfig(ctx context.Context, cfg *config.Config, hooks serveHooks) error {
	log := newLogger(cfg.Log)
	logNullnetHint(log, cfg)
	warnInsecureListener(log, cfg)

	// Validate the oracle/treasury kind before the store is ever opened.
	orc, mockOrc, err := buildOracle(cfg.Oracle)
	if err != nil {
		return err
	}
	tr, err := buildTreasury(cfg.Treasury, mockOrc)
	if err != nil {
		return err
	}

	clock := pool.RealClock{}
	newStore := hooks.newStore
	if newStore == nil {
		newStore = factory.New
	}
	st, err := newStore(ctx, cfg.Store, clock.Now)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer func() {
		if cerr := st.Close(); cerr != nil {
			log.Error("store close failed", "err", cerr)
			return
		}
		log.Info("store closed")
	}()

	pools := make([]*pool.Pool, 0, len(cfg.Pools))
	svcs := make([]api.PoolService, 0, len(cfg.Pools))
	names := make([]string, 0, len(cfg.Pools))
	for _, pc := range cfg.Pools {
		p := pool.NewPool(st, pc, cfg.Instance, clock, log)
		pools = append(pools, p)
		svcs = append(svcs, p)
		names = append(names, pc.Name)
	}

	uncancelled := context.WithoutCancel(ctx)
	poolCtx, cancelPools := context.WithCancel(uncancelled)
	var poolWG sync.WaitGroup
	for _, p := range pools {
		poolWG.Add(1)
		go func(p *pool.Pool) {
			defer poolWG.Done()
			if err := p.Run(poolCtx); err != nil {
				log.Error("pool stopped", "pool", p.Config().Name, "err", err)
			}
		}(p)
	}

	jobCtx, cancelJobs := context.WithCancel(uncancelled)
	var jobWG sync.WaitGroup

	// Single teardown path (see the doc comment above): from here on, every
	// return -- the listen failure below, a startJobs error, or the normal
	// post-Shutdown path at the end -- runs exactly this sequence once.
	defer func() {
		cancelPools()
		poolWG.Wait()
		log.Info("pools stopped")
		cancelJobs()
		jobWG.Wait()
		log.Info("jobs stopped")
		if s, ok := tr.(stoppable); ok {
			s.stopPendingTimers()
		}
	}()

	if cfg.Jobs.Enabled {
		if err := startJobs(jobCtx, &jobWG, st, orc, tr, names, cfg, clock, log); err != nil {
			return err
		}
	}

	h := api.NewHandler(api.Deps{
		Pools:          svcs,
		Store:          st,
		Log:            log,
		APIKeys:        cfg.API.APIKeys,
		AdminKeys:      cfg.API.AdminKeys,
		OpenAPI:        openapi.Spec,
		MaxBodyBytes:   cfg.API.MaxBodyBytes,
		RequestTimeout: cfg.API.RequestTimeout,
		Version:        version,
	})
	srv := api.NewServer(cfg.API.Listen, h, cfg.API.ReadTimeout, cfg.API.WriteTimeout, cfg.API.IdleTimeout)
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.API.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.API.Listen, err)
	}
	log.Info("fuelkeeper listening", "addr", ln.Addr().String(), "version", version, "pools", names, "store", cfg.Store.Backend)
	if hooks.onListen != nil {
		hooks.onListen(ln.Addr().String())
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	var retErr error
	select {
	case <-ctx.Done():
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			retErr = err
		}
	}
	log.Info("shutting down")

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	shutdownErr := srv.Shutdown(shutdownCtx)
	cancelShutdown()
	if shutdownErr != nil && !errors.Is(shutdownErr, http.ErrServerClosed) {
		if retErr == nil {
			retErr = shutdownErr
		}
	} else {
		log.Info("http drained")
	}

	// The deferred teardown above now cancels the pools and jobs (only after
	// HTTP has stopped accepting new work and drained what it already had),
	// then the store-close defer runs last.
	return retErr
}

// startJobs constructs the reaper, watcher and minter, registers them on a
// new jobs.Runner, and runs the runner under jobCtx in a goroutine tracked by
// wg. A construction or registration error is returned without starting the
// goroutine, so the caller need not cancel/wait on wg in that case.
func startJobs(
	jobCtx context.Context,
	wg *sync.WaitGroup,
	st store.Store,
	orc oracle.Oracle,
	tr treasury.Treasury,
	names []string,
	cfg *config.Config,
	clock pool.Clock,
	log *slog.Logger,
) error {
	reaper, err := jobs.NewReaper(st, orc, names, cfg.Jobs.Reaper, clock, log)
	if err != nil {
		return fmt.Errorf("reaper: %w", err)
	}
	watcher, err := jobs.NewWatcher(st, orc, tr, names, cfg.Jobs.Watcher, clock, log)
	if err != nil {
		return fmt.Errorf("watcher: %w", err)
	}
	minter, err := jobs.NewMinter(st, tr, cfg.Pools, cfg.Jobs.Minter, clock, log)
	if err != nil {
		return fmt.Errorf("minter: %w", err)
	}
	runner := jobs.NewRunner(st, pool.NewHolder(cfg.Instance.InstanceID), cfg.Jobs.LeaseTTL, log)
	if err := runner.Add(reaper); err != nil {
		return fmt.Errorf("jobs: %w", err)
	}
	if err := runner.Add(watcher); err != nil {
		return fmt.Errorf("jobs: %w", err)
	}
	if err := runner.Add(minter); err != nil {
		return fmt.Errorf("jobs: %w", err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runner.Run(jobCtx); err != nil {
			log.Error("jobs stopped", "err", err)
		}
	}()
	return nil
}
