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

// serveCmd parses the "serve" subcommand's flags and runs the service until
// ctx is cancelled.
func serveCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	cfgPath := fs.String("config", "fuelkeeper.yaml", "path to the YAML config")
	if err := fs.Parse(args); err != nil {
		return err
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
// sendConfirmer/buildTreasury). runServe type-asserts for it during
// shutdown; a treasury that owns no such timers need not implement it.
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
// backs its WithOnSend callback, so runServe can stop pending timers on
// shutdown via the stoppable interface.
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
// case sends are never confirmed.
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

// runServe loads cfgPath, wires config -> store -> pools -> jobs -> API, and
// serves until ctx is cancelled or the server fails. onListen, if non-nil, is
// called once with the bound address ("host:port") right after the listener
// is opened -- tests use it to discover the ephemeral port from
// "127.0.0.1:0".
//
// Shutdown order: stop accepting HTTP, cancel the pools and wait for
// Pool.Run to return (Pool.Run only returns once its coalescer has flushed
// every in-flight request, so the pool is quiescent), cancel the job runner
// and wait, stop any pending mock-treasury timers, then close the store
// (deferred, so it always runs last -- including on every early-return error
// path below).
func runServe(ctx context.Context, cfgPath string, onListen func(string)) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	log := newLogger(cfg.Log)
	clock := pool.RealClock{}

	st, err := factory.New(ctx, cfg.Store, clock.Now)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer func() {
		if cerr := st.Close(); cerr != nil {
			log.Error("store close failed", "err", cerr)
		}
	}()

	orc, mockOrc, err := buildOracle(cfg.Oracle)
	if err != nil {
		return err
	}
	tr, err := buildTreasury(cfg.Treasury, mockOrc)
	if err != nil {
		return err
	}

	pools := make([]*pool.Pool, 0, len(cfg.Pools))
	svcs := make([]api.PoolService, 0, len(cfg.Pools))
	names := make([]string, 0, len(cfg.Pools))
	for _, pc := range cfg.Pools {
		p := pool.NewPool(st, pc, cfg.Instance, clock, log)
		pools = append(pools, p)
		svcs = append(svcs, p)
		names = append(names, pc.Name)
	}

	poolCtx, cancelPools := context.WithCancel(ctx)
	defer cancelPools()
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

	jobCtx, cancelJobs := context.WithCancel(ctx)
	defer cancelJobs()
	var jobWG sync.WaitGroup
	if cfg.Jobs.Enabled {
		if err := startJobs(jobCtx, &jobWG, st, orc, tr, names, cfg, clock, log); err != nil {
			cancelPools()
			poolWG.Wait()
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
		cancelJobs()
		jobWG.Wait()
		cancelPools()
		poolWG.Wait()
		return fmt.Errorf("listen %s: %w", cfg.API.Listen, err)
	}
	log.Info("fuelkeeper listening", "addr", ln.Addr().String(), "version", version, "pools", names, "store", cfg.Store.Backend)
	if onListen != nil {
		onListen(ln.Addr().String())
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

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	shutdownErr := srv.Shutdown(shutdownCtx)
	cancelShutdown()
	if retErr == nil && shutdownErr != nil && !errors.Is(shutdownErr, http.ErrServerClosed) {
		retErr = shutdownErr
	}

	cancelPools()
	poolWG.Wait()
	cancelJobs()
	jobWG.Wait()

	if s, ok := tr.(stoppable); ok {
		s.stopPendingTimers()
	}

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
