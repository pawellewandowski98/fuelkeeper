// Command fueldemo is a demand generator against a running FuelKeeper
// instance: N workers loop reserve(batch) -> spend, driving fuel outputs
// through a real settlement path instead of fuelbench's synthetic
// fake-txid spend calls.
//
// In raw mode (Plan 6 Task 4) each fuel output is spent into its own
// OP_RETURN transaction, submitted to an arcade instance in extended format
// (arcade needs no BEEF for this), and reported to FuelKeeper with spend.
// This measures FuelKeeper's own ceiling: the cheapest possible transaction
// that still exercises the real reserve/spend round trip and a real
// broadcast.
//
// In toolbox mode (Plan 6 Task 5) a go-sdk wallet drives createAction over
// reserved fuel inputs the way a real integrator would, funding itself and
// broadcasting through arcade and verifying inputs against chaintracks.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/bsv-blockchain/fuelkeeper/pkg/arcadeclient"
	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
)

// usageError marks an error as a command-line usage mistake -- a bad flag, a
// missing required flag -- as opposed to a runtime failure. main uses it to
// choose between the two conventional exit codes: 2 for usage, 1 for
// everything else. Mirrors cmd/fuelkeeper/main.go's convention of the same
// name.
type usageError struct{ err error }

// Error implements error.
func (e *usageError) Error() string { return e.err.Error() }

// Unwrap lets errors.Is/errors.As see through to the wrapped error.
func (e *usageError) Unwrap() error { return e.err }

// usageErrorf builds a usageError from a format string.
func usageErrorf(format string, args ...any) error {
	return &usageError{err: fmt.Errorf(format, args...)}
}

// wrapUsageError marks err (e.g. a flag.FlagSet.Parse failure) as a
// usageError; a nil err stays nil. flag.ErrHelp passes through wrapped, so
// errors.Is(_, flag.ErrHelp) still sees it (see exitCode).
func wrapUsageError(err error) error {
	if err == nil {
		return nil
	}
	return &usageError{err: err}
}

// exitCode maps a run's error to the process exit code: 0 for success and
// for -h/--help (the flag set has already printed its usage), 2 for a usage
// mistake, 1 for anything else.
func exitCode(err error) int {
	var uerr *usageError
	switch {
	case err == nil, errors.Is(err, flag.ErrHelp):
		return 0
	case errors.As(err, &uerr):
		return 2
	default:
		return 1
	}
}

// options configures one fueldemo run.
type options struct {
	mode        string
	url         string
	key         string
	pool        string
	arcadeURL   string
	arcadeToken string
	workers     int
	batch       int
	rateLim     float64 // submissions/s across all workers, 0 = unbounded
	duration    time.Duration
	out         string
	feeKB       uint64
	changeTo    string // hex locking script, "" = none

	// toolbox-mode flags. --arcade and --chaintracks are both required in
	// toolbox mode (checked in toolbox.go, not options.validate: the wallet
	// funds itself and broadcasts through arcade, and verifies every input
	// BEEF against chaintracks). --change-to is raw-mode only and ignored in
	// toolbox mode: the wallet's own createAction call sizes its own change.
	network     string
	walletKey   string
	chaintracks string
	storage     string
}

// parseFlags parses args into an options value. A flag.FlagSet.Parse failure
// (including -h/--help, which flag itself already reported to the flag
// set's output) is returned wrapped as a usageError.
func parseFlags(args []string) (options, error) {
	var o options
	fs := flag.NewFlagSet("fueldemo", flag.ContinueOnError)
	fs.StringVar(&o.mode, "mode", "raw", "raw | toolbox")
	fs.StringVar(&o.url, "url", "", "FuelKeeper base URL")
	fs.StringVar(&o.key, "key", os.Getenv("FUELKEEPER_API_KEY"), "FuelKeeper API key (default: $FUELKEEPER_API_KEY)")
	fs.StringVar(&o.pool, "pool", "main", "pool name")
	fs.StringVar(&o.arcadeURL, "arcade", os.Getenv("ARCADE_URL"), "arcade base URL (default: $ARCADE_URL)")
	fs.StringVar(&o.arcadeToken, "arcade-token", os.Getenv("ARCADE_CALLBACK_TOKEN"), "arcade callback token (default: $ARCADE_CALLBACK_TOKEN)")
	fs.IntVar(&o.workers, "workers", 16, "concurrent workers")
	fs.IntVar(&o.batch, "batch", 20, "fuel outputs reserved per worker round")
	fs.Float64Var(&o.rateLim, "rate", 0, "submissions per second across all workers, 0 = unbounded")
	fs.DurationVar(&o.duration, "duration", 60*time.Second, "how long to run")
	fs.StringVar(&o.out, "out", "", "write the JSON report to this file (default: stdout)")
	fs.Uint64Var(&o.feeKB, "fee-kb", 100, "fee rate, satoshis per KB, for the OP_RETURN transaction and any change output")
	fs.StringVar(&o.changeTo, "change-to", "", "hex locking script for an optional change output (default: none, whole denomination is fee)")
	fs.StringVar(&o.network, "network", "ttn", "toolbox mode: network name")
	fs.StringVar(&o.walletKey, "wallet-key", os.Getenv("FUELDEMO_WALLET_KEY"), "toolbox mode: wallet private key (default: $FUELDEMO_WALLET_KEY)")
	fs.StringVar(&o.chaintracks, "chaintracks", os.Getenv("CHAINTRACKS_URL"), "toolbox mode: chaintracks URL (default: $CHAINTRACKS_URL)")
	fs.StringVar(&o.storage, "storage", "fueldemo.sqlite", "toolbox mode: sqlite storage path")
	if err := fs.Parse(args); err != nil {
		return options{}, wrapUsageError(err)
	}
	return o, nil
}

// validate checks o before run dials anything.
func (o options) validate() error {
	if o.url == "" {
		return usageErrorf("fueldemo: --url is required")
	}
	if o.pool == "" {
		return usageErrorf("fueldemo: --pool is required")
	}
	switch o.mode {
	case "raw":
		if o.arcadeURL == "" {
			return usageErrorf("fueldemo: raw mode requires --arcade (or $ARCADE_URL)")
		}
	case "toolbox":
	default:
		return usageErrorf("fueldemo: --mode must be raw or toolbox (got %q)", o.mode)
	}
	if o.workers <= 0 {
		return usageErrorf("fueldemo: --workers must be > 0")
	}
	if o.batch <= 0 {
		return usageErrorf("fueldemo: --batch must be > 0")
	}
	if o.duration <= 0 {
		return usageErrorf("fueldemo: --duration must be > 0")
	}
	if o.rateLim < 0 {
		return usageErrorf("fueldemo: --rate must be >= 0")
	}
	if o.changeTo != "" {
		if _, err := hex.DecodeString(o.changeTo); err != nil {
			return usageErrorf("fueldemo: --change-to: invalid hex: %w", err)
		}
	}
	return nil
}

// run drives options against a FuelKeeper instance until o.duration elapses
// or ctx is cancelled (e.g. by a signal), whichever comes first. It always
// returns a non-nil *report once past option validation and client
// construction, even when the run itself later fails or is cut short: only
// a bad configuration or a client construction failure returns a nil report
// alongside the error.
func run(ctx context.Context, o options) (*report, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}

	fk, err := client.New(o.url, client.WithAPIKey(o.key))
	if err != nil {
		return nil, fmt.Errorf("fueldemo: fuelkeeper client: %w", err)
	}

	// Toolbox mode's report does carry o.network (its only consumer); raw
	// mode leaves report.Network empty, since --network is a toolbox-only
	// flag (see options.network's doc comment).
	if o.mode == "toolbox" {
		rep := newReport(o.mode, o.network, o.workers)
		return rep, newToolboxRunner(ctx, o, fk, rep)
	}

	arc, err := arcadeclient.New(o.arcadeURL, arcadeclient.WithCallbackToken(o.arcadeToken))
	if err != nil {
		// Both clients must be up before a report is even constructed: a
		// caller must never mistake a report for a completed (even empty)
		// run when construction itself failed before anything ran.
		return nil, fmt.Errorf("fueldemo: arcade client: %w", err)
	}

	rep := newReport(o.mode, "", o.workers)

	var changeTo []byte
	if o.changeTo != "" {
		changeTo, _ = hex.DecodeString(o.changeTo) // validated in options.validate
	}

	var limiter *rate.Limiter
	if o.rateLim > 0 {
		limiter = rate.NewLimiter(rate.Limit(o.rateLim), 1)
	}

	r := &rawRunner{
		fk:       fk,
		arc:      arc,
		pool:     o.pool,
		batch:    o.batch,
		rep:      rep,
		limiter:  limiter,
		memo:     []byte("fueldemo"),
		satPerKB: o.feeKB,
		changeTo: changeTo,
	}

	runCtx, cancel := context.WithTimeout(ctx, o.duration)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(o.workers)
	for i := 0; i < o.workers; i++ {
		go func(id int) {
			defer wg.Done()
			r.worker(runCtx, id)
		}(i)
	}
	wg.Wait()

	return rep, nil
}

func main() {
	o, err := parseFlags(os.Args[1:])

	var rep *report
	if err == nil {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		rep, err = run(ctx, o)
	}

	if rep != nil {
		rep.finish()
		if werr := rep.write(o.out); werr != nil {
			fmt.Fprintln(os.Stderr, "error:", werr)
		}
		fmt.Fprintf(os.Stderr, "fueldemo: mode=%s sent=%d reserves=%d failed=%d tps=%.1f p50=%.1fms p95=%.1fms p99=%.1fms\n",
			rep.Mode, rep.Sent, rep.Reserves, failedTotal(rep.Failed), rep.TPS, rep.P50Ms, rep.P95Ms, rep.P99Ms)
	}

	if code := exitCode(err); code != 0 {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(code)
	}
}
