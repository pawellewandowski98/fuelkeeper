// Command fuelbench is an HTTP load generator for the FuelKeeper public API
// (pkg/client): N workers loop reserve(count) -> spend|release|hold over one
// or more FuelKeeper URLs (round-robin), with an optional rate limit, and
// report reserves/s, outputs/s, latency percentiles and error counts by
// code. "fuelbench render a.json [b.json...]" turns saved JSON results into
// a markdown comparison table.
package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	mathrand "math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"golang.org/x/time/rate"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
)

// errRetryPause is how long a worker waits after a retryable error before
// trying again, so a temporarily exhausted or backpressured pool is not
// hammered in a tight loop.
const errRetryPause = 20 * time.Millisecond

// histogramMaxMicros bounds the latency histogram at 60s; a reserve call
// slower than that is not something fuelbench needs finer resolution on.
const histogramMaxMicros = 60_000_000

// options configures one fuelbench run.
type options struct {
	urls     []string
	apiKey   string
	pool     string
	workers  int
	count    int
	duration time.Duration
	rateLim  float64 // reserves per second across all workers, 0 = unbounded
	mode     string  // spend | release | hold
	label    string
	backend  string // report only; not used to reach the server
}

// validate checks options before run dials anything.
func (o options) validate() error {
	if len(o.urls) == 0 {
		return errors.New("fuelbench: at least one --url is required")
	}
	if o.workers <= 0 {
		return errors.New("fuelbench: workers must be > 0")
	}
	if o.count <= 0 {
		return errors.New("fuelbench: count must be > 0")
	}
	if o.duration <= 0 {
		return errors.New("fuelbench: duration must be > 0")
	}
	if o.pool == "" {
		return errors.New("fuelbench: pool is required")
	}
	switch o.mode {
	case "spend", "release", "hold":
	default:
		return fmt.Errorf("fuelbench: mode must be spend, release or hold (got %q)", o.mode)
	}
	if o.rateLim < 0 {
		return errors.New("fuelbench: rate must be >= 0")
	}
	return nil
}

// result is one run's outcome, in the shape written to --out and read back
// by "fuelbench render".
type result struct {
	Label          string           `json:"label"`
	Backend        string           `json:"backend,omitempty"`
	Instances      int              `json:"instances"`
	Workers        int              `json:"workers"`
	Count          int              `json:"count"`
	Mode           string           `json:"mode"`
	Seconds        float64          `json:"seconds"`
	Reserves       int64            `json:"reserves"`
	Outputs        int64            `json:"outputs"`
	ReservesPerSec float64          `json:"reserves_per_sec"`
	OutputsPerSec  float64          `json:"outputs_per_sec"`
	P50Micros      int64            `json:"p50_us"`
	P95Micros      int64            `json:"p95_us"`
	P99Micros      int64            `json:"p99_us"`
	MaxMicros      int64            `json:"max_us"`
	Errors         map[string]int64 `json:"errors,omitempty"`
	GoVersion      string           `json:"go_version"`
	Timestamp      time.Time        `json:"timestamp"`
}

// histogram is a mutex-guarded hdrhistogram.Histogram: Histogram itself is
// not safe for concurrent use, but every worker goroutine records into the
// same one.
type histogram struct {
	mu   sync.Mutex
	hist *hdrhistogram.Histogram
}

func newHistogram() *histogram {
	return &histogram{hist: hdrhistogram.New(1, histogramMaxMicros, 3)}
}

// record adds a latency in microseconds, clamping anything below 1 (the
// histogram's lowest discernible value) up to it.
func (h *histogram) record(micros int64) {
	if micros < 1 {
		micros = 1
	}
	h.mu.Lock()
	_ = h.hist.RecordValue(micros) // only errors on a value above highestTrackableValue, which this bench tool just under-reports rather than fail the whole run for
	h.mu.Unlock()
}

func (h *histogram) quantile(q float64) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hist.ValueAtQuantile(q)
}

func (h *histogram) max() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hist.Max()
}

// errTracker tallies errors by code under a mutex.
type errTracker struct {
	mu   sync.Mutex
	errs map[string]int64
}

func newErrTracker() *errTracker { return &errTracker{errs: make(map[string]int64)} }

func (t *errTracker) add(code string) {
	t.mu.Lock()
	t.errs[code]++
	t.mu.Unlock()
}

// snapshot returns a copy of the tallies, or nil if there were none (so
// result.Errors is omitted from JSON on a clean run).
func (t *errTracker) snapshot() map[string]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.errs) == 0 {
		return nil
	}
	out := make(map[string]int64, len(t.errs))
	for k, v := range t.errs {
		out[k] = v
	}
	return out
}

// errCode maps err to the code it is tallied under: an *client.APIError's
// own Code, or "transport" for anything else (a dial/timeout failure, or the
// caller's own context ending outside of an APIError).
func errCode(err error) string {
	var apiErr *client.APIError
	if errors.As(err, &apiErr) {
		return apiErr.Code
	}
	return "transport"
}

// newBenchTransport clones http.DefaultTransport and raises
// MaxIdleConnsPerHost to at least workers, so a run driving many concurrent
// workers at one host is not starved of idle connections and forced to
// re-dial.
func newBenchTransport(workers int) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert // http.DefaultTransport is always *http.Transport
	if t.MaxIdleConnsPerHost < workers {
		t.MaxIdleConnsPerHost = workers
	}
	if t.MaxIdleConns < workers {
		t.MaxIdleConns = workers
	}
	return t
}

// fakeTxID returns a random 64-hex-character string that looks like a txid,
// for the "spend" mode's report calls (fuelbench never builds or broadcasts
// a real transaction).
func fakeTxID() string {
	var b [32]byte
	for i := 0; i < len(b); i += 8 {
		binary.BigEndian.PutUint64(b[i:], mathrand.Uint64())
	}
	return hex.EncodeToString(b[:])
}

// outpointsOf extracts the wire outpoint strings from a reserve response's
// outputs.
func outpointsOf(outs []wire.Output) []string {
	ops := make([]string, len(outs))
	for i, o := range outs {
		ops[i] = o.Outpoint
	}
	return ops
}

// run drives options against one or more FuelKeeper URLs until o.duration
// elapses or ctx is cancelled (e.g. by a signal), whichever comes first, and
// returns the aggregated result. A run cut short by ctx cancellation still
// returns a result over whatever was measured up to that point, not an
// error: only a bad configuration or a client construction failure is
// returned as an error.
func run(ctx context.Context, o options) (result, error) {
	if err := o.validate(); err != nil {
		return result{}, err
	}

	hc := &http.Client{Transport: newBenchTransport(o.workers)}
	clients := make([]*client.Client, len(o.urls))
	for i, u := range o.urls {
		c, err := client.New(u, client.WithAPIKey(o.apiKey), client.WithHTTPClient(hc))
		if err != nil {
			return result{}, fmt.Errorf("fuelbench: client for %s: %w", u, err)
		}
		clients[i] = c
	}

	var limiter *rate.Limiter
	if o.rateLim > 0 {
		limiter = rate.NewLimiter(rate.Limit(o.rateLim), 1)
	}

	hist := newHistogram()
	errs := newErrTracker()
	var reserves, outputs atomic.Int64
	var idx atomic.Int64

	runCtx, cancel := context.WithTimeout(ctx, o.duration)
	defer cancel()

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(o.workers)
	for i := 0; i < o.workers; i++ {
		go func() {
			defer wg.Done()
			runWorker(runCtx, clients, &idx, o, limiter, hist, errs, &reserves, &outputs)
		}()
	}
	wg.Wait()
	seconds := time.Since(start).Seconds()

	res := result{
		Label:          o.label,
		Backend:        o.backend,
		Instances:      len(o.urls),
		Workers:        o.workers,
		Count:          o.count,
		Mode:           o.mode,
		Seconds:        seconds,
		Reserves:       reserves.Load(),
		Outputs:        outputs.Load(),
		ReservesPerSec: float64(reserves.Load()) / seconds,
		OutputsPerSec:  float64(outputs.Load()) / seconds,
		P50Micros:      hist.quantile(50),
		P95Micros:      hist.quantile(95),
		P99Micros:      hist.quantile(99),
		MaxMicros:      hist.max(),
		Errors:         errs.snapshot(),
		GoVersion:      runtime.Version(),
		Timestamp:      time.Now().UTC(),
	}
	return res, nil
}

// runWorker is one fuelbench worker's loop: reserve, then spend/release/hold
// per o.mode, until ctx is done. Clients are chosen round-robin across every
// worker via the shared idx counter, so requests spread evenly over
// o.urls regardless of how many workers there are.
func runWorker(
	ctx context.Context,
	clients []*client.Client,
	idx *atomic.Int64,
	o options,
	limiter *rate.Limiter,
	hist *histogram,
	errs *errTracker,
	reserves, outputs *atomic.Int64,
) {
	for {
		if ctx.Err() != nil {
			return
		}
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return // ctx ended while waiting for a token; graceful stop
			}
		}
		n := idx.Add(1) - 1
		c := clients[n%int64(len(clients))]

		reqStart := time.Now()
		res, err := c.Reserve(ctx, o.pool, client.ReserveRequest{Count: o.count, MinCount: o.count})
		if err != nil {
			if ctx.Err() != nil {
				return // shutdown mid-call; not a real error
			}
			recordErr(errs, err)
			continue
		}
		hist.record(time.Since(reqStart).Microseconds())
		reserves.Add(1)
		outputs.Add(int64(len(res.Outputs)))

		switch o.mode {
		case "spend":
			if _, serr := c.Spend(ctx, o.pool, res.Token, outpointsOf(res.Outputs), fakeTxID()); serr != nil && ctx.Err() == nil {
				recordErr(errs, serr)
			}
		case "release":
			if _, rerr := c.Release(ctx, o.pool, res.Token, outpointsOf(res.Outputs)); rerr != nil && ctx.Err() == nil {
				recordErr(errs, rerr)
			}
		case "hold":
			// Intentionally does nothing: the reservation is held until its TTL
			// expires, to measure behaviour under sustained pool drain.
		}
	}
}

// recordErr tallies err by its code and, if retrying is likely to help,
// pauses errRetryPause before the worker's next attempt.
func recordErr(errs *errTracker, err error) {
	errs.add(errCode(err))
	if client.IsRetryable(err) {
		time.Sleep(errRetryPause)
	}
}

// splitURLs splits a comma-separated --url flag value, trimming whitespace
// and dropping empty entries.
func splitURLs(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "render" {
		if err := renderCmd(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}

	fs := flag.NewFlagSet("fuelbench", flag.ContinueOnError)
	urlFlag := fs.String("url", "", "comma-separated FuelKeeper base URLs (round-robin)")
	key := fs.String("key", os.Getenv("FUELKEEPER_API_KEY"), "API key (default: $FUELKEEPER_API_KEY)")
	poolName := fs.String("pool", "main", "pool name")
	workers := fs.Int("workers", 64, "concurrent workers")
	count := fs.Int("count", 1, "outputs requested per reserve call")
	duration := fs.Duration("duration", 30*time.Second, "how long to run")
	rateFlag := fs.Float64("rate", 0, "reserve calls per second across all workers, 0 = unbounded")
	mode := fs.String("mode", "spend", "spend | release | hold")
	label := fs.String("label", "", "label recorded in the result, for comparing runs")
	backend := fs.String("backend", "", "backend name recorded in the result (report only)")
	out := fs.String("out", "", "write the JSON result to this file")
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}

	o := options{
		urls:     splitURLs(*urlFlag),
		apiKey:   *key,
		pool:     *poolName,
		workers:  *workers,
		count:    *count,
		duration: *duration,
		rateLim:  *rateFlag,
		mode:     *mode,
		label:    *label,
		backend:  *backend,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	res, err := run(ctx, o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	data, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "error: encode result:", err)
		os.Exit(1)
	}
	if *out != "" {
		if err := os.WriteFile(*out, data, 0o644); err != nil { //nolint:gosec // a results file is meant to be readable
			fmt.Fprintln(os.Stderr, "error: write", *out, ":", err)
			os.Exit(1)
		}
	}
	fmt.Println(string(data))
}
