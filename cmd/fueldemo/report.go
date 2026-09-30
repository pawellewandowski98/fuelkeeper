package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

// report aggregates one fueldemo run's outcome: how many fuel spends were
// successfully built and submitted, how many of each kind of failure
// occurred, and latency percentiles over every successful submission
// (build+submit, not the follow-up report to FuelKeeper). It is safe for
// concurrent use by every worker goroutine; finish must be called exactly
// once, after every worker has stopped, before the percentile and rate
// fields are meaningful.
type report struct {
	mu    sync.Mutex
	start time.Time

	Mode     string           `json:"mode"`
	Workers  int              `json:"workers"`
	Sent     int64            `json:"sent"`
	Failed   map[string]int64 `json:"failed,omitempty"`
	Reserves int64            `json:"reserves"`
	lat      []int64          // µs per successful tx (build+submit)

	Seconds   float64   `json:"seconds"`
	TPS       float64   `json:"tps"`
	P50Ms     float64   `json:"p50_ms"`
	P95Ms     float64   `json:"p95_ms"`
	P99Ms     float64   `json:"p99_ms"`
	GoVersion string    `json:"go_version"`
	Network   string    `json:"network"`
	Timestamp time.Time `json:"timestamp"`
}

// newReport starts a report for a run in mode against network, driven by
// workers concurrent workers. Its clock (for Seconds/TPS at finish) starts
// now.
func newReport(mode, network string, workers int) *report {
	return &report{
		Mode:      mode,
		Network:   network,
		Workers:   workers,
		Failed:    make(map[string]int64),
		start:     time.Now(),
		GoVersion: runtime.Version(),
	}
}

// ok records one successful submission, taking d (elapsed build+submit
// time) into the latency sample.
func (r *report) ok(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Sent++
	r.lat = append(r.lat, d.Microseconds())
}

// fail tallies one failure under code (e.g. "reserve:exhausted",
// "arcade:backpressure", "spend:insufficient").
func (r *report) fail(code string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Failed[code]++
}

// reserved tallies one successful Reserve call.
func (r *report) reserved() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Reserves++
}

// percentile returns the nearest-rank percentile p (0..1) of sorted, a
// latency sample of length n already sorted ascending, converted from
// microseconds to milliseconds. The nearest rank is ceil(n*p), a 1-indexed
// rank into sorted; idx converts that to sorted's 0-indexed position,
// clamped to [0, n-1] to guard the p=0 case (ceil(0)-1 == -1) and any
// floating-point rounding at p=1. sorted must be non-empty.
func percentile(sorted []int64, p float64) float64 {
	n := len(sorted)
	idx := int(math.Ceil(float64(n)*p)) - 1
	idx = max(idx, 0)
	idx = min(idx, n-1)
	return float64(sorted[idx]) / 1000
}

// finish stops the run's clock and computes Seconds, TPS, the latency
// percentiles and Timestamp. It must be called exactly once, after every
// worker goroutine has stopped writing to r.
func (r *report) finish() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Seconds = time.Since(r.start).Seconds()
	if r.Seconds > 0 {
		r.TPS = float64(r.Sent) / r.Seconds
	}
	if len(r.Failed) == 0 {
		r.Failed = nil
	}
	r.Timestamp = time.Now().UTC()
	if len(r.lat) == 0 {
		return
	}
	sorted := append([]int64(nil), r.lat...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	r.P50Ms = percentile(sorted, 0.50)
	r.P95Ms = percentile(sorted, 0.95)
	r.P99Ms = percentile(sorted, 0.99)
}

// write encodes the report as indented JSON to path, or to stdout when path
// is "".
func (r *report) write(path string) error {
	r.mu.Lock()
	data, err := json.MarshalIndent(r, "", "  ")
	r.mu.Unlock()
	if err != nil {
		return fmt.Errorf("fueldemo: encode report: %w", err)
	}
	if path == "" {
		fmt.Println(string(data))
		return nil
	}
	if err := os.WriteFile(path, data, 0o644); err != nil { //nolint:gosec // a results file is meant to be readable
		return fmt.Errorf("fueldemo: write %s: %w", path, err)
	}
	return nil
}

// failedTotal sums every tally in a Failed map (nil-safe).
func failedTotal(m map[string]int64) int64 {
	var n int64
	for _, v := range m {
		n += v
	}
	return n
}
