package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

// tableColumns is renderTable's header, in order.
var tableColumns = []string{
	"label", "backend", "instances", "workers", "count", "mode",
	"reserves/s", "outputs/s", "p50 ms", "p95 ms", "p99 ms", "errors",
}

// renderCmd is "fuelbench render a.json [b.json...]": it prints a markdown
// comparison table of the given result files to os.Stdout.
func renderCmd(files []string) error {
	return runRenderCmd(os.Stdout, files)
}

// runRenderCmd is renderCmd with its output stream factored out for testing.
func runRenderCmd(w io.Writer, files []string) error {
	if len(files) == 0 {
		return fmt.Errorf("fuelbench render: at least one JSON result file is required")
	}
	results := make([]result, 0, len(files))
	for _, f := range files {
		data, err := os.ReadFile(f) //nolint:gosec // f is an operator-supplied CLI argument, not attacker-controlled input
		if err != nil {
			return fmt.Errorf("fuelbench render: read %s: %w", f, err)
		}
		var r result
		if err := json.Unmarshal(data, &r); err != nil {
			return fmt.Errorf("fuelbench render: parse %s: %w", f, err)
		}
		results = append(results, r)
	}
	_, err := fmt.Fprintln(w, renderTable(results))
	return err
}

// renderTable formats results as a markdown table, one row per result, in
// the order given.
func renderTable(results []result) string {
	var b strings.Builder
	b.WriteString("| " + strings.Join(tableColumns, " | ") + " |\n")
	b.WriteString("|" + strings.Repeat(" --- |", len(tableColumns)) + "\n")
	for _, r := range results {
		row := []string{
			r.Label,
			r.Backend,
			strconv.Itoa(r.Instances),
			strconv.Itoa(r.Workers),
			strconv.Itoa(r.Count),
			r.Mode,
			fmt.Sprintf("%.1f", r.ReservesPerSec),
			fmt.Sprintf("%.1f", r.OutputsPerSec),
			microsToMillis(r.P50Micros),
			microsToMillis(r.P95Micros),
			microsToMillis(r.P99Micros),
			errsString(r.Errors),
		}
		b.WriteString("| " + strings.Join(row, " | ") + " |\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

// microsToMillis formats a microsecond latency as milliseconds with one
// decimal place.
func microsToMillis(micros int64) string {
	return fmt.Sprintf("%.1f", float64(micros)/1000)
}

// errsString formats an error-code -> count map as "code=n code=n", sorted
// by code so the same result always renders the same table, or "-" when
// there were no errors.
func errsString(errs map[string]int64) string {
	if len(errs) == 0 {
		return "-"
	}
	codes := make([]string, 0, len(errs))
	for c := range errs {
		codes = append(codes, c)
	}
	sort.Strings(codes)
	parts := make([]string, len(codes))
	for i, c := range codes {
		parts[i] = fmt.Sprintf("%s=%d", c, errs[c])
	}
	return strings.Join(parts, " ")
}
