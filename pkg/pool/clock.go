// Package pool is the in-process hot path: allocator, coalescer, shard manager
// and the Pool service that the HTTP API calls.
package pool

import "time"

// Clock is the time source for leases, expiries and rescans.
type Clock interface{ Now() time.Time }

// RealClock uses time.Now.
type RealClock struct{}

// Now returns the wall-clock time.
func (RealClock) Now() time.Time { return time.Now() }

// ClockFunc adapts a func() time.Time (e.g. storetest.Clock.Now).
type ClockFunc func() time.Time

// Now calls the adapted function.
func (f ClockFunc) Now() time.Time { return f() }
