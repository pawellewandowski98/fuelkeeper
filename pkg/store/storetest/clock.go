// Package storetest is the conformance suite every store backend must pass.
package storetest

import (
	"sync"
	"time"
)

// Clock is a manual clock. Backends under test are constructed with clk.Now as
// their clock so expiry logic is deterministic.
type Clock struct {
	mu sync.Mutex
	t  time.Time
}

// NewClock starts at t.
func NewClock(t time.Time) *Clock { return &Clock{t: t} }

// Now returns the current manual time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// Advance moves the clock forward by d and returns the new time.
func (c *Clock) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
	return c.t
}

// Set moves the clock to t directly (e.g. to a time carrying a sub-microsecond
// nanosecond component, for tests that pin microsecond-truncation behaviour)
// and returns it.
func (c *Clock) Set(t time.Time) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
	return c.t
}
