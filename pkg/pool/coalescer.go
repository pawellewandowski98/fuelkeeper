package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// CoalescerConfig tunes group commit.
type CoalescerConfig struct {
	Slots          int           // concurrent flushes (store Caps.MaxConcurrentFlushes)
	MaxBatch       int           // ops per flush (<= store Caps.MaxBatch)
	Window         time.Duration // optional accumulation window when a slot is free (0 = none)
	MaxInflightOps int           // backpressure threshold; 0 = 4*Slots*MaxBatch
	// Retries is how many additional attempts follow the first on ErrUnavailable.
	// 0 means the default (2); negative means no retries (the first attempt is final).
	Retries        int
	RetryBackoff   time.Duration // first backoff; doubles; default 20ms
	FlushTimeout   time.Duration // per-attempt store timeout; default 5s
	CompensateTime time.Duration // budget for compensating releases; default 2s
}

func (c CoalescerConfig) withDefaults() CoalescerConfig {
	if c.Slots <= 0 {
		c.Slots = 1
	}
	if c.MaxBatch <= 0 {
		c.MaxBatch = 256
	}
	if c.MaxInflightOps <= 0 {
		c.MaxInflightOps = 4 * c.Slots * c.MaxBatch
	}
	// A single max-size request must never be rejected by construction: whatever
	// MaxInflightOps ends up being (defaulted or explicit), it can't be smaller
	// than one full batch.
	c.MaxInflightOps = max(c.MaxInflightOps, c.MaxBatch)
	if c.Retries < 0 {
		c.Retries = 0
	} else if c.Retries == 0 {
		c.Retries = 2
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 20 * time.Millisecond
	}
	if c.FlushTimeout <= 0 {
		c.FlushTimeout = 5 * time.Second
	}
	if c.CompensateTime <= 0 {
		c.CompensateTime = 2 * time.Second
	}
	return c
}

// Result is what Submit returns.
type Result struct {
	Applied   []store.ReserveOp // committed under the request's token; safe to hand out
	Missed    []store.Outpoint  // CAS failed (not available / other token); caller forgets and picks others
	Unflushed bool              // the request never reached the store; caller may Return its rows
	Err       error             // ErrBadRequest, ErrBackpressure, ctx error, or ErrUnavailable
}

const (
	reqPending   int32 = 0
	reqDelivered int32 = 1
	reqAbandoned int32 = 2
)

type request struct {
	ops    []store.ReserveOp
	state  atomic.Int32
	done   chan struct{}
	result Result
	// drained is set under Coalescer.mu by drain, before it clears pending, to
	// tell a racing withdraw that this request was pulled out by shutdown (and
	// will be delivered as Unflushed) rather than by a flush. Without this flag
	// withdraw can miss a request that drain already removed from pending and
	// wrongly fall through to the "taken by a flush" path, reporting
	// Unflushed=false for a request the store never saw.
	drained bool
}

// Coalescer groups concurrent reserve requests into batched, idempotent
// store.Reserve calls. Start it with Run; submit with Submit.
type Coalescer struct {
	st   store.Store
	pool string
	cfg  CoalescerConfig

	mu         sync.Mutex
	pending    []*request
	pendingOps int
	inflight   atomic.Int64 // ops in pending + in flight
	kick       chan struct{}
	slots      chan struct{}
	stopped    atomic.Bool
	flushes    sync.WaitGroup // every dispatched flush goroutine; Run waits on this before returning
}

// NewCoalescer creates a coalescer for one pool.
func NewCoalescer(st store.Store, pool string, cfg CoalescerConfig) *Coalescer {
	cfg = cfg.withDefaults()
	return &Coalescer{
		st:    st,
		pool:  pool,
		cfg:   cfg,
		kick:  make(chan struct{}, 1),
		slots: make(chan struct{}, cfg.Slots),
	}
}

// PendingOps returns the number of ops queued and not yet taken by a flush (tests, metrics).
func (c *Coalescer) PendingOps() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pendingOps
}

// InflightOps returns ops queued plus ops in flight (backpressure gauge).
func (c *Coalescer) InflightOps() int64 { return c.inflight.Load() }

// Submit enqueues ops under one token (all ops must share it) and waits for the
// flush that carries them, or for ctx.
func (c *Coalescer) Submit(ctx context.Context, ops []store.ReserveOp) Result {
	if len(ops) == 0 || len(ops) > c.cfg.MaxBatch {
		return Result{Unflushed: true, Err: fmt.Errorf("%w: reserve batch must have 1..%d ops", ErrBadRequest, c.cfg.MaxBatch)}
	}
	if err := store.ValidateReserveOps(ops, c.cfg.MaxBatch); err != nil {
		return Result{Unflushed: true, Err: fmt.Errorf("%w: %w", ErrBadRequest, err)}
	}
	tok := ops[0].Token
	for _, op := range ops[1:] {
		if op.Token != tok {
			return Result{Unflushed: true, Err: fmt.Errorf("%w: reserve batch ops must share one token", ErrBadRequest)}
		}
	}
	if c.stopped.Load() {
		return Result{Unflushed: true, Err: ErrUnavailable}
	}
	req := &request{ops: ops, done: make(chan struct{})}
	if err := c.enqueue(req); err != nil {
		return Result{Unflushed: true, Err: err}
	}
	select {
	case <-req.done:
		return req.result
	case <-ctx.Done():
	}
	if c.withdraw(req) {
		return Result{Unflushed: true, Err: ctx.Err()}
	}
	// Already taken by a flush. Whoever wins the state CAS handles applied rows.
	if req.state.CompareAndSwap(reqPending, reqAbandoned) {
		return Result{Err: ctx.Err()}
	}
	<-req.done // delivered concurrently: the result is ours to clean up
	c.releaseApplied(req.result.Applied)
	return Result{Err: ctx.Err()}
}

// addInflight adjusts the inflight counter and mirrors it onto the exported
// gauge, so every enqueue/withdraw/drain/flush accounting site stays in sync
// with fuelkeeper_coalescer_inflight_ops without each call site touching the
// gauge itself.
func (c *Coalescer) addInflight(n int64) int64 {
	v := c.inflight.Add(n)
	coalescerInflightOps.WithLabelValues(c.pool).Set(float64(v))
	return v
}

// enqueue appends req to the pending queue, or reports why it could not:
// ErrBackpressure when the inflight cap is exceeded, ErrUnavailable when the
// coalescer has stopped.
func (c *Coalescer) enqueue(req *request) error {
	n := int64(len(req.ops))
	if c.addInflight(n) > int64(c.cfg.MaxInflightOps) {
		c.addInflight(-n)
		return ErrBackpressure
	}
	c.mu.Lock()
	if c.stopped.Load() { // drain already ran; do not strand this request
		c.mu.Unlock()
		c.addInflight(-n)
		return ErrUnavailable
	}
	c.pending = append(c.pending, req)
	c.pendingOps += len(req.ops)
	c.mu.Unlock()
	select {
	case c.kick <- struct{}{}:
	default:
	}
	return nil
}

// withdraw removes a request that has not been taken by a flush yet. It
// reports true both when it physically removes req from pending and when
// drain already claimed req (req.drained): either way the caller never
// reached the store and the request is (or will be) delivered as Unflushed.
func (c *Coalescer) withdraw(req *request) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if req.drained {
		return true
	}
	for i, p := range c.pending {
		if p == req {
			c.pending = append(c.pending[:i], c.pending[i+1:]...)
			c.pendingOps -= len(req.ops)
			c.addInflight(-int64(len(req.ops)))
			return true
		}
	}
	return false
}

// takeBatch pops whole requests until MaxBatch ops are collected.
func (c *Coalescer) takeBatch() []*request {
	c.mu.Lock()
	defer c.mu.Unlock()
	var batch []*request
	n := 0
	for len(c.pending) > 0 {
		next := c.pending[0]
		if n > 0 && n+len(next.ops) > c.cfg.MaxBatch {
			break
		}
		batch = append(batch, next)
		n += len(next.ops)
		c.pending = c.pending[1:]
		c.pendingOps -= len(next.ops)
	}
	return batch
}

func (c *Coalescer) hasPending() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending) > 0
}

// Run dispatches flushes until ctx is done, then fails queued requests as
// Unflushed and waits for every dispatched flush goroutine to finish before
// returning: each flush (including its retries and, on failure, its
// compensating release) runs on its own context detached from ctx, so it is
// unaffected by ctx being cancelled, but Run does not return while one is
// still writing to the store. That wait is bounded by the flush's own
// budgets, never open-ended: at most Retries+1 attempts of up to FlushTimeout
// each, plus retry backoff, plus CompensateTime for a final compensating
// release. With the package defaults (FlushTimeout=5s, Retries=2,
// RetryBackoff=20ms doubling, CompensateTime=2s) that is about
// 3*5s + 60ms + 2s =~ 17s worst case. A caller that tears down the store
// after Run returns can rely on this: no flush goroutine is still using it.
func (c *Coalescer) Run(ctx context.Context) error {
	base := context.WithoutCancel(ctx)
	for {
		select {
		case <-ctx.Done():
			return c.shutdown(ctx.Err())
		case <-c.kick:
		}
		for c.hasPending() {
			select {
			case c.slots <- struct{}{}:
			case <-ctx.Done():
				return c.shutdown(ctx.Err())
			}
			// Skip the accumulation window once a batch is already full size:
			// waiting can only delay a flush that has nothing left to gain from it.
			if c.cfg.Window > 0 && c.PendingOps() < c.cfg.MaxBatch {
				timer := time.NewTimer(c.cfg.Window)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
				}
			}
			if cerr := ctx.Err(); cerr != nil {
				// Cancelled while waiting on the window (or the outer select raced
				// slot-acquisition against ctx.Done and happened to pick the slot):
				// give the slot back and shut down without dispatching a new flush.
				<-c.slots
				return c.shutdown(cerr)
			}
			batch := c.takeBatch()
			if len(batch) == 0 {
				<-c.slots
				continue
			}
			c.flushes.Add(1)
			go c.flush(base, batch)
		}
	}
}

// shutdown drains queued requests as Unflushed, then blocks until every
// flush goroutine dispatched so far has finished (see Run's doc comment for
// the bound), so Run never returns while one might still be writing to the
// store.
func (c *Coalescer) shutdown(err error) error {
	c.drain(err)
	c.flushes.Wait()
	return nil
}

func (c *Coalescer) drain(err error) {
	c.mu.Lock()
	c.stopped.Store(true)
	pending := c.pending
	c.pending = nil
	c.pendingOps = 0
	for _, req := range pending {
		req.drained = true
	}
	c.mu.Unlock()
	for _, req := range pending {
		c.addInflight(-int64(len(req.ops)))
		c.deliver(req, Result{Unflushed: true, Err: err})
	}
}

// flush sends one batch of requests to the store as a single Reserve call,
// retrying in-doubt failures idempotently and compensating as a last resort.
// base is a context already detached from Run's ctx (see Run), so a flush in
// progress when the coalescer is asked to shut down still gets to finish.
//
// Requests are guaranteed by the allocator's known set never to name the same
// outpoint twice, but flush defends against it anyway: if two requests in the
// same batch do name the same outpoint, only the first request's op is sent
// to the store; every later request that named it is reported Missed without
// ever reaching the store, so at most one request can ever believe it owns a
// given outpoint out of a single flush.
func (c *Coalescer) flush(base context.Context, batch []*request) {
	defer c.flushes.Done()
	defer func() {
		<-c.slots
		select {
		case c.kick <- struct{}{}:
		default:
		}
	}()
	total := 0
	for _, r := range batch {
		total += len(r.ops)
	}
	defer c.addInflight(-int64(total))
	coalescerBatchSize.WithLabelValues(c.pool).Observe(float64(total))

	ops := make([]store.ReserveOp, 0, total)
	owner := make(map[store.Outpoint]int, total)
	forcedMissed := make([]map[store.Outpoint]struct{}, len(batch))
	for i, r := range batch {
		for _, op := range r.ops {
			if _, dup := owner[op.Outpoint]; dup {
				if forcedMissed[i] == nil {
					forcedMissed[i] = make(map[store.Outpoint]struct{})
				}
				forcedMissed[i][op.Outpoint] = struct{}{}
				continue
			}
			owner[op.Outpoint] = i
			ops = append(ops, op)
		}
	}

	var applied []store.ReserveOp
	var err error
	backoff := c.cfg.RetryBackoff
	flushStart := time.Now()
	for attempt := 0; attempt <= c.cfg.Retries; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff *= 2
		}
		fctx, cancel := context.WithTimeout(base, c.cfg.FlushTimeout)
		opStart := time.Now()
		applied, err = c.st.Reserve(fctx, c.pool, ops)
		storeOpLatency.WithLabelValues(c.pool, "reserve").Observe(time.Since(opStart).Seconds())
		cancel()
		if err == nil {
			break
		}
		if !errors.Is(err, store.ErrUnavailable) && !errors.Is(err, context.DeadlineExceeded) {
			break // not retryable
		}
	}
	coalescerFlushLatency.WithLabelValues(c.pool).Observe(time.Since(flushStart).Seconds())
	if err != nil {
		// Outcome unknown: compensate once over the deduplicated ops actually sent,
		// not once per request, so N requests sharing a flush don't each pay their
		// own CompensateTime budget while still holding the slot. Release on a
		// never-applied row is a no-op.
		casUnknown.WithLabelValues(c.pool).Add(float64(len(ops)))
		c.compensate(ops)
		for _, r := range batch {
			c.deliver(r, Result{Err: fmt.Errorf("%w: %w", ErrUnavailable, err)})
		}
		return
	}

	appliedBy := make(map[store.Outpoint]store.ReserveOp, len(applied))
	for _, op := range applied {
		appliedBy[op.Outpoint] = op
	}
	for i, r := range batch {
		res := Result{}
		for _, op := range r.ops {
			if _, forced := forcedMissed[i][op.Outpoint]; forced {
				res.Missed = append(res.Missed, op.Outpoint)
				continue
			}
			// Trust only an applied op reported under our own token: a backend
			// that lies about the token must never cause us to hand out a row.
			if a, ok := appliedBy[op.Outpoint]; ok && a.Token == op.Token {
				res.Applied = append(res.Applied, a)
			} else {
				res.Missed = append(res.Missed, op.Outpoint)
			}
		}
		c.deliver(r, res)
	}
}

// deliver hands the result to the waiter, or releases applied rows if the
// waiter abandoned the request.
func (c *Coalescer) deliver(req *request, res Result) {
	req.result = res
	if req.state.CompareAndSwap(reqPending, reqDelivered) {
		close(req.done)
		return
	}
	// abandoned: nobody will use these rows
	close(req.done)
	c.releaseApplied(res.Applied)
}

// releaseApplied best-effort releases ops grouped by token under a detached
// context. Used to give back rows an abandoned request no longer needs; not
// counted as a compensating release (see compensate).
func (c *Coalescer) releaseApplied(ops []store.ReserveOp) {
	c.releaseByToken(ops)
}

// compensate best-effort releases ops whose Reserve outcome is unknown after
// retries were exhausted, counting each Release call it issues.
func (c *Coalescer) compensate(ops []store.ReserveOp) {
	n := c.releaseByToken(ops)
	if n > 0 {
		compensatingRelease.WithLabelValues(c.pool).Add(float64(n))
	}
}

// releaseByToken groups ops by token and issues one Release call per token
// under a detached context, returning how many calls it made.
func (c *Coalescer) releaseByToken(ops []store.ReserveOp) int {
	if len(ops) == 0 {
		return 0
	}
	byToken := map[store.Token][]store.Outpoint{}
	for _, op := range ops {
		byToken[op.Token] = append(byToken[op.Token], op.Outpoint)
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.CompensateTime)
	defer cancel()
	for tok, outs := range byToken {
		start := time.Now()
		_, err := c.st.Release(ctx, c.pool, tok, outs) // errors leave rows to the expiry path
		storeOpLatency.WithLabelValues(c.pool, "release").Observe(time.Since(start).Seconds())
		_ = err
	}
	return len(byToken)
}
