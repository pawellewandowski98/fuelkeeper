package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// SpendCoalescerConfig tunes spend group commit. It mirrors CoalescerConfig.
type SpendCoalescerConfig struct {
	Slots          int           // concurrent flushes (store Caps.MaxConcurrentFlushes)
	MaxBatch       int           // outpoints per flush, summed over its requests (<= store Caps.MaxBatch)
	Window         time.Duration // optional accumulation window when a slot is free (0 = none)
	MaxInflightOps int           // backpressure threshold in outpoints; 0 = 4*Slots*MaxBatch
	// Retries is how many additional attempts follow the first on
	// ErrUnavailable (or a per-attempt timeout). 0 means the default (2);
	// negative means no retries.
	Retries      int
	RetryBackoff time.Duration // first backoff; doubles; default 20ms
	FlushTimeout time.Duration // per-attempt store timeout; default 5s
}

func (c SpendCoalescerConfig) withDefaults() SpendCoalescerConfig {
	if c.Slots <= 0 {
		c.Slots = 1
	}
	if c.MaxBatch <= 0 {
		c.MaxBatch = 1000
	}
	if c.MaxInflightOps <= 0 {
		c.MaxInflightOps = 4 * c.Slots * c.MaxBatch
	}
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
	return c
}

type spendReq struct {
	req  store.SpendRequest
	done chan struct{}
	n    int
	err  error
	// taken is set under SpendCoalescer.mu when a flush (or shutdown's drain)
	// removes the request from pending; withdraw then knows the request will
	// be answered and must be waited for.
	taken bool
}

// SpendCoalescer groups concurrent Spend requests into one store.SpendBatch
// call per flush, so many spend reports share one durable commit -- the
// spend-side counterpart of Coalescer. Requests that arrive while a flush is
// in flight join the next one; nothing waits for company unless Window is
// set. Start it with Run; submit with Submit.
//
// Unlike Reserve, a spend needs no compensation: SpendBatch is all-or-nothing
// and a retry is state-safe (store.SpendBatcher), so a failed flush simply
// fails each of its requests and the rows stay with whatever the store
// committed.
type SpendCoalescer struct {
	st   store.SpendBatcher
	pool string
	cfg  SpendCoalescerConfig

	mu       sync.Mutex
	pending  []*spendReq
	inflight atomic.Int64 // outpoints queued + in flight
	kick     chan struct{}
	slots    chan struct{}
	stopped  atomic.Bool
	flushes  sync.WaitGroup

	batchSizeObs   prometheus.Observer
	flushObs       prometheus.Observer
	storeOpObs     prometheus.Observer
	retriesCounter prometheus.Counter
}

// NewSpendCoalescer creates a spend coalescer for one pool.
func NewSpendCoalescer(st store.SpendBatcher, pool string, cfg SpendCoalescerConfig) *SpendCoalescer {
	cfg = cfg.withDefaults()
	return &SpendCoalescer{
		st:             st,
		pool:           pool,
		cfg:            cfg,
		kick:           make(chan struct{}, 1),
		slots:          make(chan struct{}, cfg.Slots),
		batchSizeObs:   spendBatchSize.WithLabelValues(pool),
		flushObs:       spendFlushSeconds.WithLabelValues(pool),
		storeOpObs:     storeOpLatency.WithLabelValues(pool, "spend_batch"),
		retriesCounter: spendFlushRetries.WithLabelValues(pool),
	}
}

// PendingRequests returns how many requests are queued and not yet taken by a flush.
func (c *SpendCoalescer) PendingRequests() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// Submit queues one spend request and waits for the flush that carries it.
// It returns the request's own count (what Spend would have returned for it),
// or:
//   - ErrBadRequest wrapping store.ErrInvalidArgument when the request is one
//     Spend itself would reject (it never reaches the store, so it can never
//     fail a batch for the others);
//   - ErrBackpressure when too many outpoints are already queued or in flight;
//   - ErrUnavailable when the coalescer has stopped;
//   - ctx.Err() when ctx ends before a flush takes the request (it is
//     withdrawn and never applied); once a flush has taken it, Submit waits
//     for and returns that flush's result regardless of ctx;
//   - the store's error when the flush failed after its retries (a
//     per-attempt timeout is reported as store.ErrUnavailable).
func (c *SpendCoalescer) Submit(ctx context.Context, req store.SpendRequest) (int, error) {
	if err := store.ValidateSpendRequest(req, c.cfg.MaxBatch); err != nil {
		return 0, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if c.stopped.Load() {
		return 0, ErrUnavailable
	}
	r := &spendReq{req: req, done: make(chan struct{})}
	if err := c.enqueue(r); err != nil {
		return 0, err
	}
	select {
	case <-r.done:
		return r.n, r.err
	case <-ctx.Done():
	}
	if c.withdraw(r) {
		return 0, ctx.Err()
	}
	<-r.done
	return r.n, r.err
}

func (c *SpendCoalescer) enqueue(r *spendReq) error {
	n := int64(len(r.req.Ops))
	if c.inflight.Add(n) > int64(c.cfg.MaxInflightOps) {
		c.inflight.Add(-n)
		return ErrBackpressure
	}
	c.mu.Lock()
	if c.stopped.Load() {
		c.mu.Unlock()
		c.inflight.Add(-n)
		return ErrUnavailable
	}
	c.pending = append(c.pending, r)
	c.mu.Unlock()
	select {
	case c.kick <- struct{}{}:
	default:
	}
	return nil
}

// withdraw removes r from pending, reporting false when a flush or the
// shutdown drain already took it (r will then be answered).
func (c *SpendCoalescer) withdraw(r *spendReq) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.taken {
		return false
	}
	for i, p := range c.pending {
		if p == r {
			c.pending = append(c.pending[:i], c.pending[i+1:]...)
			c.inflight.Add(-int64(len(r.req.Ops)))
			return true
		}
	}
	return false
}

// takeBatch pops whole requests, in arrival order, until MaxBatch outpoints
// are collected.
func (c *SpendCoalescer) takeBatch() []*spendReq {
	c.mu.Lock()
	defer c.mu.Unlock()
	var batch []*spendReq
	n := 0
	for len(c.pending) > 0 {
		next := c.pending[0]
		if n > 0 && n+len(next.req.Ops) > c.cfg.MaxBatch {
			break
		}
		next.taken = true
		batch = append(batch, next)
		n += len(next.req.Ops)
		c.pending = c.pending[1:]
	}
	return batch
}

func (c *SpendCoalescer) pendingOps() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, r := range c.pending {
		n += len(r.req.Ops)
	}
	return n
}

// Run dispatches flushes until ctx is done, then fails queued requests with
// ErrUnavailable and waits for every dispatched flush to finish (each runs on
// a context detached from ctx, bounded by Retries+1 attempts of FlushTimeout
// plus backoff), so no flush is still using the store when Run returns.
func (c *SpendCoalescer) Run(ctx context.Context) error {
	base := context.WithoutCancel(ctx)
	for {
		select {
		case <-ctx.Done():
			return c.shutdown()
		case <-c.kick:
		}
		for c.PendingRequests() > 0 {
			select {
			case c.slots <- struct{}{}:
			case <-ctx.Done():
				return c.shutdown()
			}
			if c.cfg.Window > 0 && c.pendingOps() < c.cfg.MaxBatch {
				timer := time.NewTimer(c.cfg.Window)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
				}
			}
			if ctx.Err() != nil {
				<-c.slots
				return c.shutdown()
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

func (c *SpendCoalescer) shutdown() error {
	c.mu.Lock()
	c.stopped.Store(true)
	pending := c.pending
	c.pending = nil
	for _, r := range pending {
		r.taken = true
	}
	c.mu.Unlock()
	for _, r := range pending {
		c.inflight.Add(-int64(len(r.req.Ops)))
		r.err = ErrUnavailable
		close(r.done)
	}
	c.flushes.Wait()
	return nil
}

// flush sends one batch as a single SpendBatch call, retrying ErrUnavailable
// and per-attempt timeouts. A retry is state-safe (see store.SpendBatcher),
// but a retry that follows an attempt which in fact committed reports 0 for
// the rows that attempt moved -- the same answer a client's own retry of the
// spend report would get.
func (c *SpendCoalescer) flush(base context.Context, batch []*spendReq) {
	defer c.flushes.Done()
	defer func() {
		<-c.slots
		select {
		case c.kick <- struct{}{}:
		default:
		}
	}()
	total := 0
	reqs := make([]store.SpendRequest, len(batch))
	for i, r := range batch {
		reqs[i] = r.req
		total += len(r.req.Ops)
	}
	defer c.inflight.Add(-int64(total))
	c.batchSizeObs.Observe(float64(len(batch)))

	var counts []int
	var err error
	backoff := c.cfg.RetryBackoff
	start := time.Now()
	for attempt := 0; attempt <= c.cfg.Retries; attempt++ {
		if attempt > 0 {
			c.retriesCounter.Inc()
			time.Sleep(backoff)
			backoff *= 2
		}
		fctx, cancel := context.WithTimeout(base, c.cfg.FlushTimeout)
		opStart := time.Now()
		counts, err = c.st.SpendBatch(fctx, c.pool, reqs)
		c.storeOpObs.Observe(time.Since(opStart).Seconds())
		cancel()
		if err == nil && len(counts) != len(reqs) {
			err = fmt.Errorf("%w: SpendBatch returned %d counts for %d requests", store.ErrUnavailable, len(counts), len(reqs))
		}
		if err == nil {
			break
		}
		if errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, store.ErrUnavailable) {
			err = fmt.Errorf("%w: %w", store.ErrUnavailable, err)
		}
		if !errors.Is(err, store.ErrUnavailable) {
			break // not retryable
		}
	}
	c.flushObs.Observe(time.Since(start).Seconds())
	for i, r := range batch {
		if err != nil {
			r.err = err
		} else {
			r.n = counts[i]
		}
		close(r.done)
	}
}
