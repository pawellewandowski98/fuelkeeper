package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// ReserveRequest is what a client asks for.
type ReserveRequest struct {
	Count    int
	MinCount int           // 0 = Count
	TTL      time.Duration // 0 = pool default
}

// Output is one reserved fuel output with its unlocking data.
type Output struct {
	store.Outpoint
	Satoshis        uint64
	LockingScript   []byte
	UnlockingScript []byte
}

// ShardRef points a client at the BEEF it needs (GET /v1/beef/{txid}). Version
// is this instance's last-seen shard version; it is 0 ("unknown") when the
// shard was no longer owned locally by the time the response was built.
// Clients treat 0 as "fetch": always re-GET the BEEF/version rather than
// trusting a cached copy.
type ShardRef struct {
	TxID    chainhash.Hash
	Version int64
}

// Reservation is a successful reserve.
type Reservation struct {
	Token     store.Token
	ExpiresAt time.Time
	TTL       time.Duration
	LockMode  store.LockMode
	Outputs   []Output
	Shards    []ShardRef
}

// Pool serves one configured pool on this instance.
type Pool struct {
	cfg   Config
	icfg  InstanceConfig
	st    store.Store
	alloc *Allocator
	mgr   *ShardManager
	coal  *Coalescer
	clock Clock
	log   *slog.Logger
}

// NewPool wires allocator, shard manager and coalescer for pc. The coalescer
// batch is at least MaxCount so one request always fits one flush.
//
// pc.MaxCount can never exceed the store's max batch size: a single Reserve
// call submits up to MaxCount ops in one coalescer flush, and the coalescer
// rejects an over-size batch with ErrBadRequest. A config asking for more
// than the store can batch is a misconfiguration, not a per-request fault, so
// it is clamped here (and logged) rather than surfaced to callers.
func NewPool(st store.Store, pc Config, ic InstanceConfig, clock Clock, log *slog.Logger) *Pool {
	caps := st.Capabilities()
	if pc.MaxCount > caps.MaxBatch {
		log.Error("pool max_count exceeds store max batch; clamping",
			"pool", pc.Name, "max_count", pc.MaxCount, "store_max_batch", caps.MaxBatch)
		pc.MaxCount = caps.MaxBatch
	}
	maxBatch := ic.MaxBatch
	if maxBatch <= 0 {
		maxBatch = 256
	}
	maxBatch = max(maxBatch, pc.MaxCount)
	maxBatch = min(maxBatch, caps.MaxBatch)
	alloc := NewAllocator()
	return &Pool{
		cfg:   pc,
		icfg:  ic,
		st:    st,
		alloc: alloc,
		mgr:   NewShardManager(st, pc, ic, alloc, clock, log),
		coal: NewCoalescer(st, pc.Name, CoalescerConfig{
			Slots:        max(1, caps.MaxConcurrentFlushes),
			MaxBatch:     maxBatch,
			Window:       ic.FlushWindow,
			FlushTimeout: ic.FlushTimeout,
		}),
		clock: clock,
		log:   log.With("component", "pool", "pool", pc.Name),
	}
}

// Config returns the pool configuration.
func (p *Pool) Config() Config { return p.cfg }

// Holder returns the shard-lease holder id of this instance for this pool.
func (p *Pool) Holder() string { return p.mgr.Holder() }

// EnsureRows exposes the bounded adoption path (tests, readiness).
func (p *Pool) EnsureRows(ctx context.Context) bool { return p.mgr.EnsureRows(ctx) }

// Ready reports whether this pool can serve a Reserve call without first
// waiting on shard adoption: true when at least one row is already queued in
// the allocator, or at least one shard is owned (rows are expected shortly,
// e.g. mid-adoption or between rescans). It reads only in-memory state -- no
// store call -- so Plan 3's readiness probe can call it on every health check.
func (p *Pool) Ready(_ context.Context) bool {
	return p.alloc.Available() > 0 || len(p.alloc.Shards()) > 0
}

// Run runs the coalescer and shard manager until ctx is done.
func (p *Pool) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	run := func(f func(context.Context) error) {
		defer wg.Done()
		if err := f(ctx); err != nil {
			mu.Lock()
			if first == nil {
				first = err
			}
			mu.Unlock()
		}
	}
	wg.Add(2)
	go run(p.coal.Run)
	go run(p.mgr.Run)
	wg.Wait()
	return first
}

func (p *Pool) validate(req ReserveRequest) (count, minCount int, ttl time.Duration, err error) {
	count = req.Count
	if count <= 0 || count > p.cfg.MaxCount {
		return 0, 0, 0, fmt.Errorf("%w: count must be 1..%d", ErrBadRequest, p.cfg.MaxCount)
	}
	minCount = req.MinCount
	if minCount == 0 {
		minCount = count
	}
	if minCount < 1 || minCount > count {
		return 0, 0, 0, fmt.Errorf("%w: min_count must be 1..count", ErrBadRequest)
	}
	ttl = req.TTL
	if ttl == 0 {
		ttl = p.cfg.DefaultTTL
	}
	if ttl < p.cfg.MinTTL || ttl > p.cfg.MaxTTL {
		return 0, 0, 0, fmt.Errorf("%w: ttl must be %s..%s", ErrBadRequest, p.cfg.MinTTL, p.cfg.MaxTTL)
	}
	return count, minCount, ttl, nil
}

// Reserve implements the spec's request flow.
func (p *Pool) Reserve(ctx context.Context, req ReserveRequest) (*Reservation, error) {
	start := time.Now()
	defer func() { reserveLatency.WithLabelValues(p.cfg.Name).Observe(time.Since(start).Seconds()) }()
	count, minCount, ttl, err := p.validate(req)
	if err != nil {
		reserveRequests.WithLabelValues(p.cfg.Name, "bad_request").Inc()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, p.icfg.ReserveTimeout)
	defer cancel()

	token := store.NewToken()
	expiresAt := p.clock.Now().Add(ttl)
	need := count
	var applied []store.ReserveOp
	byOutpoint := make(map[store.Outpoint]store.Row, count)
	var flowErr error
	triedAdopt := false

	for round := 0; round < 3 && need > 0; round++ {
		rows := p.alloc.Pop(need)
		if len(rows) < need && !triedAdopt {
			// One bounded adoption attempt per request, whether the FIFO was
			// empty or merely short of need: a partially filled request must
			// still get a chance to top up from another owned or newly
			// adopted shard before falling back to min_count/insufficient.
			triedAdopt = true
			if p.mgr.EnsureRows(ctx) {
				rows = append(rows, p.alloc.Pop(need-len(rows))...)
			}
		}
		if len(rows) == 0 {
			break
		}
		for _, r := range rows {
			byOutpoint[r.Outpoint] = r
		}
		ops := make([]store.ReserveOp, len(rows))
		for i, r := range rows {
			ops[i] = store.ReserveOp{Outpoint: r.Outpoint, Token: token, ExpiresAt: expiresAt}
		}
		res := p.coal.Submit(ctx, ops)
		if res.Unflushed {
			p.alloc.Return(rows)
			flowErr = res.Err
			break
		}
		p.alloc.Forget(opsOf(rows))
		if res.Err != nil {
			flowErr = res.Err
			break
		}
		applied = append(applied, res.Applied...)
		need -= len(res.Applied)
		if len(res.Missed) > 0 {
			casMissed.WithLabelValues(p.cfg.Name).Add(float64(len(res.Missed)))
			seen := map[chainhash.Hash]bool{}
			for _, op := range res.Missed {
				if !seen[op.TxID] {
					seen[op.TxID] = true
					p.mgr.NoteCASMiss(op.TxID)
				}
			}
		}
	}
	allocatorAvailable.WithLabelValues(p.cfg.Name).Set(float64(p.alloc.Available()))
	shardsOwned.WithLabelValues(p.cfg.Name).Set(float64(len(p.alloc.Shards())))
	if p.alloc.Available() < p.icfg.LowWaterRows {
		p.mgr.Hungry()
	}

	if len(applied) < minCount {
		if len(applied) > 0 {
			p.releaseBestEffort(token, applied)
		}
		switch {
		case flowErr != nil:
			reserveRequests.WithLabelValues(p.cfg.Name, "error").Inc()
			return nil, mapFlowErr(flowErr)
		case len(applied) == 0:
			reserveRequests.WithLabelValues(p.cfg.Name, "exhausted").Inc()
			return nil, ErrExhausted
		default:
			reserveRequests.WithLabelValues(p.cfg.Name, "insufficient").Inc()
			return nil, ErrInsufficient
		}
	}

	if err := ctx.Err(); err != nil {
		// Enough rows were gathered, but the client is gone (or our own
		// ReserveTimeout fired) between the last round and here: don't hand a
		// reservation to nobody, release it back instead.
		p.releaseBestEffort(token, applied)
		reserveRequests.WithLabelValues(p.cfg.Name, "canceled").Inc()
		return nil, err
	}

	out := &Reservation{Token: token, ExpiresAt: expiresAt, TTL: ttl, LockMode: p.cfg.LockMode, Outputs: make([]Output, 0, len(applied))}
	shardSeen := map[chainhash.Hash]bool{}
	versions := map[chainhash.Hash]int64{}
	for _, sh := range p.alloc.Shards() {
		versions[sh.TxID] = sh.Version
	}
	for _, op := range applied {
		r := byOutpoint[op.Outpoint]
		out.Outputs = append(out.Outputs, Output{Outpoint: r.Outpoint, Satoshis: r.Satoshis, LockingScript: r.LockingScript, UnlockingScript: r.UnlockingScript})
		if !shardSeen[op.TxID] {
			shardSeen[op.TxID] = true
			// 0 ("unknown") when the shard is no longer owned; see ShardRef.
			out.Shards = append(out.Shards, ShardRef{TxID: op.TxID, Version: versions[op.TxID]})
		}
	}
	reserveRequests.WithLabelValues(p.cfg.Name, "ok").Inc()
	reserveOutputs.WithLabelValues(p.cfg.Name).Add(float64(len(out.Outputs)))
	return out, nil
}

func mapFlowErr(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		// The client went away; that is not a store outage.
		return err
	case errors.Is(err, ErrBackpressure), errors.Is(err, ErrUnavailable), errors.Is(err, ErrBadRequest):
		return err
	default:
		// Includes context.DeadlineExceeded: our own timeout firing looks like
		// unavailability to the caller.
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
}

func opsOf(rows []store.Row) []store.Outpoint {
	ops := make([]store.Outpoint, len(rows))
	for i, r := range rows {
		ops[i] = r.Outpoint
	}
	return ops
}

func (p *Pool) releaseBestEffort(token store.Token, applied []store.ReserveOp) {
	ops := make([]store.Outpoint, len(applied))
	for i, a := range applied {
		ops[i] = a.Outpoint
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := p.st.Release(ctx, p.cfg.Name, token, ops); err != nil {
		p.log.Warn("release partial reservation", "err", err)
		return
	}
	p.noteReleased(ctx, ops)
}

// noteReleased re-adds released rows of owned shards to the allocator.
func (p *Pool) noteReleased(ctx context.Context, ops []store.Outpoint) {
	var owned []store.Outpoint
	for _, op := range ops {
		if p.alloc.Has(op.TxID) {
			owned = append(owned, op)
		}
	}
	if len(owned) == 0 {
		return
	}
	rows, err := p.st.GetRows(ctx, p.cfg.Name, owned)
	if err != nil {
		return
	}
	avail := rows[:0]
	for _, r := range rows {
		if r.State == store.StateAvailable {
			avail = append(avail, r)
		}
	}
	p.mgr.NoteReleased(avail)
}

// Release returns reserved rows to the pool (trust) or parks them for the
// reaper to verify (verify). Returns how many rows changed.
func (p *Pool) Release(ctx context.Context, token store.Token, ops []store.Outpoint) (int, error) {
	if token.IsZero() || len(ops) == 0 || len(ops) > p.cfg.MaxCount {
		releaseTotal.WithLabelValues(p.cfg.Name, "bad_request").Inc()
		return 0, fmt.Errorf("%w: token and 1..%d outpoints required", ErrBadRequest, p.cfg.MaxCount)
	}
	var n int
	var err error
	if p.cfg.ReleasePolicy == ReleaseVerify {
		n, err = p.st.Spend(ctx, p.cfg.Name, token, ops, chainhash.Hash{}, false) // a zero txid records no hint: SpentHint stays nil
	} else {
		n, err = p.st.Release(ctx, p.cfg.Name, token, ops)
	}
	if err != nil {
		releaseTotal.WithLabelValues(p.cfg.Name, "error").Inc()
		return 0, mapStoreErr(err)
	}
	releaseTotal.WithLabelValues(p.cfg.Name, "ok").Inc()
	if n > 0 && p.cfg.ReleasePolicy == ReleaseTrust {
		p.noteReleased(ctx, ops)
	}
	return n, nil
}

// Spend records that the client spent rows in txid: a hint that quarantines
// them (verify) or a fact that spends them (trust).
func (p *Pool) Spend(ctx context.Context, token store.Token, ops []store.Outpoint, txid chainhash.Hash) (int, error) {
	if token.IsZero() || len(ops) == 0 || len(ops) > p.cfg.MaxCount || txid == (chainhash.Hash{}) {
		spendTotal.WithLabelValues(p.cfg.Name, "bad_request").Inc()
		return 0, fmt.Errorf("%w: token, txid and 1..%d outpoints required", ErrBadRequest, p.cfg.MaxCount)
	}
	n, err := p.st.Spend(ctx, p.cfg.Name, token, ops, txid, p.cfg.SpendPolicy == SpendTrust)
	if err != nil {
		spendTotal.WithLabelValues(p.cfg.Name, "error").Inc()
		return 0, mapStoreErr(err)
	}
	spendTotal.WithLabelValues(p.cfg.Name, "ok").Inc()
	return n, nil
}

// Stats reads pool counts from the store.
func (p *Pool) Stats(ctx context.Context) (store.Stats, error) {
	st, err := p.st.Stats(ctx, p.cfg.Name)
	if err != nil {
		return st, mapStoreErr(err)
	}
	return st, nil
}

// BEEF returns a shard's BEEF, version and status.
func (p *Pool) BEEF(ctx context.Context, txid chainhash.Hash) ([]byte, int64, store.ShardStatus, error) {
	sh, err := p.st.GetShard(ctx, p.cfg.Name, txid)
	if err != nil {
		return nil, 0, 0, mapStoreErr(err)
	}
	return sh.BEEF, sh.Version, sh.Status, nil
}

func mapStoreErr(err error) error {
	if errors.Is(err, store.ErrInvalidArgument) {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}
	if errors.Is(err, store.ErrUnavailable) {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return err
}
