package pool_test

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// chunkedRowsByOutpoint pages through storetest.RowsByOutpoint since a single
// GetRows call is capped at the backend's Caps.MaxBatch (1000 for memory.Store).
func chunkedRowsByOutpoint(t *testing.T, st store.Store, pool string, ops []store.Outpoint, chunk int) map[store.Outpoint]store.Row {
	t.Helper()
	out := make(map[store.Outpoint]store.Row, len(ops))
	for len(ops) > 0 {
		n := chunk
		if n > len(ops) {
			n = len(ops)
		}
		for op, row := range storetest.RowsByOutpoint(t, st, pool, ops[:n]) {
			out[op] = row
		}
		ops = ops[n:]
	}
	return out
}

// rowFreeList is a threadsafe pool of rows not currently checked out by any
// goroutine. take blocks (via polling) until enough rows are available, so
// concurrent chaos-test goroutines never draw the same outpoint at once.
type rowFreeList struct {
	mu   sync.Mutex
	rows []store.Row
}

func newRowFreeList(rows []store.Row) *rowFreeList {
	return &rowFreeList{rows: append([]store.Row(nil), rows...)}
}

// take polls for n rows, returning ok=false after a 20s deadline. It
// deliberately does not call any *testing.T failure method itself: Fatal/
// FailNow must only be called from the goroutine running the test function,
// and take is called from worker goroutines, so the caller reports failure
// (via t.Errorf, which is goroutine-safe) and stops itself.
func (f *rowFreeList) take(n int) ([]store.Row, bool) {
	deadline := time.Now().Add(20 * time.Second)
	for {
		f.mu.Lock()
		if len(f.rows) >= n {
			out := append([]store.Row(nil), f.rows[:n]...)
			f.rows = f.rows[n:]
			f.mu.Unlock()
			return out, true
		}
		f.mu.Unlock()
		if time.Now().After(deadline) {
			return nil, false
		}
		time.Sleep(time.Millisecond)
	}
}

func (f *rowFreeList) put(rows []store.Row) {
	if len(rows) == 0 {
		return
	}
	f.mu.Lock()
	f.rows = append(f.rows, rows...)
	f.mu.Unlock()
}

// TestCoalescerChaos hammers a Coalescer with concurrent Submits over a
// Faulty store armed with a steady drip of in-doubt and partial-apply faults,
// plus client-side timeouts, and checks that the coalescer's invariants hold
// no matter how the faults and cancellations land: a successful Result's
// Applied rows are always reserved under the caller's own token, a failed or
// unflushed Result never leaves a row behind reserved, a flush never carries
// a duplicate outpoint, and once everything settles every row is available
// again.
func TestCoalescerChaos(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	var allRows []store.Row
	for i := 0; i < 4; i++ {
		_, rows := storetest.MintShard(t, st, "main", 500, store.ShardMined)
		allRows = append(allRows, rows...)
	}
	require.Len(t, allRows, 2000)

	f := storetest.NewFaulty(st)
	f.InDoubtEvery(7)

	var dupFound atomic.Bool
	f.BeforeReserve = func(ops []store.ReserveOp) {
		seen := make(map[store.Outpoint]struct{}, len(ops))
		for _, op := range ops {
			if _, dup := seen[op.Outpoint]; dup {
				dupFound.Store(true)
				return
			}
			seen[op.Outpoint] = struct{}{}
		}
	}

	c := pool.NewCoalescer(f, "main", pool.CoalescerConfig{Slots: 3, MaxBatch: 32, Retries: 2, RetryBackoff: time.Millisecond})
	runCtx, stop := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = c.Run(runCtx) }()
	t.Cleanup(func() { stop(); <-runDone })

	fl := newRowFreeList(allRows)

	var parkedMu sync.Mutex
	var parked []store.Outpoint

	const goroutines = 32
	const submitsEach = 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for s := 0; s < submitsEach; s++ {
				n := 1 + rand.Intn(8)
				rows, ok := fl.take(n)
				if !ok {
					t.Errorf("goroutine %d submit %d: row free list starved waiting for %d rows", g, s, n)
					return
				}
				tok := store.NewToken()
				ops := storetest.ReserveOps(rows, tok, clk.Now().Add(time.Hour))

				if rand.Intn(100) < 2 {
					f.FailAfterApplying(rand.Intn(4)) // 0..3
				}

				ctx := context.Background()
				var cancel context.CancelFunc
				if rand.Intn(100) < 10 {
					ctx, cancel = context.WithTimeout(ctx, time.Duration(1+rand.Intn(5))*time.Millisecond)
				}

				res := c.Submit(ctx, ops)
				if cancel != nil {
					cancel()
				}

				switch {
				case res.Err == nil:
					if !assert.Len(t, res.Applied, len(rows), "goroutine %d submit %d: a clean flush must apply every uniquely-owned op", g, s) {
						fl.put(rows)
						continue
					}
					assert.Empty(t, res.Missed, "goroutine %d submit %d", g, s)
					got := storetest.RowsByOutpoint(t, st, "main", storetest.Ops(rows))
					for _, op := range res.Applied {
						assert.Equal(t, tok, got[op.Outpoint].ReservedBy, "goroutine %d submit %d", g, s)
					}
					_, err := st.Release(context.Background(), "main", tok, storetest.Ops(rows))
					assert.NoError(t, err, "goroutine %d submit %d", g, s)
					fl.put(rows)
				case errors.Is(res.Err, pool.ErrUnavailable) || res.Unflushed:
					got := storetest.RowsByOutpoint(t, st, "main", storetest.Ops(rows))
					for _, r := range rows {
						assert.Equal(t, store.StateAvailable, got[r.Outpoint].State, "goroutine %d submit %d: ErrUnavailable/Unflushed must leave rows available", g, s)
					}
					fl.put(rows)
				default:
					// A ctx error after the request reached the store: the coalescer's
					// own compensation may still be settling. Park the rows and
					// reconcile once the coalescer is fully idle (below).
					parkedMu.Lock()
					parked = append(parked, storetest.Ops(rows)...)
					parkedMu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()

	require.Eventually(t, func() bool {
		return c.InflightOps() == 0 && c.PendingOps() == 0
	}, 10*time.Second, 5*time.Millisecond, "coalescer drains to idle")

	got := chunkedRowsByOutpoint(t, st, "main", storetest.Ops(allRows), 1000)
	for _, r := range allRows {
		assert.Equal(t, store.StateAvailable, got[r.Outpoint].State, "row %s must end up available", r.Outpoint)
	}
	assert.False(t, dupFound.Load(), "no flush should ever carry a duplicate outpoint")
	t.Logf("chaos: %d goroutines x %d submits, %d rows parked pending reconciliation, all %d rows available", goroutines, submitsEach, len(parked), len(allRows))
}

// TestCoalescerInflightReturnsToZero submits many single-row requests under
// random short timeouts and in-doubt faults, tears the coalescer down
// part-way through, and checks that InflightOps and PendingOps both settle
// back to zero and that every row ends up available once the caller releases
// whatever it was handed.
func TestCoalescerInflightReturnsToZero(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	const n = 400
	_, rows := storetest.MintShard(t, st, "main", n, store.ShardMined)
	f := storetest.NewFaulty(st)
	f.InDoubtEvery(3)

	c := pool.NewCoalescer(f, "main", pool.CoalescerConfig{
		Slots: 2, MaxBatch: 8, MaxInflightOps: 24, Retries: 1, Window: 50 * time.Microsecond,
	})
	runCtx, stop := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = c.Run(runCtx) }()

	type outcome struct {
		res pool.Result
		tok store.Token
	}
	results := make([]outcome, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tok := store.NewToken()
			ops := storetest.ReserveOps(rows[i:i+1], tok, clk.Now().Add(time.Hour))
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1+rand.Intn(5))*time.Millisecond)
			defer cancel()
			results[i] = outcome{res: c.Submit(ctx, ops), tok: tok}
		}(i)
	}

	time.Sleep(2 * time.Millisecond) // let some submits land before shutdown
	stop()
	<-runDone
	wg.Wait()

	require.Eventually(t, func() bool {
		return c.InflightOps() == 0 && c.PendingOps() == 0
	}, 10*time.Second, 5*time.Millisecond, "inflight and pending settle to zero")

	var released int
	for _, o := range results {
		if o.res.Err == nil {
			for _, op := range o.res.Applied {
				_, err := st.Release(context.Background(), "main", o.tok, []store.Outpoint{op.Outpoint})
				require.NoError(t, err)
				released++
			}
		}
	}

	got := chunkedRowsByOutpoint(t, st, "main", storetest.Ops(rows), 1000)
	for _, r := range rows {
		assert.Equal(t, store.StateAvailable, got[r.Outpoint].State, "row %s must end up available", r.Outpoint)
	}
	t.Logf("inflight-to-zero: %d/%d submits applied and released, all %d rows available", released, n, n)
}

// TestCoalescerSubmitRacingShutdownNeverStrands repeatedly races a batch of
// concurrent Submits against Run's shutdown and checks that every Submit call
// returns promptly regardless of which side of the race it lands on: neither
// a queued request nor one caught mid-flush should ever be left hanging past
// shutdown.
func TestCoalescerSubmitRacingShutdownNeverStrands(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	const iterations = 500
	const perIteration = 16
	_, rows := storetest.MintShard(t, st, "main", iterations*perIteration, store.ShardMined)

	for i := 0; i < iterations; i++ {
		c := pool.NewCoalescer(st, "main", pool.CoalescerConfig{Slots: 2, MaxBatch: 8})
		runCtx, stop := context.WithCancel(context.Background())
		runDone := make(chan struct{})
		go func() { defer close(runDone); _ = c.Run(runCtx) }()

		var wg sync.WaitGroup
		done := make(chan struct{})
		for j := 0; j < perIteration; j++ {
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				row := rows[i*perIteration+j : i*perIteration+j+1]
				_ = c.Submit(context.Background(), storetest.ReserveOps(row, store.NewToken(), clk.Now().Add(time.Hour)))
			}(j)
		}
		go func() { wg.Wait(); close(done) }()

		stop()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: submits did not return within 2s of shutdown", i)
		}
		<-runDone
	}
}

// foreignTokenStore wraps a store.Store and simulates a buggy backend that
// reports an applied op under the wrong token, to prove the coalescer never
// trusts an applied outpoint whose reported token doesn't match the request
// that asked for it.
type foreignTokenStore struct {
	store.Store
}

func (s foreignTokenStore) Reserve(ctx context.Context, pool string, ops []store.ReserveOp) ([]store.ReserveOp, error) {
	applied, err := s.Store.Reserve(ctx, pool, ops)
	if err == nil && len(applied) > 0 {
		applied[0].Token = store.NewToken() // corrupt the reported token
	}
	return applied, err
}

func TestCoalescerIgnoresAppliedUnderForeignToken(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	_, rows := storetest.MintShard(t, st, "main", 3, store.ShardMined)
	wrapped := foreignTokenStore{Store: st}
	c := startCoalescer(t, wrapped, pool.CoalescerConfig{Slots: 1, MaxBatch: 64})

	tok := store.NewToken()
	res := c.Submit(context.Background(), storetest.ReserveOps(rows[:3], tok, clk.Now().Add(time.Minute)))
	require.NoError(t, res.Err)
	assert.Len(t, res.Applied, 2, "the op reported under a foreign token must not count as applied")
	assert.Len(t, res.Missed, 1)
	for _, a := range res.Applied {
		assert.Equal(t, tok, a.Token, "every Applied op must carry the request's own token")
	}
}
