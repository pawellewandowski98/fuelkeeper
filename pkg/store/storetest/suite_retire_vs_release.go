package storetest

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

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// testRetireVsReleaseRace proves that RetireShard cannot miss a row whose
// state changes while the cascade runs. The bug this generalises (see
// pkg/store/aerospike's own TestRetireVsReleaseRace, the backend-specific
// case that motivated this): a backend that discovers non-terminal rows
// through several separate queries -- one for available, one for reserved,
// one for quarantined -- can lose a row that crosses between those states
// between the queries (reserved -> available via Release, or quarantined ->
// available via Resolve) and leave it live on a shard the caller was just
// told is now invalid.
//
// Concurrent workers churn rows through every non-terminal transition
// (reserved -> available, reserved -> quarantined -> available, and,
// separately, reserved -> quarantined via real expiry rather than Spend)
// while a single RetireShard call races them. No row is ever spent
// terminally, so every row must still be invalid, and RetireShard's own
// changed count must equal the full row count, once the race settles.
func testRetireVsReleaseRace(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	const pool = "retire-vs-release" // isolated so per-pool Stats reflects only this shard

	n := 200 * stress()
	if mb := s.Capabilities().MaxBatch; n > mb {
		n = mb
	}
	sh, rows := MintShard(t, s, pool, n, store.ShardMined)

	workers := 4 * stress()
	if workers > 16 {
		workers = 16
	}
	warmupTarget := int64(20 * stress())

	stop := make(chan struct{})
	// retryTolerant runs do until it succeeds, stop closes, or it returns an
	// error that is neither nil nor one of the documented retryable errors
	// (store.go, "Errors"): ErrUnavailable, context.Canceled or
	// context.DeadlineExceeded. Any other error fails the test immediately.
	// It reports whether do eventually succeeded.
	retryTolerant := func(do func() error) bool {
		for {
			err := do()
			if err == nil {
				return true
			}
			if errors.Is(err, store.ErrUnavailable) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				select {
				case <-stop:
					return false
				default:
					continue
				}
			}
			assert.NoError(t, err, "unexpected error racing RetireShard")
			return false
		}
	}

	var iters atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			<-start
			rng := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}

				k := 1 + rng.Intn(8)
				if k > len(rows) {
					k = len(rows)
				}
				batch := pick(rng, rows, k)
				tok := store.NewToken()
				branch := rng.Intn(5) // branch 0 is the minority (~20%) expiry path

				exp := clk.Now().Add(time.Hour)
				if branch == 0 {
					exp = clk.Now()
				}

				var applied []store.ReserveOp
				if !retryTolerant(func() error {
					a, err := s.Reserve(ctx, pool, ReserveOps(batch, tok, exp))
					applied = a
					return err
				}) {
					return
				}
				if len(applied) == 0 {
					iters.Add(1)
					continue
				}
				ops := make([]store.Outpoint, len(applied))
				for i, op := range applied {
					ops[i] = op.Outpoint
				}

				var ok bool
				switch {
				case branch == 0:
					// Clock.Advance is mutex-protected, so it is safe to call
					// concurrently: advance past these rows' expiry and sweep
					// them the way the reaper would -- a different discovery
					// path than Spend's, exercising reserved -> quarantined
					// without ever touching Spend.
					clk.Advance(time.Millisecond)
					ok = retryTolerant(func() error {
						_, err := s.Quarantine(ctx, pool, clk.Now(), len(ops))
						return err
					})
				case branch%2 == 1:
					ok = retryTolerant(func() error {
						_, err := s.Release(ctx, pool, tok, ops)
						return err
					})
				default:
					ok = retryTolerant(func() error {
						_, err := s.Spend(ctx, pool, tok, ops, NewTxID(), false)
						return err
					})
					if ok {
						ok = retryTolerant(func() error {
							_, err := s.Resolve(ctx, pool, ops, store.StateAvailable, nil)
							return err
						})
					}
				}
				if !ok {
					return
				}
				iters.Add(1)
			}
		}(int64(w) + 1)
	}
	close(start)

	for iters.Load() < warmupTarget {
		time.Sleep(100 * time.Microsecond)
	}
	changed, err := s.RetireShard(ctx, pool, sh.TxID)
	require.NoError(t, err)
	close(stop)
	wg.Wait()

	assert.Equal(t, n, changed, "no row is ever spent in this test, so the single RetireShard call must move every non-terminal row to invalid exactly once")

	got := RowsByOutpoint(t, s, pool, Ops(rows))
	require.Len(t, got, n)
	for _, r := range got {
		assert.Equal(t, store.StateInvalid, r.State, "row %s must be invalid after the shard is retired", r.Outpoint)
	}

	avail, err := s.ListAvailable(ctx, pool, sh.TxID, 0, n+10)
	require.NoError(t, err)
	assert.Empty(t, avail)

	lq, err := s.ListQuarantined(ctx, pool, clk.Now().Add(24*time.Hour), nil, n+10)
	require.NoError(t, err)
	assert.Empty(t, lq)

	st, err := s.Stats(ctx, pool)
	require.NoError(t, err)
	assert.Zero(t, st.Available)
	assert.Zero(t, st.Reserved)
	assert.Zero(t, st.Quarantined)
	assert.Zero(t, st.Spent)
	assert.Equal(t, int64(n), st.Invalid)
	assert.Equal(t, int64(n), st.Available+st.Reserved+st.Quarantined+st.Spent+st.Invalid,
		"G2: rows are conserved across every transition")

	freshApplied, err := s.Reserve(ctx, pool, ReserveOps(rows[:1], store.NewToken(), clk.Now().Add(time.Hour)))
	require.NoError(t, err)
	assert.Empty(t, freshApplied, "an invalid row must never be reservable")

	again, err := s.RetireShard(ctx, pool, sh.TxID)
	require.NoError(t, err)
	assert.Zero(t, again, "the shard is already invalid and every row is now terminal (invalid), so a second RetireShard returns 0")

	gotShard, err := s.GetShard(ctx, pool, sh.TxID)
	require.NoError(t, err)
	assert.Empty(t, gotShard.Owner)
	assert.Equal(t, store.ShardInvalid, gotShard.Status)
}
