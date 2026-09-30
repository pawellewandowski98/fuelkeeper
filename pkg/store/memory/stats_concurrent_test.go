package memory

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// liveRow is one concurrently-shared tracked row for
// TestStatsCountersConcurrent. Its own mutex keeps two workers from
// interleaving on this one row's reserve/token bookkeeping (e.g. one
// worker's Reserve succeeding while another reads a stale token and tries to
// Release with it), without serializing work on any *other* row: 16 workers
// still drive genuinely concurrent calls into the store, contending only on
// the store's own s.mu -- which is what this test is meant to stress.
type liveRow struct {
	mu    sync.Mutex
	op    store.Outpoint
	pool  string
	txid  chainhash.Hash // owning shard, for RetireShard
	token store.Token    // meaningful only while the row is actually reserved
}

// mintedShard is one shard TestStatsCountersConcurrent has created, tracked
// so workers can replay PutShard against it (same pool/txid/size/
// denomination/lock mode -- the idempotent case documented on PutShard,
// store.go) instead of only ever minting new ones, and can UpdateShard it.
type mintedShard struct {
	pool string
	txid chainhash.Hash
	size int
}

// liveRegistry is shared bookkeeping for TestStatsCountersConcurrent, guarded
// by its own mutex, distinct from the store's: adding a row/shard or picking
// one at random never itself calls into the store, so holding this lock
// never serializes workers against each other the way holding it across a
// Store call would.
type liveRegistry struct {
	mu     sync.Mutex
	rows   []*liveRow
	shards []mintedShard
}

func (r *liveRegistry) addRows(pool string, txid chainhash.Hash, rows []store.Row) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, row := range rows {
		r.rows = append(r.rows, &liveRow{op: row.Outpoint, pool: pool, txid: txid})
	}
}

func (r *liveRegistry) addShard(pool string, txid chainhash.Hash, size int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shards = append(r.shards, mintedShard{pool: pool, txid: txid, size: size})
}

func (r *liveRegistry) pickRow(rng *rand.Rand) *liveRow {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.rows) == 0 {
		return nil
	}
	return r.rows[rng.Intn(len(r.rows))]
}

func (r *liveRegistry) pickShard(rng *rand.Rand) (mintedShard, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.shards) == 0 {
		return mintedShard{}, false
	}
	return r.shards[rng.Intn(len(r.shards))], true
}

// TestStatsCountersConcurrent runs 16 goroutines hammering one shared store
// with PutShard (fresh mints and idempotent replays of an existing shard's
// txid), Reserve (including an immediate same-token replay right after a
// fresh reserve), Release, both Spend modes, Quarantine's expiry sweep, both
// Resolve destinations, RetireShard and UpdateShard(..., StateRetired, ...),
// while a monitor goroutine repeatedly samples Stats and fails the test the
// instant any row-state counter goes negative -- something an incremental
// counter can actually do on a bad transition, unlike a full scan. After
// every worker finishes, Stats is checked once more against scanStats (the
// full-scan ground truth) for each pool.
//
// This pins two bugs seen mid-edit in this package: commenting out
// PutShard's addCount call reproduces a negative Available gauge as soon as
// a newly minted row is reserved (moveCount decrements a counter the row was
// never added to), caught by the monitor within the run; commenting out
// RetireShard's moveCount call keeps every counter non-negative (a retired
// row just stays wrongly attributed to its pre-retire state forever) but is
// caught by the final Stats-vs-scanStats comparison.
func TestStatsCountersConcurrent(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	s := New(WithClock(clk.Now))
	pools := []string{"pool-a", "pool-b", "pool-c"}
	reg := &liveRegistry{}

	// Seed enough shards/rows that every worker has something to act on
	// from its very first iteration, instead of all 16 racing to mint first.
	for _, pool := range pools {
		for i := 0; i < 4; i++ {
			shard, rows := storetest.BuildShard(pool, 10, store.ShardUnmined)
			require.NoError(t, s.PutShard(ctx, shard, rows))
			reg.addRows(pool, shard.TxID, rows)
			reg.addShard(pool, shard.TxID, shard.Size)
		}
	}

	// Monitor: samples Stats for every pool in a tight loop until the
	// workers finish, failing (via t.Errorf -- safe to call from any
	// goroutine, unlike t.Fatal/FailNow) the instant it sees a negative
	// row-state counter.
	done := make(chan struct{})
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		for {
			select {
			case <-done:
				return
			default:
			}
			for _, pool := range pools {
				st, err := s.Stats(ctx, pool)
				if err != nil {
					t.Errorf("Stats(%q) during concurrent run: %v", pool, err)
					continue
				}
				if st.Available < 0 || st.Reserved < 0 || st.Quarantined < 0 || st.Spent < 0 || st.Invalid < 0 {
					t.Errorf("Stats(%q) went negative mid-run: %+v", pool, st)
					return
				}
			}
		}
	}()

	const workers = 16
	const perWorker = 400
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic-enough test fuzz, not a security use
			for i := 0; i < perWorker; i++ {
				concurrentStep(t, s, clk, reg, rng, pools)
			}
		}(time.Now().UnixNano() + int64(w))
	}
	wg.Wait()
	close(done)
	<-monitorDone

	for _, pool := range pools {
		want := s.scanStats(pool)
		got, err := s.Stats(ctx, pool)
		require.NoError(t, err)
		require.Equalf(t, want, got, "pool %q counters diverged from a full scan after the concurrent run", pool)
	}
}

// assertNoError records a failure and returns false if err != nil, via
// t.Errorf, without going through testify's assert or require. concurrentStep
// and applyConcurrentOp run on worker goroutines, never on the test's own
// goroutine: require.NoError calls t.FailNow, which testing.T documents safe
// only on the goroutine running the test itself (off that goroutine it stops
// just the worker via runtime.Goexit without ending the test, which
// golangci-lint's testifylint go-require check flags); testify's assert.NoError
// avoids that, but golangci-lint's testifylint require-error check then wants
// a plain, unguarded error check to use require instead -- the two checks
// contradict each other for this exact goroutine-side-assertion shape. t.Errorf
// is documented safe for concurrent use from any goroutine, so routing through
// it directly (instead of either testify entry point) satisfies both checks
// and the actual safety requirement.
func assertNoError(t *testing.T, err error) bool {
	t.Helper()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return false
	}
	return true
}

// concurrentStep performs one randomly chosen operation: minting a shard,
// replaying an existing shard's PutShard, retiring a shard via UpdateShard,
// or acting on a randomly picked live row according to its actual current
// state (fetched fresh via GetRows, never assumed).
func concurrentStep(t *testing.T, s *Store, clk *storetest.Clock, reg *liveRegistry, rng *rand.Rand, pools []string) {
	t.Helper()
	ctx := context.Background()
	switch rng.Intn(20) {
	case 0, 1, 2: // mint a new shard
		pool := pools[rng.Intn(len(pools))]
		n := 1 + rng.Intn(5)
		shard, rows := storetest.BuildShard(pool, n, store.ShardUnmined)
		if !assertNoError(t, s.PutShard(ctx, shard, rows)) {
			return
		}
		reg.addRows(pool, shard.TxID, rows)
		reg.addShard(pool, shard.TxID, shard.Size)
	case 3: // idempotent PutShard replay of an existing shard
		ms, ok := reg.pickShard(rng)
		if !ok {
			return
		}
		shard, rows := storetest.BuildShardWithTxID(ms.pool, ms.txid, ms.size, store.ShardUnmined)
		assertNoError(t, s.PutShard(ctx, shard, rows))
	case 4: // UpdateShard to retired
		ms, ok := reg.pickShard(rng)
		if !ok {
			return
		}
		err := s.UpdateShard(ctx, ms.pool, ms.txid, store.ShardRetired, nil)
		if err != nil && !errors.Is(err, store.ErrTerminalShard) {
			assertNoError(t, err)
		}
	default: // act on a live row per its actual current state
		lr := reg.pickRow(rng)
		if lr == nil {
			return
		}
		lr.mu.Lock()
		defer lr.mu.Unlock()
		got, err := s.GetRows(ctx, lr.pool, []store.Outpoint{lr.op})
		if !assertNoError(t, err) {
			return
		}
		if len(got) != 1 {
			return
		}
		applyConcurrentOp(t, s, clk, rng, lr, got[0].State)
	}
}

// applyConcurrentOp is TestStatsCountersConcurrent's per-state op table,
// structurally the same as applyRandomOp (stats_internal_test.go) but taking
// a *liveRow (whose caller already holds lr.mu) instead of a *trackedRow, and
// asserting rather than requiring (see concurrentStep's doc comment).
func applyConcurrentOp(t *testing.T, s *Store, clk *storetest.Clock, rng *rand.Rand, lr *liveRow, state store.State) {
	t.Helper()
	ctx := context.Background()
	switch state {
	case store.StateAvailable:
		switch rng.Intn(3) {
		case 0, 1:
			tok := store.NewToken()
			applied, err := s.Reserve(ctx, lr.pool, []store.ReserveOp{
				{Outpoint: lr.op, Token: tok, ExpiresAt: clk.Now().Add(time.Minute)},
			})
			if !assertNoError(t, err) || len(applied) != 1 {
				return
			}
			lr.token = tok
			if rng.Intn(2) == 0 {
				// Same-token replay (Reserve's replay branch, rows.go): it
				// normally reapplies without moving any counter, but a
				// concurrent pool-wide Quarantine sweep or a RetireShard from
				// another worker sharing this row's shard can legitimately
				// move the row out of reserved between the two calls here
				// (lr.mu only serializes *this* row's own bookkeeping, not
				// whole-shard/whole-pool operations another worker drives),
				// so only the error is asserted, not the applied count.
				_, err := s.Reserve(ctx, lr.pool, []store.ReserveOp{
					{Outpoint: lr.op, Token: tok, ExpiresAt: clk.Now().Add(time.Minute)},
				})
				assertNoError(t, err)
			}
		default:
			_, err := s.RetireShard(ctx, lr.pool, lr.txid)
			assertNoError(t, err)
		}
	case store.StateReserved:
		switch rng.Intn(5) {
		case 0:
			_, err := s.Release(ctx, lr.pool, lr.token, []store.Outpoint{lr.op})
			assertNoError(t, err)
		case 1:
			_, err := s.Spend(ctx, lr.pool, lr.token, []store.Outpoint{lr.op}, storetest.NewTxID(), false)
			assertNoError(t, err)
		case 2:
			_, err := s.Spend(ctx, lr.pool, lr.token, []store.Outpoint{lr.op}, storetest.NewTxID(), true)
			assertNoError(t, err)
		case 3:
			_, err := s.RetireShard(ctx, lr.pool, lr.txid)
			assertNoError(t, err)
		default:
			// Expire it and let Quarantine's batch sweep pick it up, rather
			// than only exercising Spend's direct reserved->quarantined path.
			clk.Advance(2 * time.Minute)
			_, err := s.Quarantine(ctx, lr.pool, clk.Now(), 10000)
			assertNoError(t, err)
		}
	case store.StateQuarantined:
		switch rng.Intn(3) {
		case 0:
			_, err := s.Resolve(ctx, lr.pool, []store.Outpoint{lr.op}, store.StateAvailable, nil)
			assertNoError(t, err)
		case 1:
			h := storetest.NewTxID()
			_, err := s.Resolve(ctx, lr.pool, []store.Outpoint{lr.op}, store.StateSpent, &h)
			assertNoError(t, err)
		default:
			_, err := s.RetireShard(ctx, lr.pool, lr.txid)
			assertNoError(t, err)
		}
	case store.StateSpent, store.StateInvalid:
		// Terminal; RetireShard on an already-terminal row is a documented
		// no-op, still worth occasionally exercising under contention.
		if rng.Intn(8) == 0 {
			_, err := s.RetireShard(ctx, lr.pool, lr.txid)
			assertNoError(t, err)
		}
	}
}
