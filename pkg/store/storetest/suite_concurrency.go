package storetest

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// stress returns the FUELKEEPER_STRESS multiplier (default 1).
func stress() int {
	if v, err := strconv.Atoi(os.Getenv("FUELKEEPER_STRESS")); err == nil && v > 0 {
		return v
	}
	return 1
}

func pick(rng *rand.Rand, rows []store.Row, n int) []store.Row {
	perm := rng.Perm(len(rows))
	out := make([]store.Row, n)
	for i := range out {
		out[i] = rows[perm[i]]
	}
	return out
}

// rowsByOutpointChunked is RowsByOutpoint for a row count that may exceed the
// store's MaxBatch under FUELKEEPER_STRESS.
func rowsByOutpointChunked(t *testing.T, s store.Store, pool string, ops []store.Outpoint) map[store.Outpoint]store.Row {
	t.Helper()
	maxBatch := s.Capabilities().MaxBatch
	out := make(map[store.Outpoint]store.Row, len(ops))
	for i := 0; i < len(ops); i += maxBatch {
		end := i + maxBatch
		if end > len(ops) {
			end = len(ops)
		}
		for op, r := range RowsByOutpoint(t, s, pool, ops[i:end]) {
			out[op] = r
		}
	}
	return out
}

// testReserveExclusivityConcurrent: W workers with distinct tokens reserve random
// overlapping subsets, drawn across many shards, and never release. The applied
// sets must partition the reserved rows exactly (G1), and the store must agree
// with the workers' view.
func testReserveExclusivityConcurrent(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	shardCount := 2 * stress()
	const rowsPerShard = 100
	var rows []store.Row
	for i := 0; i < shardCount; i++ {
		_, rs := MintShard(t, s, "main", rowsPerShard, store.ShardMined)
		rows = append(rows, rs...)
	}
	const workers = 8
	iters := 30 * stress()
	exp := clk.Now().Add(time.Hour)

	var mu sync.Mutex
	ownerOf := map[store.Outpoint]store.Token{}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			<-start
			rng := rand.New(rand.NewSource(seed))
			tok := store.NewToken()
			for i := 0; i < iters; i++ {
				batch := pick(rng, rows, 10) // rows spans every shard, so batches routinely span shards too
				applied, err := s.Reserve(ctx, "main", ReserveOps(batch, tok, exp))
				if !assert.NoError(t, err) {
					return
				}
				mu.Lock()
				for _, op := range applied {
					if prev, ok := ownerOf[op.Outpoint]; ok {
						assert.Equal(t, tok, prev, "outpoint %s applied to two tokens", op.Outpoint)
					}
					ownerOf[op.Outpoint] = tok
				}
				mu.Unlock()
			}
		}(int64(w) + 1)
	}
	close(start)
	wg.Wait()

	got := rowsByOutpointChunked(t, s, "main", Ops(rows))
	reserved := 0
	for op, r := range got {
		switch r.State {
		case store.StateReserved:
			reserved++
			assert.Equal(t, ownerOf[op], r.ReservedBy, "store and workers disagree on %s", op)
		case store.StateAvailable:
			_, claimed := ownerOf[op]
			assert.False(t, claimed, "%s reported applied but is available", op)
		default:
			t.Errorf("unexpected state %s for %s", r.State, op)
		}
	}
	assert.Len(t, ownerOf, reserved)
	assert.Positive(t, reserved)
}

// testReserveReleaseCycleConcurrent: workers loop reserve→release. A second
// reserve of a row may only ever follow an acknowledged release of the previous
// holder. Workers clear their claim BEFORE calling Release so the check cannot
// race the store; a GetRows read-back while the claim is still held widens the
// window and checks the store's own view directly, not just the worker's map.
func testReserveReleaseCycleConcurrent(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	_, rows := MintShard(t, s, "main", 50, store.ShardMined)
	const workers = 8
	iters := 100 * stress()
	exp := clk.Now().Add(time.Hour)

	var mu sync.Mutex
	holder := map[store.Outpoint]store.Token{}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			<-start
			rng := rand.New(rand.NewSource(seed))
			tok := store.NewToken()
			for i := 0; i < iters; i++ {
				batch := pick(rng, rows, 5)
				applied, err := s.Reserve(ctx, "main", ReserveOps(batch, tok, exp))
				if !assert.NoError(t, err) {
					return
				}
				if len(applied) == 0 {
					continue
				}
				ops := make([]store.Outpoint, len(applied))
				mu.Lock()
				for j, op := range applied {
					ops[j] = op.Outpoint
					prev, held := holder[op.Outpoint]
					assert.False(t, held, "%s applied to %s while still held by %s", op.Outpoint, tok, prev)
					holder[op.Outpoint] = tok
				}
				mu.Unlock()

				got, err := s.GetRows(ctx, "main", ops)
				if assert.NoError(t, err) {
					for _, r := range got {
						assert.Equal(t, store.StateReserved, r.State, "%s must read back reserved while held", r.Outpoint)
						assert.Equal(t, tok, r.ReservedBy, "%s must read back this worker's token while held", r.Outpoint)
					}
				}

				mu.Lock()
				for _, op := range ops {
					delete(holder, op)
				}
				mu.Unlock()
				n, err := s.Release(ctx, "main", tok, ops)
				if !assert.NoError(t, err) {
					return
				}
				assert.Equal(t, len(ops), n, "every applied row must release")
			}
		}(int64(w) + 100)
	}
	close(start)
	wg.Wait()

	st, err := s.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, int64(50), st.Available)
	assert.Equal(t, int64(0), st.Reserved)
}

// testAcquireShardExclusiveConcurrent: N goroutines adopt and release shards in
// a loop; at every instant each shard has at most one holder.
func testAcquireShardExclusiveConcurrent(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	const shards, workers = 4, 8
	iters := 50 * stress()
	ids := make([]chainhash.Hash, shards)
	for i := range ids {
		sh, _ := MintShard(t, s, "main", 1, store.ShardMined)
		ids[i] = sh.TxID
	}
	mined := []store.ShardStatus{store.ShardMined}

	var mu sync.Mutex
	owned := map[chainhash.Hash]string{}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(holder string) {
			defer wg.Done()
			<-start
			for i := 0; i < iters; i++ {
				now := clk.Now()
				sh, err := s.AcquireShard(ctx, "main", holder, now, now.Add(time.Hour), mined, nil)
				if !assert.NoError(t, err) {
					return
				}
				if sh == nil {
					continue
				}
				mu.Lock()
				prev, taken := owned[sh.TxID]
				assert.False(t, taken, "shard %s acquired by %s while owned by %s", sh.TxID, holder, prev)
				owned[sh.TxID] = holder
				mu.Unlock()

				head, err := s.RenewShard(ctx, "main", sh.TxID, holder, now.Add(2*time.Hour))
				if assert.NoError(t, err) {
					assert.Equal(t, store.ShardMined, head.Status)
				}

				mu.Lock()
				delete(owned, sh.TxID)
				mu.Unlock()
				assert.NoError(t, s.ReleaseShard(ctx, "main", sh.TxID, holder))
			}
		}("h" + strconv.Itoa(w))
	}
	close(start)
	wg.Wait()

	// Everything must be adoptable again, and each acquisition distinct.
	seen := map[chainhash.Hash]bool{}
	for i := 0; i < shards; i++ {
		sh, err := s.AcquireShard(ctx, "main", "final", clk.Now(), clk.Now().Add(time.Hour), mined, nil)
		require.NoError(t, err)
		require.NotNil(t, sh)
		assert.False(t, seen[sh.TxID])
		seen[sh.TxID] = true
	}
	sh, err := s.AcquireShard(ctx, "main", "final2", clk.Now(), clk.Now().Add(time.Hour), mined, nil)
	require.NoError(t, err)
	assert.Nil(t, sh)
}

// testReleaseVsQuarantineRace: rows whose reservation has expired are released,
// one row per call, by three concurrent client goroutines while the reaper
// quarantines them one row at a time. A start barrier and single-row batches on
// both sides maximise real overlap on the same row instead of each side quietly
// sweeping its own disjoint chunk. Each row ends in exactly one of the two
// states and the two counts add up to the row count.
func testReleaseVsQuarantineRace(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	rounds := 20 * stress()
	for round := 0; round < rounds; round++ {
		_, rows := MintShard(t, s, "main", 40, store.ShardMined)
		tok := store.NewToken()
		MustReserve(t, s, "main", rows, tok, clk.Now().Add(time.Second))
		clk.Advance(2 * time.Second)

		thirds := make([][]store.Row, 3)
		for i, r := range rows {
			thirds[i%3] = append(thirds[i%3], r)
		}

		var released atomic.Int64
		var quarantined int
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(4)
		for g := 0; g < 3; g++ {
			go func(part []store.Row) {
				defer wg.Done()
				<-start
				for _, r := range part {
					n, err := s.Release(ctx, "main", tok, []store.Outpoint{r.Outpoint})
					assert.NoError(t, err)
					released.Add(int64(n))
				}
			}(thirds[g])
		}
		go func() {
			defer wg.Done()
			<-start
			zeros := 0
			for {
				n, err := s.Quarantine(ctx, "main", clk.Now(), 1)
				assert.NoError(t, err)
				quarantined += n
				if n == 0 {
					zeros++
					if zeros >= 2 {
						return
					}
					continue
				}
				zeros = 0
			}
		}()
		close(start)
		wg.Wait()

		// the reaper may have stopped early if the client released everything first;
		// drain once more so the invariant check below is exact.
		n, err := s.Quarantine(ctx, "main", clk.Now(), 100)
		require.NoError(t, err)
		quarantined += n

		got := RowsByOutpoint(t, s, "main", Ops(rows))
		var a, q int
		for _, r := range got {
			switch r.State {
			case store.StateAvailable:
				a++
			case store.StateQuarantined:
				q++
			default:
				t.Fatalf("round %d: row %s in state %s", round, r.Outpoint, r.State)
			}
		}
		assert.Equal(t, int(released.Load()), a, "released count matches available rows")
		assert.Equal(t, quarantined, q, "quarantined count matches quarantined rows")
		assert.Equal(t, len(rows), a+q)
	}
}

// testSpendVsQuarantineRace: a client spends reserved rows (both the
// quarantine-producing hint path and the directly-terminal path), one row at a
// time, concurrently with the reaper's expiry sweep and a client releasing a
// random subset of the very same rows. The spending goroutine visits every row
// exactly once, so after it returns no row can still be reserved: whichever
// operation actually wins a given row's CAS determines its final state, and the
// per-op success counts must add up to the row count.
func testSpendVsQuarantineRace(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	rounds := 10 * stress()
	for round := 0; round < rounds; round++ {
		_, rows := MintShard(t, s, "main", 40, store.ShardMined)
		tok := store.NewToken()
		MustReserve(t, s, "main", rows, tok, clk.Now().Add(-time.Second)) // already expired

		half := len(rows) / 2
		hintRows, terminalRows := rows[:half], rows[half:]
		rng := rand.New(rand.NewSource(int64(round) + 1))
		var releaseSubset []store.Row
		for _, r := range rows {
			if rng.Intn(2) == 0 {
				releaseSubset = append(releaseSubset, r)
			}
		}

		txid := NewTxID()
		hintOK := map[store.Outpoint]bool{}
		terminalOK := map[store.Outpoint]bool{}
		releasedOK := map[store.Outpoint]bool{}
		var quarantinedByReaper int

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			<-start
			for _, r := range hintRows {
				n, err := s.Spend(ctx, "main", tok, []store.Outpoint{r.Outpoint}, txid, false)
				assert.NoError(t, err)
				if n == 1 {
					hintOK[r.Outpoint] = true
				}
			}
			for _, r := range terminalRows {
				n, err := s.Spend(ctx, "main", tok, []store.Outpoint{r.Outpoint}, txid, true)
				assert.NoError(t, err)
				if n == 1 {
					terminalOK[r.Outpoint] = true
				}
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			for {
				n, err := s.Quarantine(ctx, "main", clk.Now(), 1)
				assert.NoError(t, err)
				quarantinedByReaper += n
				if n == 0 {
					// The spend goroutine visits every row exactly once, so once it
					// finishes no row remains reserved: no drain-after-Wait is needed.
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			for _, r := range releaseSubset {
				n, err := s.Release(ctx, "main", tok, []store.Outpoint{r.Outpoint})
				assert.NoError(t, err)
				if n == 1 {
					releasedOK[r.Outpoint] = true
				}
			}
		}()
		close(start)
		wg.Wait()

		got := RowsByOutpoint(t, s, "main", Ops(rows))
		var avail, quar, spent int
		for _, r := range got {
			switch r.State {
			case store.StateAvailable:
				avail++
				assert.True(t, releasedOK[r.Outpoint], "round %d: %s available but its release was not recorded as successful", round, r.Outpoint)
			case store.StateQuarantined:
				quar++
				if hintOK[r.Outpoint] {
					if assert.NotNil(t, r.SpentHint, "round %d: %s quarantined via spend must carry SpentHint", round, r.Outpoint) {
						assert.Equal(t, txid, *r.SpentHint)
					}
				} else {
					assert.Nil(t, r.SpentHint, "round %d: %s quarantined by the reaper must not carry a spend hint", round, r.Outpoint)
				}
			case store.StateSpent:
				spent++
				assert.NotNil(t, r.SpentBy, "round %d: %s spent must carry SpentBy", round, r.Outpoint)
				assert.True(t, terminalOK[r.Outpoint], "round %d: %s spent but its terminal spend was not recorded as successful", round, r.Outpoint)
			default:
				t.Fatalf("round %d: row %s in unexpected state %s", round, r.Outpoint, r.State)
			}
		}
		assert.Equal(t, len(rows), avail+quar+spent)
		assert.Equal(t, len(rows), len(hintOK)+len(terminalOK)+len(releasedOK)+quarantinedByReaper,
			"round %d: per-op success counts must add up to the row count", round)
	}
}

// testLeaserExclusiveConcurrent: 8 holders race TryAcquireOrRenew on one lease
// name while the clock stands still, so only the current holder may ever see a
// non-zero expiry; a different holder doing so is a mutual-exclusion violation.
// The clock is then advanced past the ttl and every holder races exactly once
// more: the contract allows only one winner of that takeover.
func testLeaserExclusiveConcurrent(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	const name = "jobs/x"
	const holders = 8
	iters := 50 * stress()
	ttl := 10 * time.Second

	var mu sync.Mutex
	var currentHolder string

	start := make(chan struct{})
	var wg sync.WaitGroup
	for h := 0; h < holders; h++ {
		wg.Add(1)
		go func(holder string) {
			defer wg.Done()
			<-start
			for i := 0; i < iters; i++ {
				until, err := s.TryAcquireOrRenew(ctx, name, holder, ttl)
				if !assert.NoError(t, err) {
					return
				}
				if until.IsZero() {
					continue
				}
				mu.Lock()
				if currentHolder != "" && currentHolder != holder {
					assert.Failf(t, "leaser exclusivity violated",
						"%s obtained a non-zero grant while the clock stood still and %s's grant should still be unexpired", holder, currentHolder)
				}
				currentHolder = holder
				mu.Unlock()
			}
		}("h" + strconv.Itoa(h))
	}
	close(start)
	wg.Wait()
	require.NotEmpty(t, currentHolder, "one holder must have acquired the lease")

	// Advance the clock past the ttl so the lease is up for a takeover, then have
	// every holder race exactly once: the contract allows only one winner.
	clk.Advance(ttl + time.Second)

	var wins atomic.Int64
	start2 := make(chan struct{})
	var wg2 sync.WaitGroup
	for h := 0; h < holders; h++ {
		wg2.Add(1)
		go func(holder string) {
			defer wg2.Done()
			<-start2
			until, err := s.TryAcquireOrRenew(ctx, name, holder, ttl)
			assert.NoError(t, err)
			if !until.IsZero() {
				wins.Add(1)
			}
		}("h" + strconv.Itoa(h))
	}
	close(start2)
	wg2.Wait()
	assert.Equal(t, int64(1), wins.Load(), "exactly one holder must win the lease takeover")
}

// testLeaseTakeoverRace: a shard is owned by "old" whose lease has since
// expired. Four new holders race AcquireShard while "old" races RenewShard.
// Per the contract "old" may legitimately win by renewing before anyone else
// takes the shard, since only a successful AcquireShard by someone else (or
// ReleaseShard) clears its ownership — but exactly one of the five racers must
// end up owning the shard.
func testLeaseTakeoverRace(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	sh, _ := MintShard(t, s, "main", 1, store.ShardMined)
	now := clk.Now()
	_, err := s.AcquireShard(ctx, "main", "old", now, now.Add(time.Hour), []store.ShardStatus{store.ShardMined}, nil)
	require.NoError(t, err)
	clk.Advance(2 * time.Hour) // past the lease

	const racers = 4
	mined := []store.ShardStatus{store.ShardMined}
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(racers + 1)
	for i := 0; i < racers; i++ {
		go func(holder string) {
			defer wg.Done()
			<-start
			now := clk.Now()
			_, err := s.AcquireShard(ctx, "main", holder, now, now.Add(time.Hour), mined, nil)
			assert.NoError(t, err)
		}("new" + strconv.Itoa(i))
	}
	go func() {
		defer wg.Done()
		<-start
		_, err := s.RenewShard(ctx, "main", sh.TxID, "old", clk.Now().Add(time.Hour))
		if err != nil {
			assert.ErrorIs(t, err, store.ErrNotOwner)
		}
	}()
	close(start)
	wg.Wait()

	candidates := make([]string, 0, racers+1)
	candidates = append(candidates, "old")
	for i := 0; i < racers; i++ {
		candidates = append(candidates, "new"+strconv.Itoa(i))
	}
	winners, winner := 0, ""
	for _, h := range candidates {
		_, err := s.RenewShard(ctx, "main", sh.TxID, h, clk.Now().Add(time.Hour))
		if err == nil {
			winners++
			winner = h
		} else {
			require.ErrorIs(t, err, store.ErrNotOwner)
		}
	}
	assert.Equal(t, 1, winners, "exactly one holder must own the shard after the takeover race")

	got, err := s.GetShard(ctx, "main", sh.TxID)
	require.NoError(t, err)
	assert.Equal(t, winner, got.Owner)
}

// testRetireVsReserveRace: goroutines reserve random rows of a shard under
// distinct tokens while another goroutine retires the shard. Retiring cascades
// every non-terminal row (available or reserved) to invalid, so whichever rows
// a racing Reserve call reports as applied must still end up invalid, not stuck
// reserved: the retire must never miss a row a concurrent Reserve just touched.
func testRetireVsReserveRace(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	sh, rows := MintShard(t, s, "main", 40, store.ShardMined)
	exp := clk.Now().Add(time.Hour)
	iters := 20 * stress()

	var mu sync.Mutex
	appliedSet := map[store.Outpoint]bool{}
	const workers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers + 1)
	for w := 0; w < workers; w++ {
		go func(seed int64) {
			defer wg.Done()
			<-start
			rng := rand.New(rand.NewSource(seed))
			tok := store.NewToken()
			for i := 0; i < iters; i++ {
				batch := pick(rng, rows, 5)
				applied, err := s.Reserve(ctx, "main", ReserveOps(batch, tok, exp))
				if !assert.NoError(t, err) {
					return
				}
				mu.Lock()
				for _, op := range applied {
					appliedSet[op.Outpoint] = true
				}
				mu.Unlock()
			}
		}(int64(w) + 1)
	}
	go func() {
		defer wg.Done()
		<-start
		_, err := s.RetireShard(ctx, "main", sh.TxID)
		assert.NoError(t, err)
	}()
	close(start)
	wg.Wait()

	got := RowsByOutpoint(t, s, "main", Ops(rows))
	for _, r := range got {
		assert.Equal(t, store.StateInvalid, r.State, "row %s must be invalid after the shard is retired", r.Outpoint)
		assert.NotEqual(t, store.StateReserved, r.State, "no row may remain reserved after the shard is retired")
	}
	for op := range appliedSet {
		assert.Equal(t, store.StateInvalid, got[op].State, "an op reported applied must not survive the retire as anything but invalid")
	}

	gotShard, err := s.GetShard(ctx, "main", sh.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardInvalid, gotShard.Status)
}

// testReserveAllOrNothingOnCancel pins store.Caps.AtomicBatch: a backend that
// declares it must never let a Reserve batch apply partway when the caller's
// context expires mid-call, regardless of how tight the deadline is or what
// error (if any) Reserve returns. This is a regression pin for a class of bug
// where a backend's write-transaction wrapper hand-rolls
// BEGIN/COMMIT/ROLLBACK over a context-aware Exec: once that context is
// already cancelled, the final COMMIT or the failure-path ROLLBACK never
// reaches the server, leaving the batch's per-row statements applied without
// the transaction that was supposed to make them atomic ever actually
// closing -- or, worse, poisoning the connection for every later call. The
// test only runs for backends that claim AtomicBatch; a backend without it
// makes no such promise.
//
// A 1000-op Reserve on a fast backend can take on the order of tens of
// milliseconds end to end, so a timeout tier needs to sit near that duration
// for cancellation to have a real chance of landing during Commit() rather
// than always firing somewhere in the per-row loop. Rather than hard-code a
// guess, this dry-runs one uncancelled 1000-op Reserve to measure that
// backend's actual duration, then derives six tiers from it: an
// already-expired context (0), a context that expires within microseconds
// (lands at BeginTx or the first rows), and four tiers scaled off the
// baseline (0.5x, 0.95x, 1.0x, 1.05x) spanning the rest of the per-row loop
// and the window around Commit. Whichever branch a given tier exercises, the
// all-or-nothing invariant below is checked unconditionally, so the
// assertions stay deterministic even though timing decides which branch
// runs. Any attempt that does apply the full batch re-mints a fresh set of
// rows before the next attempt, so the invariant stays testable throughout
// the run instead of going vacuous once the batch first lands (every later
// attempt on the same already-applied rows would otherwise trivially report
// delta == len(rows) without exercising the cancel path at all). Reserve's
// returned error, when non-nil, must be a cancellation error
// (context.Canceled or context.DeadlineExceeded) or store.ErrUnavailable --
// anything else fails the test immediately instead of only at the final
// not-poisoned check. Iteration counts are kept low (this runs per tier, per
// backend, under -race, and modernc.org/sqlite's measured duration inflates
// roughly 25x under the race detector) so the whole subtest stays at several
// seconds on memory, sqlite and postgres (measured ≈4.5s per test on SQLite
// under -race).
func testReserveAllOrNothingOnCancel(t *testing.T, s store.Store, clk *Clock) {
	if !s.Capabilities().AtomicBatch {
		t.Skip("backend does not declare AtomicBatch")
	}
	ctx := context.Background()

	_, dryRows := MintShard(t, s, "main", 1000, store.ShardMined)
	dryStart := time.Now()
	MustReserve(t, s, "main", dryRows, store.NewToken(), clk.Now().Add(time.Hour))
	baseline := time.Since(dryStart)

	_, rows := MintShard(t, s, "main", 1000, store.ShardMined)
	before, err := s.Stats(ctx, "main")
	require.NoError(t, err)

	timeouts := []time.Duration{0, 50 * time.Microsecond, baseline / 2, baseline * 19 / 20, baseline, baseline * 21 / 20}
	for _, to := range timeouts {
		for i := 0; i < 2; i++ {
			tctx, cancel := context.WithTimeout(ctx, to)
			tok := store.NewToken()
			_, rerr := s.Reserve(tctx, "main", ReserveOps(rows, tok, clk.Now().Add(time.Hour)))
			cancel()
			if rerr != nil {
				require.True(t, errors.Is(rerr, context.DeadlineExceeded) || errors.Is(rerr, context.Canceled) || errors.Is(rerr, store.ErrUnavailable),
					"Reserve under a %s deadline returned a non-cancellation error: %v", to, rerr)
			}

			st, statErr := s.Stats(ctx, "main")
			require.NoError(t, statErr)
			delta := st.Reserved - before.Reserved
			require.True(t, delta == 0 || delta == int64(len(rows)),
				"Reserve under a %s deadline must apply all-or-nothing, got delta %d more reserved than before", to, delta)

			if delta != 0 {
				_, rows = MintShard(t, s, "main", 1000, store.ShardMined)
				before, err = s.Stats(ctx, "main")
				require.NoError(t, err)
			}
		}
	}

	// The store must not be poisoned: a normal Reserve of a fresh, unrelated
	// batch still succeeds.
	_, freshRows := MintShard(t, s, "main", 10, store.ShardMined)
	applied, err := s.Reserve(ctx, "main", ReserveOps(freshRows, store.NewToken(), clk.Now().Add(time.Hour)))
	require.NoError(t, err)
	require.Len(t, applied, 10, "store must not be poisoned by the cancelled attempts above")
}
