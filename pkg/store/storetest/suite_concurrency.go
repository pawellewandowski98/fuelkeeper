package storetest

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"sync"
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

// testReserveExclusivityConcurrent: W workers with distinct tokens reserve random
// overlapping subsets and never release. The applied sets must partition the
// reserved rows exactly (G1), and the store must agree with the workers' view.
func testReserveExclusivityConcurrent(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	_, rows := MintShard(t, s, "main", 200, store.ShardMined)
	const workers = 8
	iters := 30 * stress()
	exp := clk.Now().Add(time.Hour)

	var mu sync.Mutex
	ownerOf := map[store.Outpoint]store.Token{}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			tok := store.NewToken()
			for i := 0; i < iters; i++ {
				batch := pick(rng, rows, 10)
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
	wg.Wait()

	got := RowsByOutpoint(t, s, "main", Ops(rows))
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
// race the store.
func testReserveReleaseCycleConcurrent(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	_, rows := MintShard(t, s, "main", 50, store.ShardMined)
	const workers = 8
	iters := 100 * stress()
	exp := clk.Now().Add(time.Hour)

	var mu sync.Mutex
	holder := map[store.Outpoint]store.Token{}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
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
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(holder string) {
			defer wg.Done()
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
				assert.NoError(t, err)
				assert.Equal(t, store.ShardMined, head.Status)

				mu.Lock()
				delete(owned, sh.TxID)
				mu.Unlock()
				assert.NoError(t, s.ReleaseShard(ctx, "main", sh.TxID, holder))
			}
		}("h" + strconv.Itoa(w))
	}
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

// testReserveVsQuarantineRace: rows whose reservation has expired are released
// by the client while the reaper quarantines them. Each row ends in exactly one
// of the two states and the two counts add up to the row count.
func testReserveVsQuarantineRace(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	rounds := 5 * stress()
	for round := 0; round < rounds; round++ {
		_, rows := MintShard(t, s, "main", 40, store.ShardMined)
		tok := store.NewToken()
		MustReserve(t, s, "main", rows, tok, clk.Now().Add(time.Second))
		clk.Advance(2 * time.Second)

		var released, quarantined int
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < len(rows); i += 4 {
				n, err := s.Release(ctx, "main", tok, Ops(rows[i:i+4]))
				assert.NoError(t, err)
				released += n
			}
		}()
		go func() {
			defer wg.Done()
			for {
				n, err := s.Quarantine(ctx, "main", clk.Now(), 7)
				assert.NoError(t, err)
				quarantined += n
				if n == 0 {
					return
				}
			}
		}()
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
		assert.Equal(t, released, a, "released count matches available rows")
		assert.Equal(t, quarantined, q, "quarantined count matches quarantined rows")
		assert.Equal(t, len(rows), a+q)
	}
}
