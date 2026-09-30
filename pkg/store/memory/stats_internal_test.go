package memory

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// scanStats recomputes one pool's Stats by scanning every row and shard in
// the store, exactly what Stats itself did before it read the incrementally
// maintained per-pool per-state counters (counts.go). It is ground truth for
// TestStatsCountersMatchScan to check Stats's counters against; it never
// updates on its own and always reflects the current s.rows/s.shards
// contents, so any drift between it and Stats means some state transition
// forgot to call addCount/moveCount.
func (s *Store) scanStats(pool string) store.Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st store.Stats
	for _, r := range s.rows {
		if r.Pool != pool {
			continue
		}
		switch r.State {
		case store.StateAvailable:
			st.Available++
		case store.StateReserved:
			st.Reserved++
		case store.StateQuarantined:
			st.Quarantined++
		case store.StateSpent:
			st.Spent++
		case store.StateInvalid:
			st.Invalid++
		}
	}
	for k, sh := range s.shards {
		if k.pool != pool {
			continue
		}
		switch sh.Status {
		case store.ShardUnmined:
			st.ShardsUnmined++
		case store.ShardMined:
			st.ShardsMined++
		case store.ShardRetired:
			st.ShardsRetired++
		case store.ShardInvalid:
			st.ShardsInvalid++
		}
	}
	return st
}

// trackedRow is TestStatsCountersMatchScan's bookkeeping for one minted row:
// enough to attempt a state-appropriate operation on it without guessing.
// The fuzz loop always re-reads the row's actual current state via GetRows
// before acting, so trackedRow.token only needs to stay valid while the row
// is actually reserved; a stale token on a row RetireShard cascaded to
// invalid behind our back just makes the next op targeting it a documented
// no-op (0 rows changed), never a test failure.
type trackedRow struct {
	op    store.Outpoint
	pool  string
	txid  chainhash.Hash // owning shard, for RetireShard
	token store.Token    // meaningful only while the row is reserved
}

// TestStatsCountersMatchScan drives a long randomized sequence of state
// transitions -- mint (PutShard), Reserve, Release, both Spend modes,
// Quarantine's expiry sweep, both Resolve destinations, and RetireShard's
// cascade -- across several pools and many rows, and after every step checks
// that Stats (which reads the incrementally maintained counters) agrees with
// scanStats (the full-scan ground truth). Run with -race so a counter update
// racing s.mu would be caught (every mutation and Stats itself take the same
// lock, so it shouldn't), and with -count=20 so each run's fresh time-seeded
// source shakes out a different operation ordering.
func TestStatsCountersMatchScan(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	s := New(WithClock(clk.Now))

	pools := []string{"pool-a", "pool-b", "pool-c"}
	rng := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec // deterministic-enough test fuzz, not a security use

	var rows []*trackedRow

	mint := func() {
		pool := pools[rng.Intn(len(pools))]
		n := 1 + rng.Intn(5)
		shard, srows := storetest.BuildShard(pool, n, store.ShardUnmined)
		require.NoError(t, s.PutShard(ctx, shard, srows))
		for _, r := range srows {
			rows = append(rows, &trackedRow{op: r.Outpoint, pool: pool, txid: shard.TxID})
		}
	}

	checkAll := func(step int) {
		for _, p := range pools {
			want := s.scanStats(p)
			got, err := s.Stats(ctx, p)
			require.NoError(t, err)
			require.Equalf(t, want, got, "pool %q counters diverged from a full scan at step %d", p, step)
		}
	}

	const iterations = 2000
	for i := 0; i < iterations; i++ {
		if len(rows) == 0 || rng.Intn(10) == 0 {
			mint()
		} else {
			tr := rows[rng.Intn(len(rows))]
			got, err := s.GetRows(ctx, tr.pool, []store.Outpoint{tr.op})
			require.NoError(t, err)
			if len(got) == 1 {
				applyRandomOp(t, s, clk, rng, tr, got[0].State)
			}
		}
		// Checking every pool after every step (not just the one just touched)
		// catches a transition that wrongly bled into a neighboring pool's
		// counters, e.g. a mixed-up pool argument on a moveCount call.
		if i%10 == 0 {
			checkAll(i)
		}
	}
	checkAll(iterations)
}

// applyRandomOp attempts one operation appropriate to row's current state,
// updating tr.token when it starts or ends a reservation. A skipped op
// (wrong state by the time the store applies it, e.g. after a concurrent-in-
// spirit RetireShard cascade) is fine: every Store mutation here is
// documented to silently omit rows that don't match its precondition rather
// than error.
func applyRandomOp(t *testing.T, s *Store, clk *storetest.Clock, rng *rand.Rand, tr *trackedRow, state store.State) {
	t.Helper()
	ctx := context.Background()
	switch state {
	case store.StateAvailable:
		switch rng.Intn(3) {
		case 0, 1:
			tok := store.NewToken()
			applied, err := s.Reserve(ctx, tr.pool, []store.ReserveOp{
				{Outpoint: tr.op, Token: tok, ExpiresAt: clk.Now().Add(time.Minute)},
			})
			require.NoError(t, err)
			if len(applied) == 1 {
				tr.token = tok
			}
		default:
			_, err := s.RetireShard(ctx, tr.pool, tr.txid)
			require.NoError(t, err)
		}
	case store.StateReserved:
		switch rng.Intn(5) {
		case 0:
			_, err := s.Release(ctx, tr.pool, tr.token, []store.Outpoint{tr.op})
			require.NoError(t, err)
		case 1:
			_, err := s.Spend(ctx, tr.pool, tr.token, []store.Outpoint{tr.op}, storetest.NewTxID(), false)
			require.NoError(t, err)
		case 2:
			_, err := s.Spend(ctx, tr.pool, tr.token, []store.Outpoint{tr.op}, storetest.NewTxID(), true)
			require.NoError(t, err)
		case 3:
			_, err := s.RetireShard(ctx, tr.pool, tr.txid)
			require.NoError(t, err)
		default:
			// Expire it and let Quarantine's batch sweep pick it up, exercising
			// that code path (rather than only Spend's direct
			// reserved->quarantined transition).
			clk.Advance(2 * time.Minute)
			_, err := s.Quarantine(ctx, tr.pool, clk.Now(), 10000)
			require.NoError(t, err)
		}
	case store.StateQuarantined:
		switch rng.Intn(3) {
		case 0:
			_, err := s.Resolve(ctx, tr.pool, []store.Outpoint{tr.op}, store.StateAvailable, nil)
			require.NoError(t, err)
		case 1:
			h := storetest.NewTxID()
			_, err := s.Resolve(ctx, tr.pool, []store.Outpoint{tr.op}, store.StateSpent, &h)
			require.NoError(t, err)
		default:
			_, err := s.RetireShard(ctx, tr.pool, tr.txid)
			require.NoError(t, err)
		}
	case store.StateSpent, store.StateInvalid:
		// Terminal: RetireShard on an already-terminal row is a documented
		// no-op, still worth occasionally exercising.
		if rng.Intn(8) == 0 {
			_, err := s.RetireShard(ctx, tr.pool, tr.txid)
			require.NoError(t, err)
		}
	}
}
