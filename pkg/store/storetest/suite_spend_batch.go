package storetest

import (
	"context"
	"fmt"
	"math/rand/v2"
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

// runSpendBatchSuite runs the store.SpendBatcher conformance tests when the
// backend implements it, and skips them otherwise. The differential tests
// open a second, fresh store from the same factory and hold the batch
// against sequential Spend calls on it, row for row and count for count.
func runSpendBatchSuite(t *testing.T, newStore Factory) {
	t.Run("SpendBatch", func(t *testing.T) {
		probe := newStore(t, NewClock(Start).Now)
		_, ok := probe.(store.SpendBatcher)
		_ = probe.Close()
		if !ok {
			t.Skip("backend does not implement store.SpendBatcher")
		}
		for _, tc := range []struct {
			name string
			fn   func(t *testing.T, newStore Factory)
		}{
			{"Differential", testSpendBatchDifferential},
			{"Replay", testSpendBatchReplay},
			{"InvalidRejectsWholeBatch", testSpendBatchInvalid},
			{"Concurrent", testSpendBatchConcurrent},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				tc.fn(t, newStore)
			})
		}
	})
}

// spendBatchSeed is the seed for the randomised spend-batch tests:
// FUELKEEPER_STORETEST_SEED when set (to reproduce a failure), else the clock.
func spendBatchSeed(t *testing.T) uint64 {
	t.Helper()
	if v := os.Getenv("FUELKEEPER_STORETEST_SEED"); v != "" {
		seed, err := strconv.ParseUint(v, 10, 64)
		require.NoError(t, err)
		return seed
	}
	seed := uint64(time.Now().UnixNano()) //nolint:gosec // a test seed, logged below
	t.Logf("spend-batch seed %d (FUELKEEPER_STORETEST_SEED=%d to reproduce)", seed, seed)
	return seed
}

// twin is a pair of fresh stores of one backend, with independent clocks
// kept in lockstep: batch receives SpendBatch calls, seq the equivalent
// sequential Spend calls.
type twin struct {
	batch, seq       store.Store
	batchClk, seqClk *Clock
	ops              []store.Outpoint // every outpoint the test knows, including other-pool and missing ones
	mainOps          []store.Outpoint // outpoints of rows in pool "main"
	rows             map[store.Outpoint]store.Row
}

func newTwin(t *testing.T, newStore Factory, shards, size int) *twin {
	t.Helper()
	tw := &twin{batchClk: NewClock(Start), seqClk: NewClock(Start), rows: map[store.Outpoint]store.Row{}}
	tw.batch = newStore(t, tw.batchClk.Now)
	t.Cleanup(func() { _ = tw.batch.Close() })
	tw.seq = newStore(t, tw.seqClk.Now)
	t.Cleanup(func() { _ = tw.seq.Close() })
	ctx := context.Background()
	put := func(pool string, n int) {
		shard, rows := BuildShard(pool, n, store.ShardMined)
		require.NoError(t, tw.batch.PutShard(ctx, shard, rows))
		require.NoError(t, tw.seq.PutShard(ctx, shard, rows))
		for _, r := range rows {
			tw.ops = append(tw.ops, r.Outpoint)
			if pool == "main" {
				tw.mainOps = append(tw.mainOps, r.Outpoint)
				tw.rows[r.Outpoint] = r
			}
		}
	}
	for range shards {
		put("main", size)
	}
	put("other", 4)                                                   // outpoints that exist, but in another pool
	tw.ops = append(tw.ops, store.Outpoint{TxID: NewTxID(), Vout: 0}) // an outpoint no store has
	return tw
}

// tick advances both clocks by one second.
func (tw *twin) tick() {
	tw.batchClk.Advance(time.Second)
	tw.seqClk.Advance(time.Second)
}

// reserve reserves ops in pool under tok in both stores and requires the
// same outcome from each (both pools: "other" rows are reserved too, so a
// request naming one finds it reserved under a matching token in the wrong
// pool).
func (tw *twin) reserve(t *testing.T, pool string, ops []store.Outpoint, tok store.Token) {
	t.Helper()
	if len(ops) == 0 {
		return
	}
	ctx := context.Background()
	exp := tw.batchClk.Now().Add(time.Hour)
	rops := make([]store.ReserveOp, len(ops))
	for i, op := range ops {
		rops[i] = store.ReserveOp{Outpoint: op, Token: tok, ExpiresAt: exp}
	}
	a, err := tw.batch.Reserve(ctx, pool, rops)
	require.NoError(t, err)
	b, err := tw.seq.Reserve(ctx, pool, rops)
	require.NoError(t, err)
	require.Len(t, a, len(b), "reserve diverged between twin stores")
}

// apply runs reqs as one SpendBatch on batch and as sequential Spend calls
// on seq, requiring identical errors, counts, rows and stats.
func (tw *twin) apply(t *testing.T, reqs []store.SpendRequest) []int {
	t.Helper()
	ctx := context.Background()
	got, err := tw.batch.(store.SpendBatcher).SpendBatch(ctx, "main", reqs)
	require.NoError(t, err)
	want := make([]int, len(reqs))
	for i, r := range reqs {
		n, err := tw.seq.Spend(ctx, "main", r.Token, r.Ops, r.TxID, r.Terminal)
		require.NoError(t, err)
		want[i] = n
	}
	require.Equal(t, want, got, "SpendBatch counts differ from sequential Spend (%s)", describe(reqs))
	tw.requireSameRows(t)
	return got
}

func (tw *twin) requireSameRows(t *testing.T) {
	t.Helper()
	for _, pool := range []string{"main", "other"} {
		a := RowsByOutpoint(t, tw.batch, pool, tw.ops)
		b := RowsByOutpoint(t, tw.seq, pool, tw.ops)
		require.Len(t, a, len(b), "pool %s row count", pool)
		for op, rb := range b {
			ra, ok := a[op]
			require.True(t, ok, "row %s missing from batch store", op)
			require.Equal(t, normRow(rb), normRow(ra), "row %s differs from sequential Spend", op)
		}
		sa, err := tw.batch.Stats(context.Background(), pool)
		require.NoError(t, err)
		sb, err := tw.seq.Stats(context.Background(), pool)
		require.NoError(t, err)
		require.Equal(t, sb, sa, "pool %s stats differ from sequential Spend", pool)
	}
}

// normRow puts every time in UTC so rows read back from different stores of
// the same backend compare with require.Equal.
func normRow(r store.Row) store.Row {
	r.ReservedAt, r.ExpiresAt = r.ReservedAt.UTC(), r.ExpiresAt.UTC()
	r.QuarantinedAt, r.UpdatedAt = r.QuarantinedAt.UTC(), r.UpdatedAt.UTC()
	return r
}

func describe(reqs []store.SpendRequest) string {
	s := ""
	for i, r := range reqs {
		s += fmt.Sprintf("[%d tok=%x ops=%d terminal=%v zero=%v]", i, r.Token[:2], len(r.Ops), r.Terminal, r.TxID == (chainhash.Hash{}))
	}
	return s
}

// randomRequest builds one valid request from rng: a random token from toks,
// 1..maxOps distinct outpoints drawn from ops, a random mode and txid
// (a zero txid only when non-terminal: the verify-release shape).
func randomRequest(rng *rand.Rand, toks []store.Token, ops []store.Outpoint, txids []chainhash.Hash, maxOps int) store.SpendRequest {
	r := store.SpendRequest{Token: toks[rng.IntN(len(toks))], Terminal: rng.IntN(2) == 0}
	n := 1 + rng.IntN(min(maxOps, len(ops)))
	for _, i := range rng.Perm(len(ops))[:n] {
		r.Ops = append(r.Ops, ops[i])
	}
	switch k := rng.IntN(4); {
	case k == 0 && !r.Terminal:
		// zero txid: Release under release_policy=verify
	default:
		r.TxID = txids[rng.IntN(len(txids))]
	}
	return r
}

func testSpendBatchDifferential(t *testing.T, newStore Factory) {
	rng := rand.New(rand.NewPCG(spendBatchSeed(t), 1)) //nolint:gosec // deterministic test randomness
	tw := newTwin(t, newStore, 3, 16)
	toks := []store.Token{store.NewToken(), store.NewToken(), store.NewToken(), store.NewToken()}
	stranger := store.NewToken() // never reserves anything
	reqToks := append(append([]store.Token{}, toks...), stranger)
	txids := []chainhash.Hash{NewTxID(), NewTxID(), NewTxID()}

	// "other" pool rows reserved under toks[0]: a request naming them under
	// the right token must still count 0 for them, since the batch is for "main".
	tw.reserve(t, "other", tw.ops[len(tw.mainOps):len(tw.mainOps)+4], toks[0])

	for round := range 25 {
		// Reserve a random subset of whatever is still available.
		avail := RowsByOutpoint(t, tw.seq, "main", tw.mainOps)
		byTok := map[store.Token][]store.Outpoint{}
		for _, op := range tw.mainOps {
			if avail[op].State == store.StateAvailable && rng.IntN(3) != 0 {
				tok := toks[rng.IntN(len(toks))]
				byTok[tok] = append(byTok[tok], op)
			}
		}
		for _, tok := range toks {
			tw.reserve(t, "main", byTok[tok], tok)
		}
		tw.tick()

		reqs := make([]store.SpendRequest, 1+rng.IntN(8))
		for i := range reqs {
			reqs[i] = randomRequest(rng, reqToks, tw.ops, txids, 6)
		}
		if len(reqs) > 1 && round%3 == 0 {
			// The same outpoint in two requests of one batch, both under the
			// token that actually holds it: only the earlier request may
			// count it.
			for op, r := range RowsByOutpoint(t, tw.seq, "main", tw.mainOps) {
				if r.State != store.StateReserved {
					continue
				}
				first, last := &reqs[0], &reqs[len(reqs)-1]
				first.Token, last.Token = r.ReservedBy, r.ReservedBy
				for _, q := range []*store.SpendRequest{first, last} {
					if !containsOp(q.Ops, op) {
						q.Ops = append(q.Ops, op)
					}
				}
				break
			}
		}
		tw.apply(t, reqs)
		tw.tick()

		if round%5 == 4 {
			// Return some quarantined rows to available so they can be
			// reserved and spent again under a different token.
			var quar []store.Outpoint
			for op, r := range RowsByOutpoint(t, tw.seq, "main", tw.mainOps) {
				if r.State == store.StateQuarantined && rng.IntN(2) == 0 {
					quar = append(quar, op)
				}
			}
			if len(quar) > 0 {
				ctx := context.Background()
				na, err := tw.batch.Resolve(ctx, "main", quar, store.StateAvailable, nil)
				require.NoError(t, err)
				nb, err := tw.seq.Resolve(ctx, "main", quar, store.StateAvailable, nil)
				require.NoError(t, err)
				require.Equal(t, nb, na)
			}
		}
	}
}

func containsOp(ops []store.Outpoint, op store.Outpoint) bool {
	for _, o := range ops {
		if o == op {
			return true
		}
	}
	return false
}

func testSpendBatchReplay(t *testing.T, newStore Factory) {
	rng := rand.New(rand.NewPCG(spendBatchSeed(t), 2)) //nolint:gosec // deterministic test randomness
	tw := newTwin(t, newStore, 2, 12)
	toks := []store.Token{store.NewToken(), store.NewToken(), store.NewToken()}
	txids := []chainhash.Hash{NewTxID(), NewTxID()}
	for i, op := range tw.mainOps {
		tw.reserve(t, "main", []store.Outpoint{op}, toks[i%len(toks)])
	}
	tw.tick()
	reqs := make([]store.SpendRequest, 6)
	for i := range reqs {
		reqs[i] = randomRequest(rng, toks, tw.mainOps, txids, 5)
	}
	first := tw.apply(t, reqs)
	total := 0
	for _, n := range first {
		total += n
	}
	require.Positive(t, total, "the first application should move something")

	// A retry (e.g. after an in-doubt commit) changes nothing and, like a
	// sequential Spend replay, counts 0 for every row the first one moved.
	tw.tick()
	again := tw.apply(t, reqs)
	assert.Equal(t, make([]int, len(reqs)), again, "a replayed batch moves nothing")
}

func testSpendBatchInvalid(t *testing.T, newStore Factory) {
	ctx := context.Background()
	clk := NewClock(Start)
	s := newStore(t, clk.Now)
	t.Cleanup(func() { _ = s.Close() })
	b := s.(store.SpendBatcher)
	_, rows := MintShard(t, s, "main", 4, store.ShardMined)
	tok := store.NewToken()
	MustReserve(t, s, "main", rows, tok, clk.Now().Add(time.Hour))
	before := RowsByOutpoint(t, s, "main", Ops(rows))
	good := store.SpendRequest{Token: tok, Ops: Ops(rows[:2]), TxID: NewTxID(), Terminal: true}
	maxBatch := s.Capabilities().MaxBatch

	tooMany := make([]store.Outpoint, maxBatch+1)
	for i := range tooMany {
		tooMany[i] = store.Outpoint{TxID: rows[0].TxID, Vout: uint32(i)} //nolint:gosec // bounded
	}
	half := store.SpendRequest{Token: tok, Ops: tooMany[maxBatch/2:], TxID: good.TxID}
	cases := map[string][]store.SpendRequest{
		"empty batch":             nil,
		"empty ops":               {good, {Token: tok, TxID: good.TxID}},
		"duplicate in a request":  {good, {Token: tok, Ops: []store.Outpoint{rows[2].Outpoint, rows[2].Outpoint}, TxID: good.TxID}},
		"zero token":              {good, {Ops: Ops(rows[2:3]), TxID: good.TxID}},
		"terminal with zero txid": {good, {Token: tok, Ops: Ops(rows[2:3]), Terminal: true}},
		"too many outpoints":      {{Token: tok, Ops: tooMany[:maxBatch/2+1], TxID: good.TxID}, half},
	}
	for name, reqs := range cases {
		counts, err := b.SpendBatch(ctx, "main", reqs)
		require.ErrorIs(t, err, store.ErrInvalidArgument, name)
		assert.Nil(t, counts, name)
	}
	_, err := b.SpendBatch(ctx, "", []store.SpendRequest{good})
	require.ErrorIs(t, err, store.ErrInvalidArgument, "empty pool")
	assert.Equal(t, before, RowsByOutpoint(t, s, "main", Ops(rows)), "a rejected batch must apply nothing, not even its valid requests")

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = b.SpendBatch(cctx, "main", []store.SpendRequest{good})
	require.Error(t, err, "a cancelled context is rejected")
	assert.Equal(t, before, RowsByOutpoint(t, s, "main", Ops(rows)), "a cancelled batch must apply nothing")
}

// testSpendBatchConcurrent has several goroutines issue overlapping batches
// (and plain Spend calls) against one store, each request under a random
// token that may or may not hold its rows. Every row must end in exactly one
// state, and G1 must hold: a row moved off reserved was moved by its own
// token, once -- the counts each token was told add up to exactly the rows
// it held that are no longer reserved, so no row was counted twice or for
// the wrong token.
func testSpendBatchConcurrent(t *testing.T, newStore Factory) {
	ctx := context.Background()
	clk := NewClock(Start)
	s := newStore(t, clk.Now)
	t.Cleanup(func() { _ = s.Close() })
	b := s.(store.SpendBatcher)
	seed := spendBatchSeed(t)

	const nTok, perTok = 4, 24
	toks := make([]store.Token, nTok)
	all := make([]store.Outpoint, 0, nTok*perTok)
	holder := map[store.Outpoint]store.Token{}
	for i := range toks {
		toks[i] = store.NewToken()
		_, rows := MintShard(t, s, "main", perTok, store.ShardMined)
		MustReserve(t, s, "main", rows, toks[i], clk.Now().Add(time.Hour))
		for _, r := range rows {
			all = append(all, r.Outpoint)
			holder[r.Outpoint] = toks[i]
		}
	}
	txids := []chainhash.Hash{NewTxID(), NewTxID()}

	const workers = 6
	var mu sync.Mutex
	counted := map[store.Token]int{}
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, uint64(100+w))) //nolint:gosec // deterministic test randomness
			for range 15 {
				reqs := make([]store.SpendRequest, 1+rng.IntN(6))
				for i := range reqs {
					reqs[i] = randomRequest(rng, toks, all, txids, 8)
				}
				var counts []int
				if w%3 == 2 {
					for _, r := range reqs {
						n, err := s.Spend(ctx, "main", r.Token, r.Ops, r.TxID, r.Terminal)
						if !assert.NoError(t, err) {
							return
						}
						counts = append(counts, n)
					}
				} else {
					var err error
					counts, err = b.SpendBatch(ctx, "main", reqs)
					if !assert.NoError(t, err) {
						return
					}
				}
				mu.Lock()
				for i, r := range reqs {
					counted[r.Token] += counts[i]
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	final := RowsByOutpoint(t, s, "main", all)
	require.Len(t, final, len(all))
	moved := map[store.Token]int{}
	for op, r := range final {
		require.Equal(t, holder[op], r.ReservedBy, "row %s: provenance names a different token (G1)", op)
		switch r.State {
		case store.StateReserved:
		case store.StateSpent:
			require.NotNil(t, r.SpentBy, "row %s", op)
			moved[r.ReservedBy]++
		case store.StateQuarantined:
			require.Nil(t, r.SpentBy, "row %s", op)
			moved[r.ReservedBy]++
		default:
			t.Fatalf("row %s ended in unexpected state %s", op, r.State)
		}
	}
	for _, tok := range toks {
		assert.Equal(t, moved[tok], counted[tok], "token %x: counts reported vs rows moved (no row counted twice or for another token)", tok[:2])
	}
	st, err := s.Stats(ctx, "main")
	require.NoError(t, err)
	total := 0
	for _, n := range moved {
		total += n
	}
	assert.Equal(t, int64(len(all)-total), st.Reserved)
	assert.Equal(t, int64(total), st.Quarantined+st.Spent)
}
