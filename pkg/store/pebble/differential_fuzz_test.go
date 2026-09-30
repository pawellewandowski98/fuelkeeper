package pebble_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	pebblestore "github.com/bsv-blockchain/fuelkeeper/pkg/store/pebble"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// TestDifferentialFuzz drives an identical random operation stream against a
// pebble store and a memory store sharing one manual clock. memory
// (pkg/store/memory) is the reference implementation the whole conformance
// suite is written against, so any divergence here is a pebble bug the
// suite's fixed scenarios didn't happen to exercise: this is what caught
// several counter mutations the old, pebble-only TestCountersMatchScans
// missed, since it could only check pebble's counters against pebble's own
// (possibly identically wrong) scans.
//
// After every single operation it compares Stats, every known row (via
// GetRows), every shard (via ListShards), ListAvailable for every known
// shard, and a full ListQuarantined cursor-paging walk, between the two
// stores; and separately re-derives pebble's own is/, ie/, iq/, c/ and x/
// index/counter keys straight from its r/ and s/ primary records and checks
// pebble against itself (VerifyInternalConsistency), independent of whatever
// memory does.
//
// The three pool names are deliberately chosen so one is a byte-prefix of
// another ("a" of "ab"): pebble's key layout embeds the pool name in every
// index/counter/shard key, so this stresses the length-prefix encoding that
// keeps two such pools from aliasing each other's keys.
func TestDifferentialFuzz(t *testing.T) {
	pools := []string{"a", "ab", "a-b"}
	for _, seed := range []int64{1, 2, 3} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runDifferentialFuzz(t, seed, pools, 2000)
		})
	}
}

// maxShardsPerPool bounds how many shards runDifferentialFuzz mints per pool,
// keeping the full-scan comparisons (row-by-row, shard-by-shard, and
// VerifyInternalConsistency's r/+s/ scans) run after every single one of the
// 2000 iterations cheap, without changing what the fuzz actually exercises:
// PutShard is just one of many op kinds tried at random, so the cap only ever
// turns an already-at-cap PutShard attempt into a no-op for that iteration.
const maxShardsPerPool = 20

func runDifferentialFuzz(t *testing.T, seed int64, pools []string, iters int) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	mem := memory.New(memory.WithClock(clk.Now))
	peb, err := pebblestore.Open(ctx, pebblestore.Config{Path: t.TempDir(), NoSync: true}, pebblestore.WithClock(clk.Now))
	require.NoError(t, err)
	defer func() { _ = peb.Close() }()

	tokens := make([]store.Token, 24)
	for i := range tokens {
		tokens[i] = store.NewToken()
	}

	f := &fuzzState{
		t: t, ctx: ctx, clk: clk, mem: mem, peb: peb,
		rng:          rand.New(rand.NewSource(seed)), //nolint:gosec // deterministic test fuzz, not a security use
		pools:        pools,
		tokens:       tokens,
		rowsByPool:   map[string][]store.Outpoint{},
		shardsByPool: map[string][]chainhash.Hash{},
	}

	for i := 0; i < iters; i++ {
		f.step(i)
		f.verify(i)
	}
}

// fuzzState carries the two stores under comparison, the shared manual clock
// driving both, and the fuzzer's own bookkeeping of what it has minted so far
// (so later ops have real outpoints/txids/tokens to act on instead of only
// ever missing).
type fuzzState struct {
	t     *testing.T
	ctx   context.Context
	clk   *storetest.Clock
	mem   store.Store
	peb   *pebblestore.Store
	rng   *rand.Rand
	pools []string

	tokens       []store.Token
	rowsByPool   map[string][]store.Outpoint
	shardsByPool map[string][]chainhash.Hash
}

// step applies exactly one randomly chosen operation, on a randomly chosen
// pool, identically to both stores.
func (f *fuzzState) step(i int) {
	pool := f.pools[f.rng.Intn(len(f.pools))]
	switch f.rng.Intn(20) {
	case 0, 1, 2:
		f.opPutShard(pool, i)
	case 3, 4, 5, 6, 7:
		f.opReserve(pool, i)
	case 8, 9:
		f.opRelease(pool, i)
	case 10:
		f.opSpend(pool, i, false)
	case 11:
		f.opSpend(pool, i, true)
	case 12, 13:
		f.opQuarantine(pool, i)
	case 14:
		f.opResolve(pool, i)
	case 15:
		f.opUpdateShard(pool, i)
	case 16:
		f.opRetireShard(pool, i)
	case 17:
		f.opShardLease(pool, i)
	case 18:
		f.opLease(i)
	case 19:
		f.opAdvanceClock()
	}
}

func (f *fuzzState) randOutpoint(pool string) (store.Outpoint, bool) {
	ops := f.rowsByPool[pool]
	if len(ops) == 0 {
		return store.Outpoint{}, false
	}
	return ops[f.rng.Intn(len(ops))], true
}

func (f *fuzzState) randShardTxid(pool string) (chainhash.Hash, bool) {
	ids := f.shardsByPool[pool]
	if len(ids) == 0 {
		return chainhash.Hash{}, false
	}
	return ids[f.rng.Intn(len(ids))], true
}

func (f *fuzzState) randToken() store.Token { return f.tokens[f.rng.Intn(len(f.tokens))] }

func (f *fuzzState) opPutShard(pool string, i int) {
	if len(f.shardsByPool[pool]) >= maxShardsPerPool {
		return
	}
	txid := storetest.NewTxID()
	n := 1 + f.rng.Intn(2) // 1..2 rows
	status := store.ShardUnmined
	if f.rng.Intn(2) == 1 {
		status = store.ShardMined
	}
	shard, rows := storetest.BuildShardWithTxID(pool, txid, n, status)
	mErr := f.mem.PutShard(f.ctx, shard, rows)
	pErr := f.peb.PutShard(f.ctx, shard, rows)
	f.requireSameErr(i, "PutShard", mErr, pErr)
	if mErr == nil {
		f.shardsByPool[pool] = append(f.shardsByPool[pool], txid)
		for _, r := range rows {
			f.rowsByPool[pool] = append(f.rowsByPool[pool], r.Outpoint)
		}
	}
}

func (f *fuzzState) opReserve(pool string, i int) {
	op, ok := f.randOutpoint(pool)
	if !ok {
		return
	}
	tok := f.randToken()
	exp := f.clk.Now().Add(time.Duration(1+f.rng.Intn(120)) * time.Second)
	ops := []store.ReserveOp{{Outpoint: op, Token: tok, ExpiresAt: exp}}
	mApplied, mErr := f.mem.Reserve(f.ctx, pool, ops)
	pApplied, pErr := f.peb.Reserve(f.ctx, pool, ops)
	f.requireSameErr(i, "Reserve", mErr, pErr)
	require.Equalf(f.t, normalize(mApplied), normalize(pApplied), "iter %d: Reserve applied-set mismatch", i)
}

func (f *fuzzState) opRelease(pool string, i int) {
	op, ok := f.randOutpoint(pool)
	if !ok {
		return
	}
	tok := f.randToken()
	mN, mErr := f.mem.Release(f.ctx, pool, tok, []store.Outpoint{op})
	pN, pErr := f.peb.Release(f.ctx, pool, tok, []store.Outpoint{op})
	f.requireSameErr(i, "Release", mErr, pErr)
	require.Equalf(f.t, mN, pN, "iter %d: Release count mismatch", i)
}

func (f *fuzzState) opSpend(pool string, i int, terminal bool) {
	op, ok := f.randOutpoint(pool)
	if !ok {
		return
	}
	tok := f.randToken()
	var txid chainhash.Hash
	if terminal || f.rng.Intn(2) == 0 {
		txid = storetest.NewTxID()
	} // else zero txid, exercising the non-terminal "no hint" path
	mN, mErr := f.mem.Spend(f.ctx, pool, tok, []store.Outpoint{op}, txid, terminal)
	pN, pErr := f.peb.Spend(f.ctx, pool, tok, []store.Outpoint{op}, txid, terminal)
	f.requireSameErr(i, "Spend", mErr, pErr)
	require.Equalf(f.t, mN, pN, "iter %d: Spend count mismatch", i)
}

func (f *fuzzState) opQuarantine(pool string, i int) {
	before := f.clk.Now().Add(time.Duration(f.rng.Intn(5)) * time.Second)
	limit := 1 + f.rng.Intn(5)
	mN, mErr := f.mem.Quarantine(f.ctx, pool, before, limit)
	pN, pErr := f.peb.Quarantine(f.ctx, pool, before, limit)
	f.requireSameErr(i, "Quarantine", mErr, pErr)
	require.Equalf(f.t, mN, pN, "iter %d: Quarantine count mismatch", i)
}

func (f *fuzzState) opResolve(pool string, i int) {
	op, ok := f.randOutpoint(pool)
	if !ok {
		return
	}
	if f.rng.Intn(2) == 0 {
		mN, mErr := f.mem.Resolve(f.ctx, pool, []store.Outpoint{op}, store.StateAvailable, nil)
		pN, pErr := f.peb.Resolve(f.ctx, pool, []store.Outpoint{op}, store.StateAvailable, nil)
		f.requireSameErr(i, "Resolve(available)", mErr, pErr)
		require.Equalf(f.t, mN, pN, "iter %d: Resolve(available) count mismatch", i)
		return
	}
	txid := storetest.NewTxID()
	mN, mErr := f.mem.Resolve(f.ctx, pool, []store.Outpoint{op}, store.StateSpent, &txid)
	pN, pErr := f.peb.Resolve(f.ctx, pool, []store.Outpoint{op}, store.StateSpent, &txid)
	f.requireSameErr(i, "Resolve(spent)", mErr, pErr)
	require.Equalf(f.t, mN, pN, "iter %d: Resolve(spent) count mismatch", i)
}

func (f *fuzzState) opUpdateShard(pool string, i int) {
	txid, ok := f.randShardTxid(pool)
	if !ok {
		return
	}
	statuses := []store.ShardStatus{store.ShardUnmined, store.ShardMined, store.ShardRetired}
	status := statuses[f.rng.Intn(len(statuses))]
	var beef []byte
	if f.rng.Intn(2) == 0 {
		beef = []byte(fmt.Sprintf("beef-%d", i))
	}
	mErr := f.mem.UpdateShard(f.ctx, pool, txid, status, beef)
	pErr := f.peb.UpdateShard(f.ctx, pool, txid, status, beef)
	f.requireSameErr(i, "UpdateShard", mErr, pErr)
}

func (f *fuzzState) opRetireShard(pool string, i int) {
	txid, ok := f.randShardTxid(pool)
	if !ok {
		return
	}
	mN, mErr := f.mem.RetireShard(f.ctx, pool, txid)
	pN, pErr := f.peb.RetireShard(f.ctx, pool, txid)
	f.requireSameErr(i, "RetireShard", mErr, pErr)
	require.Equalf(f.t, mN, pN, "iter %d: RetireShard count mismatch", i)
}

func (f *fuzzState) opShardLease(pool string, i int) {
	holder := fmt.Sprintf("h%d", f.rng.Intn(4))
	switch f.rng.Intn(3) {
	case 0:
		statuses := []store.ShardStatus{store.ShardUnmined, store.ShardMined}
		until := f.clk.Now().Add(time.Duration(1+f.rng.Intn(60)) * time.Second)
		mSh, mErr := f.mem.AcquireShard(f.ctx, pool, holder, f.clk.Now(), until, statuses, nil)
		pSh, pErr := f.peb.AcquireShard(f.ctx, pool, holder, f.clk.Now(), until, statuses, nil)
		f.requireSameErr(i, "AcquireShard", mErr, pErr)
		require.Equalf(f.t, mSh, pSh, "iter %d: AcquireShard result mismatch", i)
	case 1:
		txid, ok := f.randShardTxid(pool)
		if !ok {
			return
		}
		until := f.clk.Now().Add(time.Duration(1+f.rng.Intn(60)) * time.Second)
		mHead, mErr := f.mem.RenewShard(f.ctx, pool, txid, holder, until)
		pHead, pErr := f.peb.RenewShard(f.ctx, pool, txid, holder, until)
		f.requireSameErr(i, "RenewShard", mErr, pErr)
		require.Equalf(f.t, mHead, pHead, "iter %d: RenewShard result mismatch", i)
	case 2:
		txid, ok := f.randShardTxid(pool)
		if !ok {
			return
		}
		mErr := f.mem.ReleaseShard(f.ctx, pool, txid, holder)
		pErr := f.peb.ReleaseShard(f.ctx, pool, txid, holder)
		f.requireSameErr(i, "ReleaseShard", mErr, pErr)
	}
}

func (f *fuzzState) opLease(i int) {
	name := fmt.Sprintf("jobs/%d", f.rng.Intn(3))
	holder := fmt.Sprintf("h%d", f.rng.Intn(4))
	if f.rng.Intn(2) == 0 {
		ttl := time.Duration(1+f.rng.Intn(30)) * time.Second
		mExp, mErr := f.mem.TryAcquireOrRenew(f.ctx, name, holder, ttl)
		pExp, pErr := f.peb.TryAcquireOrRenew(f.ctx, name, holder, ttl)
		f.requireSameErr(i, "TryAcquireOrRenew", mErr, pErr)
		require.Truef(f.t, mExp.Equal(pExp), "iter %d: TryAcquireOrRenew expiry mismatch: memory=%v pebble=%v", i, mExp, pExp)
		return
	}
	mErr := f.mem.ReleaseLease(f.ctx, name, holder)
	pErr := f.peb.ReleaseLease(f.ctx, name, holder)
	f.requireSameErr(i, "ReleaseLease", mErr, pErr)
}

func (f *fuzzState) opAdvanceClock() {
	f.clk.Advance(time.Duration(1+f.rng.Intn(10)) * time.Second)
}

// verify compares every observable result between the two stores after one
// operation, for every pool, plus pebble's internal index/counter
// self-consistency.
func (f *fuzzState) verify(i int) {
	t := f.t
	for _, pool := range f.pools {
		mStats, mErr := f.mem.Stats(f.ctx, pool)
		pStats, pErr := f.peb.Stats(f.ctx, pool)
		require.NoError(t, mErr)
		require.NoError(t, pErr)
		require.Equalf(t, mStats, pStats, "iter %d pool %s: Stats mismatch", i, pool)

		if ops := f.rowsByPool[pool]; len(ops) > 0 {
			mRows, mErr := f.mem.GetRows(f.ctx, pool, ops)
			pRows, pErr := f.peb.GetRows(f.ctx, pool, ops)
			require.NoError(t, mErr)
			require.NoError(t, pErr)
			require.Equalf(t, rowsByOutpoint(mRows), rowsByOutpoint(pRows), "iter %d pool %s: row state mismatch", i, pool)
		}

		mShards, mErr := f.mem.ListShards(f.ctx, pool, store.ShardFilter{})
		pShards, pErr := f.peb.ListShards(f.ctx, pool, store.ShardFilter{})
		require.NoError(t, mErr)
		require.NoError(t, pErr)
		require.Equalf(t, normalize(mShards), normalize(pShards), "iter %d pool %s: ListShards mismatch", i, pool)

		for _, txid := range f.shardsByPool[pool] {
			mAvail, mErr := f.mem.ListAvailable(f.ctx, pool, txid, 0, 100)
			pAvail, pErr := f.peb.ListAvailable(f.ctx, pool, txid, 0, 100)
			require.NoError(t, mErr)
			require.NoError(t, pErr)
			require.Equalf(t, normalize(mAvail), normalize(pAvail), "iter %d pool %s shard %s: ListAvailable mismatch", i, pool, txid)
		}

		f.verifyQuarantinedPaging(pool, i)
	}
	pebblestore.VerifyInternalConsistency(t, f.peb)
}

// verifyQuarantinedPaging walks ListQuarantined page by page (limit 1) on
// both stores, using each page's own cursor to fetch the next, until both
// report empty: this exercises the cursor-paging contract itself (not just
// "the final list matches"), since a boundary bug in the exclusive-cursor
// comparison would otherwise only show up as a page appearing twice or a row
// being skipped, not as a difference in a single unpaged listing.
func (f *fuzzState) verifyQuarantinedPaging(pool string, i int) {
	t := f.t
	before := f.clk.Now().Add(time.Hour) // safely past every quarantined_at seen so far
	var cursor *store.RowCursor
	for {
		mRows, mErr := f.mem.ListQuarantined(f.ctx, pool, before, cursor, 1)
		pRows, pErr := f.peb.ListQuarantined(f.ctx, pool, before, cursor, 1)
		require.NoError(t, mErr)
		require.NoError(t, pErr)
		require.Equalf(t, normalize(mRows), normalize(pRows), "iter %d pool %s: ListQuarantined page mismatch at cursor %+v", i, pool, cursor)
		if len(mRows) == 0 {
			return
		}
		c := mRows[0].QuarantineCursor()
		cursor = &c
	}
}

// normalize maps a nil slice to a non-nil empty one before comparison: the
// store.Store contract never distinguishes "no results" from "no results,
// but here's an empty slice" (see e.g. ListAvailable's doc comment, "An
// unknown txid returns an empty slice, not ErrNotFound"), but the two
// backends happen to construct their empty results differently (memory
// always allocates via make([]T, n); pebble's iterate-and-append helpers
// leave a var declaration nil when nothing matched), and reflect.DeepEqual
// (which require.Equal falls back to for non-[]byte slices) treats a nil
// slice and a non-nil empty slice as unequal. Without this, every op that
// happens to leave a pool's shard/row/quarantine list momentarily empty would
// report a spurious mismatch that has nothing to do with either backend's
// actual, observable behavior.
func normalize[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

func rowsByOutpoint(rows []store.Row) map[store.Outpoint]store.Row {
	m := make(map[store.Outpoint]store.Row, len(rows))
	for _, r := range rows {
		m[r.Outpoint] = r
	}
	return m
}

// requireSameErr classifies both errors down to the shared sentinel each
// backend is required to return (or nil, or something unrecognized) and
// requires the classification to match; the two backends' error strings
// legitimately differ (each wraps its own context), so comparing errors
// directly would fail on that irrelevant difference rather than a real one.
func (f *fuzzState) requireSameErr(i int, what string, memErr, pebErr error) {
	f.t.Helper()
	cm, cp := classifyErr(memErr), classifyErr(pebErr)
	// cm and cp are always either nil or one of the fixed sentinel values
	// classifyErr returns verbatim, so an equality assertion here compares
	// normalized sentinel identity, not two arbitrarily wrapped errors.
	require.Equalf(f.t, cm, cp, "iter %d: %s error mismatch: memory=%v pebble=%v", i, what, memErr, pebErr)
}

func classifyErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotFound):
		return store.ErrNotFound
	case errors.Is(err, store.ErrNotOwner):
		return store.ErrNotOwner
	case errors.Is(err, store.ErrShardExists):
		return store.ErrShardExists
	case errors.Is(err, store.ErrTerminalShard):
		return store.ErrTerminalShard
	case errors.Is(err, store.ErrInvalidArgument):
		return store.ErrInvalidArgument
	case errors.Is(err, store.ErrUnavailable):
		return store.ErrUnavailable
	default:
		return err
	}
}
