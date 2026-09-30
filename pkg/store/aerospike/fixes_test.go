//go:build aerospike

package aerospike

import (
	"context"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// markShardInvalidOnly writes the shard record invalid without touching its
// rows: the state an older RetireShard left behind when its row cascade
// failed after the shard write.
func markShardInvalidOnly(t *testing.T, s *Store, pool string, sh store.Shard) {
	t.Helper()
	key, err := s.shardKey(pool, sh.TxID)
	require.NoError(t, err)
	aerr := s.client.PutBins(s.writePolicy(context.Background()), key,
		as.NewBin(binStatus, int64(store.ShardInvalid)), as.NewBin(binPS, s.psFor(pool, store.ShardInvalid)))
	require.Nil(t, aerr)
}

// stressFactor mirrors storetest's FUELKEEPER_STRESS multiplier.
func stressFactor() int {
	if v, err := strconv.Atoi(os.Getenv("FUELKEEPER_STRESS")); err == nil && v > 0 {
		return v
	}
	return 1
}

func nonTerminal(st store.Stats) int64 { return st.Available + st.Reserved + st.Quarantined }

// C1: a shard left invalid with live rows (cascade failed after the shard
// write) must be repaired by the next RetireShard.
func TestRetireShardRepairsPartialCascade(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, testHost(t), clk.Now)
	ctx := context.Background()
	sh, rows := storetest.MintShard(t, s, "main", 4, store.ShardMined)
	tok := store.NewToken()
	storetest.MustReserve(t, s, "main", rows[:2], tok, clk.Now().Add(time.Hour))
	n, err := s.Spend(ctx, "main", tok, []store.Outpoint{rows[1].Outpoint}, storetest.NewTxID(), false)
	require.NoError(t, err)
	require.Equal(t, 1, n) // rows: available, available, reserved, quarantined

	markShardInvalidOnly(t, s, "main", sh)

	n, err = s.RetireShard(ctx, "main", sh.TxID)
	require.NoError(t, err)
	assert.Equal(t, 4, n, "the retry must cascade every row the failed call left behind")

	for _, r := range storetest.RowsByOutpoint(t, s, "main", storetest.Ops(rows)) {
		assert.Equal(t, store.StateInvalid, r.State, "row %d", r.Vout)
	}
	avail, err := s.ListAvailable(ctx, "main", sh.TxID, 0, 10)
	require.NoError(t, err)
	assert.Empty(t, avail)
	got, err := s.Reserve(ctx, "main", storetest.ReserveOps(rows, store.NewToken(), clk.Now().Add(time.Hour)))
	require.NoError(t, err)
	assert.Empty(t, got, "rows of an invalid shard must not be reservable")
	st, err := s.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Zero(t, nonTerminal(st))

	// Once repaired, a further retire is the documented no-op.
	n, err = s.RetireShard(ctx, "main", sh.TxID)
	require.NoError(t, err)
	assert.Zero(t, n)
}

// C1 converse: rows already cascaded but the shard write never landed. The
// retry must still retire the shard (and clear its lease, bump Version).
func TestRetireShardRepairsUnwrittenShard(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, testHost(t), clk.Now)
	ctx := context.Background()
	sh, rows := storetest.MintShard(t, s, "main", 3, store.ShardMined)
	_, err := s.AcquireShard(ctx, "main", "h", clk.Now(), clk.Now().Add(time.Minute), []store.ShardStatus{store.ShardMined}, nil)
	require.NoError(t, err)

	n, err := s.retireRows(ctx, "main", storetest.Ops(rows), clk.Now())
	require.NoError(t, err)
	require.Equal(t, 3, n)

	n, err = s.RetireShard(ctx, "main", sh.TxID)
	require.NoError(t, err)
	assert.Zero(t, n, "no row left to change")
	got, err := s.GetShard(ctx, "main", sh.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardInvalid, got.Status)
	assert.Equal(t, int64(2), got.Version)
	assert.Empty(t, got.Owner)
	assert.True(t, got.LeaseUntil.IsZero())
}

// C2: rows cycling reserved<->available and quarantined->available while
// RetireShard runs must never escape the cascade.
func TestRetireVsReleaseRace(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, testHost(t), clk.Now)
	ctx := context.Background()
	iters := 60 * stressFactor()
	var allRows []store.Row
	for iter := 0; iter < iters; iter++ {
		sh, rows := storetest.MintShard(t, s, "main", 8, store.ShardMined)
		allRows = append(allRows, rows...)
		stop := make(chan struct{})
		var wg sync.WaitGroup
		var cycles atomic.Int64
		for w := 0; w < 6; w++ {
			wg.Add(1)
			go func(w int, r store.Row) {
				defer wg.Done()
				tok := store.NewToken()
				ops := []store.Outpoint{r.Outpoint}
				for {
					select {
					case <-stop:
						return
					default:
					}
					_, _ = s.Reserve(ctx, "main", storetest.ReserveOps([]store.Row{r}, tok, clk.Now().Add(time.Hour)))
					if w%2 == 0 {
						_, _ = s.Release(ctx, "main", tok, ops)
					} else {
						_, _ = s.Spend(ctx, "main", tok, ops, storetest.NewTxID(), false)
						_, _ = s.Resolve(ctx, "main", ops, store.StateAvailable, nil)
					}
					cycles.Add(1)
				}
			}(w, rows[w])
		}
		for cycles.Load() < 6 {
			time.Sleep(100 * time.Microsecond)
		}
		_, err := s.RetireShard(ctx, "main", sh.TxID)
		require.NoError(t, err)
		close(stop)
		wg.Wait()
	}
	leaks := 0
	for _, r := range storetest.RowsByOutpoint(t, s, "main", storetest.Ops(allRows[:min(len(allRows), maxBatch)])) {
		if r.State != store.StateInvalid {
			leaks++
		}
	}
	assert.Zero(t, leaks, "rows escaped the retire cascade")
	st, err := s.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Zero(t, nonTerminal(st), "every row of every retired shard must be terminal")
	assert.Equal(t, int64(len(allRows)), st.Invalid)
}

// I1 (deterministic): a shard retired between AcquireShard's discovery query
// and its winning write must not be adopted.
func TestAcquireShardRechecksStatusInWrite(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, testHost(t), clk.Now)
	ctx := context.Background()
	sh, _ := storetest.MintShard(t, s, "main", 1, store.ShardMined)

	var once sync.Once
	s.queryHook = func(set string, _ []*as.Record) {
		if set != s.shardsSet {
			return
		}
		once.Do(func() {
			_, err := s.RetireShard(ctx, "main", sh.TxID)
			assert.NoError(t, err)
		})
	}
	got, err := s.AcquireShard(ctx, "main", "h", clk.Now(), clk.Now().Add(time.Minute), []store.ShardStatus{store.ShardMined}, nil)
	s.queryHook = nil
	require.NoError(t, err)
	assert.Nil(t, got, "a shard retired after discovery must not be adopted")
	cur, err := s.GetShard(ctx, "main", sh.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardInvalid, cur.Status)
	assert.Empty(t, cur.Owner, "the retired shard must not carry an owner")
}

// I1 (race): acquires racing a retire loop never return a non-matching status.
func TestAcquireVsRetireRace(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, testHost(t), clk.Now)
	ctx := context.Background()
	shards := make([]store.Shard, 0, 150)
	for range 150 {
		sh, _ := storetest.MintShard(t, s, "main", 1, store.ShardMined)
		shards = append(shards, sh)
		clk.Advance(time.Millisecond)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, sh := range shards {
			_, err := s.RetireShard(ctx, "main", sh.TxID)
			assert.NoError(t, err)
		}
	}()
	for {
		select {
		case <-done:
			list, err := s.ListShards(ctx, "main", store.ShardFilter{Statuses: []store.ShardStatus{store.ShardInvalid}})
			require.NoError(t, err)
			for _, sh := range list {
				assert.Empty(t, sh.Owner, "retired shard %x adopted after retire", sh.TxID[:4])
			}
			return
		default:
		}
		got, err := s.AcquireShard(ctx, "main", "h", clk.Now(), clk.Now().Add(time.Minute), []store.ShardStatus{store.ShardMined}, nil)
		if err != nil {
			<-done
			require.NoError(t, err)
		}
		if got != nil {
			assert.Equal(t, store.ShardMined, got.Status, "adopted a shard whose status is not in statuses")
			assert.NoError(t, s.ReleaseShard(ctx, "main", got.TxID, "h"))
		}
	}
}

// I3: edge before/after values match nothing instead of erroring.
func TestQuarantineEdgeTimes(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, testHost(t), clk.Now)
	ctx := context.Background()
	_, rows := storetest.MintShard(t, s, "main", 2, store.ShardMined)
	tok := store.NewToken()
	storetest.MustReserve(t, s, "main", rows, tok, clk.Now().Add(time.Minute))
	_, err := s.Spend(ctx, "main", tok, storetest.Ops(rows[:1]), storetest.NewTxID(), false)
	require.NoError(t, err) // one quarantined, one reserved: both queries have something to (not) find

	for _, before := range []time.Time{{}, time.Unix(0, 0), time.Unix(-100, 0), time.Unix(0, 1000)} {
		n, err := s.Quarantine(ctx, "main", before, 10)
		require.NoError(t, err, "Quarantine(before=%v)", before)
		assert.Zero(t, n)
		got, err := s.ListQuarantined(ctx, "main", before, nil, 10)
		require.NoError(t, err, "ListQuarantined(before=%v)", before)
		assert.Empty(t, got)
	}
	for _, at := range []time.Time{clk.Now(), clk.Now().Add(time.Hour)} {
		got, err := s.ListQuarantined(ctx, "main", clk.Now(), &store.RowCursor{At: at}, 10)
		require.NoError(t, err, "after.At=%v >= before", at)
		assert.Empty(t, got)
	}
	got, err := s.ListQuarantined(ctx, "main", clk.Now().Add(time.Second), nil, 10)
	require.NoError(t, err)
	assert.Len(t, got, 1, "control: the quarantined row is listed for a normal before")
}

// countRange counts this tenant's records with bin in [1, hi], evaluating
// no state filter at all: exactly what a range query on that index has to
// examine.
func countRange(t *testing.T, s *Store, bin string, hi int64) int {
	t.Helper()
	stmt := as.NewStatement(s.ns, s.rowsSet)
	stmt.Filter = as.NewRangeFilter(bin, 1, hi)
	qp := s.queryPolicy(context.Background())
	qp.IncludeBinData = false
	qp.FilterExpression = s.tenantExpr()
	n := 0
	require.NoError(t, s.query(qp, stmt, func(*as.Record) { n++ }))
	return n
}

// I4: terminal rows leave the expiry/quarantine indexes (so Quarantine and
// ListQuarantined never examine them again) while keeping provenance.
func TestTerminalRowsLeaveExpiryAndQuarantineIndexes(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, testHost(t), clk.Now)
	ctx := context.Background()
	far := us(clk.Now().Add(24 * time.Hour))
	const n = 1000
	exp := clk.Now().Add(time.Minute)

	_, spentRows := storetest.MintShard(t, s, "main", n, store.ShardMined)
	tok := store.NewToken()
	storetest.MustReserve(t, s, "main", spentRows, tok, exp)
	assert.Equal(t, n, countRange(t, s, binRX, far), "control: reserved rows are in rx")
	got, err := s.Spend(ctx, "main", tok, storetest.Ops(spentRows), storetest.NewTxID(), true)
	require.NoError(t, err)
	require.Equal(t, n, got)

	_, resolvedRows := storetest.MintShard(t, s, "main", n, store.ShardMined)
	tok2 := store.NewToken()
	storetest.MustReserve(t, s, "main", resolvedRows, tok2, exp)
	_, err = s.Spend(ctx, "main", tok2, storetest.Ops(resolvedRows), storetest.NewTxID(), false)
	require.NoError(t, err)
	assert.Equal(t, n, countRange(t, s, binQX, far), "control: quarantined rows are in qx")
	spentBy := storetest.NewTxID()
	got, err = s.Resolve(ctx, "main", storetest.Ops(resolvedRows), store.StateSpent, &spentBy)
	require.NoError(t, err)
	require.Equal(t, n, got)

	assert.Zero(t, countRange(t, s, binRX, far), "terminal rows must not stay in the expiry index")
	assert.Zero(t, countRange(t, s, binQX, far), "terminal rows must not stay in the quarantine index")

	q, err := s.Quarantine(ctx, "main", clk.Now().Add(time.Hour), 100)
	require.NoError(t, err)
	assert.Zero(t, q)
	lq, err := s.ListQuarantined(ctx, "main", clk.Now().Add(time.Hour), nil, 100)
	require.NoError(t, err)
	assert.Empty(t, lq)

	// Provenance survives.
	for _, r := range storetest.RowsByOutpoint(t, s, "main", storetest.Ops(spentRows[:5])) {
		assert.Equal(t, exp.Truncate(time.Microsecond), r.ExpiresAt)
	}
	for _, r := range storetest.RowsByOutpoint(t, s, "main", storetest.Ops(resolvedRows[:5])) {
		assert.False(t, r.QuarantinedAt.IsZero())
		assert.Equal(t, exp.Truncate(time.Microsecond), r.ExpiresAt)
	}
}

// I4: retire removes rx/qx too.
func TestRetireRemovesIndexOnlyBins(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, testHost(t), clk.Now)
	ctx := context.Background()
	sh, rows := storetest.MintShard(t, s, "main", 4, store.ShardMined)
	tok := store.NewToken()
	storetest.MustReserve(t, s, "main", rows[:2], tok, clk.Now().Add(time.Minute))
	_, err := s.Spend(ctx, "main", tok, storetest.Ops(rows[1:2]), storetest.NewTxID(), false)
	require.NoError(t, err)
	_, err = s.RetireShard(ctx, "main", sh.TxID)
	require.NoError(t, err)
	far := us(clk.Now().Add(24 * time.Hour))
	assert.Zero(t, countRange(t, s, binRX, far))
	assert.Zero(t, countRange(t, s, binQX, far))
}

// I5: AcquireShard/ListShards discovery queries never transfer BEEF.
func TestShardDiscoveryNeverFetchesBEEF(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, testHost(t), clk.Now)
	ctx := context.Background()
	for range 3 {
		sh, rows := storetest.BuildShard("main", 1, store.ShardMined)
		sh.BEEF = make([]byte, 50_000)
		require.NoError(t, s.PutShard(ctx, sh, rows))
		clk.Advance(time.Millisecond)
	}
	var mu sync.Mutex
	var seen []*as.Record
	s.queryHook = func(set string, recs []*as.Record) {
		if set == s.shardsSet {
			mu.Lock()
			seen = append(seen, recs...)
			mu.Unlock()
		}
	}
	got, err := s.AcquireShard(ctx, "main", "h", clk.Now(), clk.Now().Add(time.Minute), []store.ShardStatus{store.ShardMined}, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Len(t, got.BEEF, 50_000, "the adopted shard is returned in full")
	owner := "h"
	list, err := s.ListShards(ctx, "main", store.ShardFilter{Owner: &owner})
	require.NoError(t, err)
	require.Len(t, list, 1, "owner filter")
	s.queryHook = nil

	require.NotEmpty(t, seen)
	for _, rec := range seen {
		_, has := rec.Bins[binBEEF]
		assert.False(t, has, "discovery query transferred BEEF")
	}
}

// I6: New refuses a non-SC namespace unless explicitly opted out.
func TestNewRequiresStrongConsistency(t *testing.T) {
	host := testHost(t)
	_, err := New(context.Background(), Config{Hosts: []string{host}, Namespace: "test", SetPrefix: testSetPrefix, KeyPrefix: randKeyPrefix(), PoolSize: 2})
	require.ErrorIs(t, err, ErrNotStrongConsistency, "the test server is CE (strong-consistency=false)")
}

// I6: ReleaseLease is a guarded update, never a delete.
func TestReleaseLeaseKeepsRecord(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, testHost(t), clk.Now)
	ctx := context.Background()
	exp, err := s.TryAcquireOrRenew(ctx, "job", "a", time.Minute)
	require.NoError(t, err)
	require.False(t, exp.IsZero())

	require.NoError(t, s.ReleaseLease(ctx, "job", "b"), "non-owner release is a no-op")
	key, err := s.leaseKey("job")
	require.NoError(t, err)
	rec, aerr := s.client.Get(nil, key)
	require.Nil(t, aerr)
	assert.Equal(t, "a", rec.Bins[binHolder])

	require.NoError(t, s.ReleaseLease(ctx, "job", "a"))
	rec, aerr = s.client.Get(nil, key)
	require.Nil(t, aerr, "the lease record must survive a release")
	assert.Empty(t, rec.Bins[binHolder])
	assert.Equal(t, int64(0), toInt64(rec.Bins[binExp]))

	exp, err = s.TryAcquireOrRenew(ctx, "job", "b", time.Minute)
	require.NoError(t, err)
	assert.False(t, exp.IsZero(), "a released lease is free")
}

// I7: stores with different KeyPrefixes sharing the fixed sets are fully
// isolated, even for the same pool and txid, and Truncate removes exactly
// one tenant's records.
func TestKeyPrefixIsolationAndTruncate(t *testing.T) {
	host := testHost(t)
	clk := storetest.NewClock(storetest.Start)
	ctx := context.Background()
	a := openTestStore(t, host, randKeyPrefix(), clk.Now)
	b := isolatedStore(t, host, clk.Now)
	txid := storetest.NewTxID()
	_, rowsA := storetest.MintShardWithTxID(t, a, "main", txid, 3, store.ShardMined)
	storetest.MintShardWithTxID(t, b, "main", txid, 5, store.ShardMined)
	_, err := a.TryAcquireOrRenew(ctx, "job", "x", time.Minute)
	require.NoError(t, err)
	exp, err := b.TryAcquireOrRenew(ctx, "job", "y", time.Minute)
	require.NoError(t, err)
	require.False(t, exp.IsZero(), "leases are per tenant")
	tok := store.NewToken()
	storetest.MustReserve(t, a, "main", rowsA[:1], tok, clk.Now().Add(time.Minute))
	_, err = a.Spend(ctx, "main", tok, storetest.Ops(rowsA[:1]), storetest.NewTxID(), false)
	require.NoError(t, err)

	availB, err := b.ListAvailable(ctx, "main", txid, 0, 100)
	require.NoError(t, err)
	assert.Len(t, availB, 5)
	lqB, err := b.ListQuarantined(ctx, "main", clk.Now().Add(time.Hour), nil, 100)
	require.NoError(t, err)
	assert.Empty(t, lqB, "range queries are tenant-filtered")
	stA, err := a.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, store.Stats{Available: 2, Quarantined: 1, ShardsMined: 1}, stA)

	require.NoError(t, a.Truncate())
	for _, set := range a.allSets() {
		n, err := a.countEqual(ctx, set, binTN, a.keyPrefix)
		require.NoError(t, err)
		assert.Zero(t, n, "set %s still holds tenant records after Truncate", set)
	}
	stB, err := b.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, store.Stats{Available: 5, ShardsMined: 1}, stB, "the other tenant is untouched")

	plain := &Store{keyPrefix: ""}
	require.ErrorIs(t, plain.Truncate(), store.ErrInvalidArgument, "Truncate without a KeyPrefix would wipe shared sets")
}

// RenewShard only moves the lease: no UpdatedAt stamp, no Version bump.
func TestRenewShardTouchesOnlyLease(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, testHost(t), clk.Now)
	ctx := context.Background()
	sh, _ := storetest.MintShard(t, s, "main", 1, store.ShardMined)
	acq, err := s.AcquireShard(ctx, "main", "h", clk.Now(), clk.Now().Add(time.Minute), []store.ShardStatus{store.ShardMined}, nil)
	require.NoError(t, err)
	require.NotNil(t, acq)
	clk.Advance(time.Second)
	_, err = s.RenewShard(ctx, "main", sh.TxID, "h", clk.Now().Add(time.Hour))
	require.NoError(t, err)
	got, err := s.GetShard(ctx, "main", sh.TxID)
	require.NoError(t, err)
	assert.Equal(t, acq.UpdatedAt, got.UpdatedAt)
	assert.Equal(t, acq.Version, got.Version)
	assert.Equal(t, clk.Now().Add(time.Hour).Truncate(time.Microsecond), got.LeaseUntil)
}
