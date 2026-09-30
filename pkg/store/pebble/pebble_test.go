package pebble_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	pebblestore "github.com/bsv-blockchain/fuelkeeper/pkg/store/pebble"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.RunSuite(t, func(t *testing.T, now func() time.Time) store.Store {
		s, err := pebblestore.Open(context.Background(), pebblestore.Config{Path: t.TempDir(), NoSync: true}, pebblestore.WithClock(now))
		require.NoError(t, err)
		return s
	})
}

func TestReopenKeepsState(t *testing.T) {
	dir := t.TempDir()
	clk := storetest.NewClock(storetest.Start)
	s, err := pebblestore.Open(context.Background(), pebblestore.Config{Path: dir}, pebblestore.WithClock(clk.Now))
	require.NoError(t, err)
	sh, rows := storetest.MintShard(t, s, "main", 3, store.ShardMined)
	tok := store.NewToken()
	storetest.MustReserve(t, s, "main", rows[:1], tok, clk.Now().Add(time.Minute))
	require.NoError(t, s.Close())

	s, err = pebblestore.Open(context.Background(), pebblestore.Config{Path: dir}, pebblestore.WithClock(clk.Now))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	st, err := s.Stats(context.Background(), "main")
	require.NoError(t, err)
	require.Equal(t, int64(2), st.Available)
	require.Equal(t, int64(1), st.Reserved)
	got, err := s.GetShard(context.Background(), "main", sh.TxID)
	require.NoError(t, err)
	require.Equal(t, sh.BEEF, got.BEEF)
	avail, err := s.ListAvailable(context.Background(), "main", sh.TxID, 0, 10)
	require.NoError(t, err)
	require.Len(t, avail, 2)
}

func TestClosedStoreReturnsErrors(t *testing.T) {
	ctx := context.Background()
	s, err := pebblestore.Open(ctx, pebblestore.Config{Path: t.TempDir(), NoSync: true})
	require.NoError(t, err)
	sh, rows := storetest.MintShard(t, s, "main", 1, store.ShardMined)
	require.NoError(t, s.Close())
	require.NoError(t, s.Close(), "Close must be idempotent")

	require.Error(t, s.Ping(ctx))

	_, err = s.GetShard(ctx, "main", sh.TxID)
	require.Error(t, err)
	require.Error(t, s.UpdateShard(ctx, "main", sh.TxID, store.ShardMined, nil))
	_, err = s.RetireShard(ctx, "main", sh.TxID)
	require.Error(t, err)
	_, err = s.AcquireShard(ctx, "main", "h", time.Now(), time.Now().Add(time.Minute), []store.ShardStatus{store.ShardMined}, nil)
	require.Error(t, err)
	_, err = s.RenewShard(ctx, "main", sh.TxID, "h", time.Now().Add(time.Minute))
	require.Error(t, err)
	require.Error(t, s.ReleaseShard(ctx, "main", sh.TxID, "h"))
	_, err = s.ListShards(ctx, "main", store.ShardFilter{})
	require.Error(t, err)
	require.Error(t, s.PutShard(ctx, sh, rows))
	_, err = s.ListAvailable(ctx, "main", sh.TxID, 0, 10)
	require.Error(t, err)
	_, err = s.GetRows(ctx, "main", storetest.Ops(rows))
	require.Error(t, err)
	_, err = s.Reserve(ctx, "main", storetest.ReserveOps(rows, store.NewToken(), time.Now().Add(time.Minute)))
	require.Error(t, err)
	_, err = s.Release(ctx, "main", store.NewToken(), storetest.Ops(rows))
	require.Error(t, err)
	_, err = s.Spend(ctx, "main", store.NewToken(), storetest.Ops(rows), storetest.NewTxID(), false)
	require.Error(t, err)
	_, err = s.Quarantine(ctx, "main", time.Now(), 10)
	require.Error(t, err)
	_, err = s.ListQuarantined(ctx, "main", time.Now(), nil, 10)
	require.Error(t, err)
	_, err = s.Resolve(ctx, "main", storetest.Ops(rows), store.StateAvailable, nil)
	require.Error(t, err)
	_, err = s.Stats(ctx, "main")
	require.Error(t, err)
	_, err = s.TryAcquireOrRenew(ctx, "jobs/x", "h", time.Minute)
	require.Error(t, err)
	require.Error(t, s.ReleaseLease(ctx, "jobs/x", "h"))
}

func TestCountersMatchScans(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	s, err := pebblestore.Open(ctx, pebblestore.Config{Path: t.TempDir(), NoSync: true}, pebblestore.WithClock(clk.Now))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	const pool = "main"
	var rows []store.Row
	for i := 0; i < 6; i++ {
		_, rs := storetest.MintShard(t, s, pool, 5, store.ShardMined)
		rows = append(rows, rs...)
	}

	rng := newRNG(12345)
	var tokens []store.Token
	for i := 0; i < 20; i++ {
		tokens = append(tokens, store.NewToken())
	}

	for i := 0; i < 400; i++ {
		clk.Advance(time.Millisecond)
		op := rows[rng.Intn(len(rows))].Outpoint
		tok := tokens[rng.Intn(len(tokens))]
		switch rng.Intn(7) {
		case 0:
			_, _ = s.Reserve(ctx, pool, []store.ReserveOp{{Outpoint: op, Token: tok, ExpiresAt: clk.Now().Add(time.Minute)}})
		case 1:
			_, _ = s.Release(ctx, pool, tok, []store.Outpoint{op})
		case 2:
			_, _ = s.Spend(ctx, pool, tok, []store.Outpoint{op}, storetest.NewTxID(), false)
		case 3:
			_, _ = s.Spend(ctx, pool, tok, []store.Outpoint{op}, storetest.NewTxID(), true)
		case 4:
			_, _ = s.Quarantine(ctx, pool, clk.Now(), 5)
		case 5:
			txid := storetest.NewTxID()
			_, _ = s.Resolve(ctx, pool, []store.Outpoint{op}, store.StateAvailable, &txid)
		case 6:
			if rng.Intn(10) == 0 {
				sh := rows[rng.Intn(len(rows))].TxID
				_, _ = s.RetireShard(ctx, pool, sh)
			}
		}
	}

	st, err := s.Stats(ctx, pool)
	require.NoError(t, err)
	scanned := pebblestore.ScanStats(t, s, pool)
	require.Equal(t, scanned, st)
}

// newRNG returns a tiny deterministic PRNG (avoids importing math/rand here
// twice with differing seeding conventions across Go versions).
func newRNG(seed int64) *rngState { return &rngState{state: uint64(seed) | 1} }

type rngState struct{ state uint64 }

func (r *rngState) Intn(n int) int {
	// xorshift64*
	r.state ^= r.state >> 12
	r.state ^= r.state << 25
	r.state ^= r.state >> 27
	v := r.state * 2685821657736338717
	return int(v % uint64(n)) //nolint:gosec // test-only PRNG
}
