package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

func testReserveBasic(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	_, rows := MintShard(t, s, "main", 4, store.ShardMined)
	tokA, tokB := store.NewToken(), store.NewToken()
	exp := clk.Now().Add(time.Minute)
	t0 := clk.Now()

	applied, err := s.Reserve(ctx, "main", ReserveOps(rows[:2], tokA, exp))
	require.NoError(t, err)
	require.Len(t, applied, 2)
	for _, op := range applied {
		assert.Equal(t, tokA, op.Token)
		assert.Equal(t, exp, op.ExpiresAt.UTC())
	}
	got := RowsByOutpoint(t, s, "main", Ops(rows[:2]))
	for _, r := range rows[:2] {
		g := got[r.Outpoint]
		assert.Equal(t, store.StateReserved, g.State)
		assert.Equal(t, tokA, g.ReservedBy)
		assert.Equal(t, t0, g.ReservedAt.UTC())
		assert.Equal(t, exp, g.ExpiresAt.UTC())
		assert.Equal(t, t0, g.UpdatedAt.UTC())
	}

	applied, err = s.Reserve(ctx, "main", ReserveOps(rows[:2], tokB, exp))
	require.NoError(t, err)
	assert.Empty(t, applied, "another token cannot take reserved rows")

	clk.Advance(5 * time.Second)
	applied, err = s.Reserve(ctx, "main", ReserveOps(rows[:2], tokA, clk.Now().Add(time.Hour)))
	require.NoError(t, err)
	require.Len(t, applied, 2, "same-token replay is applied (idempotent)")
	for _, op := range applied {
		assert.Equal(t, exp, op.ExpiresAt.UTC(), "replay reports the original expiry")
	}
	got = RowsByOutpoint(t, s, "main", Ops(rows[:2]))
	assert.Equal(t, t0, got[rows[0].Outpoint].ReservedAt.UTC(), "replay does not re-stamp reserved_at")
	assert.Equal(t, exp, got[rows[0].Outpoint].ExpiresAt.UTC(), "replay does not extend expiry")

	unknown := []store.ReserveOp{{Outpoint: store.Outpoint{TxID: NewTxID(), Vout: 0}, Token: tokA, ExpiresAt: exp}}
	applied, err = s.Reserve(ctx, "main", unknown)
	require.NoError(t, err)
	assert.Empty(t, applied, "unknown outpoints are skipped, not errors")

	applied, err = s.Reserve(ctx, "other", ReserveOps(rows[2:3], tokA, exp))
	require.NoError(t, err)
	assert.Empty(t, applied, "pool must match")

	_, err = s.Reserve(ctx, "main", nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.Reserve(ctx, "main", append(ReserveOps(rows[2:3], tokA, exp), ReserveOps(rows[2:3], tokB, exp)...))
	require.ErrorIs(t, err, store.ErrInvalidArgument, "duplicate outpoint in one batch")
	_, err = s.Reserve(ctx, "main", ReserveOps(rows[2:3], store.Token{}, exp))
	require.ErrorIs(t, err, store.ErrInvalidArgument, "zero token")
	tooMany := make([]store.ReserveOp, s.Capabilities().MaxBatch+1)
	for i := range tooMany {
		tooMany[i] = store.ReserveOp{Outpoint: store.Outpoint{TxID: NewTxID(), Vout: uint32(i)}, Token: tokA, ExpiresAt: exp}
	}
	_, err = s.Reserve(ctx, "main", tooMany)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "batch above MaxBatch")
}

func testReserveMixedTokensMixedShards(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	_, a := MintShard(t, s, "main", 3, store.ShardMined)
	_, b := MintShard(t, s, "main", 3, store.ShardMined)
	x, y, z := store.NewToken(), store.NewToken(), store.NewToken()
	e := func(sec int) time.Time { return clk.Now().Add(time.Duration(sec) * time.Second) }
	ops := []store.ReserveOp{
		{Outpoint: a[0].Outpoint, Token: x, ExpiresAt: e(10)},
		{Outpoint: b[1].Outpoint, Token: y, ExpiresAt: e(20)},
		{Outpoint: a[2].Outpoint, Token: x, ExpiresAt: e(30)},
		{Outpoint: b[0].Outpoint, Token: z, ExpiresAt: e(40)},
	}
	applied, err := s.Reserve(ctx, "main", ops)
	require.NoError(t, err)
	require.Len(t, applied, 4)

	got := RowsByOutpoint(t, s, "main", []store.Outpoint{a[0].Outpoint, b[1].Outpoint, a[2].Outpoint, b[0].Outpoint, a[1].Outpoint, b[2].Outpoint})
	for _, op := range ops {
		g := got[op.Outpoint]
		assert.Equal(t, store.StateReserved, g.State, op.Outpoint)
		assert.Equal(t, op.Token, g.ReservedBy, "row carries exactly its own token")
		assert.Equal(t, op.ExpiresAt, g.ExpiresAt.UTC(), "row carries exactly its own expiry")
	}
	assert.Equal(t, store.StateAvailable, got[a[1].Outpoint].State)
	assert.Equal(t, store.StateAvailable, got[b[2].Outpoint].State)
}

func testReleaseGuard(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	shard, rows := MintShard(t, s, "main", 3, store.ShardMined)
	tokA, tokB := store.NewToken(), store.NewToken()
	MustReserve(t, s, "main", rows, tokA, clk.Now().Add(time.Minute))

	n, err := s.Release(ctx, "main", tokB, Ops(rows[:1]))
	require.NoError(t, err)
	assert.Equal(t, 0, n, "wrong token")

	n, err = s.Release(ctx, "main", tokA, Ops(rows[:2]))
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	got := RowsByOutpoint(t, s, "main", Ops(rows))
	for _, r := range rows[:2] {
		g := got[r.Outpoint]
		assert.Equal(t, store.StateAvailable, g.State)
		assert.True(t, g.ReservedBy.IsZero())
		assert.True(t, g.ReservedAt.IsZero())
		assert.True(t, g.ExpiresAt.IsZero())
	}
	assert.Equal(t, store.StateReserved, got[rows[2].Outpoint].State)

	n, err = s.Release(ctx, "main", tokA, Ops(rows[:1]))
	require.NoError(t, err)
	assert.Equal(t, 0, n, "already available")

	n, err = s.Spend(ctx, "main", tokA, Ops(rows[2:3]), NewTxID(), false)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	n, err = s.Release(ctx, "main", tokA, Ops(rows[2:3]))
	require.NoError(t, err)
	assert.Equal(t, 0, n, "quarantined rows are not releasable")

	avail, err := s.ListAvailable(ctx, "main", shard.TxID, 0, 10)
	require.NoError(t, err)
	require.Len(t, avail, 2)

	_, err = s.Release(ctx, "main", tokA, nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
}

func testSpendGuard(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	_, rows := MintShard(t, s, "main", 3, store.ShardMined)
	tokA, tokB := store.NewToken(), store.NewToken()
	tx1, tx2 := NewTxID(), NewTxID()
	MustReserve(t, s, "main", rows, tokA, clk.Now().Add(time.Minute))
	clk.Advance(time.Second)
	t1 := clk.Now()

	n, err := s.Spend(ctx, "main", tokB, Ops(rows[:1]), tx1, false)
	require.NoError(t, err)
	assert.Equal(t, 0, n, "wrong token")

	n, err = s.Spend(ctx, "main", tokA, Ops(rows[:1]), tx1, false)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	g := RowsByOutpoint(t, s, "main", Ops(rows[:1]))[rows[0].Outpoint]
	assert.Equal(t, store.StateQuarantined, g.State)
	require.NotNil(t, g.SpentHint)
	assert.Equal(t, tx1, *g.SpentHint)
	assert.Nil(t, g.SpentBy)
	assert.Equal(t, t1, g.QuarantinedAt.UTC())
	assert.Equal(t, tokA, g.ReservedBy, "provenance kept")

	n, err = s.Spend(ctx, "main", tokA, Ops(rows[:1]), tx2, false)
	require.NoError(t, err)
	assert.Equal(t, 0, n, "second spend is ignored")
	g = RowsByOutpoint(t, s, "main", Ops(rows[:1]))[rows[0].Outpoint]
	assert.Equal(t, tx1, *g.SpentHint, "hint unchanged")

	n, err = s.Spend(ctx, "main", tokA, Ops(rows[1:2]), tx1, true)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	g = RowsByOutpoint(t, s, "main", Ops(rows[1:2]))[rows[1].Outpoint]
	assert.Equal(t, store.StateSpent, g.State)
	require.NotNil(t, g.SpentBy)
	assert.Equal(t, tx1, *g.SpentBy)
	assert.Nil(t, g.SpentHint)

	applied, err := s.Reserve(ctx, "main", ReserveOps(rows[1:2], tokB, clk.Now().Add(time.Minute)))
	require.NoError(t, err)
	assert.Empty(t, applied, "spent is terminal")
	n, err = s.Release(ctx, "main", tokA, Ops(rows[1:2]))
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func testQuarantineByExpiry(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	_, rows := MintShard(t, s, "main", 6, store.ShardMined)
	tokA, tokB, tokC := store.NewToken(), store.NewToken(), store.NewToken()
	t0 := clk.Now()
	MustReserve(t, s, "main", rows[0:2], tokA, t0.Add(10*time.Second))
	MustReserve(t, s, "main", rows[2:4], tokB, t0.Add(20*time.Second))
	MustReserve(t, s, "main", rows[4:5], tokC, t0.Add(30*time.Second))

	n, err := s.Quarantine(ctx, "main", clk.Now(), 10)
	require.NoError(t, err)
	assert.Equal(t, 0, n, "nothing expired yet")
	_, err = s.Quarantine(ctx, "main", clk.Now(), 0)
	require.ErrorIs(t, err, store.ErrInvalidArgument)

	clk.Advance(15 * time.Second)
	n, err = s.Quarantine(ctx, "main", clk.Now(), 1)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "limit honoured")
	n, err = s.Quarantine(ctx, "main", clk.Now(), 10)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "the other tokA row")
	got := RowsByOutpoint(t, s, "main", Ops(rows))
	for _, r := range rows[0:2] {
		g := got[r.Outpoint]
		assert.Equal(t, store.StateQuarantined, g.State)
		assert.Equal(t, clk.Now(), g.QuarantinedAt.UTC())
		assert.Nil(t, g.SpentHint)
		assert.Equal(t, tokA, g.ReservedBy)
	}
	for _, r := range rows[2:5] {
		assert.Equal(t, store.StateReserved, got[r.Outpoint].State)
	}
	assert.Equal(t, store.StateAvailable, got[rows[5].Outpoint].State)

	q, err := s.ListQuarantined(ctx, "main", clk.Now().Add(time.Second), nil, 10)
	require.NoError(t, err)
	require.Len(t, q, 2)
	q, err = s.ListQuarantined(ctx, "main", clk.Now(), nil, 10)
	require.NoError(t, err)
	assert.Empty(t, q, "before is exclusive")

	n, err = s.Release(ctx, "main", tokA, Ops(rows[0:1]))
	require.NoError(t, err)
	assert.Equal(t, 0, n, "expired-and-quarantined rows cannot be released")

	clk.Advance(10 * time.Second) // t0+25s: tokB expired
	n, err = s.Release(ctx, "main", tokB, Ops(rows[2:3]))
	require.NoError(t, err)
	assert.Equal(t, 1, n, "a not-yet-quarantined expired row can still be released")
	n, err = s.Quarantine(ctx, "main", clk.Now(), 10)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "only rows[3]")

	q, err = s.ListQuarantined(ctx, "main", clk.Now().Add(time.Second), nil, 10)
	require.NoError(t, err)
	require.Len(t, q, 3)
	assert.Equal(t, rows[3].Outpoint, q[2].Outpoint, "oldest quarantined first")
	q, err = s.ListQuarantined(ctx, "main", clk.Now().Add(time.Second), nil, 2)
	require.NoError(t, err)
	require.Len(t, q, 2)
}

func testResolveGuard(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	shard, rows := MintShard(t, s, "main", 3, store.ShardMined)
	tokA := store.NewToken()
	tx1 := NewTxID()
	MustReserve(t, s, "main", rows, tokA, clk.Now().Add(time.Minute))
	_, err := s.Spend(ctx, "main", tokA, Ops(rows[:2]), tx1, false)
	require.NoError(t, err)

	n, err := s.Resolve(ctx, "main", Ops(rows[:1]), store.StateAvailable, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	g := RowsByOutpoint(t, s, "main", Ops(rows[:1]))[rows[0].Outpoint]
	assert.Equal(t, store.StateAvailable, g.State)
	assert.True(t, g.ReservedBy.IsZero())
	assert.True(t, g.ExpiresAt.IsZero())
	assert.True(t, g.QuarantinedAt.IsZero())
	assert.Nil(t, g.SpentHint)
	avail, err := s.ListAvailable(ctx, "main", shard.TxID, 0, 10)
	require.NoError(t, err)
	require.Len(t, avail, 1)

	n, err = s.Resolve(ctx, "main", Ops(rows[:1]), store.StateAvailable, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, n, "only quarantined rows resolve")

	n, err = s.Resolve(ctx, "main", Ops(rows[1:2]), store.StateSpent, &tx1)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	g = RowsByOutpoint(t, s, "main", Ops(rows[1:2]))[rows[1].Outpoint]
	assert.Equal(t, store.StateSpent, g.State)
	require.NotNil(t, g.SpentBy)
	assert.Equal(t, tx1, *g.SpentBy)

	n, err = s.Resolve(ctx, "main", Ops(rows[2:3]), store.StateSpent, &tx1)
	require.NoError(t, err)
	assert.Equal(t, 0, n, "reserved rows do not resolve")

	_, err = s.Resolve(ctx, "main", Ops(rows[2:3]), store.StateReserved, nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.Resolve(ctx, "main", Ops(rows[2:3]), store.StateSpent, nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument)

	// spent is terminal for every operation
	applied, err := s.Reserve(ctx, "main", ReserveOps(rows[1:2], store.NewToken(), clk.Now().Add(time.Minute)))
	require.NoError(t, err)
	assert.Empty(t, applied)
	n, err = s.Release(ctx, "main", tokA, Ops(rows[1:2]))
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	n, err = s.Spend(ctx, "main", tokA, Ops(rows[1:2]), tx1, false)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	n, err = s.Resolve(ctx, "main", Ops(rows[1:2]), store.StateAvailable, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func testListAvailablePagination(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	shard, rows := MintShard(t, s, "main", 25, store.ShardMined)
	MintShard(t, s, "main", 5, store.ShardMined) // another shard must not leak in

	const pageLimit = 10
	page := func(from uint32) []uint32 {
		got, err := s.ListAvailable(ctx, "main", shard.TxID, from, pageLimit)
		require.NoError(t, err)
		vouts := make([]uint32, len(got))
		for i, r := range got {
			require.Equal(t, shard.TxID, r.TxID)
			vouts[i] = r.Vout
		}
		return vouts
	}
	assert.Equal(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, page(0))
	assert.Equal(t, []uint32{10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, page(10))
	assert.Equal(t, []uint32{20, 21, 22, 23, 24}, page(20))
	assert.Empty(t, page(25))

	tok := store.NewToken()
	MustReserve(t, s, "main", rows[3:4], tok, clk.Now().Add(time.Minute))
	assert.Equal(t, []uint32{0, 1, 2, 4, 5, 6, 7, 8, 9, 10}, page(0), "reserved row skipped")

	n, err := s.Spend(ctx, "main", tok, Ops(rows[3:4]), NewTxID(), false)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	n, err = s.Resolve(ctx, "main", Ops(rows[3:4]), store.StateAvailable, nil)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	assert.Equal(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, page(0), "resolved row is back")

	_, err = s.ListAvailable(ctx, "main", shard.TxID, 0, 0)
	require.ErrorIs(t, err, store.ErrInvalidArgument)

	all := RowsByOutpoint(t, s, "main", Ops(rows))
	require.Len(t, all, 25)
	_, err = s.GetRows(ctx, "main", nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	missing, err := s.GetRows(ctx, "main", []store.Outpoint{{TxID: NewTxID(), Vout: 0}})
	require.NoError(t, err)
	assert.Empty(t, missing)
}

func testStatsCounts(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	_, a := MintShard(t, s, "main", 5, store.ShardUnmined)
	b, _ := MintShard(t, s, "main", 5, store.ShardMined)
	MintShard(t, s, "other", 2, store.ShardMined)
	tok := store.NewToken()
	tx := NewTxID()
	MustReserve(t, s, "main", a[:3], tok, clk.Now().Add(time.Minute))
	_, err := s.Spend(ctx, "main", tok, Ops(a[:1]), tx, false)
	require.NoError(t, err)
	_, err = s.Spend(ctx, "main", tok, Ops(a[1:2]), tx, true)
	require.NoError(t, err)
	_, err = s.RetireShard(ctx, "main", b.TxID)
	require.NoError(t, err)

	st, err := s.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, store.Stats{
		Available: 2, Reserved: 1, Quarantined: 1, Spent: 1, Invalid: 5,
		ShardsUnmined: 1, ShardsMined: 0, ShardsRetired: 0, ShardsInvalid: 1,
	}, st)

	empty, err := s.Stats(ctx, "nothing")
	require.NoError(t, err)
	assert.Equal(t, store.Stats{}, empty)
}
