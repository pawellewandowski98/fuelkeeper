package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

func testLeaserContract(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	const name = "jobs/reaper"

	_, err := s.TryAcquireOrRenew(ctx, "", "a", time.Second)
	require.ErrorIs(t, err, store.ErrInvalidArgument)

	until, err := s.TryAcquireOrRenew(ctx, name, "a", 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, clk.Now().Add(10*time.Second), until.UTC())

	lost, err := s.TryAcquireOrRenew(ctx, name, "b", 10*time.Second)
	require.NoError(t, err)
	assert.True(t, lost.IsZero(), "b must not steal an unexpired lease")

	clk.Advance(5 * time.Second)
	until, err = s.TryAcquireOrRenew(ctx, name, "a", 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, clk.Now().Add(10*time.Second), until.UTC(), "holder renews")

	clk.Advance(11 * time.Second)
	until, err = s.TryAcquireOrRenew(ctx, name, "b", 10*time.Second)
	require.NoError(t, err)
	assert.False(t, until.IsZero(), "expired lease is adoptable")

	lost, err = s.TryAcquireOrRenew(ctx, name, "a", 10*time.Second)
	require.NoError(t, err)
	assert.True(t, lost.IsZero(), "old holder lost it")

	require.NoError(t, s.ReleaseLease(ctx, name, "a"), "non-holder release is a no-op")
	lost, err = s.TryAcquireOrRenew(ctx, name, "a", 10*time.Second)
	require.NoError(t, err)
	assert.True(t, lost.IsZero())

	require.NoError(t, s.ReleaseLease(ctx, name, "b"))
	until, err = s.TryAcquireOrRenew(ctx, name, "a", 10*time.Second)
	require.NoError(t, err)
	assert.False(t, until.IsZero(), "released lease is free")

	other, err := s.TryAcquireOrRenew(ctx, "jobs/minter", "b", time.Second)
	require.NoError(t, err)
	assert.False(t, other.IsZero(), "leases are independent by name")
}

func testPutShardAtomicAndIdempotent(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	shard, rows := MintShard(t, s, "main", 5, store.ShardUnmined)

	got, err := s.GetShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, 5, got.Size)
	assert.Equal(t, uint64(50), got.Denomination)
	assert.Equal(t, store.LockPuzzle, got.LockMode)
	assert.Equal(t, store.ShardUnmined, got.Status)
	assert.Empty(t, got.Owner)
	assert.True(t, got.LeaseUntil.IsZero())
	assert.Equal(t, int64(1), got.Version)
	assert.Equal(t, shard.BEEF, got.BEEF)
	assert.Equal(t, clk.Now(), got.CreatedAt.UTC())

	avail, err := s.ListAvailable(ctx, "main", shard.TxID, 0, 100)
	require.NoError(t, err)
	require.Len(t, avail, 5)
	for i, r := range avail {
		assert.Equal(t, uint32(i), r.Vout)
		assert.Equal(t, store.StateAvailable, r.State)
		assert.Equal(t, rows[i].LockingScript, r.LockingScript)
		assert.Equal(t, rows[i].UnlockingScript, r.UnlockingScript)
		assert.Equal(t, uint64(50), r.Satoshis)
		assert.Equal(t, "main", r.Pool)
		assert.True(t, r.ReservedBy.IsZero())
	}

	// Replay must not touch existing rows: reserve one, replay PutShard, still reserved.
	tok := store.NewToken()
	MustReserve(t, s, "main", rows[:1], tok, clk.Now().Add(time.Minute))
	require.NoError(t, s.PutShard(ctx, shard, rows))
	after := RowsByOutpoint(t, s, "main", Ops(rows[:1]))
	assert.Equal(t, store.StateReserved, after[rows[0].Outpoint].State)
	assert.Equal(t, tok, after[rows[0].Outpoint].ReservedBy)

	diff := shard
	diff.Size = 4
	require.ErrorIs(t, s.PutShard(ctx, diff, rows[:4]), store.ErrShardExists)

	bad := shard
	bad.TxID = NewTxID()
	require.ErrorIs(t, s.PutShard(ctx, bad, rows), store.ErrInvalidArgument, "rows of another txid")

	_, err = s.GetShard(ctx, "main", NewTxID())
	require.ErrorIs(t, err, store.ErrNotFound)
	_, err = s.GetShard(ctx, "other", shard.TxID)
	require.ErrorIs(t, err, store.ErrNotFound, "pool is part of the key")
}

func testUpdateShardBumpsVersion(t *testing.T, s store.Store, _ *Clock) {
	ctx := context.Background()
	shard, _ := MintShard(t, s, "main", 2, store.ShardUnmined)

	require.NoError(t, s.UpdateShard(ctx, "main", shard.TxID, store.ShardMined, []byte("beef2")))
	got, err := s.GetShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardMined, got.Status)
	assert.Equal(t, int64(2), got.Version)
	assert.Equal(t, []byte("beef2"), got.BEEF)

	require.NoError(t, s.UpdateShard(ctx, "main", shard.TxID, store.ShardRetired, nil))
	got, err = s.GetShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardRetired, got.Status)
	assert.Equal(t, int64(3), got.Version)
	assert.Equal(t, []byte("beef2"), got.BEEF, "nil beef keeps the old one")

	require.ErrorIs(t, s.UpdateShard(ctx, "main", shard.TxID, store.ShardMined, nil), store.ErrTerminalShard)
	require.ErrorIs(t, s.UpdateShard(ctx, "main", NewTxID(), store.ShardMined, nil), store.ErrNotFound)
	require.ErrorIs(t, s.UpdateShard(ctx, "main", shard.TxID, store.ShardInvalid, nil), store.ErrInvalidArgument)
}

func testListShards(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	a, _ := MintShard(t, s, "main", 1, store.ShardUnmined)
	clk.Advance(time.Second)
	b, _ := MintShard(t, s, "main", 1, store.ShardMined)
	clk.Advance(time.Second)
	c, _ := MintShard(t, s, "main", 1, store.ShardMined)
	MintShard(t, s, "other", 1, store.ShardMined)

	all, err := s.ListShards(ctx, "main", store.ShardFilter{})
	require.NoError(t, err)
	require.Len(t, all, 3)
	assert.Equal(t, []chainhash.Hash{a.TxID, b.TxID, c.TxID}, []chainhash.Hash{all[0].TxID, all[1].TxID, all[2].TxID}, "ordered by created_at")
	for _, sh := range all {
		assert.Nil(t, sh.BEEF, "ListShards omits BEEF")
	}

	mined, err := s.ListShards(ctx, "main", store.ShardFilter{Statuses: []store.ShardStatus{store.ShardMined}})
	require.NoError(t, err)
	require.Len(t, mined, 2)

	one, err := s.ListShards(ctx, "main", store.ShardFilter{Limit: 1})
	require.NoError(t, err)
	require.Len(t, one, 1)
	assert.Equal(t, a.TxID, one[0].TxID)

	got, err := s.AcquireShard(ctx, "main", "h1", clk.Now(), clk.Now().Add(time.Minute), []store.ShardStatus{store.ShardMined}, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	owner := "h1"
	owned, err := s.ListShards(ctx, "main", store.ShardFilter{Owner: &owner})
	require.NoError(t, err)
	require.Len(t, owned, 1)
	assert.Equal(t, b.TxID, owned[0].TxID)
	none := ""
	free, err := s.ListShards(ctx, "main", store.ShardFilter{Owner: &none})
	require.NoError(t, err)
	require.Len(t, free, 2)
}

func testAcquireRenewRelease(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	mined := []store.ShardStatus{store.ShardMined}
	s1, _ := MintShard(t, s, "main", 2, store.ShardMined)
	clk.Advance(time.Second)
	s2, _ := MintShard(t, s, "main", 2, store.ShardMined)
	MintShard(t, s, "main", 2, store.ShardUnmined)

	_, err := s.AcquireShard(ctx, "main", "", clk.Now(), clk.Now().Add(time.Minute), mined, nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.AcquireShard(ctx, "main", "a", clk.Now(), clk.Now().Add(time.Minute), nil, nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument)

	until := clk.Now().Add(30 * time.Second)
	got, err := s.AcquireShard(ctx, "main", "a", clk.Now(), until, mined, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, s1.TxID, got.TxID, "oldest first")
	assert.Equal(t, "a", got.Owner)
	assert.Equal(t, until, got.LeaseUntil.UTC())
	assert.Equal(t, s1.BEEF, got.BEEF, "AcquireShard returns the full record")

	got, err = s.AcquireShard(ctx, "main", "b", clk.Now(), until, mined, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, s2.TxID, got.TxID)

	got, err = s.AcquireShard(ctx, "main", "c", clk.Now(), until, mined, nil)
	require.NoError(t, err)
	assert.Nil(t, got, "nothing adoptable")

	head, err := s.RenewShard(ctx, "main", s1.TxID, "a", clk.Now().Add(time.Minute))
	require.NoError(t, err)
	assert.Equal(t, store.ShardHead{Status: store.ShardMined, Version: 1}, head)
	_, err = s.RenewShard(ctx, "main", s1.TxID, "b", clk.Now().Add(time.Minute))
	require.ErrorIs(t, err, store.ErrNotOwner)
	_, err = s.RenewShard(ctx, "main", NewTxID(), "a", clk.Now().Add(time.Minute))
	require.ErrorIs(t, err, store.ErrNotFound)

	require.NoError(t, s.ReleaseShard(ctx, "main", s1.TxID, "b"), "non-owner release is a no-op")
	_, err = s.RenewShard(ctx, "main", s1.TxID, "a", clk.Now().Add(time.Minute))
	require.NoError(t, err, "a still owns s1")
	require.ErrorIs(t, s.ReleaseShard(ctx, "main", NewTxID(), "a"), store.ErrNotFound)

	require.NoError(t, s.UpdateShard(ctx, "main", s1.TxID, store.ShardMined, []byte("new")))
	head, err = s.RenewShard(ctx, "main", s1.TxID, "a", clk.Now().Add(time.Minute))
	require.NoError(t, err)
	assert.Equal(t, int64(2), head.Version, "renew reports the bumped version")

	clk.Advance(2 * time.Minute) // both leases expired
	got, err = s.AcquireShard(ctx, "main", "c", clk.Now(), clk.Now().Add(time.Minute), mined, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, s1.TxID, got.TxID, "expired lease is adoptable, oldest first")
	_, err = s.RenewShard(ctx, "main", s1.TxID, "a", clk.Now().Add(time.Minute))
	require.ErrorIs(t, err, store.ErrNotOwner, "old owner lost it")

	got, err = s.AcquireShard(ctx, "main", "d", clk.Now(), clk.Now().Add(time.Minute), mined, []chainhash.Hash{s2.TxID})
	require.NoError(t, err)
	assert.Nil(t, got, "exclude honoured (s1 is owned by c, s2 excluded)")

	require.NoError(t, s.ReleaseShard(ctx, "main", s1.TxID, "c"))
	got, err = s.AcquireShard(ctx, "main", "a", clk.Now(), clk.Now().Add(time.Minute), mined, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, s1.TxID, got.TxID, "released shard is adoptable again")

	got, err = s.AcquireShard(ctx, "main", "e", clk.Now(), clk.Now().Add(time.Minute), []store.ShardStatus{store.ShardUnmined}, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, store.ShardUnmined, got.Status, "status filter selects the unmined shard")
}

func testRetireShardCascade(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	shard, rows := MintShard(t, s, "main", 6, store.ShardMined)
	tok := store.NewToken()
	spendTx := NewTxID()
	MustReserve(t, s, "main", rows[:2], tok, clk.Now().Add(time.Minute))
	n, err := s.Spend(ctx, "main", tok, Ops(rows[:1]), spendTx, false)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	n, err = s.Spend(ctx, "main", tok, Ops(rows[1:2]), spendTx, true)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	got, err := s.AcquireShard(ctx, "main", "a", clk.Now(), clk.Now().Add(time.Minute), []store.ShardStatus{store.ShardMined}, nil)
	require.NoError(t, err)
	require.NotNil(t, got)

	changed, err := s.RetireShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, 5, changed, "quarantined + 4 available; spent row untouched")

	sh, err := s.GetShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardInvalid, sh.Status)
	assert.Empty(t, sh.Owner)
	assert.Equal(t, int64(2), sh.Version)
	_, err = s.RenewShard(ctx, "main", shard.TxID, "a", clk.Now().Add(time.Minute))
	require.ErrorIs(t, err, store.ErrNotOwner)

	states := RowsByOutpoint(t, s, "main", Ops(rows))
	assert.Equal(t, store.StateInvalid, states[rows[0].Outpoint].State)
	assert.Equal(t, tok, states[rows[0].Outpoint].ReservedBy, "retire keeps provenance fields")
	assert.Equal(t, store.StateSpent, states[rows[1].Outpoint].State)
	for _, r := range rows[2:] {
		assert.Equal(t, store.StateInvalid, states[r.Outpoint].State)
	}
	avail, err := s.ListAvailable(ctx, "main", shard.TxID, 0, 10)
	require.NoError(t, err)
	assert.Empty(t, avail)

	applied, err := s.Reserve(ctx, "main", ReserveOps(rows[2:3], store.NewToken(), clk.Now().Add(time.Minute)))
	require.NoError(t, err)
	assert.Empty(t, applied, "invalid rows cannot be reserved")

	changed, err = s.RetireShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, 0, changed, "idempotent")
	_, err = s.RetireShard(ctx, "main", NewTxID())
	require.ErrorIs(t, err, store.ErrNotFound)
}
