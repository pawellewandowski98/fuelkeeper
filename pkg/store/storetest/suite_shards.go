package storetest

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

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

	// A replay carrying different BEEF/status is not a conflict: it keeps Version 1
	// and the ORIGINAL BEEF/status.
	replay := shard
	replay.BEEF = []byte("different-beef")
	replay.Status = store.ShardMined
	require.NoError(t, s.PutShard(ctx, replay, rows))
	afterReplay, err := s.GetShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, int64(1), afterReplay.Version, "replay keeps the original version")
	assert.Equal(t, shard.BEEF, afterReplay.BEEF, "replay keeps the original BEEF")
	assert.Equal(t, shard.Status, afterReplay.Status, "replay keeps the original status")

	diff := shard
	diff.Size = 4
	require.ErrorIs(t, s.PutShard(ctx, diff, rows[:4]), store.ErrShardExists, "size mismatch")

	diffDenom := shard
	diffDenom.Denomination = shard.Denomination + 1
	require.ErrorIs(t, s.PutShard(ctx, diffDenom, rows), store.ErrShardExists, "denomination mismatch")

	diffLock := shard
	diffLock.LockMode = store.LockP2PKH
	require.ErrorIs(t, s.PutShard(ctx, diffLock, rows), store.ErrShardExists, "lock mode mismatch")

	crossPool := shard
	crossPool.Pool = "other"
	crossPoolRows := make([]store.Row, len(rows))
	copy(crossPoolRows, rows)
	for i := range crossPoolRows {
		crossPoolRows[i].Pool = "other"
	}
	require.ErrorIs(t, s.PutShard(ctx, crossPool, crossPoolRows), store.ErrShardExists, "txid is globally unique across pools")

	bad := shard
	bad.TxID = NewTxID()
	require.ErrorIs(t, s.PutShard(ctx, bad, rows), store.ErrInvalidArgument, "rows of another txid")
	_, err = s.GetShard(ctx, "main", bad.TxID)
	require.ErrorIs(t, err, store.ErrNotFound, "rejected PutShard must not create a partial shard record")

	// Both SQL schemas declare beef NOT NULL, so a nil/empty BEEF must be rejected
	// uniformly as ErrInvalidArgument before it ever reaches the backend, not left
	// to surface as a raw constraint violation on some backends and silently
	// succeed on others.
	noBEEF, noBEEFRows := BuildShard("main", 2, store.ShardUnmined)
	noBEEF.BEEF = nil
	require.ErrorIs(t, s.PutShard(ctx, noBEEF, noBEEFRows), store.ErrInvalidArgument, "PutShard requires non-empty BEEF")
	_, err = s.GetShard(ctx, "main", noBEEF.TxID)
	require.ErrorIs(t, err, store.ErrNotFound, "a rejected nil-BEEF PutShard must not store anything")

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

	// f.Owner matches the stored owner string regardless of lease expiry (store.go):
	// an expired lease still has a non-empty Owner, and ListShards must still find it.
	clk.Advance(2 * time.Minute)
	ownedAfterExpiry, err := s.ListShards(ctx, "main", store.ShardFilter{Owner: &owner})
	require.NoError(t, err)
	require.Len(t, ownedAfterExpiry, 1, "ListShards with an Owner filter must still find a shard whose lease has since expired")
	assert.Equal(t, b.TxID, ownedAfterExpiry[0].TxID)
}

// testListShardsTieBreak pins the (created_at, txid) ordering's tie-break: when
// created_at is identical, ties are broken by the txid's 32 internal bytes, not
// its display (reversed) hex string. It uses TxIDPairDisplayReversed rather than
// random txids so a display-hex ordering bug is caught deterministically instead
// of only part of the time (whenever a random pair happens to disagree that way).
func testListShardsTieBreak(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	a, b := TxIDPairDisplayReversed()
	require.Negative(t, bytes.Compare(a[:], b[:]), "test setup: a must sort before b by internal bytes")
	require.Less(t, b.String(), a.String(), "test setup: b must sort before a by display (reversed) hex string")

	shardA, _ := MintShardWithTxID(t, s, "main", a, 1, store.ShardMined)
	shardB, _ := MintShardWithTxID(t, s, "main", b, 1, store.ShardMined)

	all, err := s.ListShards(ctx, "main", store.ShardFilter{})
	require.NoError(t, err)
	require.Len(t, all, 2)
	assert.Equal(t, shardA.TxID, all[0].TxID, "ListShards ties broken by txid internal bytes, not display hex")
	assert.Equal(t, shardB.TxID, all[1].TxID)

	mined := []store.ShardStatus{store.ShardMined}
	got, err := s.AcquireShard(ctx, "main", "h", clk.Now(), clk.Now().Add(time.Minute), mined, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, shardA.TxID, got.TxID, "AcquireShard follows the same tie-break order")

	got, err = s.AcquireShard(ctx, "main", "h", clk.Now(), clk.Now().Add(time.Minute), mined, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, shardB.TxID, got.TxID)
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
	_, err = s.AcquireShard(ctx, "main", "a", clk.Now(), clk.Now(), mined, nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "until must be strictly after now")
	_, err = s.AcquireShard(ctx, "main", "a", clk.Now(), clk.Now().Add(-time.Second), mined, nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "until before now is invalid")
	_, err = s.RenewShard(ctx, "main", s1.TxID, "a", time.Time{})
	require.ErrorIs(t, err, store.ErrInvalidArgument, "zero until is invalid")

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

	// RenewShard is a heartbeat: it must update only lease_until, never
	// stamping UpdatedAt or bumping Version (store.go, RenewShard).
	pre, err := s.GetShard(ctx, "main", s1.TxID)
	require.NoError(t, err)
	clk.Advance(time.Second)
	_, err = s.RenewShard(ctx, "main", s1.TxID, "a", clk.Now().Add(time.Minute))
	require.NoError(t, err)
	post, err := s.GetShard(ctx, "main", s1.TxID)
	require.NoError(t, err)
	assert.Equal(t, pre.UpdatedAt, post.UpdatedAt, "RenewShard is a heartbeat: must not stamp UpdatedAt")
	assert.Equal(t, pre.Version, post.Version, "RenewShard must not bump Version")

	// Between s1's old 30s lease (now expired) and its renewed 1m lease (still
	// active), s1 must stay protected by the renewal. s2 is excluded here so this
	// check is only about s1 (s2's own, never-renewed lease is examined next).
	clk.Advance(40 * time.Second)
	skipped, err := s.AcquireShard(ctx, "main", "x", clk.Now(), clk.Now().Add(time.Minute), mined, []chainhash.Hash{s2.TxID})
	require.NoError(t, err)
	assert.Nil(t, skipped, "s1's renewed lease is still active; s2 is excluded from this check")

	// s2's original 30s lease has independently expired by now (it was never
	// renewed), but its owner "b" can still renew it: RenewShard succeeds for the
	// owner even past expiry (only a new AcquireShard by someone else, or
	// ReleaseShard, changes Owner).
	s2Head, err := s.RenewShard(ctx, "main", s2.TxID, "b", clk.Now().Add(time.Minute))
	require.NoError(t, err, "RenewShard succeeds for the owner even after the lease expired")
	assert.Equal(t, store.ShardHead{Status: store.ShardMined, Version: 1}, s2Head)

	_, err = s.RenewShard(ctx, "main", s1.TxID, "b", clk.Now().Add(time.Minute))
	require.ErrorIs(t, err, store.ErrNotOwner)
	_, err = s.RenewShard(ctx, "main", NewTxID(), "a", clk.Now().Add(time.Minute))
	require.ErrorIs(t, err, store.ErrNotFound)

	require.NoError(t, s.ReleaseShard(ctx, "main", s1.TxID, "b"), "non-owner release is a no-op")
	_, err = s.RenewShard(ctx, "main", s1.TxID, "a", clk.Now().Add(time.Minute))
	require.NoError(t, err, "a still owns s1")
	require.ErrorIs(t, s.ReleaseShard(ctx, "main", NewTxID(), "a"), store.ErrNotFound)

	before, err := s.GetShard(ctx, "main", s1.TxID)
	require.NoError(t, err)
	require.NoError(t, s.UpdateShard(ctx, "main", s1.TxID, store.ShardMined, []byte("new")))
	afterUpdate, err := s.GetShard(ctx, "main", s1.TxID)
	require.NoError(t, err)
	assert.Equal(t, before.Owner, afterUpdate.Owner, "UpdateShard must not touch the lease owner")
	assert.Equal(t, before.LeaseUntil, afterUpdate.LeaseUntil, "UpdateShard must not touch the lease expiry")

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
	_, err = s.RenewShard(ctx, "main", s1.TxID, "c", clk.Now().Add(time.Minute))
	require.ErrorIs(t, err, store.ErrNotOwner, "release cleared ownership")

	got, err = s.AcquireShard(ctx, "main", "a", clk.Now(), clk.Now().Add(time.Minute), mined, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, s1.TxID, got.TxID, "released shard is adoptable again")

	got, err = s.AcquireShard(ctx, "main", "e", clk.Now(), clk.Now().Add(time.Minute), []store.ShardStatus{store.ShardUnmined}, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, store.ShardUnmined, got.Status, "status filter selects the unmined shard")

	// Pool isolation: an adoptable shard in another pool must not leak into this
	// pool's results. main's only unmined shard is now held by "e" (unexpired), so
	// this can only pass if the "other" pool's fresh unmined shard is excluded.
	MintShard(t, s, "other", 2, store.ShardUnmined)
	got, err = s.AcquireShard(ctx, "main", "z", clk.Now(), clk.Now().Add(time.Minute), []store.ShardStatus{store.ShardUnmined}, nil)
	require.NoError(t, err)
	assert.Nil(t, got, `pool isolation: an unmined shard in pool "other" must not be adoptable from "main"`)
}

// testShardLeaseBoundary pins two properties across a single lease's life:
// AcquireShard stamps UpdatedAt from the store's clock (not the caller's now), and a
// lease is free at exactly LeaseUntil == now (the <= rule documented in store.go),
// not just strictly after it. It also pins the Owner/LeaseUntil invariant
// (Owner == "" iff LeaseUntil is zero) after both ReleaseShard and RetireShard.
func testShardLeaseBoundary(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	mined := []store.ShardStatus{store.ShardMined}

	// AcquireShard stamps UpdatedAt from the store's clock, not the passed `now`.
	clockShard, _ := MintShard(t, s, "main", 1, store.ShardMined)
	now := clk.Now().Add(-time.Second)
	until := clk.Now().Add(time.Minute)
	got, err := s.AcquireShard(ctx, "main", "a", now, until, mined, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, clockShard.TxID, got.TxID)
	assert.Equal(t, clk.Now(), got.UpdatedAt.UTC(), "AcquireShard stamps UpdatedAt from the store clock, not the passed now")

	// Boundary: a lease is free at exactly LeaseUntil == now.
	shard, _ := MintShard(t, s, "main", 1, store.ShardMined)
	got, err = s.AcquireShard(ctx, "main", "a", clk.Now(), clk.Now().Add(10*time.Second), mined, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, shard.TxID, got.TxID)

	clk.Advance(10 * time.Second)

	got, err = s.AcquireShard(ctx, "main", "b", clk.Now(), clk.Now().Add(time.Minute), mined, nil)
	require.NoError(t, err)
	require.NotNil(t, got, "lease is free at exactly its expiry instant (LeaseUntil == now)")
	assert.Equal(t, shard.TxID, got.TxID)
	assert.Equal(t, "b", got.Owner)

	_, err = s.RenewShard(ctx, "main", shard.TxID, "a", clk.Now().Add(time.Minute))
	require.ErrorIs(t, err, store.ErrNotOwner, "a lost ownership when b acquired the freed lease")

	require.NoError(t, s.ReleaseShard(ctx, "main", shard.TxID, "b"))
	afterRelease, err := s.GetShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Empty(t, afterRelease.Owner, "ReleaseShard clears Owner")
	assert.True(t, afterRelease.LeaseUntil.IsZero(), "Owner == \"\" iff LeaseUntil is zero")

	_, err = s.RetireShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	afterRetire, err := s.GetShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Empty(t, afterRetire.Owner, "RetireShard clears Owner")
	assert.True(t, afterRetire.LeaseUntil.IsZero(), "Owner == \"\" iff LeaseUntil is zero")
}

func testRetireShardCascade(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	shard, rows := MintShard(t, s, "main", 6, store.ShardMined)
	tok := store.NewToken()
	spendTx := NewTxID()
	reservedAt := clk.Now()
	exp := clk.Now().Add(time.Minute)
	MustReserve(t, s, "main", rows[:2], tok, exp)
	n, err := s.Spend(ctx, "main", tok, Ops(rows[:1]), spendTx, false)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	quarantinedAt := clk.Now()
	n, err = s.Spend(ctx, "main", tok, Ops(rows[1:2]), spendTx, true)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// rows[2] stays reserved (under a second token) into the retire, proving the
	// cascade also invalidates reserved rows, not just available/quarantined ones.
	secondTok := store.NewToken()
	secondReservedAt := clk.Now()
	secondExp := clk.Now().Add(time.Minute)
	MustReserve(t, s, "main", rows[2:3], secondTok, secondExp)

	got, err := s.AcquireShard(ctx, "main", "a", clk.Now(), clk.Now().Add(time.Minute), []store.ShardStatus{store.ShardMined}, nil)
	require.NoError(t, err)
	require.NotNil(t, got)

	before := RowsByOutpoint(t, s, "main", Ops(rows))
	clk.Advance(time.Second) // so the cascade's UpdatedAt stamp is distinguishable from before's
	retireAt := clk.Now()

	changed, err := s.RetireShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, 5, changed, "quarantined + reserved + 3 available; spent row untouched")

	sh, err := s.GetShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardInvalid, sh.Status)
	assert.Empty(t, sh.Owner)
	assert.Equal(t, int64(2), sh.Version)
	_, err = s.RenewShard(ctx, "main", shard.TxID, "a", clk.Now().Add(time.Minute))
	require.ErrorIs(t, err, store.ErrNotOwner)

	states := RowsByOutpoint(t, s, "main", Ops(rows))

	// rows[0]: was quarantined via Spend(false) before the retire; the cascade
	// invalidates it but every provenance field (including the spend hint) and the
	// re-stamped UpdatedAt must survive.
	r0 := states[rows[0].Outpoint]
	assert.Equal(t, store.StateInvalid, r0.State)
	assert.Equal(t, tok, r0.ReservedBy, "retire keeps ReservedBy for provenance")
	assert.Equal(t, reservedAt, r0.ReservedAt.UTC(), "retire keeps ReservedAt for provenance")
	assert.Equal(t, exp, r0.ExpiresAt.UTC(), "retire keeps ExpiresAt for provenance")
	assert.Equal(t, quarantinedAt, r0.QuarantinedAt.UTC(), "retire keeps QuarantinedAt for provenance")
	assert.Equal(t, &spendTx, r0.SpentHint, "quarantined row keeps its spent hint")
	assert.Equal(t, retireAt, r0.UpdatedAt.UTC(), "retire cascade stamps UpdatedAt")

	// rows[1]: already terminal (spent) before the retire, so the cascade must
	// leave it untouched entirely, including UpdatedAt.
	r1 := states[rows[1].Outpoint]
	assert.Equal(t, store.StateSpent, r1.State)
	assert.Equal(t, tok, r1.ReservedBy, "retire keeps ReservedBy for provenance (already-terminal row)")
	assert.Equal(t, reservedAt, r1.ReservedAt.UTC(), "retire keeps ReservedAt for provenance (already-terminal row)")
	assert.Equal(t, exp, r1.ExpiresAt.UTC(), "retire keeps ExpiresAt for provenance (already-terminal row)")
	assert.True(t, r1.QuarantinedAt.IsZero(), "a terminal (true) spend never set QuarantinedAt")
	assert.Nil(t, r1.SpentHint, "a terminal (true) spend never set SpentHint")
	require.NotNil(t, r1.SpentBy)
	assert.Equal(t, spendTx, *r1.SpentBy)
	assert.Equal(t, before[rows[1].Outpoint].UpdatedAt, r1.UpdatedAt, "retire must not touch an already-terminal row's UpdatedAt")

	// rows[2]: still reserved (under the second token) going into the retire; the
	// cascade invalidates it and keeps its provenance too.
	r2 := states[rows[2].Outpoint]
	assert.Equal(t, store.StateInvalid, r2.State)
	assert.Equal(t, secondTok, r2.ReservedBy, "retire keeps ReservedBy for provenance (reserved row)")
	assert.Equal(t, secondReservedAt, r2.ReservedAt.UTC(), "retire keeps ReservedAt for provenance (reserved row)")
	assert.Equal(t, secondExp, r2.ExpiresAt.UTC(), "retire keeps ExpiresAt for provenance too")
	assert.True(t, r2.QuarantinedAt.IsZero(), "a never-quarantined row keeps QuarantinedAt zero")
	assert.Nil(t, r2.SpentHint)
	assert.Equal(t, retireAt, r2.UpdatedAt.UTC(), "retire cascade stamps UpdatedAt")

	for _, r := range rows[3:] {
		gotRow := states[r.Outpoint]
		assert.Equal(t, store.StateInvalid, gotRow.State)
		assert.Equal(t, retireAt, gotRow.UpdatedAt.UTC(), "retire cascade stamps UpdatedAt for available rows too")
	}
	avail, err := s.ListAvailable(ctx, "main", shard.TxID, 0, 10)
	require.NoError(t, err)
	assert.Empty(t, avail)

	applied, err := s.Reserve(ctx, "main", ReserveOps(rows[3:4], store.NewToken(), clk.Now().Add(time.Minute)))
	require.NoError(t, err)
	assert.Empty(t, applied, "invalid rows cannot be reserved")

	changed, err = s.RetireShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, 0, changed, "the shard is already invalid and every row is now terminal (invalid), so this returns 0")
	shAfterSecondRetire, err := s.GetShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, sh.Version, shAfterSecondRetire.Version, "second retire does not bump Version")
	assert.Equal(t, sh.UpdatedAt, shAfterSecondRetire.UpdatedAt, "second retire does not touch UpdatedAt")

	require.ErrorIs(t, s.UpdateShard(ctx, "main", shard.TxID, store.ShardMined, nil), store.ErrTerminalShard)

	_, err = s.RetireShard(ctx, "main", NewTxID())
	require.ErrorIs(t, err, store.ErrNotFound)

	// A retired (not just invalid) shard may still be retired: retired -> invalid.
	retiredShard, retiredRows := MintShard(t, s, "main", 2, store.ShardMined)
	require.NoError(t, s.UpdateShard(ctx, "main", retiredShard.TxID, store.ShardRetired, nil))
	changedRetired, err := s.RetireShard(ctx, "main", retiredShard.TxID)
	require.NoError(t, err)
	assert.Equal(t, len(retiredRows), changedRetired, "retiring a retired shard still cascades its non-terminal rows")
	shRetired, err := s.GetShard(ctx, "main", retiredShard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardInvalid, shRetired.Status, "a retired shard can still be retired to invalid")
}
