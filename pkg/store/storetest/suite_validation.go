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

// testArgumentValidation pins the uniform argument-validation rule: every method
// taking a pool rejects "" with ErrInvalidArgument, regardless of what else it does.
// The row methods (Reserve, Release, ...) are still stubs until Task 5; they
// implement the pool check now so this assertion already holds for them.
func testArgumentValidation(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	shard, rows := MintShard(t, s, "main", 1, store.ShardMined)
	txid := shard.TxID
	tok := store.NewToken()
	future := clk.Now().Add(time.Minute)

	_, err := s.GetShard(ctx, "", txid)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	require.ErrorIs(t, s.UpdateShard(ctx, "", txid, store.ShardMined, nil), store.ErrInvalidArgument)
	_, err = s.RetireShard(ctx, "", txid)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.AcquireShard(ctx, "", "h", clk.Now(), future, []store.ShardStatus{store.ShardMined}, nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.RenewShard(ctx, "", txid, "h", future)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	require.ErrorIs(t, s.ReleaseShard(ctx, "", txid, "h"), store.ErrInvalidArgument)
	_, err = s.ListShards(ctx, "", store.ShardFilter{})
	require.ErrorIs(t, err, store.ErrInvalidArgument)

	_, err = s.ListAvailable(ctx, "", txid, 0, 10)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.GetRows(ctx, "", Ops(rows))
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.Reserve(ctx, "", ReserveOps(rows, tok, future))
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.Release(ctx, "", tok, Ops(rows))
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.Spend(ctx, "", tok, Ops(rows), txid, false)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.Quarantine(ctx, "", future, 10)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.ListQuarantined(ctx, "", future, nil, 10)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.Resolve(ctx, "", Ops(rows), store.StateAvailable, nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.Stats(ctx, "")
	require.ErrorIs(t, err, store.ErrInvalidArgument)

	_, err = s.ListShards(ctx, "main", store.ShardFilter{Limit: -1})
	require.ErrorIs(t, err, store.ErrInvalidArgument, "negative limit")
	_, err = s.ListShards(ctx, "main", store.ShardFilter{Statuses: []store.ShardStatus{store.ShardStatus(7)}})
	require.ErrorIs(t, err, store.ErrInvalidArgument, "unknown status")

	require.ErrorIs(t, s.ReleaseShard(ctx, "main", txid, ""), store.ErrInvalidArgument, "empty holder")
	require.ErrorIs(t, s.ReleaseLease(ctx, "", "h"), store.ErrInvalidArgument, "empty lease name")

	zeroTok := store.Token{}
	_, err = s.Release(ctx, "main", zeroTok, Ops(rows))
	require.ErrorIs(t, err, store.ErrInvalidArgument, "zero token")
	_, err = s.Spend(ctx, "main", zeroTok, Ops(rows), txid, false)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "zero token")

	_, err = s.Spend(ctx, "main", tok, Ops(rows), chainhash.Hash{}, true)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "terminal spend requires a non-zero txid")

	dup := append(Ops(rows), Ops(rows)[0])
	_, err = s.Release(ctx, "main", tok, dup)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "duplicate outpoint")
	_, err = s.Spend(ctx, "main", tok, dup, txid, false)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "duplicate outpoint")
	_, err = s.Resolve(ctx, "main", dup, store.StateAvailable, nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "duplicate outpoint")
	_, err = s.GetRows(ctx, "main", dup)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "duplicate outpoint")

	tooMany := make([]store.Outpoint, s.Capabilities().MaxBatch+1)
	for i := range tooMany {
		tooMany[i] = store.Outpoint{TxID: NewTxID(), Vout: uint32(i)}
	}
	_, err = s.Release(ctx, "main", tok, tooMany)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "batch above MaxBatch")
	_, err = s.Spend(ctx, "main", tok, tooMany, txid, false)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "batch above MaxBatch")
	_, err = s.Resolve(ctx, "main", tooMany, store.StateAvailable, nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "batch above MaxBatch")
	_, err = s.GetRows(ctx, "main", tooMany)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "batch above MaxBatch")

	zeroExpOp := []store.ReserveOp{{Outpoint: rows[0].Outpoint, Token: tok, ExpiresAt: time.Time{}}}
	_, err = s.Reserve(ctx, "main", zeroExpOp)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "zero ExpiresAt")
}

// testPoolIsolation pins that every row and shard read/write is scoped strictly to
// its pool: calling any of them against pool "other" while passing outpoints or a
// txid that belong to pool "main" must neither find nor mutate anything, and must
// leave "main" completely untouched.
func testPoolIsolation(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	shard, rows := MintShard(t, s, "main", 3, store.ShardMined)
	tok := store.NewToken()
	MustReserve(t, s, "main", rows[:1], tok, clk.Now().Add(time.Minute))

	// Actually quarantine one main row so the cross-pool Resolve and ListQuarantined
	// checks below are non-vacuous: with nothing quarantined anywhere, "other" would
	// trivially find nothing regardless of whether pool scoping works at all.
	qTok := store.NewToken()
	MustReserve(t, s, "main", rows[1:2], qTok, clk.Now().Add(time.Second))
	clk.Advance(2 * time.Second)
	qn, err := s.Quarantine(ctx, "main", clk.Now(), 10)
	require.NoError(t, err)
	require.Equal(t, 1, qn, "the quarantine setup for this test must actually quarantine the row")

	before := RowsByOutpoint(t, s, "main", Ops(rows))

	n, err := s.Quarantine(ctx, "other", clk.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	require.Equal(t, 0, n, "Quarantine against another pool must not touch main's reserved row")

	q, err := s.ListQuarantined(ctx, "other", clk.Now().Add(time.Hour), nil, 10)
	require.NoError(t, err)
	require.Empty(t, q, "ListQuarantined must not see main's rows from another pool")

	mainQ, err := s.ListQuarantined(ctx, "main", clk.Now().Add(time.Hour), nil, 10)
	require.NoError(t, err)
	require.Len(t, mainQ, 1, "ListQuarantined for main must return the quarantined row")
	require.Equal(t, rows[1].Outpoint, mainQ[0].Outpoint)

	rn, err := s.Resolve(ctx, "other", Ops(rows[1:2]), store.StateAvailable, nil)
	require.NoError(t, err)
	require.Equal(t, 0, rn, "Resolve must not cross pools")
	stillQuarantined := RowsByOutpoint(t, s, "main", Ops(rows[1:2]))[rows[1].Outpoint]
	require.Equal(t, store.StateQuarantined, stillQuarantined.State, "cross-pool Resolve must not change main's row")

	relN, err := s.Release(ctx, "other", tok, Ops(rows[:1]))
	require.NoError(t, err)
	require.Equal(t, 0, relN, "Release must not cross pools even with the right token")

	spendN, err := s.Spend(ctx, "other", tok, Ops(rows[:1]), NewTxID(), false)
	require.NoError(t, err)
	require.Equal(t, 0, spendN, "Spend must not cross pools even with the right token")

	got, err := s.GetRows(ctx, "other", Ops(rows))
	require.NoError(t, err)
	require.Empty(t, got, "GetRows must not see main's rows from another pool")

	avail, err := s.ListAvailable(ctx, "other", shard.TxID, 0, 10)
	require.NoError(t, err)
	require.Empty(t, avail, "ListAvailable must not see main's shard from another pool")

	otherStats, err := s.Stats(ctx, "other")
	require.NoError(t, err)
	require.Equal(t, store.Stats{}, otherStats, "Stats for an untouched pool must be all zero")

	shards, err := s.ListShards(ctx, "other", store.ShardFilter{})
	require.NoError(t, err)
	require.Empty(t, shards, "ListShards must not see main's shard from another pool")

	after := RowsByOutpoint(t, s, "main", Ops(rows))
	require.Equal(t, before, after, "none of the above may have touched main's rows")

	mainShard, err := s.GetShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	require.Equal(t, store.ShardMined, mainShard.Status, "none of the above may have touched main's shard")

	// Shard write methods must also be scoped to their pool. A shard's txid is
	// globally unique across pools, so a mutation that drops the pool predicate
	// would still find and mutate main's shard when called against "other" with
	// main's txid: this must instead find nothing (ErrNotFound per store.go).
	leaseShard, leaseRows := MintShard(t, s, "main", 2, store.ShardMined)
	_, err = s.AcquireShard(ctx, "main", "h", clk.Now(), clk.Now().Add(time.Minute), []store.ShardStatus{store.ShardMined}, nil)
	require.NoError(t, err)
	beforeLease, err := s.GetShard(ctx, "main", leaseShard.TxID)
	require.NoError(t, err)

	require.ErrorIs(t, s.UpdateShard(ctx, "other", leaseShard.TxID, store.ShardUnmined, nil), store.ErrNotFound,
		"UpdateShard must not cross pools even via a globally-unique txid")
	_, err = s.RetireShard(ctx, "other", leaseShard.TxID)
	require.ErrorIs(t, err, store.ErrNotFound, "RetireShard must not cross pools")
	_, err = s.RenewShard(ctx, "other", leaseShard.TxID, "h", clk.Now().Add(time.Hour))
	require.ErrorIs(t, err, store.ErrNotFound, "RenewShard must not cross pools")
	require.ErrorIs(t, s.ReleaseShard(ctx, "other", leaseShard.TxID, "h"), store.ErrNotFound, "ReleaseShard must not cross pools")

	afterLease, err := s.GetShard(ctx, "main", leaseShard.TxID)
	require.NoError(t, err)
	assert.Equal(t, beforeLease.Status, afterLease.Status, "cross-pool shard-write calls must not change main's shard status")
	assert.Equal(t, beforeLease.Owner, afterLease.Owner, "cross-pool shard-write calls must not change main's shard owner")
	assert.Equal(t, beforeLease.LeaseUntil, afterLease.LeaseUntil, "cross-pool shard-write calls must not change main's shard lease")
	assert.Equal(t, beforeLease.Version, afterLease.Version, "cross-pool shard-write calls must not bump main's shard version")

	leaseAvail, err := s.ListAvailable(ctx, "main", leaseShard.TxID, 0, 10)
	require.NoError(t, err)
	assert.Len(t, leaseAvail, len(leaseRows), "cross-pool shard-write calls must not touch main's rows")
}
