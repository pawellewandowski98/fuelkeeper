//go:build mongodb

package mongodb_test

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// shardDocID mirrors shardID's unexported formula ("main" + "|" + hex(txid)):
// the tests here reach into the raw fuel_shards collection, so they need the
// same _id a real Store would use, without importing the mongodb package's
// unexported helpers. Every call in this file uses pool "main", so that is
// hardcoded rather than threaded through as an always-identical argument.
func shardDocID(txid [32]byte) string {
	return "main|" + hex.EncodeToString(txid[:])
}

// TestRetireShardRepairsPartialCascade reproduces the review's HIGH finding
// 1: a previous RetireShard attempt that flipped the shard document to
// invalid but crashed or timed out before the row cascade ran must be
// repaired by a later RetireShard call, not silently treated as an
// already-done no-op.
func TestRetireShardRepairsPartialCascade(t *testing.T) {
	uri := testURI(t)
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, uri, clk.Now)
	ctx := context.Background()
	shardsColl := s.Rows().Database().Collection("fuel_shards")

	shard, rows := storetest.MintShard(t, s, "main", 10, store.ShardMined)
	require.Len(t, rows, 10)

	// Simulate a partial RetireShard: the shard document already flipped to
	// invalid, but the row cascade never ran (all 10 rows are still
	// available).
	id := shardDocID(shard.TxID)
	_, err := shardsColl.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{
		{Key: "$set", Value: bson.D{{Key: "s", Value: int32(store.ShardInvalid)}}},
	})
	require.NoError(t, err)

	before, err := s.GetShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	require.Equal(t, store.ShardInvalid, before.Status, "precondition: shard document is already invalid")
	avail, err := s.ListAvailable(ctx, "main", shard.TxID, 0, 100)
	require.NoError(t, err)
	require.Len(t, avail, 10, "precondition: the cascade never ran, rows are still available")

	// The repair: RetireShard must still cascade even though the shard is
	// already invalid, and report the rows it actually changed (not 0).
	n, err := s.RetireShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, 10, n, "RetireShard must repair the un-cascaded rows and report how many it changed")

	avail, err = s.ListAvailable(ctx, "main", shard.TxID, 0, 100)
	require.NoError(t, err)
	assert.Empty(t, avail, "ListAvailable must not see any of this shard's rows as available any more")

	got, err := s.GetRows(ctx, "main", storetest.Ops(rows))
	require.NoError(t, err)
	require.Len(t, got, 10)
	for _, r := range got {
		assert.Equal(t, store.StateInvalid, r.State, "outpoint %v must be invalid after the repair", r.Outpoint)
	}

	stats, err := s.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Zero(t, stats.Available, "Stats must not still count this shard's rows as available")
	assert.EqualValues(t, 10, stats.Invalid)
	assert.EqualValues(t, 1, stats.ShardsInvalid)

	// A further retry (nothing left to repair) must converge to a true no-op.
	n, err = s.RetireShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Zero(t, n, "once fully repaired, RetireShard is a true idempotent no-op")
}

// TestRetireShardConvergesRegardlessOfPartialOrder covers the complementary
// partial-failure ordering: rows cascaded (by a previous attempt, or here
// simulated directly) but the shard document itself never flipped. A later
// RetireShard call must still finish the job -- flip the shard, and report 0
// rows changed, since the cascade already happened -- and, chained with the
// opposite-order scenario above, demonstrates convergence regardless of which
// half of a partial RetireShard completed first.
func TestRetireShardConvergesRegardlessOfPartialOrder(t *testing.T) {
	uri := testURI(t)
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, uri, clk.Now)
	ctx := context.Background()
	rowsColl := s.Rows()

	shard, rows := storetest.MintShard(t, s, "main", 6, store.ShardMined)
	require.Len(t, rows, 6)

	// Simulate the cascade half of a partial RetireShard completing first:
	// mark every row invalid directly, but leave the shard document as-is
	// (still mined, not yet invalid).
	_, err := rowsColl.UpdateMany(ctx,
		bson.D{{Key: "p", Value: "main"}, {Key: "sh", Value: shard.TxID[:]}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "st", Value: int32(store.StateInvalid)}}}},
	)
	require.NoError(t, err)

	before, err := s.GetShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	require.Equal(t, store.ShardMined, before.Status, "precondition: the shard document never flipped")

	// RetireShard must still finish: flip the shard, and report 0 rows
	// changed since the cascade already ran.
	n, err := s.RetireShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Zero(t, n, "the cascade already ran; this call only has the shard document left to flip")

	after, err := s.GetShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardInvalid, after.Status)
	assert.Equal(t, before.Version+1, after.Version, "RetireShard still bumps Version when it is the one flipping the shard")

	got, err := s.GetRows(ctx, "main", storetest.Ops(rows))
	require.NoError(t, err)
	require.Len(t, got, 6)
	for _, r := range got {
		assert.Equal(t, store.StateInvalid, r.State)
	}

	// A further retry converges to a true no-op either way.
	n, err = s.RetireShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Zero(t, n)
}

// TestRetireShardOrphanRowsWithoutShardDocument guards the safety check
// RetireShard's fix relies on: cascading rows for a txid before confirming
// its shard document exists would corrupt a later, successful PutShard retry
// for the same txid (which never touches an existing row). This simulates
// exactly that window -- rows inserted, shard document never written -- and
// confirms RetireShard reports ErrNotFound without touching them.
func TestRetireShardOrphanRowsWithoutShardDocument(t *testing.T) {
	uri := testURI(t)
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, uri, clk.Now)
	ctx := context.Background()

	shard, rows := storetest.BuildShard("main", 5, store.ShardMined)
	// Insert both rows and shard document via the real PutShard so the
	// on-disk shape matches production exactly, then delete just the shard
	// document to reproduce the orphan window a crashed (rows-first,
	// shard-record-last) PutShard would have left behind.
	require.NoError(t, s.PutShard(ctx, shard, rows))
	shardsColl := s.Rows().Database().Collection("fuel_shards")
	id := shardDocID(shard.TxID)
	_, err := shardsColl.DeleteOne(ctx, bson.D{{Key: "_id", Value: id}})
	require.NoError(t, err)

	_, err = s.RetireShard(ctx, "main", shard.TxID)
	require.ErrorIs(t, err, store.ErrNotFound)

	got, err := s.GetRows(ctx, "main", storetest.Ops(rows))
	require.NoError(t, err)
	require.Len(t, got, 5)
	for _, r := range got {
		assert.Equal(t, store.StateAvailable, r.State, "orphan rows must be untouched by a RetireShard that found no shard document")
	}

	// A subsequent, legitimate PutShard retry must still succeed and produce
	// a fully consistent shard: this is what RetireShard must never corrupt.
	require.NoError(t, s.PutShard(ctx, shard, rows))
	restored, err := s.GetShard(ctx, "main", shard.TxID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardMined, restored.Status)
	avail, err := s.ListAvailable(ctx, "main", shard.TxID, 0, 100)
	require.NoError(t, err)
	assert.Len(t, avail, 5)
}

// TestRetireShardReturnsZeroCountOnShardUpdateFailure reproduces the review's
// nit that RetireShard returned the row cascade's own count (n, err) when the
// final shard-document update itself failed, even though a count is only
// meaningful when err == nil (see store.go "Errors"). It must return 0 in
// that case, matching aerospike's RetireShard.
//
// The failure is injected cheaply and deterministically, with no fault
// injection framework: the shard document's "ver" field is corrupted to a
// non-numeric BSON type directly in the collection, so RetireShard's own
// "$inc": {"ver": 1} write -- the shard-flipping half of the call, which
// runs after the row cascade -- fails with a real server-side type-mismatch
// error, while the row cascade (which never touches "ver") still goes
// through untouched.
func TestRetireShardReturnsZeroCountOnShardUpdateFailure(t *testing.T) {
	uri := testURI(t)
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, uri, clk.Now)
	ctx := context.Background()
	shardsColl := s.Rows().Database().Collection("fuel_shards")

	shard, rows := storetest.MintShard(t, s, "main", 3, store.ShardMined)
	require.Len(t, rows, 3)

	id := shardDocID(shard.TxID)
	_, err := shardsColl.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{
		{Key: "$set", Value: bson.D{{Key: "ver", Value: "not-a-number"}}},
	})
	require.NoError(t, err, "precondition: corrupt the shard document's ver field")

	n, err := s.RetireShard(ctx, "main", shard.TxID)
	require.Error(t, err, "the corrupted ver field must make the shard-flipping $inc fail")
	assert.Zero(t, n, "RetireShard must report 0, not the row cascade's own count, when the shard update itself fails")

	avail, err := s.ListAvailable(ctx, "main", shard.TxID, 0, 100)
	require.NoError(t, err)
	assert.Empty(t, avail, "the row cascade runs before the shard update and is unaffected by its later failure")
}
