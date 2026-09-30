//go:build mongodb

package mongodb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// TestPutShardRejectsMismatchedTxidClaimBeforeInsertingRows reproduces the
// review's LOW finding 5: a second PutShard for a txid already claimed (by a
// concurrent or crashed-partial first attempt) with different parameters
// must be rejected before it inserts any of its own rows, not after.
//
// Without the fix, this reproduces a real corruption: the first attempt's
// txid claim and its 3 rows land, but (simulated here, exactly as
// TestRetireShardOrphanRowsWithoutShardDocument does) its shard document
// never does, because it crashed first. A second, mismatched PutShard for
// the same txid (10 rows instead of 3) then sees the txid claim conflict as
// "same pool, partial write in flight, continue", inserts its own new
// (non-colliding by vout) rows, and -- since no shard document exists yet to
// catch the mismatch -- successfully creates one recording size 10, leaving
// a shard whose rows are a mix of both calls' incompatible data.
func TestPutShardRejectsMismatchedTxidClaimBeforeInsertingRows(t *testing.T) {
	uri := testURI(t)
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, uri, clk.Now)
	ctx := context.Background()

	shardA, rowsA := storetest.BuildShard("main", 3, store.ShardMined)
	require.NoError(t, s.PutShard(ctx, shardA, rowsA))

	// Simulate the first attempt crashing right before it wrote the shard
	// document (PutShard writes rows first, the shard record last): the txid
	// claim and shardA's 3 rows are in place, but the shard document is not.
	shardsColl := s.Rows().Database().Collection("fuel_shards")
	_, err := shardsColl.DeleteOne(ctx, bson.D{{Key: "_id", Value: shardDocID(shardA.TxID)}})
	require.NoError(t, err)

	// A second, mismatched PutShard for the same txid: same pool, but 10 rows
	// instead of 3 (and thus a different declared Size).
	shardB, rowsB := storetest.BuildShardWithTxID("main", shardA.TxID, 10, store.ShardMined)
	err = s.PutShard(ctx, shardB, rowsB)
	require.ErrorIs(t, err, store.ErrShardExists, "a mismatched claim must be rejected, not silently adopted")

	// None of shardB's rows may have been inserted: fuel_rows must still hold
	// exactly shardA's original 3 rows for this txid.
	got, err := s.GetRows(ctx, "main", storetest.Ops(rowsB))
	require.NoError(t, err)
	assert.Len(t, got, 3, "only the original 3 rows may exist; none of the mismatched call's extra rows were inserted")
	for _, r := range got {
		assert.Less(t, r.Vout, uint32(3), "no row beyond the original claim's vout range may exist")
	}

	// GetShard must still report ErrNotFound: the shard document was never
	// written by either call.
	_, err = s.GetShard(ctx, "main", shardA.TxID)
	require.ErrorIs(t, err, store.ErrNotFound)
}
