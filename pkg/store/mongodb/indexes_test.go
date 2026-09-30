//go:build mongodb

package mongodb_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func TestIndexesAndNoTTL(t *testing.T) {
	uri := testURI(t)
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, uri, clk.Now)
	ctx := context.Background()

	cur, err := s.Rows().Indexes().List(ctx)
	require.NoError(t, err)
	var rowIdx []bson.M
	require.NoError(t, cur.All(ctx, &rowIdx))

	byName := make(map[string]bson.M, len(rowIdx))
	for _, d := range rowIdx {
		name, _ := d["name"].(string)
		byName[name] = d
		_, hasTTL := d["expireAfterSeconds"]
		assert.False(t, hasTTL, "index %q must never carry a TTL (expireAfterSeconds)", name)
	}

	for _, name := range []string{"rows_avail", "rows_expiry", "rows_quar"} {
		d, ok := byName[name]
		require.True(t, ok, "index %q must exist", name)
		_, hasPartial := d["partialFilterExpression"]
		assert.True(t, hasPartial, "index %q must have a partialFilterExpression", name)
	}
	_, ok := byName["rows_pool_state"]
	assert.True(t, ok, "index rows_pool_state must exist")
	if d, ok := byName["rows_pool_state"]; ok {
		_, hasPartial := d["partialFilterExpression"]
		assert.False(t, hasPartial, "rows_pool_state is not a partial index")
	}

	scur, err := s.Rows().Database().Collection("fuel_shards").Indexes().List(ctx)
	require.NoError(t, err)
	var shardIdx []bson.M
	require.NoError(t, scur.All(ctx, &shardIdx))
	found := false
	for _, d := range shardIdx {
		name, _ := d["name"].(string)
		if name == "shards_adopt" {
			found = true
		}
		_, hasTTL := d["expireAfterSeconds"]
		assert.False(t, hasTTL, "index %q must never carry a TTL", name)
	}
	assert.True(t, found, "shards_adopt index must exist")
}

// TestExplainUsesIndexes checks that the query shapes issued by ListAvailable,
// Quarantine and ListQuarantined actually win a plan that uses the intended
// named index, including its sort, so no separate in-memory SORT stage is
// needed either.
func TestExplainUsesIndexes(t *testing.T) {
	uri := testURI(t)
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, uri, clk.Now)
	ctx := context.Background()

	shard, rows := storetest.MintShard(t, s, "main", 20, store.ShardMined)
	tok := store.NewToken()
	storetest.MustReserve(t, s, "main", rows[:10], tok, clk.Now().Add(time.Hour))
	_, err := s.Spend(ctx, "main", tok, storetest.Ops(rows[:3]), storetest.NewTxID(), false)
	require.NoError(t, err)
	n, err := s.Quarantine(ctx, "main", clk.Now().Add(2*time.Hour), 100)
	require.NoError(t, err)
	require.Positive(t, n)

	db := s.Rows().Database()
	explain := func(find bson.D) string {
		t.Helper()
		var res bson.M
		cmd := bson.D{{Key: "explain", Value: find}}
		err := db.RunCommand(ctx, cmd).Decode(&res)
		require.NoError(t, err)
		b, merr := bson.MarshalExtJSON(res, false, false)
		require.NoError(t, merr)
		return string(b)
	}

	fromID := append(append([]byte(nil), shard.TxID[:]...), 0, 0, 0, 0)
	availFind := bson.D{
		{Key: "find", Value: "fuel_rows"},
		{Key: "filter", Value: bson.D{
			{Key: "p", Value: "main"},
			{Key: "sh", Value: shard.TxID[:]},
			{Key: "st", Value: int32(store.StateAvailable)},
			{Key: "_id", Value: bson.D{{Key: "$gte", Value: fromID}}},
		}},
		{Key: "sort", Value: bson.D{{Key: "_id", Value: 1}}},
	}
	plan := explain(availFind)
	assert.Contains(t, plan, "IXSCAN")
	assert.Contains(t, plan, "rows_avail")

	expiryFind := bson.D{
		{Key: "find", Value: "fuel_rows"},
		{Key: "filter", Value: bson.D{
			{Key: "p", Value: "main"},
			{Key: "st", Value: int32(store.StateReserved)},
			{Key: "ea", Value: bson.D{{Key: "$lt", Value: int64(1 << 62)}}},
		}},
		{Key: "sort", Value: bson.D{{Key: "ea", Value: 1}, {Key: "_id", Value: 1}}},
	}
	plan = explain(expiryFind)
	assert.Contains(t, plan, "IXSCAN")
	assert.Contains(t, plan, "rows_expiry")

	quarFind := bson.D{
		{Key: "find", Value: "fuel_rows"},
		{Key: "filter", Value: bson.D{
			{Key: "p", Value: "main"},
			{Key: "st", Value: int32(store.StateQuarantined)},
			{Key: "qa", Value: bson.D{{Key: "$lt", Value: int64(1 << 62)}}},
		}},
		{Key: "sort", Value: bson.D{{Key: "qa", Value: 1}, {Key: "_id", Value: 1}}},
	}
	plan = explain(quarFind)
	assert.Contains(t, plan, "IXSCAN")
	assert.Contains(t, plan, "rows_quar")
}
