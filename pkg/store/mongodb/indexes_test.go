//go:build mongodb

package mongodb_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/mongodb"
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

	for _, name := range []string{"rows_avail", "rows_expiry_v2", "rows_quar"} {
		d, ok := byName[name]
		require.True(t, ok, "index %q must exist", name)
		_, hasPartial := d["partialFilterExpression"]
		assert.True(t, hasPartial, "index %q must have a partialFilterExpression", name)
	}
	_, ok := byName["rows_pool_state_v2"]
	assert.True(t, ok, "index rows_pool_state_v2 must exist")
	if d, ok := byName["rows_pool_state_v2"]; ok {
		_, hasPartial := d["partialFilterExpression"]
		assert.False(t, hasPartial, "rows_pool_state_v2 is not a partial index")
	}

	// The v1 index names/shapes this build replaced must never appear on a
	// freshly-created (schema v2 from birth) database: only migrateSchema
	// creates-then-drops them, for a database that was already at v1. See
	// TestNewMigratesSchemaV1Indexes for that path.
	for _, name := range []string{"rows_expiry", "rows_pool_state"} {
		_, exists := byName[name]
		assert.False(t, exists, "v1 index %q must not exist on a freshly-created database", name)
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

// --- winning-plan tree walker -----------------------------------------------
//
// db.runCommand({explain: ...}) decodes its embedded documents as bson.D (not
// bson.M) when the target is a generic bson.D/bson.M, so every helper below
// walks bson.D directly rather than assuming map-like access.

// planInfo is what walkStage extracts from an explain winning-plan tree:
// every IXSCAN's index name (in traversal order) and whether a SORT stage
// (a blocking, in-memory sort) appears anywhere in the tree.
type planInfo struct {
	indexNames []string
	sawSort    bool
}

func bsonDGet(d bson.D, key string) (any, bool) {
	for _, e := range d {
		if e.Key == key {
			return e.Value, true
		}
	}
	return nil, false
}

// walkStage recursively walks one node of an explain plan tree, descending
// through inputStage (single-child stages) and inputStages (fan-in stages
// such as OR/SORT_MERGE), collecting IXSCAN index names and noting any SORT.
func walkStage(v any, info *planInfo) {
	d, ok := v.(bson.D)
	if !ok {
		return
	}
	if stageVal, ok := bsonDGet(d, "stage"); ok {
		switch name, _ := stageVal.(string); name {
		case "SORT":
			info.sawSort = true
		case "IXSCAN":
			if idxVal, ok := bsonDGet(d, "indexName"); ok {
				if name, ok := idxVal.(string); ok {
					info.indexNames = append(info.indexNames, name)
				}
			}
		}
	}
	if in, ok := bsonDGet(d, "inputStage"); ok {
		walkStage(in, info)
	}
	if ins, ok := bsonDGet(d, "inputStages"); ok {
		if arr, ok := ins.(bson.A); ok {
			for _, item := range arr {
				walkStage(item, info)
			}
		}
	}
}

// explainWinningPlan runs cmd through the explain command (at executionStats
// verbosity, so totalKeysExamined/totalDocsExamined are available) and
// extracts its winning-plan tree, handling both a find/update/findAndModify
// shape (queryPlanner at the top level) and an aggregate shape (queryPlanner
// nested under stages[0].$cursor, when the pipeline's leading $match is
// pushed down into a cursor stage). It returns the walked planInfo plus
// totalKeysExamined/totalDocsExamined for diagnostics.
func explainWinningPlan(ctx context.Context, t *testing.T, db *mongo.Database, cmd bson.D) (info planInfo, keysExamined, docsExamined int32) {
	t.Helper()
	full := bson.D{{Key: "explain", Value: cmd}, {Key: "verbosity", Value: "executionStats"}}
	var res bson.D
	require.NoError(t, db.RunCommand(ctx, full).Decode(&res))

	var qp, execStats bson.D
	if v, ok := bsonDGet(res, "queryPlanner"); ok {
		qp, _ = v.(bson.D)
		if v, ok := bsonDGet(res, "executionStats"); ok {
			execStats, _ = v.(bson.D)
		}
	} else if v, ok := bsonDGet(res, "stages"); ok {
		arr, _ := v.(bson.A)
		require.NotEmpty(t, arr, "aggregate explain must have at least one stage")
		firstStage, _ := arr[0].(bson.D)
		cursor, ok := bsonDGet(firstStage, "$cursor")
		require.True(t, ok, "aggregate's first stage must be a $cursor (leading $match not pushed down)")
		curD, _ := cursor.(bson.D)
		if v, ok := bsonDGet(curD, "queryPlanner"); ok {
			qp, _ = v.(bson.D)
		}
		if v, ok := bsonDGet(curD, "executionStats"); ok {
			execStats, _ = v.(bson.D)
		}
	}
	require.NotNil(t, qp, "no queryPlanner found in explain result")

	wp, ok := bsonDGet(qp, "winningPlan")
	require.True(t, ok, "queryPlanner has no winningPlan")
	wpD, _ := wp.(bson.D)
	walkStage(wpD, &info)

	if execStats != nil {
		if v, ok := bsonDGet(execStats, "totalKeysExamined"); ok {
			keysExamined = toInt32(v)
		}
		if v, ok := bsonDGet(execStats, "totalDocsExamined"); ok {
			docsExamined = toInt32(v)
		}
	}
	return info, keysExamined, docsExamined
}

func toInt32(v any) int32 {
	switch n := v.(type) {
	case int32:
		return n
	case int64:
		return int32(n)
	case float64:
		return int32(n)
	default:
		return 0
	}
}

// TestExplainUsesIndexes checks, for every hot query this backend issues
// (Quarantine discovery, RetireShard's cascade, ListQuarantined, Reserve's
// read-back, AcquireShard and Stats), that the *winning* plan -- walked from
// queryPlanner.winningPlan, not the marshalled explain output as a whole --
// uses the intended named index and includes no SORT stage. Asserting
// against the winning plan specifically (rather than checking that the
// index name merely appears somewhere in the explain output) matters
// because rejectedPlans can otherwise make a broken index look "used": see
// the mongodb backend review, MEDIUM finding 2.
//
// The pool is populated at the same scale the review's finding reproduced
// the bug at (20 shards x 500 rows, plus a shard in another pool) so the
// winning plan reflects real selectivity, not an artifact of a tiny
// collection.
func TestExplainUsesIndexes(t *testing.T) {
	uri := testURI(t)
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, uri, clk.Now)
	ctx := context.Background()
	db := s.Rows().Database()

	const shardsInPool, rowsPerShard = 20, 500
	var shards []store.Shard
	for i := 0; i < shardsInPool; i++ {
		sh, rows := storetest.MintShard(t, s, "main", rowsPerShard, store.ShardMined)
		shards = append(shards, sh)
		tok := store.NewToken()
		storetest.MustReserve(t, s, "main", rows, tok, clk.Now().Add(time.Hour))
	}
	// Another pool's shard must never inflate what "main"'s queries examine.
	storetest.MintShard(t, s, "other", rowsPerShard, store.ShardMined)
	target := shards[0]

	// A small, separate shard exercises Spend/Quarantine's own transitions so
	// ListQuarantined has real quarantined rows to page over.
	_, detailRows := storetest.MintShard(t, s, "main", 20, store.ShardMined)
	detailTok := store.NewToken()
	storetest.MustReserve(t, s, "main", detailRows[:10], detailTok, clk.Now().Add(time.Hour))
	_, err := s.Spend(ctx, "main", detailTok, storetest.Ops(detailRows[:3]), storetest.NewTxID(), false)
	require.NoError(t, err)
	n, err := s.Quarantine(ctx, "main", clk.Now().Add(2*time.Hour), 100)
	require.NoError(t, err)
	require.Positive(t, n)

	t.Run("QuarantineDiscovery", func(t *testing.T) {
		// Matches rows.go Quarantine's own filter/sort/limit/projection.
		cmd := bson.D{
			{Key: "find", Value: "fuel_rows"},
			{Key: "filter", Value: bson.D{
				{Key: "p", Value: "main"},
				{Key: "st", Value: int32(store.StateReserved)},
				{Key: "ea", Value: bson.D{{Key: "$lt", Value: int64(1 << 62)}}},
			}},
			{Key: "sort", Value: bson.D{{Key: "ea", Value: 1}, {Key: "_id", Value: 1}}},
			{Key: "limit", Value: int64(100)},
			{Key: "projection", Value: bson.D{{Key: "_id", Value: 1}}},
		}
		info, keys, docs := explainWinningPlan(ctx, t, db, cmd)
		assert.Equal(t, []string{"rows_expiry_v2"}, info.indexNames)
		assert.False(t, info.sawSort, "must not need a blocking SORT stage")
		assert.LessOrEqual(t, keys, int32(100), "keysExamined must be bounded by the limit, not the pool")
		assert.Zero(t, docs, "the {_id} projection must be answered from the index alone")
		t.Logf("QuarantineDiscovery: indexes=%v sort=%v keysExamined=%d docsExamined=%d", info.indexNames, info.sawSort, keys, docs)
	})

	t.Run("RetireShardCascade", func(t *testing.T) {
		// Built through mongodb.RetireCascadeFilter -- the exact function
		// shards.go's RetireShard itself calls -- not a hand-maintained copy,
		// so this test cannot silently drift from the real query. The cascade
		// is bounded by an _id range and hinted to force the _id_ index: see
		// retireCascadeFilter's own comment for why a {p, st, sh} index (st
		// being both part of the index key and the field this very update,
		// and any concurrent Release/Reserve-replay/Spend/Quarantine/Resolve,
		// changes) cannot safely drive a multi-document update.
		cmd := bson.D{
			{Key: "update", Value: "fuel_rows"},
			{Key: "updates", Value: bson.A{bson.D{
				{Key: "q", Value: mongodb.RetireCascadeFilter("main", target.TxID)},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "st", Value: int32(store.StateInvalid)}}}}},
				{Key: "multi", Value: true},
				{Key: "hint", Value: "_id_"},
			}}},
		}
		info, keys, docs := explainWinningPlan(ctx, t, db, cmd)
		assert.Equal(t, []string{"_id_"}, info.indexNames, "the cascade must be driven by the stable _id index, not one keyed on st")
		assert.False(t, info.sawSort)
		assert.LessOrEqual(t, int(keys), rowsPerShard+5, "keysExamined must be bounded by this shard's own row count, not the pool")
		assert.LessOrEqual(t, int(docs), rowsPerShard, "must only touch this shard's own rows")
		t.Logf("RetireShardCascade: indexes=%v sort=%v keysExamined=%d docsExamined=%d", info.indexNames, info.sawSort, keys, docs)
	})

	t.Run("ListQuarantinedCursor", func(t *testing.T) {
		// Matches rows.go ListQuarantined's own filter/sort (no cursor: first page).
		cmd := bson.D{
			{Key: "find", Value: "fuel_rows"},
			{Key: "filter", Value: bson.D{
				{Key: "p", Value: "main"},
				{Key: "st", Value: int32(store.StateQuarantined)},
				{Key: "qa", Value: bson.D{{Key: "$lt", Value: int64(1 << 62)}}},
			}},
			{Key: "sort", Value: bson.D{{Key: "qa", Value: 1}, {Key: "_id", Value: 1}}},
			{Key: "limit", Value: int64(100)},
		}
		info, _, _ := explainWinningPlan(ctx, t, db, cmd)
		assert.Equal(t, []string{"rows_quar"}, info.indexNames)
		assert.False(t, info.sawSort)
	})

	t.Run("ReserveReadback", func(t *testing.T) {
		// Matches rows.go Reserve's classification read, including its _id_
		// hint (added alongside the RetireShard cascade fix: an _id $in
		// already uses the primary-key index in practice, but the hint pins
		// it so a future index cannot win this query by planner heuristics
		// instead). MintShard numbers a shard's rows by vout 0..rowsPerShard-1,
		// so the target's first 50 outpoints can be built directly without a
		// round-trip.
		ids := make(bson.A, 50)
		for i := range ids {
			id := make([]byte, 0, 36)
			id = append(id, target.TxID[:]...)
			id = binary.BigEndian.AppendUint32(id, uint32(i))
			ids[i] = id
		}
		cmd := bson.D{
			{Key: "find", Value: "fuel_rows"},
			{Key: "filter", Value: bson.D{
				{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}},
				{Key: "p", Value: "main"},
				{Key: "st", Value: int32(store.StateReserved)},
			}},
			{Key: "projection", Value: bson.D{{Key: "_id", Value: 1}, {Key: "rb", Value: 1}, {Key: "ea", Value: 1}}},
			{Key: "hint", Value: "_id_"},
		}
		info, keys, docs := explainWinningPlan(ctx, t, db, cmd)
		assert.Equal(t, []string{"_id_"}, info.indexNames, "an _id $in must use the default primary-key index")
		assert.False(t, info.sawSort)
		assert.LessOrEqual(t, int(keys), len(ids)+1, "keysExamined must be bounded by the requested ids, not the pool")
		t.Logf("ReserveReadback: indexes=%v sort=%v keysExamined=%d docsExamined=%d", info.indexNames, info.sawSort, keys, docs)
	})

	t.Run("AcquireShard", func(t *testing.T) {
		// Matches shards.go AcquireShard's own filter/sort.
		cmd := bson.D{
			{Key: "findAndModify", Value: "fuel_shards"},
			{Key: "query", Value: bson.D{
				{Key: "p", Value: "main"},
				{Key: "s", Value: bson.D{{Key: "$in", Value: bson.A{int32(store.ShardMined)}}}},
				{Key: "t", Value: bson.D{{Key: "$nin", Value: bson.A{}}}},
				{Key: "$or", Value: bson.A{
					bson.D{{Key: "o", Value: ""}},
					bson.D{{Key: "lu", Value: bson.D{{Key: "$exists", Value: false}}}},
					bson.D{{Key: "lu", Value: bson.D{{Key: "$lte", Value: int64(1 << 62)}}}},
				}},
			}},
			{Key: "sort", Value: bson.D{{Key: "ca", Value: 1}, {Key: "t", Value: 1}}},
			{Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "o", Value: "explain-probe"}}}}},
		}
		info, _, _ := explainWinningPlan(ctx, t, db, cmd)
		assert.Equal(t, []string{"shards_adopt"}, info.indexNames)
		assert.False(t, info.sawSort)
	})

	t.Run("Stats", func(t *testing.T) {
		// Matches rows.go aggregateCounts's own pipeline (field "st").
		cmd := bson.D{
			{Key: "aggregate", Value: "fuel_rows"},
			{Key: "pipeline", Value: bson.A{
				bson.D{{Key: "$match", Value: bson.D{{Key: "p", Value: "main"}}}},
				bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$st"}, {Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}}}}},
			}},
			{Key: "cursor", Value: bson.D{}},
		}
		info, keys, docs := explainWinningPlan(ctx, t, db, cmd)
		assert.Equal(t, []string{"rows_pool_state_v2"}, info.indexNames)
		assert.False(t, info.sawSort)
		assert.Zero(t, docs, "Stats' {p}-match/{st}-group must stay a covered index scan")
		t.Logf("Stats: indexes=%v sort=%v keysExamined=%d docsExamined=%d", info.indexNames, info.sawSort, keys, docs)
	})
}
