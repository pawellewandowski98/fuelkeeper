//go:build mongodb

package mongodb_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	mgo "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store/mongodb"
)

// indexNames lists the names of every index on coll.
func indexNames(ctx context.Context, t *testing.T, coll *mgo.Collection) map[string]bool {
	t.Helper()
	cur, err := coll.Indexes().List(ctx)
	require.NoError(t, err)
	var docs []bson.M
	require.NoError(t, cur.All(ctx, &docs))
	out := make(map[string]bool, len(docs))
	for _, d := range docs {
		if name, ok := d["name"].(string); ok {
			out[name] = true
		}
	}
	return out
}

// TestNewMigratesSchemaV1Indexes reproduces a database left behind by a
// schema-v1 build (the v1-named/-shaped rows_expiry/rows_pool_state indexes,
// and fuel_meta.schema.v == 1) and confirms New migrates it cleanly to v2 --
// dropping the v1 indexes and creating the v2 ones -- and that a second New
// against the now-migrated database is a no-op (idempotent, no error, no
// index churn).
//
// Migrating in place under new index names (rather than redefining
// rows_expiry/rows_pool_state with v2's key pattern) is required: the server
// rejects re-creating an index under an existing name with a different key
// pattern (IndexKeySpecsConflict), which this test also guards against by
// asserting New succeeds at all.
func TestNewMigratesSchemaV1Indexes(t *testing.T) {
	uri := testURI(t)
	ctx := context.Background()

	var b [6]byte
	_, _ = rand.Read(b[:])
	dbName := "fk_" + hex.EncodeToString(b[:])

	t.Cleanup(func() {
		dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		admin, aerr := mgo.Connect(options.Client().ApplyURI(uri))
		if aerr != nil {
			t.Logf("cleanup: connect for drop: %v", aerr)
			return
		}
		defer func() { _ = admin.Disconnect(context.Background()) }()
		if derr := admin.Database(dbName).Drop(dctx); derr != nil {
			t.Logf("cleanup: drop database %s: %v", dbName, derr)
		}
	})

	// Build a schema-v1 database by hand: the v1-named/-shaped indexes, and a
	// fuel_meta record at version 1.
	admin, err := mgo.Connect(options.Client().ApplyURI(uri))
	require.NoError(t, err)
	db := admin.Database(dbName)
	rows := db.Collection("fuel_rows")
	_, err = rows.Indexes().CreateMany(ctx, []mgo.IndexModel{
		{
			Keys: bson.D{{Key: "p", Value: 1}, {Key: "ea", Value: 1}},
			Options: options.Index().SetName("rows_expiry").
				SetPartialFilterExpression(bson.D{{Key: "st", Value: int32(1)}}),
		},
		{
			Keys:    bson.D{{Key: "p", Value: 1}, {Key: "st", Value: 1}},
			Options: options.Index().SetName("rows_pool_state"),
		},
	})
	require.NoError(t, err)
	_, err = db.Collection("fuel_meta").InsertOne(ctx, bson.D{{Key: "_id", Value: "schema"}, {Key: "v", Value: int64(1)}})
	require.NoError(t, err)
	require.NoError(t, admin.Disconnect(context.Background()))

	// New must migrate this v1 database cleanly: v2's indexes appear, v1's
	// are gone, and the recorded schema version is now 2.
	s, err := mongodb.New(ctx, mongodb.Config{URI: uri, Database: dbName})
	require.NoError(t, err, "New must migrate a schema-v1 database, not fail with IndexKeySpecsConflict")

	names := indexNames(ctx, t, s.Rows())
	assert.True(t, names["rows_expiry_v2"], "v2 index must be created")
	assert.True(t, names["rows_pool_state_v2"], "v2 index must be created")
	assert.False(t, names["rows_expiry"], "v1 index must be dropped")
	assert.False(t, names["rows_pool_state"], "v1 index must be dropped")

	var meta struct {
		Version int64 `bson:"v"`
	}
	require.NoError(t, s.Rows().Database().Collection("fuel_meta").FindOne(ctx, bson.D{{Key: "_id", Value: "schema"}}).Decode(&meta))
	assert.Equal(t, int64(2), meta.Version, "schema version must be bumped to 2")
	require.NoError(t, s.Close())

	// A second New against the now-migrated database is a no-op: it must
	// succeed, and must not reintroduce the v1 indexes or otherwise disturb
	// the v2 ones.
	s2, err := mongodb.New(ctx, mongodb.Config{URI: uri, Database: dbName})
	require.NoError(t, err, "a second New against an already-migrated database must succeed")
	names2 := indexNames(ctx, t, s2.Rows())
	assert.Equal(t, names, names2, "a second New must not change the index set")
	require.NoError(t, s2.Close())
}
