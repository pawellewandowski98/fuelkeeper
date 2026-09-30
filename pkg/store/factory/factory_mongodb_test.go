//go:build mongodb

package factory_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	mgo "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/factory"
)

// TestNewMongoDB opens a mongodb-backed store through the factory against
// FUELKEEPER_TEST_MONGODB_URI (skipped when unset), on a fresh, randomly
// named database that is dropped on cleanup, and runs a tiny smoke:
// Capabilities, Stats on an empty pool, then Close.
func TestNewMongoDB(t *testing.T) {
	uri := os.Getenv("FUELKEEPER_TEST_MONGODB_URI")
	if uri == "" {
		t.Skip("FUELKEEPER_TEST_MONGODB_URI not set")
	}
	ctx := context.Background()

	var b [6]byte
	_, _ = rand.Read(b[:])
	dbName := "fk_factory_" + hex.EncodeToString(b[:])

	t.Cleanup(func() {
		dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		admin, err := mgo.Connect(options.Client().ApplyURI(uri))
		if err != nil {
			t.Logf("cleanup: connect to drop database %s: %v", dbName, err)
			return
		}
		defer func() { _ = admin.Disconnect(context.Background()) }()
		if err := admin.Database(dbName).Drop(dctx); err != nil {
			t.Logf("cleanup: drop database %s: %v", dbName, err)
		}
	})

	s, err := factory.New(ctx, factory.Config{
		Backend: factory.BackendMongoDB,
		MongoDB: factory.MongoDBConfig{URI: uri, Database: dbName, MaxPoolSize: 8},
	}, time.Now)
	require.NoError(t, err)

	require.True(t, s.Capabilities().MultiProcess)
	stats, err := s.Stats(ctx, "factory-smoke-pool")
	require.NoError(t, err)
	require.Equal(t, store.Stats{}, stats, "empty pool must report zero counts")
	require.NoError(t, s.Close())
}
