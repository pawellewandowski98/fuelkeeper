//go:build mongodb

package mongodb_test

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
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/mongodb"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// testURI skips the test when FUELKEEPER_TEST_MONGODB_URI is unset.
func testURI(t *testing.T) string {
	t.Helper()
	uri := os.Getenv("FUELKEEPER_TEST_MONGODB_URI")
	if uri == "" {
		t.Skip("FUELKEEPER_TEST_MONGODB_URI not set")
	}
	return uri
}

// isolatedStore opens a store scoped to a fresh, randomly-named database and
// drops that database on cleanup.
func isolatedStore(t *testing.T, uri string, now func() time.Time) *mongodb.Store {
	t.Helper()
	ctx := context.Background()

	var b [6]byte
	_, _ = rand.Read(b[:])
	dbName := "fk_" + hex.EncodeToString(b[:])

	// MaxPoolSize: 8, not the package default of 64: RunSuite runs 32 subtests in
	// parallel, each opening its own client against the same standalone mongod
	// (the local docker-compose service), so 32 * MaxPoolSize must stay
	// reasonable for one process to serve without contention skewing latencies
	// past individual operations' timeouts.
	s, err := mongodb.New(ctx, mongodb.Config{URI: uri, Database: dbName, MaxPoolSize: 8}, mongodb.WithClock(now))
	require.NoError(t, err)

	t.Cleanup(func() {
		dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		admin, aerr := mgo.Connect(options.Client().ApplyURI(uri))
		if aerr == nil {
			_ = admin.Database(dbName).Drop(dctx)
			_ = admin.Disconnect(context.Background())
		}
		_ = s.Close()
	})
	return s
}

func TestConformance(t *testing.T) {
	uri := testURI(t)
	storetest.RunSuite(t, func(t *testing.T, now func() time.Time) store.Store {
		return isolatedStore(t, uri, now)
	})
}
