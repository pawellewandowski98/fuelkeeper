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

// newStoreSem bounds concurrent mongodb.New calls across this package's
// tests. RunSuite alone opens 32 stores in parallel, each against the same
// local standalone mongod; under that much concurrent index/collection
// creation, createIndexes has been observed taking 3-20s, long enough that
// the first findAndModify against a brand-new fuel_leases collection (see
// MEDIUM finding 4 in the mongodb backend review) can queue behind it and
// blow past an individual operation's OpTimeout. Serializing New avoids that
// contention without weakening what any subtest exercises once its store is
// actually open (only setup is serialized, not the subtests themselves,
// which still run fully in parallel).
var newStoreSem = make(chan struct{}, 4)

// isolatedStore opens a store scoped to a fresh, randomly-named database and
// drops that database on cleanup.
func isolatedStore(t *testing.T, uri string, now func() time.Time) *mongodb.Store {
	t.Helper()
	ctx := context.Background()

	var b [6]byte
	_, _ = rand.Read(b[:])
	dbName := "fk_" + hex.EncodeToString(b[:])

	// Registered before New even runs: if New fails partway through (e.g. it
	// created some collections/indexes before erroring), the database it
	// touched must still be dropped, not leaked. t.Cleanup functions run in
	// LIFO order regardless of how the test that registered them exits.
	t.Cleanup(func() {
		dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		admin, aerr := mgo.Connect(options.Client().ApplyURI(uri))
		if aerr != nil {
			t.Logf("isolatedStore cleanup: connect to drop database %s: %v", dbName, aerr)
			return
		}
		defer func() { _ = admin.Disconnect(context.Background()) }()
		if derr := admin.Database(dbName).Drop(dctx); derr != nil {
			t.Logf("isolatedStore cleanup: drop database %s: %v", dbName, derr)
		}
	})

	// MaxPoolSize: 8, not the package default of 64: RunSuite runs 32 subtests in
	// parallel, each opening its own client against the same standalone mongod
	// (the local docker-compose service), so 32 * MaxPoolSize must stay
	// reasonable for one process to serve without contention skewing latencies
	// past individual operations' timeouts.
	newStoreSem <- struct{}{}
	s, err := mongodb.New(ctx, mongodb.Config{URI: uri, Database: dbName, MaxPoolSize: 8}, mongodb.WithClock(now))
	<-newStoreSem
	require.NoError(t, err)

	t.Cleanup(func() {
		if cerr := s.Close(); cerr != nil {
			t.Logf("isolatedStore cleanup: close store: %v", cerr)
		}
	})
	return s
}

func TestConformance(t *testing.T) {
	uri := testURI(t)
	storetest.RunSuite(t, func(t *testing.T, now func() time.Time) store.Store {
		return isolatedStore(t, uri, now)
	})
}
