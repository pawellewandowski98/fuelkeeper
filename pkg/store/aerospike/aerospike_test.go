//go:build aerospike

package aerospike

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"os"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// testHost skips the test unless FUELKEEPER_TEST_AEROSPIKE_HOST (or the
// Makefile's AERO_HOST) names a server.
func testHost(t *testing.T) string {
	t.Helper()
	host := os.Getenv("FUELKEEPER_TEST_AEROSPIKE_HOST")
	if host == "" {
		host = os.Getenv("AERO_HOST")
	}
	if host == "" {
		t.Skip("FUELKEEPER_TEST_AEROSPIKE_HOST not set")
	}
	return host
}

// testSetPrefix is the FIXED set-name prefix every test store shares.
// Aerospike never frees a set name and caps a namespace at ~4K of them, so
// tests must never generate set names; they isolate by KeyPrefix instead.
const testSetPrefix = "fk_"

// randKeyPrefix returns a fresh tenant KeyPrefix.
func randKeyPrefix() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return "t" + hex.EncodeToString(b[:])
}

// openTestStoreTB is openTestStore's testing.TB-based implementation, so
// benchmarks (*testing.B) can open a test store the same way tests
// (*testing.T) do.
func openTestStoreTB(tb testing.TB, host, keyPrefix string, now func() time.Time) *Store {
	tb.Helper()
	// PoolSize: 8, not the package default of 128: RunSuite runs 33 subtests
	// in parallel, each opening its own client against the same single-node
	// server, so 33 * PoolSize must stay reasonable for one process.
	s, err := New(context.Background(), Config{
		Hosts:                 []string{host},
		Namespace:             "test",
		SetPrefix:             testSetPrefix,
		KeyPrefix:             keyPrefix,
		AllowAvailabilityMode: true,
		PoolSize:              8,
	}, WithClock(now), WithLogger(slog.New(slog.DiscardHandler)))
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = s.Close() })
	return s
}

// openTestStore opens a store on the fixed test sets under keyPrefix (in
// availability mode: the test server is Community Edition). It registers
// Close but not Truncate.
func openTestStore(t *testing.T, host, keyPrefix string, now func() time.Time) *Store {
	t.Helper()
	return openTestStoreTB(t, host, keyPrefix, now)
}

// isolatedStoreTB is isolatedStore's testing.TB-based implementation, so
// benchmarks (*testing.B) can get their own isolated key prefix the same way
// tests (*testing.T) do.
func isolatedStoreTB(tb testing.TB, host string, now func() time.Time) *Store {
	tb.Helper()
	s := openTestStoreTB(tb, host, randKeyPrefix(), now)
	// Registered before RunSuite's own Close cleanup, so it runs after it:
	// Truncate uses its own connection.
	tb.Cleanup(func() { assert.NoError(tb, s.Truncate(), "truncate") })
	return s
}

// isolatedStore opens a store isolated by a fresh random KeyPrefix on the
// fixed test sets, and deletes exactly its records on cleanup; a Truncate
// failure fails the test.
func isolatedStore(t *testing.T, host string, now func() time.Time) *Store {
	t.Helper()
	return isolatedStoreTB(t, host, now)
}

func TestConformance(t *testing.T) {
	host := testHost(t)
	storetest.RunSuite(t, func(t *testing.T, now func() time.Time) store.Store {
		return isolatedStore(t, host, now)
	})
}

// TestIndexesExist asserts every secondary index this package relies on
// exists with the expected set, bin and type, via the sindex-list info
// command.
func TestIndexesExist(t *testing.T) {
	host := testHost(t)
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, host, clk.Now)

	nodes := s.client.GetNodes()
	require.NotEmpty(t, nodes)
	info, aerr := nodes[0].RequestInfo(as.NewInfoPolicy(), "sindex-list:ns=test")
	require.Nil(t, aerr)
	list := info["sindex-list:ns=test"]

	entries := parseSindexList(list)
	for _, spec := range s.indexSpecs() {
		ready, err := matchExistingIndex(entries, spec)
		require.NoError(t, err)
		assert.True(t, ready, "index %q must exist and be readable", spec.indexName)
	}
}

// TestNoTTL asserts a written row record never expires: the namespace's
// default-ttl 0, combined with this package never setting a
// WritePolicy.Expiration, means the server reports TTLDontExpire on read.
func TestNoTTL(t *testing.T) {
	host := testHost(t)
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, host, clk.Now)

	_, rows := storetest.MintShard(t, s, "main", 1, store.ShardMined)

	key, err := s.rowKey(rows[0].Outpoint)
	require.NoError(t, err)
	rec, aerr := s.client.Get(s.readPolicy(context.Background()), key)
	require.Nil(t, aerr)
	assert.Equal(t, uint32(as.TTLDontExpire), rec.Expiration, "rows must never expire under default-ttl 0")

	shardKey, err := s.shardKey("main", rows[0].TxID)
	require.NoError(t, err)
	shardRec, aerr := s.client.Get(s.readPolicy(context.Background()), shardKey)
	require.Nil(t, aerr)
	assert.Equal(t, uint32(as.TTLDontExpire), shardRec.Expiration, "shards must never expire under default-ttl 0")
}
