//go:build aerospike

package aerospike

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// testHost skips the test when FUELKEEPER_TEST_AEROSPIKE_HOST is unset.
func testHost(t *testing.T) string {
	t.Helper()
	host := os.Getenv("FUELKEEPER_TEST_AEROSPIKE_HOST")
	if host == "" {
		t.Skip("FUELKEEPER_TEST_AEROSPIKE_HOST not set")
	}
	return host
}

// isolatedStore opens a store scoped to a fresh, randomly-prefixed set of
// sets in the shared "test" namespace, and truncates them on cleanup.
func isolatedStore(t *testing.T, host string, now func() time.Time) *Store {
	t.Helper()
	ctx := context.Background()

	var b [4]byte
	_, _ = rand.Read(b[:])
	prefix := "fk" + hex.EncodeToString(b[:]) + "_"

	// PoolSize: 8, not the package default of 128: RunSuite runs 32 subtests in
	// parallel, each opening its own client against the same single-node
	// Community Edition server (the local docker-compose service), so
	// 32 * PoolSize must stay reasonable for one process to serve without
	// contention skewing latencies past individual operations' timeouts.
	s, err := New(ctx, Config{
		Hosts:     []string{host},
		Namespace: "test",
		SetPrefix: prefix,
		PoolSize:  8,
	}, WithClock(now))
	require.NoError(t, err)

	t.Cleanup(s.Truncate)
	return s
}

func TestConformance(t *testing.T) {
	host := testHost(t)
	storetest.RunSuite(t, func(t *testing.T, now func() time.Time) store.Store {
		return isolatedStore(t, host, now)
	})
}

// TestIndexesExist asserts the five secondary indexes this package relies on
// exist, via the sindex-list info command.
func TestIndexesExist(t *testing.T) {
	host := testHost(t)
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, host, clk.Now)

	nodes := s.client.GetNodes()
	require.NotEmpty(t, nodes)
	info, aerr := nodes[0].RequestInfo(as.NewInfoPolicy(), "sindex-list:ns=test")
	require.Nil(t, aerr)
	list := info["sindex-list:ns=test"]

	for _, spec := range s.indexSpecs() {
		assert.Contains(t, list, spec.indexName, "index %q must exist", spec.indexName)
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
