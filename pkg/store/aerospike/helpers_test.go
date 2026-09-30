package aerospike

import (
	"math/rand"
	"sort"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func TestParseStrongConsistency(t *testing.T) {
	sc, err := parseStrongConsistency("objects=0;replication-factor=2;strong-consistency=true;strong-consistency-allow-expunge=false")
	require.NoError(t, err)
	assert.True(t, sc)

	sc, err = parseStrongConsistency("replication-factor=1;strong-consistency=false;strong-consistency-allow-expunge=false")
	require.NoError(t, err)
	assert.False(t, sc)

	_, err = parseStrongConsistency("")
	require.Error(t, err, "an unknown namespace answers with an empty string")
	_, err = parseStrongConsistency("replication-factor=1")
	require.Error(t, err)
}

func TestValidateKeyPrefix(t *testing.T) {
	require.NoError(t, validateKeyPrefix(""))
	require.NoError(t, validateKeyPrefix("t0123abcd"))
	for _, bad := range []string{"a|b", "a\x00b", "\xff", string(make([]byte, maxKeyPrefix+1))} {
		require.ErrorIs(t, validateKeyPrefix(bad), store.ErrInvalidArgument, "%q", bad)
	}
}

func TestMatchExistingIndex(t *testing.T) {
	list := "ns=test:indexname=fk_rows_rx:set=fk_rows:bin=rx:type=numeric:indextype=default:context=null:exp=null:state=RW;" +
		"ns=test:indexname=fk_rows_ak:set=fk_rows:bin=ak:type=string:indextype=default:context=null:exp=null:state=WO;" +
		"ns=test:indexname=other_name:set=fk_rows:bin=pst:type=string:indextype=default:context=null:exp=null:state=RW;" +
		"ns=test:indexname=fk_rows_qx:set=fk_rows:bin=qx:type=string:indextype=default:context=null:exp=null:state=RW"
	entries := parseSindexList(list)
	require.Len(t, entries, 4)

	ready, err := matchExistingIndex(entries, indexSpec{"fk_rows", "fk_rows_rx", "rx", as.NUMERIC})
	require.NoError(t, err)
	assert.True(t, ready)

	ready, err = matchExistingIndex(entries, indexSpec{"fk_rows", "fk_rows_ak", "ak", as.STRING})
	require.NoError(t, err)
	assert.False(t, ready, "still building")

	ready, err = matchExistingIndex(entries, indexSpec{"fk_rows", "fk_rows_pst", "pst", as.STRING})
	require.NoError(t, err, "same set/bin/type under another name is acceptable")
	assert.True(t, ready)

	_, err = matchExistingIndex(entries, indexSpec{"fk_rows", "fk_rows_qx", "qx", as.NUMERIC})
	require.Error(t, err, "same name, wrong type must be rejected")

	_, err = matchExistingIndex(entries, indexSpec{"fk_shards", "fk_rows_rx", "rx", as.NUMERIC})
	require.Error(t, err, "same name on another set must be rejected")
}

func TestBoundedTopKeepsSmallest(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, limit := range []int{1, 3, 10, 100} {
		vals := rng.Perm(57)
		top := newBoundedTop(limit, func(a, b int) bool { return a < b })
		for _, v := range vals {
			top.push(v)
		}
		want := append([]int(nil), vals...)
		sort.Ints(want)
		want = want[:min(limit, len(want))]
		assert.Equal(t, want, top.sorted(), "limit %d", limit)
	}
}

func TestShardOutpointsRoundTrip(t *testing.T) {
	sh, rows := storetest.BuildShard("main", 5, store.ShardMined)
	rows[2].Vout = 77 // vouts need not be 0..n-1
	got, err := shardOutpoints(sh.TxID, packVouts(rows))
	require.NoError(t, err)
	assert.Equal(t, storetest.Ops(rows), got)
}

// TestShardOutpointsRejectsMissingVoutList pins the nit fix: schema v2's
// migration refuses to start against a v1 store, so a shard record with no vo
// bin is corrupt, not legacy, and must never be papered over by guessing
// vouts 0..size-1. A hand-built record (no vo bin at all, and one with an
// empty vo bin) must fail loudly instead.
func TestShardOutpointsRejectsMissingVoutList(t *testing.T) {
	txid := storetest.NewTxID()

	_, err := shardOutpoints(txid, nil)
	require.ErrorIs(t, err, store.ErrUnavailable, "a hand-built record with no vo bin at all")

	_, err = shardOutpoints(txid, []byte{})
	require.ErrorIs(t, err, store.ErrUnavailable, "a hand-built record with an empty vo bin")
}

func TestKeysAndIndexValuesCarryTenant(t *testing.T) {
	plain := &Store{ns: "test", rowsSet: "rows", shardsSet: "shards"}
	tn := &Store{ns: "test", rowsSet: "rows", shardsSet: "shards", keyPrefix: "tA", tenant: "tA|"}
	txid := storetest.NewTxID()
	op := store.Outpoint{TxID: txid, Vout: 1}

	assert.Len(t, plain.rowKeyBytes(op), 36)
	assert.Equal(t, append([]byte("tA"), plain.rowKeyBytes(op)...), tn.rowKeyBytes(op))
	assert.Equal(t, "tA|"+plain.akFor("main", txid), tn.akFor("main", txid))
	assert.Equal(t, "tA|"+plain.pstFor("main", store.StateReserved), tn.pstFor("main", store.StateReserved))
	assert.Equal(t, "tA|"+plain.psFor("main", store.ShardMined), tn.psFor("main", store.ShardMined))
	assert.NotContains(t, plain.rowBins(store.Row{Outpoint: op, Pool: "main"}), binTN)
	assert.Equal(t, "tA", tn.rowBins(store.Row{Outpoint: op, Pool: "main"})[binTN])
}
