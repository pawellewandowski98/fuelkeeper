package pebble

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// ScanStats recomputes store.Stats for pool by scanning every row and shard
// record directly, bypassing the maintained per-pool counters entirely.
// TestCountersMatchScans in pebble_test.go uses it to check the incrementally
// maintained counters never drift from the underlying data.
func ScanStats(t *testing.T, s *Store, pool string) store.Stats {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()

	var st store.Stats
	rowPrefixAll := []byte("r/")
	err := s.iterate(rowPrefixAll, prefixEnd(rowPrefixAll), func(_, v []byte) (bool, error) {
		r, derr := decodeRow(v)
		if derr != nil {
			return false, derr
		}
		if r.Pool != pool {
			return true, nil
		}
		switch r.State {
		case store.StateAvailable:
			st.Available++
		case store.StateReserved:
			st.Reserved++
		case store.StateQuarantined:
			st.Quarantined++
		case store.StateSpent:
			st.Spent++
		case store.StateInvalid:
			st.Invalid++
		}
		return true, nil
	})
	require.NoError(t, err)

	shPrefix := shardPrefix(pool)
	err = s.iterate(shPrefix, prefixEnd(shPrefix), func(_, v []byte) (bool, error) {
		sh, derr := decodeShard(v)
		if derr != nil {
			return false, derr
		}
		switch sh.Status {
		case store.ShardUnmined:
			st.ShardsUnmined++
		case store.ShardMined:
			st.ShardsMined++
		case store.ShardRetired:
			st.ShardsRetired++
		case store.ShardInvalid:
			st.ShardsInvalid++
		}
		return true, nil
	})
	require.NoError(t, err)

	return st
}
