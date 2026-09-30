package pebble

import (
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// VerifyInternalConsistency re-derives every is/, ie/, iq/, c/ and x/ key
// pebble should have purely from a scan of the r/ (row) and s/ (shard)
// primary records -- the only records any Store method's caller-visible
// behavior actually depends on -- and checks it against what pebble actually
// has stored under those secondary keys. The primary records are ground
// truth; every index key and every counter is a redundant, incrementally
// maintained derivative of them (see setRow, setShard and applyCounters) that
// must never drift, even in the hypothetical case where a drift doesn't
// happen to change any result the public store.Store API can observe.
// TestDifferentialFuzz calls this after every fuzzed operation.
func VerifyInternalConsistency(t *testing.T, s *Store) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()

	var rows []store.Row
	require.NoError(t, s.iterate([]byte("r/"), prefixEnd([]byte("r/")), func(_, v []byte) (bool, error) {
		r, err := decodeRow(v)
		if err != nil {
			return false, err
		}
		rows = append(rows, r)
		return true, nil
	}))

	var shards []store.Shard
	require.NoError(t, s.iterate([]byte("s/"), prefixEnd([]byte("s/")), func(_, v []byte) (bool, error) {
		sh, err := decodeShard(v)
		if err != nil {
			return false, err
		}
		shards = append(shards, sh)
		return true, nil
	}))

	// --- derive what is/, ie/, iq/, c/ and x/ should hold, from rows/shards alone ---
	wantExpiry := map[string]bool{}
	wantQuarantine := map[string]bool{}
	wantCounter := map[string]int64{}
	for _, r := range rows {
		wantCounter[string(rowCounterKey(r.Pool, r.State))]++
		switch r.State {
		case store.StateReserved:
			wantExpiry[string(expiryKey(r.Pool, r.ExpiresAt, r.Outpoint))] = true
		case store.StateQuarantined:
			wantQuarantine[string(quarantineKey(r.Pool, r.QuarantinedAt, r.Outpoint))] = true
		case store.StateAvailable, store.StateSpent, store.StateInvalid:
			// no index entry
		}
	}
	wantAdopt := map[string]bool{}
	wantTxidPool := map[chainhash.Hash]string{}
	for _, sh := range shards {
		wantCounter[string(shardCounterKey(sh.Pool, sh.Status))]++
		wantAdopt[string(adoptKey(sh.Pool, sh.Status, sh.CreatedAt, sh.TxID))] = true
		wantTxidPool[sh.TxID] = sh.Pool
	}

	// --- compare against what is actually stored ---
	gotExpiry := map[string]bool{}
	require.NoError(t, s.iterate([]byte("ie/"), prefixEnd([]byte("ie/")), func(k, _ []byte) (bool, error) {
		gotExpiry[string(k)] = true
		return true, nil
	}))
	require.Equal(t, wantExpiry, gotExpiry, "ie/ expiry index drifted from the r/ scan")

	gotQuarantine := map[string]bool{}
	require.NoError(t, s.iterate([]byte("iq/"), prefixEnd([]byte("iq/")), func(k, _ []byte) (bool, error) {
		gotQuarantine[string(k)] = true
		return true, nil
	}))
	require.Equal(t, wantQuarantine, gotQuarantine, "iq/ quarantine index drifted from the r/ scan")

	gotAdopt := map[string]bool{}
	require.NoError(t, s.iterate([]byte("is/"), prefixEnd([]byte("is/")), func(k, _ []byte) (bool, error) {
		gotAdopt[string(k)] = true
		return true, nil
	}))
	require.Equal(t, wantAdopt, gotAdopt, "is/ adoption index drifted from the s/ scan")

	gotCounter := map[string]int64{}
	require.NoError(t, s.iterate([]byte("c/"), prefixEnd([]byte("c/")), func(k, v []byte) (bool, error) {
		gotCounter[string(k)] = deInt64(v)
		return true, nil
	}))
	// A counter key is never deleted once created, only ever overwritten
	// (including back to 0); wantCounter only gains an entry for a
	// (pool,state)/(pool,status) pair that currently has at least one row/
	// shard, so a stored-but-currently-zero counter has no entry to compare
	// against. Drop those before comparing so a counter that legitimately
	// returned to zero doesn't read as a spurious extra key.
	for k, n := range gotCounter {
		if n == 0 {
			if _, want := wantCounter[k]; !want {
				delete(gotCounter, k)
			}
		}
	}
	require.Equal(t, wantCounter, gotCounter, "c/ counters drifted from the r/+s/ scan")

	gotTxidPool := map[chainhash.Hash]string{}
	require.NoError(t, s.iterate([]byte("x/"), prefixEnd([]byte("x/")), func(k, v []byte) (bool, error) {
		var txid chainhash.Hash
		copy(txid[:], k[2:])
		gotTxidPool[txid] = string(v)
		return true, nil
	}))
	require.Equal(t, wantTxidPool, gotTxidPool, "x/ txid->pool index drifted from the s/ scan")
}

// BenchLoadRowsPointGet resolves every op via one loadRow (DB.Get point
// lookup) call each: this is the strategy Reserve used unconditionally before
// reserveSeekThreshold, and is still what it uses below that threshold.
// Exported for BenchmarkReserveRowLookup in pebble_test.
func (s *Store) BenchLoadRowsPointGet(ops []store.Outpoint) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, op := range ops {
		r, err := s.loadRow(op)
		if err != nil {
			return 0, err
		}
		if r != nil {
			n++
		}
	}
	return n, nil
}

// BenchLoadRowsSeek resolves every op via loadRowsSorted, the single sorted
// forward-iterator walk Reserve uses above reserveSeekThreshold. ops must
// already be sorted by store.LessOutpoint. Exported for
// BenchmarkReserveRowLookup in pebble_test.
func (s *Store) BenchLoadRowsSeek(ops []store.Outpoint) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.loadRowsSorted(ops)
	if err != nil {
		return 0, err
	}
	return len(m), nil
}
