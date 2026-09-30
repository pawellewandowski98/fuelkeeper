package sqlite_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/sqlite"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// TestQueryPlansUsePartialIndexes EXPLAINs the exact statements this package
// ships (re-exported as constants in export_test.go), after ANALYZE runs with
// zero reserved/quarantined rows: the worst-case staleness window that made an
// unguarded predicate pick a partial index over the primary key in the
// Postgres backend (see postgres/rows.go's updateReservedWhereSQL comment).
// SQLite does not have that plan cliff -- it prefers an exact primary-key
// match outright -- so this test exists to pin that property empirically
// rather than to guard against a known failure mode: every per-row CAS
// statement must use fuel_rows' primary key, and every ordered range scan
// must use its own partial index without a separate sort (no "TEMP B-TREE").
func TestQueryPlansUsePartialIndexes(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	s, err := sqlite.Open(ctx, sqlite.Config{Path: filepath.Join(t.TempDir(), "plan.sqlite")}, sqlite.WithClock(clk.Now))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	var target store.Shard
	for i := 0; i < 5; i++ {
		sh, _ := storetest.MintShard(t, s, "main", 1000, store.ShardMined)
		if i == 2 {
			target = sh
		}
	}

	// ANALYZE while every row is still available and nothing is reserved or
	// quarantined: the precondition the task calls for.
	require.NoError(t, s.Analyze(ctx))

	plan := func(q string, args ...any) string {
		lines, err := s.ExplainQueryPlan(ctx, q, args...)
		require.NoError(t, err)
		return strings.Join(lines, "\n")
	}

	someTxID := target.TxID
	someVout := uint32(15)
	someTok := store.NewToken() // never actually reserved under this token; EXPLAIN never executes anything
	now := clk.Now().Add(time.Hour).UnixMicro()
	spendTx := storetest.NewTxID()

	// --- Ordered range scans: partial index, no separate sort. ---

	// fuel_rows_avail, the primary key range and fuel_rows_pool_state are all
	// acceptable here: unlike fuel_rows_avail/fuel_rows_expiry/fuel_rows_quar,
	// fuel_rows_pool_state is not a partial index conditioned on state, so it
	// carries none of the stale-ANALYZE plan-cliff risk this pin otherwise
	// guards against (see requirePKOnly below) -- and on a WITHOUT ROWID table
	// a non-covering index's entries carry the table's primary key (txid, vout)
	// trailing the declared columns, so a (pool, state) seek here still lands
	// in (txid, vout) order with no extra sort, which is the property that
	// actually matters and is checked next.
	p := plan(sqlite.ListAvailableSQL, "main", target.TxID[:], 0, 100)
	require.True(t, strings.Contains(p, "fuel_rows_avail") || strings.Contains(p, "PRIMARY KEY") || strings.Contains(p, "fuel_rows_pool_state"),
		"expected fuel_rows_avail, the primary key range, or fuel_rows_pool_state, got:\n%s", p)
	require.NotContains(t, p, "TEMP B-TREE", p)

	p = plan(sqlite.QuarantineCandidateSQL, "main", now, 1000)
	require.Contains(t, p, "fuel_rows_expiry", p)

	p = plan(sqlite.ListQuarantinedSQL, "main", now, 500)
	require.Contains(t, p, "fuel_rows_quar", p)
	require.NotContains(t, p, "TEMP B-TREE", p)

	p = plan(sqlite.ListQuarantinedAfterSQL, "main", now, clk.Now().UnixMicro(), someTxID[:], someVout, 500)
	require.Contains(t, p, "fuel_rows_quar", p)
	require.NotContains(t, p, "TEMP B-TREE", p)

	// --- Per-row CAS statements: primary key, never a partial index. ---

	p = plan(sqlite.ReserveRowSQL, someTok[:], now, now, now, "main", someTxID[:], someVout, someTok[:])
	requirePKOnly(t, p)

	p = plan(sqlite.ReleaseRowSQL, now, "main", someTxID[:], someVout, someTok[:])
	requirePKOnly(t, p)

	p = plan(sqlite.SpendTerminalRowSQL, spendTx[:], now, "main", someTxID[:], someVout, someTok[:])
	requirePKOnly(t, p)

	p = plan(sqlite.SpendHintRowSQL, now, spendTx[:], now, "main", someTxID[:], someVout, someTok[:])
	requirePKOnly(t, p)

	p = plan(sqlite.ResolveAvailableRowSQL, now, "main", someTxID[:], someVout)
	requirePKOnly(t, p)

	p = plan(sqlite.ResolveSpentRowSQL, spendTx[:], now, "main", someTxID[:], someVout)
	requirePKOnly(t, p)

	p = plan(sqlite.RetireShardRowSQL, now, "main", someTxID[:], someVout)
	requirePKOnly(t, p)
	require.NotContains(t, p, "fuel_rows_shard_live", p)
}

// TestAcquireShardProbesUseAdoptIndex pins that AcquireShard's per-status
// UNION ALL probes (see acquireShardProbeSQL) each seek fuel_shards_adopt
// directly, for both one and two requested statuses, without a separate sort
// inside any individual probe.
func TestAcquireShardProbesUseAdoptIndex(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	s, err := sqlite.Open(ctx, sqlite.Config{Path: filepath.Join(t.TempDir(), "adopt.sqlite")}, sqlite.WithClock(clk.Now))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	for i := 0; i < 5; i++ {
		storetest.MintShard(t, s, "main", 1, store.ShardMined)
	}
	require.NoError(t, s.Analyze(ctx))

	now := clk.Now().UnixMicro()

	// One status, no exclusions.
	lines, err := s.ExplainQueryPlan(ctx, sqlite.AcquireShardSQL(1, 0),
		"holder", now, now,
		"main", int(store.ShardMined), now)
	require.NoError(t, err)
	plan := strings.Join(lines, "\n")
	assert.Equal(t, 1, strings.Count(plan, "fuel_shards_adopt"),
		"one status: exactly one probe should use fuel_shards_adopt, got:\n%s", plan)
	assertNoTempBTreeInProbes(t, lines)

	// Two statuses, no exclusions.
	lines, err = s.ExplainQueryPlan(ctx, sqlite.AcquireShardSQL(2, 0),
		"holder", now, now,
		"main", int(store.ShardUnmined), now,
		"main", int(store.ShardMined), now)
	require.NoError(t, err)
	plan = strings.Join(lines, "\n")
	assert.Equal(t, 2, strings.Count(plan, "fuel_shards_adopt"),
		"two statuses: each probe should use fuel_shards_adopt, got:\n%s", plan)
	assertNoTempBTreeInProbes(t, lines)
}

// assertNoTempBTreeInProbes asserts no plan line that searches fuel_shards_adopt
// also needs a separate sort: each probe's own "ORDER BY created_at, txid
// LIMIT 1" must be satisfied directly by the index's order.
func assertNoTempBTreeInProbes(t *testing.T, lines []string) {
	t.Helper()
	for _, l := range lines {
		if strings.Contains(l, "fuel_shards_adopt") {
			assert.NotContains(t, l, "TEMP B-TREE", "a probe using fuel_shards_adopt must not also need a separate sort:\n%s", strings.Join(lines, "\n"))
		}
	}
}

// requirePKOnly asserts a per-row CAS plan uses fuel_rows' primary key and
// never one of the partial indexes a stale ANALYZE could otherwise make look
// cheap (see the Postgres backend's own requirePKOnly, which this mirrors).
func requirePKOnly(t *testing.T, plan string) {
	t.Helper()
	require.Contains(t, plan, "PRIMARY KEY", plan)
	require.NotContains(t, plan, "fuel_rows_avail", plan)
	require.NotContains(t, plan, "fuel_rows_expiry", plan)
	require.NotContains(t, plan, "fuel_rows_quar", plan)
}
