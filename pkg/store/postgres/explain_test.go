//go:build postgres

package postgres_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/postgres"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// sqlLit renders a Go value as a Postgres SQL literal, for building a literal
// "EXECUTE name(...)" call: EXPLAIN EXECUTE cannot take driver-bound
// parameters, only a literal argument list parsed by the server itself.
func sqlLit(v any) string {
	switch x := v.(type) {
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case []byte:
		return "'\\x" + hex.EncodeToString(x) + "'::bytea"
	case int:
		return strconv.Itoa(x)
	case int16:
		return strconv.Itoa(int(x))
	case int32:
		return strconv.Itoa(int(x))
	case time.Time:
		return "'" + x.UTC().Format(time.RFC3339Nano) + "'::timestamptz"
	case []int16:
		parts := make([]string, len(x))
		for i, n := range x {
			parts[i] = strconv.Itoa(int(n))
		}
		return "ARRAY[" + strings.Join(parts, ",") + "]::int2[]"
	case []int32:
		parts := make([]string, len(x))
		for i, n := range x {
			parts[i] = strconv.Itoa(int(n))
		}
		return "ARRAY[" + strings.Join(parts, ",") + "]::int4[]"
	case [][]byte:
		parts := make([]string, len(x))
		for i, b := range x {
			parts[i] = "'\\x" + hex.EncodeToString(b) + "'::bytea"
		}
		return "ARRAY[" + strings.Join(parts, ",") + "]::bytea[]"
	default:
		panic(fmt.Sprintf("sqlLit: unsupported type %T", v))
	}
}

// explainGeneric PREPAREs sqlText under the session's already-forced
// plan_cache_mode = force_generic_plan (set once by the caller on conn) and
// returns the EXPLAIN output of EXECUTE-ing it with args as literals. Using a
// literal EXECUTE, not pgx's own parameter binding, is what makes this a
// genuinely generic (parameter-value-blind) plan from the very first
// execution, rather than depending on a connection having already run the
// same statement text 5+ times in production before Postgres's own
// custom-to-generic heuristic kicks in.
func explainGeneric(t *testing.T, conn *pgxpool.Conn, name, sqlText string, args ...any) string {
	t.Helper()
	ctx := context.Background()
	_, err := conn.Exec(ctx, "PREPARE "+name+" AS "+sqlText)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DEALLOCATE "+name) })

	lits := make([]string, len(args))
	for i, a := range args {
		lits[i] = sqlLit(a)
	}
	rows, err := conn.Query(ctx, "EXPLAIN EXECUTE "+name+"("+strings.Join(lits, ", ")+")")
	require.NoError(t, err)
	defer rows.Close()
	var sb strings.Builder
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		sb.WriteString(line)
		sb.WriteByte('\n')
	}
	require.NoError(t, rows.Err())
	return sb.String()
}

// explainCustom returns the EXPLAIN output of sqlText run through pgx's own
// parameter binding (a real driver-bound query, not a literal EXECUTE): on a
// connection that has not forced plan_cache_mode, this is a custom plan
// (Postgres always plans an unnamed/first-time statement with the actual
// bound values), matching what pgx itself does for any statement whose own
// cost-based heuristic never favours switching to generic.
func explainCustom(t *testing.T, conn *pgxpool.Conn, sqlText string, args ...any) string {
	t.Helper()
	rows, err := conn.Query(context.Background(), "EXPLAIN "+sqlText, args...)
	require.NoError(t, err)
	defer rows.Close()
	var sb strings.Builder
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		sb.WriteString(line)
		sb.WriteByte('\n')
	}
	require.NoError(t, rows.Err())
	return sb.String()
}

// TestPlansUseIndexes EXPLAINs the exact statements the postgres package
// ships (re-exported as constants in export_test.go), under a forced generic
// plan taken after ANALYZE ran with zero reserved/quarantined rows: the
// worst-case staleness window the partial-index guards
// (updateReservedWhereSQL / resolveWhereSQL / retireShardRowSQL's "IS TRUE"
// predicates) exist to make irrelevant. It pins two properties: every per-row
// CAS statement uses the primary key and never a partial index a stale
// ANALYZE could make look artificially cheap, and every ordered range scan
// uses its own partial index without an extra Sort node -- except
// AcquireShard's CTE, checked separately under its own, weaker property; see
// the comment where that check lives for why.
func TestPlansUseIndexes(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(admin.Close)
	clk := storetest.NewClock(storetest.Start)
	s := isolatedStore(t, admin, dsn, clk.Now)
	t.Cleanup(func() { _ = s.Close() })

	var target store.Shard
	var rows []store.Row
	for i := 0; i < 5; i++ {
		sh, rs := storetest.MintShard(t, s, "main", 1000, store.ShardMined)
		if i == 2 {
			target, rows = sh, rs
		}
	}

	// ANALYZE while every row is still available and no shard has ever been
	// leased: exactly the stale-statistics precondition that made the
	// unguarded predicates pick a partial index in production.
	_, err = s.Pool().Exec(ctx, `ANALYZE fuel_rows, fuel_shards`)
	require.NoError(t, err)

	tok := store.NewToken()
	storetest.MustReserve(t, s, "main", rows[:20], tok, clk.Now().Add(time.Hour))
	_, err = s.Spend(ctx, "main", tok, storetest.Ops(rows[:5]), storetest.NewTxID(), false)
	require.NoError(t, err)

	held, err := s.AcquireShard(ctx, "main", "holder-a", clk.Now(), clk.Now().Add(time.Hour), []store.ShardStatus{store.ShardMined}, nil)
	require.NoError(t, err)
	require.NotNil(t, held)

	conn, err := s.Pool().Acquire(ctx)
	require.NoError(t, err)
	t.Cleanup(conn.Release)
	_, err = conn.Exec(ctx, "SET plan_cache_mode = force_generic_plan")
	require.NoError(t, err)

	someRow := rows[15]
	someTok := store.NewToken() // never actually reserved under this token; EXPLAIN never executes anything

	// --- Ordered range scans: partial index, no separate Sort. ---

	plan := explainGeneric(t, conn, "p_list_avail", postgres.ListAvailableSQL,
		"main", target.TxID[:], int32(0), 100)
	require.Contains(t, plan, "fuel_rows_avail", plan)
	require.NotContains(t, plan, "Sort", plan)

	plan = explainGeneric(t, conn, "p_quarantine", postgres.QuarantineSQL,
		"main", clk.Now().Add(time.Hour), 1000, clk.Now())
	require.Contains(t, plan, "fuel_rows_expiry", plan)
	require.NotContains(t, plan, "Sort", plan)

	plan = explainGeneric(t, conn, "p_list_quar", postgres.ListQuarantinedSQL,
		"main", clk.Now().Add(time.Hour), 500)
	require.Contains(t, plan, "fuel_rows_quar", plan)
	require.NotContains(t, plan, "Sort", plan)

	plan = explainGeneric(t, conn, "p_list_quar_after", postgres.ListQuarantinedAfterSQL,
		"main", clk.Now().Add(time.Hour), clk.Now(), someRow.TxID[:], int32(0), 500)
	require.Contains(t, plan, "fuel_rows_quar", plan)
	require.NotContains(t, plan, "Sort", plan)

	// AcquireShard's candidate CTE does NOT reliably use fuel_shards_adopt in
	// practice, on any plan mode, once fuel_shards has been ANALYZEd with a
	// realistic distribution across many pools (verified by hand: 50 pools x
	// 1000 shards/pool, under both a forced generic plan and a fully custom
	// one with literal, single-element status/exclude arrays). Postgres
	// instead picks a Bitmap Heap Scan (on fuel_shards_pkey or
	// fuel_shards_adopt, depending on the exact distribution) followed by an
	// explicit Sort. This is a property of "status = ANY(array)" and
	// "NOT (txid = ANY(exclude))" against fuel_shards_adopt's (pool, status,
	// created_at, txid) key: an array parameter of unknown-at-plan-time
	// cardinality can match several disjoint (status, created_at, txid) runs
	// in the index, so Postgres can't treat a scan of it as pre-sorted the way
	// it can for a plain equality. That's a real (if latent, since fuel_shards
	// is typically far smaller than fuel_rows) plan-quality question worth its
	// own follow-up, out of scope for this round of fixes, so this test pins
	// only what is actually true today: the CTE narrows to the given pool
	// through some index rather than an unqualified Seq Scan of the whole
	// table, checked on a dedicated, un-forced connection (this test forces
	// plan_cache_mode on conn for every other statement).
	//
	// The 50,000 rows below are bulk-inserted directly, not through PutShard
	// (one round trip per shard, far too slow at this scale). "main" is kept a
	// small fraction of the table, as in real multi-tenant use where one pool
	// is one of many: a pool that dominates the table is correctly served by a
	// Seq Scan regardless of any index, which is not the scenario this checks.
	_, err = s.Pool().Exec(ctx, `
		INSERT INTO fuel_shards (pool, txid, size, denomination, lock_mode, status, owner, lease_until, version, beef, created_at, updated_at)
		SELECT CASE WHEN g <= 1000 THEN 'main' ELSE 'other-' || (g % 50) END,
		       decode(lpad(to_hex(1000000 + g), 64, '0'), 'hex'), 1, 50, 0, 1, '', NULL, 1, '\x00'::bytea, now(), now()
		  FROM generate_series(1, 50000) g`)
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `ANALYZE fuel_shards`)
	require.NoError(t, err)
	acquireConn, err := s.Pool().Acquire(ctx)
	require.NoError(t, err)
	t.Cleanup(acquireConn.Release)
	plan = explainCustom(t, acquireConn, postgres.AcquireShardSQL,
		"main", []int16{int16(store.ShardMined)}, clk.Now(), "holder-b", clk.Now().Add(time.Hour), [][]byte{someRow.TxID[:]}, clk.Now())
	require.True(t, strings.Contains(plan, "fuel_shards_pkey") || strings.Contains(plan, "fuel_shards_adopt"),
		"expected the CTE to narrow to the pool through an index (fuel_shards_pkey or fuel_shards_adopt), got:\n%s", plan)
	require.NotContains(t, plan, "Seq Scan on fuel_shards", plan, "must narrow to the pool through an index, not an unqualified table scan")

	// --- Per-row CAS statements: primary key, never a partial index that a
	// stale ANALYZE could make look cheap. ---

	plan = explainGeneric(t, conn, "p_get_rows", postgres.GetRowsSQL,
		"main", [][]byte{someRow.TxID[:]}, []int32{int32(someRow.Vout)}) //nolint:gosec // bounded
	require.Contains(t, plan, "fuel_rows_pkey", plan)
	require.NotContains(t, plan, "Seq Scan", plan)

	// Reserve needs no IS TRUE guard: state appears only inside "state = 0 OR
	// (state = 1 AND ...)", so the WHERE clause never implies a single fixed
	// value of state and neither fuel_rows_avail nor fuel_rows_expiry's partial
	// predicate can ever be proved implied, guard or not.
	plan = explainGeneric(t, conn, "p_reserve", postgres.ReserveRowSQL,
		"main", someRow.TxID[:], int32(someRow.Vout), someTok[:], clk.Now(), clk.Now().Add(time.Hour)) //nolint:gosec // bounded
	requirePKOnly(t, plan)

	plan = explainGeneric(t, conn, "p_release", postgres.ReleaseRowSQL,
		"main", someRow.TxID[:], int32(someRow.Vout), someTok[:], clk.Now()) //nolint:gosec // bounded
	requirePKOnly(t, plan)

	spendTx := storetest.NewTxID()
	plan = explainGeneric(t, conn, "p_spend_terminal", postgres.SpendTerminalRowSQL,
		"main", someRow.TxID[:], int32(someRow.Vout), someTok[:], clk.Now(), spendTx[:]) //nolint:gosec // bounded
	requirePKOnly(t, plan)

	plan = explainGeneric(t, conn, "p_spend_hint", postgres.SpendHintRowSQL,
		"main", someRow.TxID[:], int32(someRow.Vout), someTok[:], clk.Now(), spendTx[:]) //nolint:gosec // bounded
	requirePKOnly(t, plan)

	plan = explainGeneric(t, conn, "p_resolve_avail", postgres.ResolveAvailableRowSQL,
		"main", someRow.TxID[:], int32(someRow.Vout), clk.Now()) //nolint:gosec // bounded
	requirePKOnly(t, plan)

	resolveTx := storetest.NewTxID()
	plan = explainGeneric(t, conn, "p_resolve_spent", postgres.ResolveSpentRowSQL,
		"main", someRow.TxID[:], int32(someRow.Vout), clk.Now(), resolveTx[:]) //nolint:gosec // bounded
	requirePKOnly(t, plan)

	plan = explainGeneric(t, conn, "p_retire_row", postgres.RetireShardRowSQL,
		"main", someRow.TxID[:], int32(someRow.Vout), clk.Now()) //nolint:gosec // bounded
	requirePKOnly(t, plan)
	require.NotContains(t, plan, "fuel_rows_shard_live", plan)
}

// requirePKOnly asserts a per-row CAS plan uses fuel_rows_pkey and never one
// of the partial indexes a stale ANALYZE could otherwise make look cheap.
func requirePKOnly(t *testing.T, plan string) {
	t.Helper()
	require.Contains(t, plan, "fuel_rows_pkey", plan)
	require.NotContains(t, plan, "fuel_rows_avail", plan)
	require.NotContains(t, plan, "fuel_rows_expiry", plan)
	require.NotContains(t, plan, "fuel_rows_quar", plan)
}
