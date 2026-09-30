//go:build postgres

package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// TestPlansUseIndexes pins the hot-path plans to their partial indexes so a
// schema or query edit that regresses to a sequential scan fails CI.
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
	tok := store.NewToken()
	storetest.MustReserve(t, s, "main", rows[:20], tok, clk.Now().Add(time.Second))
	_, err = s.Spend(ctx, "main", tok, storetest.Ops(rows[:5]), storetest.NewTxID(), false)
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `ANALYZE fuel_rows`)
	require.NoError(t, err)

	explain := func(sql string, args ...any) string {
		rs, err := s.Pool().Query(ctx, "EXPLAIN "+sql, args...)
		require.NoError(t, err)
		defer rs.Close()
		var sb strings.Builder
		for rs.Next() {
			var line string
			require.NoError(t, rs.Scan(&line))
			sb.WriteString(line)
			sb.WriteByte('\n')
		}
		return sb.String()
	}

	plan := explain(`SELECT txid, vout FROM fuel_rows WHERE pool = $1 AND txid = $2 AND state = 0 AND vout >= $3 ORDER BY vout LIMIT 100`, "main", target.TxID[:], int32(0))
	require.Contains(t, plan, "fuel_rows_avail", plan)
	require.NotContains(t, plan, "Sort", plan)

	plan = explain(`SELECT txid, vout FROM fuel_rows WHERE pool = $1 AND state = 1 AND expires_at < $2 ORDER BY expires_at, txid, vout LIMIT 1000`, "main", clk.Now().Add(time.Hour))
	require.Contains(t, plan, "fuel_rows_expiry", plan)

	plan = explain(`SELECT txid, vout FROM fuel_rows WHERE pool = $1 AND state = 2 AND quarantined_at < $2 ORDER BY quarantined_at, txid, vout LIMIT 500`, "main", clk.Now().Add(time.Hour))
	require.Contains(t, plan, "fuel_rows_quar", plan)

	plan = explain(`SELECT pool, txid FROM fuel_shards WHERE pool = $1 AND status = ANY($2::int2[]) AND (owner = '' OR lease_until IS NULL OR lease_until < $3) ORDER BY created_at, txid LIMIT 1`, "main", []int16{1}, clk.Now())
	require.Contains(t, plan, "fuel_shards_adopt", plan)
}
