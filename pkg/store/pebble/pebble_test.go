package pebble_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	pebblestore "github.com/bsv-blockchain/fuelkeeper/pkg/store/pebble"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.RunSuite(t, func(t *testing.T, now func() time.Time) store.Store {
		s, err := pebblestore.Open(context.Background(), pebblestore.Config{Path: t.TempDir(), NoSync: true}, pebblestore.WithClock(now))
		require.NoError(t, err)
		return s
	})
}

func TestReopenKeepsState(t *testing.T) {
	dir := t.TempDir()
	clk := storetest.NewClock(storetest.Start)
	s, err := pebblestore.Open(context.Background(), pebblestore.Config{Path: dir}, pebblestore.WithClock(clk.Now))
	require.NoError(t, err)
	sh, rows := storetest.MintShard(t, s, "main", 3, store.ShardMined)
	tok := store.NewToken()
	storetest.MustReserve(t, s, "main", rows[:1], tok, clk.Now().Add(time.Minute))
	require.NoError(t, s.Close())

	s, err = pebblestore.Open(context.Background(), pebblestore.Config{Path: dir}, pebblestore.WithClock(clk.Now))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	st, err := s.Stats(context.Background(), "main")
	require.NoError(t, err)
	require.Equal(t, int64(2), st.Available)
	require.Equal(t, int64(1), st.Reserved)
	got, err := s.GetShard(context.Background(), "main", sh.TxID)
	require.NoError(t, err)
	require.Equal(t, sh.BEEF, got.BEEF)
	avail, err := s.ListAvailable(context.Background(), "main", sh.TxID, 0, 10)
	require.NoError(t, err)
	require.Len(t, avail, 2)
}

func TestClosedStoreReturnsErrors(t *testing.T) {
	ctx := context.Background()
	s, err := pebblestore.Open(ctx, pebblestore.Config{Path: t.TempDir(), NoSync: true})
	require.NoError(t, err)
	sh, rows := storetest.MintShard(t, s, "main", 1, store.ShardMined)
	require.NoError(t, s.Close())
	require.NoError(t, s.Close(), "Close must be idempotent")

	require.Error(t, s.Ping(ctx))

	_, err = s.GetShard(ctx, "main", sh.TxID)
	require.Error(t, err)
	require.Error(t, s.UpdateShard(ctx, "main", sh.TxID, store.ShardMined, nil))
	_, err = s.RetireShard(ctx, "main", sh.TxID)
	require.Error(t, err)
	_, err = s.AcquireShard(ctx, "main", "h", time.Now(), time.Now().Add(time.Minute), []store.ShardStatus{store.ShardMined}, nil)
	require.Error(t, err)
	_, err = s.RenewShard(ctx, "main", sh.TxID, "h", time.Now().Add(time.Minute))
	require.Error(t, err)
	require.Error(t, s.ReleaseShard(ctx, "main", sh.TxID, "h"))
	_, err = s.ListShards(ctx, "main", store.ShardFilter{})
	require.Error(t, err)
	require.Error(t, s.PutShard(ctx, sh, rows))
	_, err = s.ListAvailable(ctx, "main", sh.TxID, 0, 10)
	require.Error(t, err)
	_, err = s.GetRows(ctx, "main", storetest.Ops(rows))
	require.Error(t, err)
	_, err = s.Reserve(ctx, "main", storetest.ReserveOps(rows, store.NewToken(), time.Now().Add(time.Minute)))
	require.Error(t, err)
	_, err = s.Release(ctx, "main", store.NewToken(), storetest.Ops(rows))
	require.Error(t, err)
	_, err = s.Spend(ctx, "main", store.NewToken(), storetest.Ops(rows), storetest.NewTxID(), false)
	require.Error(t, err)
	_, err = s.Quarantine(ctx, "main", time.Now(), 10)
	require.Error(t, err)
	_, err = s.ListQuarantined(ctx, "main", time.Now(), nil, 10)
	require.Error(t, err)
	_, err = s.Resolve(ctx, "main", storetest.Ops(rows), store.StateAvailable, nil)
	require.Error(t, err)
	_, err = s.Stats(ctx, "main")
	require.Error(t, err)
	_, err = s.TryAcquireOrRenew(ctx, "jobs/x", "h", time.Minute)
	require.Error(t, err)
	require.Error(t, s.ReleaseLease(ctx, "jobs/x", "h"))
}

// TestNoOpCommitWritesNothing pins commit's empty-batch short-circuit
// (pebble.go): a call that matches no rows/shards -- an idle reaper's
// Quarantine tick finding nothing due, here -- must never reach Pebble's WAL,
// since there is nothing to make durable. It checks this at the strongest
// level available from outside the package: total bytes on disk, before and
// after, must be identical, not just "no error and unaffected counters" (both
// of which are checked too, since a no-op ought to guarantee them regardless
// of whether it also avoided writing anything).
func TestNoOpCommitWritesNothing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := pebblestore.Open(ctx, pebblestore.Config{Path: dir})
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	before, err := diskUsage(t, dir)
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		n, err := s.Quarantine(ctx, "main", time.Now().Add(time.Hour), 10)
		require.NoError(t, err)
		require.Equal(t, 0, n, "nothing exists yet, so nothing can be due")
	}

	after, err := diskUsage(t, dir)
	require.NoError(t, err)
	require.Equal(t, before, after, "a no-op Quarantine must not write anything to disk (no WAL growth)")

	st, err := s.Stats(ctx, "main")
	require.NoError(t, err)
	require.Equal(t, store.Stats{}, st, "counters must stay correct after no-op calls")
}

// diskUsage sums the size of every file under dir.
func diskUsage(t *testing.T, dir string) (int64, error) {
	t.Helper()
	var total int64
	err := filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

// TestDifferentialFuzz lives in differential_fuzz_test.go: it replaces the
// former TestCountersMatchScans with a much stronger check, comparing pebble
// against pkg/store/memory op-by-op instead of only checking pebble's own
// counters against its own scans.
