package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// TestWrite_FnCancelsContextThenSubsequentWriteSucceeds is the regression test
// for the "cannot start a transaction within a transaction" bug: a write
// whose fn cancels the very context write was called with, then fails, must
// not leave the writer's sole connection wedged inside an open transaction.
// Before the fix (a hand-written "BEGIN IMMEDIATE"/"COMMIT"/"ROLLBACK" over a
// raw *sql.Conn), the ROLLBACK sent via ExecContext(ctx, ...) after fn failed
// never reached the server once ctx was already cancelled, so the connection
// went back to the *sql.DB pool still mid-transaction and every later write
// failed non-transiently. With write() using *sql.Tx (database/sql itself
// guarantees a real rollback runs on cancellation), a later write must
// succeed.
func TestWrite_FnCancelsContextThenSubsequentWriteSucceeds(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, Config{Path: filepath.Join(t.TempDir(), "cancel.sqlite")})
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	cctx, cancel := context.WithCancel(ctx)
	writeErr := s.write(cctx, func(c querier) error {
		if _, execErr := c.ExecContext(cctx, `INSERT INTO fuel_leases (name, holder, expires_at) VALUES ('poison-probe', 'h', 1)`); execErr != nil {
			return execErr
		}
		cancel() // simulate the caller's context being cancelled mid-transaction
		return errors.New("boom: fn fails after ctx cancellation")
	})
	require.Error(t, writeErr)

	// The critical assertion: an entirely unrelated, freshly-contexted write
	// must still succeed. Under the old bug this failed with SQLite's
	// non-transient "cannot start a transaction within a transaction".
	err = s.write(context.Background(), func(c querier) error {
		_, execErr := c.ExecContext(context.Background(), `INSERT INTO fuel_leases (name, holder, expires_at) VALUES ('after', 'h', 1)`)
		return execErr
	})
	require.NoError(t, err, "a write after a cancelled-context write must succeed, not fail with \"cannot start a transaction within a transaction\"")

	// And the cancelled write's own insert must have been rolled back (fn
	// returned an error), not left committed.
	var n int
	require.NoError(t, s.writer.QueryRowContext(context.Background(), `SELECT count(*) FROM fuel_leases WHERE name = 'poison-probe'`).Scan(&n))
	assert.Equal(t, 0, n, "fn's own error must roll back its writes")
}

// TestReserve_TightDeadlinesNeverApplyPartially hammers Reserve of a
// 1000-row batch with contexts whose deadlines are so tight the write's
// BeginTx/Commit routinely races the deadline, at four deadline scales and a
// few iterations each. Every attempt must either apply all 1000 ops or none
// (store.Caps.AtomicBatch is true for this backend): a partial application
// would mean the write's transaction boundary leaked below the statement
// level. A final normal Reserve must still succeed, proving the repeated
// cancellations never poisoned the writer connection.
//
// The four deadline scales are derived from a dry-run, uncancelled 1000-op
// Reserve's measured duration (0.5x, 0.9x, 1.0x, 1.1x of it) rather than
// fixed microsecond guesses, so cancellation also has a real chance of
// landing during Commit() and not only in the per-row loop; under -race that
// duration is roughly 25x its normal cost (modernc.org/sqlite is pure Go and
// lock-heavy), so iteration counts are kept low to keep this test at a few
// seconds. The all-or-nothing assertion is checked unconditionally for every
// attempt, so which branch a given tier exercises is timing-dependent but
// the expected outcome never is.
func TestReserve_TightDeadlinesNeverApplyPartially(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, Config{Path: filepath.Join(t.TempDir(), "cancel-reserve.sqlite")})
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	_, dryRows := storetest.MintShard(t, s, "main", 1000, store.ShardMined)
	dryStart := time.Now()
	storetest.MustReserve(t, s, "main", dryRows, store.NewToken(), time.Now().Add(time.Hour))
	baseline := time.Since(dryStart)

	_, rows := storetest.MintShard(t, s, "main", 1000, store.ShardMined)
	before, err := s.Stats(ctx, "main")
	require.NoError(t, err)

	timeouts := []time.Duration{baseline / 2, baseline * 9 / 10, baseline, baseline * 11 / 10}
	for _, to := range timeouts {
		for i := 0; i < 2; i++ {
			tctx, cancel := context.WithTimeout(ctx, to)
			tok := store.NewToken()
			_, _ = s.Reserve(tctx, "main", storetest.ReserveOps(rows, tok, time.Now().Add(time.Hour)))
			cancel()

			st, statErr := s.Stats(context.Background(), "main")
			require.NoError(t, statErr)
			delta := st.Reserved - before.Reserved
			require.True(t, delta == 0 || delta == int64(len(rows)),
				"Reserve under a %s deadline must apply all-or-nothing, got delta %d", to, delta)
		}
	}

	// The store must not be poisoned: a final normal Reserve still succeeds.
	_, freshRows := storetest.MintShard(t, s, "main", 10, store.ShardMined)
	applied, err := s.Reserve(context.Background(), "main", storetest.ReserveOps(freshRows, store.NewToken(), time.Now().Add(time.Hour)))
	require.NoError(t, err)
	require.Len(t, applied, 10)
}
