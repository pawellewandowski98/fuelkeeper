package sqlite

import (
	"context"
	"errors"
	"fmt"
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
	writeErr := s.write(cctx, func(_ context.Context, c querier) error {
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
	err = s.write(context.Background(), func(_ context.Context, c querier) error {
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
// BeginTx/Commit routinely races the deadline, at six deadline tiers and a
// few iterations each. Every attempt must either apply all 1000 ops or none
// (store.Caps.AtomicBatch is true for this backend): a partial application
// would mean the write's transaction boundary leaked below the statement
// level. A final normal Reserve must still succeed, proving the repeated
// cancellations never poisoned the writer connection.
//
// The six tiers are: an already-expired context (0), a context that expires
// within microseconds (lands at BeginTx or the first rows), and four tiers
// scaled off a dry-run, uncancelled 1000-op Reserve's measured duration
// (0.5x, 0.95x, 1.0x, 1.05x) rather than fixed microsecond guesses, so
// cancellation also has a real chance of landing during Commit() and not
// only in the per-row loop; under -race that duration is roughly 25x its
// normal cost (modernc.org/sqlite is pure Go and lock-heavy), so iteration
// counts are kept low to keep this test at several seconds (measured ≈4.5s
// on SQLite under -race). The all-or-nothing assertion is checked
// unconditionally for every attempt, so which branch a given tier exercises
// is timing-dependent but the expected outcome never is. Any attempt that
// does apply the full batch re-mints a fresh set of rows before the next
// attempt, so the invariant stays testable throughout the run instead of
// going vacuous once the batch first lands. Reserve's returned error, when
// non-nil, must be a cancellation error (context.Canceled or
// context.DeadlineExceeded) or store.ErrUnavailable -- anything else fails
// the test immediately instead of only at the final not-poisoned check.
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

	timeouts := []time.Duration{0, 50 * time.Microsecond, baseline / 2, baseline * 19 / 20, baseline, baseline * 21 / 20}
	for _, to := range timeouts {
		for i := 0; i < 2; i++ {
			tctx, cancel := context.WithTimeout(ctx, to)
			tok := store.NewToken()
			_, rerr := s.Reserve(tctx, "main", storetest.ReserveOps(rows, tok, time.Now().Add(time.Hour)))
			cancel()
			if rerr != nil {
				require.True(t, errors.Is(rerr, context.DeadlineExceeded) || errors.Is(rerr, context.Canceled) || errors.Is(rerr, store.ErrUnavailable),
					"Reserve under a %s deadline returned a non-cancellation error: %v", to, rerr)
			}

			st, statErr := s.Stats(context.Background(), "main")
			require.NoError(t, statErr)
			delta := st.Reserved - before.Reserved
			require.True(t, delta == 0 || delta == int64(len(rows)),
				"Reserve under a %s deadline must apply all-or-nothing, got delta %d", to, delta)

			if delta != 0 {
				_, rows = storetest.MintShard(t, s, "main", 1000, store.ShardMined)
				before, err = s.Stats(ctx, "main")
				require.NoError(t, err)
			}
		}
	}

	// The store must not be poisoned: a final normal Reserve still succeeds.
	_, freshRows := storetest.MintShard(t, s, "main", 10, store.ShardMined)
	applied, err := s.Reserve(context.Background(), "main", storetest.ReserveOps(freshRows, store.NewToken(), time.Now().Add(time.Hour)))
	require.NoError(t, err)
	require.Len(t, applied, 10)
}

// blockWriter queues a write whose fn holds writeLoop until the returned
// release func is called, so the test can queue more writes behind it; the
// returned channel yields the blocker's own result.
func blockWriter(t *testing.T, s *Store) (release func(), result <-chan error) {
	t.Helper()
	entered, hold := make(chan struct{}), make(chan struct{})
	res := make(chan error, 1)
	go func() {
		res <- s.write(context.Background(), func(ctx context.Context, c querier) error {
			close(entered)
			<-hold
			_, err := c.ExecContext(ctx, `INSERT INTO fuel_leases (name, holder, expires_at) VALUES ('blocker', 'h', 1)`)
			return err
		})
	}()
	<-entered
	return func() { close(hold) }, res
}

func countLeases(t *testing.T, s *Store, like string) int {
	t.Helper()
	var n int
	require.NoError(t, s.reader.QueryRowContext(context.Background(), `SELECT count(*) FROM fuel_leases WHERE name LIKE ?`, like).Scan(&n))
	return n
}

// TestWrite_GroupCommitIsolatesFailures pins the group commit: writes queued
// while another is in flight share one transaction (one COMMIT, so one WAL
// fsync), yet each stays atomic on its own -- a failing fn's SAVEPOINT is
// rolled back without disturbing the writes grouped with it.
func TestWrite_GroupCommitIsolatesFailures(t *testing.T) {
	s, err := Open(context.Background(), Config{Path: filepath.Join(t.TempDir(), "group.sqlite")})
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	release, blockerRes := blockWriter(t, s)
	const n = 20
	errBoom := errors.New("boom")
	type result struct {
		i   int
		err error
	}
	results := make(chan result, n)
	for i := 0; i < n; i++ {
		go func() {
			err := s.write(context.Background(), func(ctx context.Context, c querier) error {
				if _, err := c.ExecContext(ctx, `INSERT INTO fuel_leases (name, holder, expires_at) VALUES (?, 'h', 1)`, fmt.Sprintf("g-%02d", i)); err != nil {
					return err
				}
				if i%2 == 1 {
					return errBoom // after its insert: the insert must be undone
				}
				return nil
			})
			results <- result{i, err}
		}()
	}
	// Let every writer reach the queue behind the blocker before releasing it
	// (if one is late it simply lands in a later group; the outcome
	// assertions below hold either way, only the group count would differ).
	time.Sleep(100 * time.Millisecond)
	before := s.commits.Load()
	release()
	require.NoError(t, <-blockerRes)
	for k := 0; k < n; k++ {
		r := <-results
		if r.i%2 == 1 {
			require.ErrorIs(t, r.err, errBoom, "write %d", r.i)
		} else {
			require.NoError(t, r.err, "write %d", r.i)
		}
	}
	assert.Equal(t, 1, countLeases(t, s, "blocker"))
	assert.Equal(t, n/2, countLeases(t, s, "g-%"), "only the even (succeeding) writes may be committed")
	for i := 1; i < n; i += 2 {
		assert.Zero(t, countLeases(t, s, fmt.Sprintf("g-%02d", i)), "failed write %d must be rolled back", i)
	}
	assert.LessOrEqual(t, s.commits.Load()-before, uint64(2),
		"%d writes queued behind one in flight must share a group commit (blocker's own + at most one more), not commit one by one", n)
}

// TestWrite_CancelledWhileQueuedIsNotApplied pins the cancellation rule: a
// write whose context ends before writeLoop claims it returns the context
// error and is never run.
func TestWrite_CancelledWhileQueuedIsNotApplied(t *testing.T) {
	s, err := Open(context.Background(), Config{Path: filepath.Join(t.TempDir(), "cancel-queued.sqlite")})
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	release, blockerRes := blockWriter(t, s)
	cctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	werr := s.write(cctx, func(ctx context.Context, c querier) error {
		_, err := c.ExecContext(ctx, `INSERT INTO fuel_leases (name, holder, expires_at) VALUES ('queued', 'h', 1)`)
		return err
	})
	require.ErrorIs(t, werr, context.DeadlineExceeded)
	release()
	require.NoError(t, <-blockerRes)
	assert.Zero(t, countLeases(t, s, "queued"), "a write cancelled while queued must never run")

	// After Close, a write fails instead of blocking forever.
	require.NoError(t, s.Close())
	require.ErrorIs(t, s.write(context.Background(), func(context.Context, querier) error { return nil }), errClosed)
}
