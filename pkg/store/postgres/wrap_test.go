package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// fakeNetErr is a synthetic net.Error whose Timeout/Temporary are both false,
// to prove isTransient treats ANY net.Error as transient, not just timeouts
// (pgconn.Timeout covers the timeout-shaped subset separately).
type fakeNetErr struct{}

func (fakeNetErr) Error() string   { return "fake network error" }
func (fakeNetErr) Timeout() bool   { return false }
func (fakeNetErr) Temporary() bool { return false } //nolint:staticcheck // net.Error still requires it

func TestIsTransient_SQLSTATEClasses(t *testing.T) {
	// Class prefixes: every code under "08" (connection exception) and "53"
	// (insufficient resources) is transient, not just the specific codes a
	// previous version of this function hardcoded.
	assert.True(t, isTransient(&pgconn.PgError{Code: "08006"}), "08006 connection_failure")
	assert.True(t, isTransient(&pgconn.PgError{Code: "08004"}), "08004 sqlserver_rejected_establishment_of_sqlconnection")
	assert.True(t, isTransient(&pgconn.PgError{Code: "53200"}), "53200 out_of_memory (prefix match, not a hardcoded code)")
	assert.True(t, isTransient(&pgconn.PgError{Code: "53300"}), "53300 too_many_connections")
}

func TestIsTransient_DiscreteCodes(t *testing.T) {
	for _, code := range []string{
		"57P01", // admin_shutdown
		"57P02", // crash_shutdown
		"57P03", // cannot_connect_now
		"57014", // query_canceled
		"40001", // serialization_failure
		"40P01", // deadlock_detected
		"55P03", // lock_not_available
		"25006", // read_only_sql_transaction
	} {
		assert.True(t, isTransient(&pgconn.PgError{Code: code}), "code %s should be transient", code)
	}
}

func TestIsTransient_NonTransientPgError(t *testing.T) {
	// A well-formed database error that is not in any transient class must pass
	// through unwrapped: e.g. 23505 unique_violation is how PutShard detects
	// ErrShardExists, and must never be reported as ErrUnavailable.
	err := &pgconn.PgError{Code: "23505", ConstraintName: "fuel_shards_txid"}
	assert.False(t, isTransient(err))
	require.NotErrorIs(t, wrap(err), store.ErrUnavailable)
	assert.Same(t, err, wrap(err), "wrap must pass a non-transient PgError through unchanged")
}

func TestIsTransient_ConnectError(t *testing.T) {
	// *pgconn.ConnectError's own fields are unexported beyond Config; a
	// zero-value instance is enough to prove isTransient matches the type.
	assert.True(t, isTransient(&pgconn.ConnectError{}))
}

func TestIsTransient_NetError(t *testing.T) {
	assert.True(t, isTransient(fakeNetErr{}), "any net.Error is transient, timeout or not")
}

func TestIsTransient_IOErrors(t *testing.T) {
	assert.True(t, isTransient(io.EOF))
	assert.True(t, isTransient(io.ErrUnexpectedEOF))
	assert.True(t, isTransient(fmt.Errorf("read: %w", io.EOF)))
}

func TestIsTransient_TxCommitRollback(t *testing.T) {
	assert.True(t, isTransient(pgx.ErrTxCommitRollback))
}

func TestIsTransient_ContextDeadlineExceeded(t *testing.T) {
	assert.True(t, isTransient(context.DeadlineExceeded))
}

func TestIsTransient_Unrelated(t *testing.T) {
	assert.False(t, isTransient(errors.New("boom")))
	assert.False(t, isTransient(store.ErrInvalidArgument))
}

func TestWrap_Nil(t *testing.T) {
	assert.NoError(t, wrap(nil))
}

func TestWrap_WrapsTransient(t *testing.T) {
	err := wrap(&pgconn.PgError{Code: "40P01"})
	require.Error(t, err)
	assert.ErrorIs(t, err, store.ErrUnavailable)
}

// TestIsTransient_PgconnTimeout exercises the one class that cannot be
// synthesised: pgconn.Timeout(err) is only ever true for pgx's own internal
// *errTimeout wrapper, built when an in-flight network read actually times out
// against the context deadline. It needs a real server round-trip, so it
// skips like the rest of this package's integration tests when no DSN is set.
func TestIsTransient_PgconnTimeout(t *testing.T) {
	dsn := os.Getenv("FUELKEEPER_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("FUELKEEPER_TEST_POSTGRES_DSN not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	tctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	_, err = pool.Exec(tctx, "SELECT pg_sleep(2)")
	require.Error(t, err)
	assert.True(t, pgconn.Timeout(err), "expected a pgconn timeout error, got %v", err)
	assert.True(t, isTransient(err))
	assert.ErrorIs(t, wrap(err), store.ErrUnavailable)
}
