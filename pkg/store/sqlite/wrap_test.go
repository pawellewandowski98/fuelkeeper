package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// fakeCoder implements sqliteCoder so every SQLite result code classification
// can be tested deterministically: modernc.org/sqlite's own *Error has
// unexported fields, so a test in another package could not build one with an
// arbitrary code, but wrap/isTransient only require the sqliteCoder interface
// (see sqlite.go), which this satisfies too.
type fakeCoder struct{ code int }

func (e fakeCoder) Error() string { return fmt.Sprintf("fake sqlite error (code %d)", e.code) }
func (e fakeCoder) Code() int     { return e.code }

func TestIsTransient_Busy(t *testing.T) {
	assert.True(t, isTransient(fakeCoder{sqlite3.SQLITE_BUSY}))
	assert.True(t, isTransient(fakeCoder{sqlite3.SQLITE_BUSY_RECOVERY}), "extended code masks to the base BUSY code")
	assert.True(t, isTransient(fakeCoder{sqlite3.SQLITE_BUSY_SNAPSHOT}))
	assert.True(t, isTransient(fakeCoder{sqlite3.SQLITE_BUSY_TIMEOUT}))
}

func TestIsTransient_Locked(t *testing.T) {
	assert.True(t, isTransient(fakeCoder{sqlite3.SQLITE_LOCKED}))
	assert.True(t, isTransient(fakeCoder{sqlite3.SQLITE_LOCKED_SHAREDCACHE}), "extended code masks to the base LOCKED code")
	assert.True(t, isTransient(fakeCoder{sqlite3.SQLITE_LOCKED_VTAB}))
}

func TestIsTransient_NonTransientCode(t *testing.T) {
	err := fakeCoder{sqlite3.SQLITE_CONSTRAINT}
	assert.False(t, isTransient(err))
	require.NotErrorIs(t, wrap(err), store.ErrUnavailable)
	assert.Equal(t, error(err), wrap(err), "wrap must pass a non-transient sqliteCoder through unchanged")
}

func TestIsTransient_DeadlineExceeded(t *testing.T) {
	assert.True(t, isTransient(context.DeadlineExceeded))
	assert.True(t, isTransient(fmt.Errorf("op: %w", context.DeadlineExceeded)))
}

func TestIsTransient_IOErrors(t *testing.T) {
	assert.True(t, isTransient(io.EOF))
	assert.True(t, isTransient(io.ErrUnexpectedEOF))
	assert.True(t, isTransient(fmt.Errorf("read: %w", io.EOF)))
}

func TestIsTransient_Unrelated(t *testing.T) {
	assert.False(t, isTransient(errors.New("boom")))
	assert.False(t, isTransient(store.ErrInvalidArgument))
}

func TestWrap_Nil(t *testing.T) {
	assert.NoError(t, wrap(nil))
}

func TestWrap_WrapsTransient(t *testing.T) {
	err := wrap(fakeCoder{sqlite3.SQLITE_BUSY})
	require.Error(t, err)
	assert.ErrorIs(t, err, store.ErrUnavailable)
}

// TestIsTransient_RealConstraintPassesThrough exercises a genuine
// *modernc.org/sqlite.Error (a real UNIQUE-constraint violation on
// fuel_leases' primary key), proving isTransient's classification of the real
// driver's error type -- not just the fake above -- and that wrap leaves a
// non-transient database error completely unchanged, exactly like PutShard's
// ErrShardExists detection relies on (see shards.go).
func TestIsTransient_RealConstraintPassesThrough(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", DSN(filepath.Join(t.TempDir(), "constraint.sqlite"), SyncFull))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(ctx, schemaV1)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `INSERT INTO fuel_leases (name, holder, expires_at) VALUES ('x', 'a', 1)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO fuel_leases (name, holder, expires_at) VALUES ('x', 'b', 2)`)
	require.Error(t, err)

	var c sqliteCoder
	require.ErrorAsf(t, err, &c, "expected a sqliteCoder-satisfying error, got %T: %v", err, err)
	assert.False(t, isTransient(err))
	assert.Same(t, err, wrap(err), "wrap must not alter a non-transient error")
}

// TestIsTransient_RealBusy exercises the real driver end to end: two
// independent connections to the same file, the first holding an open BEGIN
// IMMEDIATE (the RESERVED lock), the second configured with a zero
// busy_timeout so its own BEGIN IMMEDIATE fails immediately with a genuine
// *sqlite.Error carrying SQLITE_BUSY, proving isTransient also classifies the
// real driver's error, not just fakeCoder.
func TestIsTransient_RealBusy(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "busy.sqlite")

	db1, err := sql.Open("sqlite", DSN(path, SyncFull))
	require.NoError(t, err)
	defer func() { _ = db1.Close() }()
	db1.SetMaxOpenConns(1)
	_, err = db1.ExecContext(ctx, schemaV1)
	require.NoError(t, err)

	conn1, err := db1.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = conn1.Close() }()
	_, err = conn1.ExecContext(ctx, "BEGIN IMMEDIATE")
	require.NoError(t, err)
	defer func() { _, _ = conn1.ExecContext(context.Background(), "ROLLBACK") }()

	db2, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(0)")
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	db2.SetMaxOpenConns(1)
	conn2, err := db2.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = conn2.Close() }()

	_, err = conn2.ExecContext(ctx, "BEGIN IMMEDIATE")
	require.Error(t, err, "the second connection must fail to acquire the RESERVED lock immediately")
	assert.True(t, isTransient(err), "a real SQLITE_BUSY from the driver must be classified transient: %v", err)
	assert.ErrorIs(t, wrap(err), store.ErrUnavailable)
}
