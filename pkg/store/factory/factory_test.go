package factory_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/factory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/sqlite"
)

func TestNew(t *testing.T) {
	ctx := context.Background()

	s, err := factory.New(ctx, factory.Config{Backend: factory.BackendMemory}, time.Now)
	require.NoError(t, err)
	require.False(t, s.Capabilities().MultiProcess)
	require.NoError(t, s.Close())

	s, err = factory.New(ctx, factory.Config{Backend: factory.BackendSQLite, SQLite: sqlite.Config{Path: filepath.Join(t.TempDir(), "f.sqlite")}}, time.Now)
	require.NoError(t, err)
	require.NoError(t, s.Ping(ctx))
	require.NoError(t, s.Close())

	_, err = factory.New(ctx, factory.Config{Backend: "nope"}, time.Now)
	require.ErrorIs(t, err, store.ErrInvalidArgument)

	_, err = factory.New(ctx, factory.Config{Backend: factory.BackendPostgres}, time.Now)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "postgres needs a DSN")

	_, err = factory.New(ctx, factory.Config{Backend: ""}, time.Now)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "empty backend is required")
	require.Contains(t, err.Error(), "backend is required")
}

// TestNew_NilClockDefaultsToTimeNow: passing a nil now func must not panic and
// must default to time.Now for backends (like memory) that need a clock.
func TestNew_NilClockDefaultsToTimeNow(t *testing.T) {
	ctx := context.Background()
	s, err := factory.New(ctx, factory.Config{Backend: factory.BackendMemory}, nil)
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

// TestNew_SQLiteErrorPassthrough: an invalid SQLite config's error must reach
// the caller through New unchanged (not swallowed or rewrapped).
func TestNew_SQLiteErrorPassthrough(t *testing.T) {
	ctx := context.Background()

	// Confirm what sqlite.Open itself returns for an empty Path, so this test
	// stays honest about what it's asserting through the factory.
	direct, directErr := sqlite.Open(ctx, sqlite.Config{})
	require.Error(t, directErr)
	require.Nil(t, direct)
	require.ErrorIs(t, directErr, store.ErrInvalidArgument)

	_, err := factory.New(ctx, factory.Config{Backend: factory.BackendSQLite, SQLite: sqlite.Config{}}, time.Now)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	require.Equal(t, directErr.Error(), err.Error(),
		"factory must pass the sqlite error through unchanged, not rewrap it")
}
