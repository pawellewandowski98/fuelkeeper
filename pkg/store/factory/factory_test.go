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
}
