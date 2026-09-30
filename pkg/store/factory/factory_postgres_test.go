//go:build postgres

package factory_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/factory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/postgres"
)

// TestNewPostgres opens a postgres-backed store through the factory against
// FUELKEEPER_TEST_POSTGRES_DSN (skipped when unset), isolated on a freshly
// created schema selected via search_path (mirroring
// pkg/store/postgres's own isolatedStore test helper, which is unexported
// and so cannot be reused directly from this package) that is dropped on
// cleanup, and runs a tiny smoke: Capabilities, Stats on an empty pool, then
// Close.
func TestNewPostgres(t *testing.T) {
	dsn := os.Getenv("FUELKEEPER_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("FUELKEEPER_TEST_POSTGRES_DSN not set")
	}
	ctx := context.Background()

	admin, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(admin.Close)

	var b [6]byte
	_, _ = rand.Read(b[:])
	schema := "fk_factory_" + hex.EncodeToString(b[:])
	_, err = admin.Exec(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })

	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}

	s, err := factory.New(ctx, factory.Config{
		Backend:  factory.BackendPostgres,
		Postgres: postgres.Config{DSN: dsn + sep + "search_path=" + schema, MaxConns: 2},
	}, time.Now)
	require.NoError(t, err)

	require.True(t, s.Capabilities().MultiProcess)
	stats, err := s.Stats(ctx, "factory-smoke-pool")
	require.NoError(t, err)
	require.Equal(t, store.Stats{}, stats, "empty pool must report zero counts")
	require.NoError(t, s.Close())
}
