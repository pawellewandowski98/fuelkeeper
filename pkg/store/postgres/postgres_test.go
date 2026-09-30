//go:build postgres

package postgres_test

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
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/postgres"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("FUELKEEPER_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("FUELKEEPER_TEST_POSTGRES_DSN not set")
	}
	return dsn
}

// isolatedStore creates a schema, opens a store scoped to it via search_path,
// and drops the schema on cleanup.
func isolatedStore(t *testing.T, admin *pgxpool.Pool, dsn string, now func() time.Time) *postgres.Store {
	t.Helper()
	ctx := context.Background()
	var b [6]byte
	_, _ = rand.Read(b[:])
	schema := "fk_" + hex.EncodeToString(b[:])
	_, err := admin.Exec(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })

	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	s, err := postgres.New(ctx, postgres.Config{DSN: dsn + sep + "search_path=" + schema, MaxConns: 8}, postgres.WithClock(now))
	require.NoError(t, err)
	return s
}

func TestConformance(t *testing.T) {
	dsn := testDSN(t)
	admin, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(admin.Close)

	storetest.RunSuite(t, func(t *testing.T, now func() time.Time) store.Store {
		return isolatedStore(t, admin, dsn, now)
	})
}
