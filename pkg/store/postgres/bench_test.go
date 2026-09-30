//go:build postgres

package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func BenchmarkStore(b *testing.B) {
	dsn := os.Getenv("FUELKEEPER_TEST_POSTGRES_DSN")
	if dsn == "" {
		b.Skip("FUELKEEPER_TEST_POSTGRES_DSN not set")
	}
	admin, err := pgxpool.New(context.Background(), dsn)
	require.NoError(b, err)
	b.Cleanup(admin.Close)

	storetest.RunBenchmarks(b, func(b *testing.B) store.Store {
		return isolatedStoreTB(b, admin, dsn, time.Now)
	})
}
