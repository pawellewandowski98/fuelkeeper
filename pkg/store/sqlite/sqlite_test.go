package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/sqlite"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.RunSuite(t, func(t *testing.T, now func() time.Time) store.Store {
		s, err := sqlite.Open(context.Background(), sqlite.Config{Path: filepath.Join(t.TempDir(), "fuel.sqlite")}, sqlite.WithClock(now))
		require.NoError(t, err)
		return s
	})
}

func TestDSNAndPragmas(t *testing.T) {
	dsn := sqlite.DSN("/tmp/x.sqlite", sqlite.SyncFull)
	require.Equal(t, "file:/tmp/x.sqlite?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(ON)", dsn)
	require.Contains(t, sqlite.DSN("/tmp/x.sqlite", sqlite.SyncNormal), "synchronous(NORMAL)")

	s, err := sqlite.Open(context.Background(), sqlite.Config{Path: filepath.Join(t.TempDir(), "p.sqlite")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	p, err := s.Pragmas(context.Background())
	require.NoError(t, err)
	require.Equal(t, "wal", p.JournalMode)
	require.Equal(t, 5000, p.BusyTimeout)
	require.Equal(t, 2, p.Synchronous, "FULL")
}
