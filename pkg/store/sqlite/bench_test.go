package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/sqlite"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func BenchmarkStore(b *testing.B) {
	storetest.RunBenchmarks(b, func(b *testing.B) store.Store {
		s, err := sqlite.Open(context.Background(), sqlite.Config{Path: filepath.Join(b.TempDir(), "fuel.sqlite")})
		require.NoError(b, err)
		return s
	})
}
