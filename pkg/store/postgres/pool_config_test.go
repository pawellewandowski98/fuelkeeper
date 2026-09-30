package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBuildPoolConfig_MaxConns covers the three ways MaxConns can end up set:
// an explicit Config.MaxConns always wins; absent that, a DSN that named its
// own pool_max_conns is honoured as-is; absent both, defaultMaxConns applies.
// None of this needs a live database: pgxpool.ParseConfig only parses.
func TestBuildPoolConfig_MaxConns(t *testing.T) {
	const dsn = "postgres://user:pass@localhost:5432/db"
	const dsnWithMaxConns = dsn + "?pool_max_conns=7"

	t.Run("explicit Config.MaxConns overrides everything", func(t *testing.T) {
		pc, err := buildPoolConfig(Config{DSN: dsnWithMaxConns, MaxConns: 3})
		require.NoError(t, err)
		require.EqualValues(t, 3, pc.MaxConns)
	})

	t.Run("DSN's own pool_max_conns is kept when Config.MaxConns is 0", func(t *testing.T) {
		pc, err := buildPoolConfig(Config{DSN: dsnWithMaxConns})
		require.NoError(t, err)
		require.EqualValues(t, 7, pc.MaxConns)
	})

	t.Run("defaultMaxConns applies when neither is set", func(t *testing.T) {
		pc, err := buildPoolConfig(Config{DSN: dsn})
		require.NoError(t, err)
		require.EqualValues(t, defaultMaxConns, pc.MaxConns)
	})
}
