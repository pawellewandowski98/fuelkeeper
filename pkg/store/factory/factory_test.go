package factory_test

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/factory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/postgres"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/sqlite"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// TestConfigValidate table-drives every branch of Config.Validate: an
// unknown or empty backend, and each backend's required fields.
func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name    string
		cfg     factory.Config
		wantErr bool
	}{
		{"empty backend", factory.Config{}, true},
		{"unknown backend", factory.Config{Backend: "nope"}, true},
		{"memory", factory.Config{Backend: factory.BackendMemory}, false},

		{"sqlite missing path", factory.Config{Backend: factory.BackendSQLite}, true},
		{"sqlite with path", factory.Config{Backend: factory.BackendSQLite, SQLite: sqlite.Config{Path: "f.sqlite"}}, false},

		{"postgres missing dsn", factory.Config{Backend: factory.BackendPostgres}, true},
		{"postgres with dsn", factory.Config{Backend: factory.BackendPostgres, Postgres: postgres.Config{DSN: "postgres://x"}}, false},

		{"pebble missing path", factory.Config{Backend: factory.BackendPebble}, true},
		{"pebble with path", factory.Config{Backend: factory.BackendPebble, Pebble: factory.PebbleConfig{Path: "/tmp/x"}}, false},

		{"mongodb missing uri and database", factory.Config{Backend: factory.BackendMongoDB}, true},
		{"mongodb missing database", factory.Config{Backend: factory.BackendMongoDB, MongoDB: factory.MongoDBConfig{URI: "mongodb://localhost:27017"}}, true},
		{"mongodb missing uri", factory.Config{Backend: factory.BackendMongoDB, MongoDB: factory.MongoDBConfig{Database: "fk"}}, true},
		{"mongodb complete", factory.Config{Backend: factory.BackendMongoDB, MongoDB: factory.MongoDBConfig{URI: "mongodb://localhost:27017", Database: "fk"}}, false},

		{"aerospike missing hosts and namespace", factory.Config{Backend: factory.BackendAerospike}, true},
		{"aerospike missing namespace", factory.Config{Backend: factory.BackendAerospike, Aerospike: factory.AerospikeConfig{Hosts: []string{"localhost:3000"}}}, true},
		{"aerospike missing hosts", factory.Config{Backend: factory.BackendAerospike, Aerospike: factory.AerospikeConfig{Namespace: "test"}}, true},
		{"aerospike complete", factory.Config{Backend: factory.BackendAerospike, Aerospike: factory.AerospikeConfig{Hosts: []string{"localhost:3000"}, Namespace: "test"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr {
				require.ErrorIs(t, err, store.ErrInvalidArgument, "%+v", tc.cfg)
				return
			}
			require.NoError(t, err, "%+v", tc.cfg)
		})
	}
}

// TestNew_MongoDBAerospikeInvalidNoNetworkCall: an incomplete mongodb or
// aerospike config must fail through Validate, before New ever attempts a
// connection. Both configs below name unreachable/nonexistent addresses, so
// a passing test that actually tried to dial would hang or time out instead
// of returning immediately with ErrInvalidArgument.
func TestNew_MongoDBAerospikeInvalidNoNetworkCall(t *testing.T) {
	ctx := context.Background()

	_, err := factory.New(ctx, factory.Config{Backend: factory.BackendMongoDB}, time.Now)
	require.ErrorIs(t, err, store.ErrInvalidArgument)

	_, err = factory.New(ctx, factory.Config{Backend: factory.BackendMongoDB, MongoDB: factory.MongoDBConfig{URI: "mongodb://192.0.2.1:27017"}}, time.Now)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "database still missing")

	_, err = factory.New(ctx, factory.Config{Backend: factory.BackendAerospike}, time.Now)
	require.ErrorIs(t, err, store.ErrInvalidArgument)

	_, err = factory.New(ctx, factory.Config{Backend: factory.BackendAerospike, Aerospike: factory.AerospikeConfig{Hosts: []string{"192.0.2.1:3000"}}}, time.Now)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "namespace still missing")
}

// TestNewPebble opens a pebble store on t.TempDir() through the factory and
// runs a tiny smoke: Capabilities (single-process, as pebble is embedded)
// then Close.
func TestNewPebble(t *testing.T) {
	ctx := context.Background()

	s, err := factory.New(ctx, factory.Config{
		Backend: factory.BackendPebble,
		Pebble:  factory.PebbleConfig{Path: filepath.Join(t.TempDir(), "pebble")},
	}, time.Now)
	require.NoError(t, err)
	require.False(t, s.Capabilities().MultiProcess)
	require.NoError(t, s.Ping(ctx))
	require.NoError(t, s.Close())
}

// TestNew_WithLoggerReachesPebble reproduces the review's nit that New had
// no way to pass a logger through, so pebble (and aerospike) always opened
// with slog.Default(): before this option existed, a caller embedding
// fuelkeeper's own store inside a larger service had no way to route
// pebble's own compaction/flush/WAL logging (see pebble.WithLogger) to that
// service's logger.
//
// pebble's WithLogger doc comment notes it prints a WAL-replay summary
// (Infof) on every reopen of an existing store; this opens the same path
// twice through the factory with a recording handler and asserts that line
// appears on the second open, proving factory.WithLogger's value reached
// pebble.Open's own WithLogger, not just slog.Default().
func TestNew_WithLoggerReachesPebble(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "pebble")
	cfg := factory.Config{Backend: factory.BackendPebble, Pebble: factory.PebbleConfig{Path: path}}

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))

	s, err := factory.New(ctx, cfg, time.Now, factory.WithLogger(log))
	require.NoError(t, err)
	shard, rows := storetest.BuildShard("main", 1, store.ShardMined)
	require.NoError(t, s.PutShard(ctx, shard, rows), "write something so there is WAL content for the next open to replay")
	require.NoError(t, s.Close())
	buf.Reset()

	s2, err := factory.New(ctx, cfg, time.Now, factory.WithLogger(log))
	require.NoError(t, err)
	defer func() { require.NoError(t, s2.Close()) }()

	require.NotZero(t, buf.Len(), "reopening an existing pebble store must log its WAL-replay summary through the logger factory.WithLogger passed in")
	require.Contains(t, buf.String(), "level=INFO")
}

// TestNew_WithoutLoggerStillOpensPebble confirms the variadic opts parameter
// is fully optional: every pre-existing 3-argument call to New (with no
// options at all) keeps compiling and opening pebble exactly as before.
func TestNew_WithoutLoggerStillOpensPebble(t *testing.T) {
	ctx := context.Background()
	s, err := factory.New(ctx, factory.Config{
		Backend: factory.BackendPebble,
		Pebble:  factory.PebbleConfig{Path: filepath.Join(t.TempDir(), "pebble")},
	}, time.Now)
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

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
	require.ErrorIs(t, err, store.ErrInvalidArgument, "empty backend must be rejected")
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
