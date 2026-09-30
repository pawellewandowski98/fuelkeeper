// Package factory builds a store.Store from configuration.
package factory

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/aerospike"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/mongodb"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/pebble"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/postgres"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/sqlite"
)

// options collects the values Option functions set for New.
type options struct {
	// logger, when non-nil, is passed to backends that log their own runtime
	// events (currently pebble and aerospike; see their own WithLogger
	// options). Left nil (the default, when WithLogger is not passed to
	// New), each such backend keeps its own default of slog.Default().
	logger *slog.Logger
}

// Option configures New.
type Option func(*options)

// WithLogger passes l to any backend that logs its own runtime events
// (currently pebble's compaction/flush/WAL logging and aerospike's
// availability-mode warning; mongodb, postgres, sqlite and memory do not log
// anything themselves). Without it, New leaves each such backend on its own
// default (slog.Default()), exactly as before this option existed.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// Backend names a store implementation.
type Backend string

// Backend values recognized by New.
const (
	BackendMemory    Backend = "memory"
	BackendSQLite    Backend = "sqlite"
	BackendPostgres  Backend = "postgres"
	BackendPebble    Backend = "pebble"
	BackendMongoDB   Backend = "mongodb"
	BackendAerospike Backend = "aerospike"
)

// PebbleConfig mirrors pebble.Config with YAML tags: pebble.Config itself
// carries none, and a strict (KnownFields) YAML decoder needs every field an
// operator can set to be addressable by a snake_case key. toPebble maps it
// onto the real pebble.Config New/Open consumes.
type PebbleConfig struct {
	// Path is the directory Pebble stores its files in. Required.
	Path string `yaml:"path"`
	// NoSync skips fsync on every write batch. Leave false in production; see
	// pebble.Config.NoSync.
	NoSync bool `yaml:"no_sync"`
	// CacheBytes sizes Pebble's block cache. 0 means pebble's own default
	// (256 MiB).
	CacheBytes int64 `yaml:"cache_bytes"`
}

func (c PebbleConfig) toPebble() pebble.Config {
	return pebble.Config{Path: c.Path, NoSync: c.NoSync, CacheBytes: c.CacheBytes}
}

// MongoDBConfig mirrors mongodb.Config with YAML tags: mongodb.Config itself
// carries none. toMongoDB maps it onto the real mongodb.Config New consumes.
type MongoDBConfig struct {
	// URI is the mongodb:// connection string. Required.
	URI string `yaml:"uri"`
	// Database is the database name this store uses. Required.
	Database string `yaml:"database"`
	// MaxPoolSize is the driver's per-client connection pool size. 0 means
	// mongodb's own default (64).
	MaxPoolSize uint64 `yaml:"max_pool_size"`
	// OpTimeout bounds every individual operation against the server. 0
	// means mongodb's own default (5s).
	OpTimeout time.Duration `yaml:"op_timeout"`
}

func (c MongoDBConfig) toMongoDB() mongodb.Config {
	return mongodb.Config{URI: c.URI, Database: c.Database, MaxPoolSize: c.MaxPoolSize, OpTimeout: c.OpTimeout}
}

// AerospikeConfig mirrors aerospike.Config with YAML tags: aerospike.Config
// itself carries none. toAerospike maps it onto the real aerospike.Config
// New consumes.
type AerospikeConfig struct {
	// Hosts are "host:port" (or bare "host", defaulting to port 3000) seed
	// addresses. Required: at least one.
	Hosts []string `yaml:"hosts"`
	// Namespace is the Aerospike namespace this store uses. Required. It
	// must be configured with strong-consistency true in production; see
	// AllowAvailabilityMode.
	Namespace string `yaml:"namespace"`
	// SetPrefix is prepended to every set name (and secondary-index name)
	// this package uses. Per-deployment constant; may be empty.
	SetPrefix string `yaml:"set_prefix"`
	// KeyPrefix isolates this store from others sharing the same sets. May
	// be empty.
	KeyPrefix string `yaml:"key_prefix"`
	// AllowAvailabilityMode opts out of the strong-consistency requirement.
	// Development and test servers only (Community Edition cannot run SC);
	// never set this in production. See package aerospike's doc comment.
	AllowAvailabilityMode bool `yaml:"allow_availability_mode"`
	// Timeout bounds every individual operation against the server. 0 means
	// aerospike's own default (3s).
	Timeout time.Duration `yaml:"timeout"`
	// PoolSize is the client's per-node connection pool size. 0 means
	// aerospike's own default (128).
	PoolSize int `yaml:"pool_size"`
}

func (c AerospikeConfig) toAerospike() aerospike.Config {
	return aerospike.Config{
		Hosts:                 c.Hosts,
		Namespace:             c.Namespace,
		SetPrefix:             c.SetPrefix,
		KeyPrefix:             c.KeyPrefix,
		AllowAvailabilityMode: c.AllowAvailabilityMode,
		Timeout:               c.Timeout,
		PoolSize:              c.PoolSize,
	}
}

// Config selects and configures a backend.
//
// Multi-process safety (store.Caps.MultiProcess): postgres, mongodb and
// aerospike may be shared by several fuelkeeper processes against the same
// backing database/cluster at once. memory, sqlite and pebble are
// single-process: run exactly one process against a given sqlite file or
// pebble directory, and never share a memory store across processes (it is
// in-process only).
type Config struct {
	Backend   Backend         `yaml:"backend"`
	SQLite    sqlite.Config   `yaml:"sqlite"`
	Postgres  postgres.Config `yaml:"postgres"`
	Pebble    PebbleConfig    `yaml:"pebble"`
	MongoDB   MongoDBConfig   `yaml:"mongodb"`
	Aerospike AerospikeConfig `yaml:"aerospike"`
}

// Validate checks that cfg names a known backend and carries whatever that
// backend requires to open, without making any network call or touching
// disk. New calls Validate first, so a misconfigured backend fails fast with
// store.ErrInvalidArgument instead of attempting (and only then rejecting) a
// connection.
//
// Validate only checks presence of the fields a backend cannot open without;
// it does not duplicate a backend's own deeper validation (such as sqlite's
// Sync enum or aerospike's KeyPrefix charset), which New still surfaces
// unchanged from the backend's own Open/New when applicable.
func (c Config) Validate() error {
	switch c.Backend {
	case BackendMemory:
		return nil
	case BackendSQLite:
		if c.SQLite.Path == "" {
			// Matches sqlite.Open's own message so New's passthrough (New
			// calling Validate first, then sqlite.Open) is indistinguishable
			// from sqlite.Open's own error to a caller.
			return fmt.Errorf("%w: sqlite path is required", store.ErrInvalidArgument)
		}
		return nil
	case BackendPostgres:
		if c.Postgres.DSN == "" {
			return fmt.Errorf("%w: postgres.dsn is required", store.ErrInvalidArgument)
		}
		return nil
	case BackendPebble:
		if c.Pebble.Path == "" {
			return fmt.Errorf("%w: pebble.path is required", store.ErrInvalidArgument)
		}
		return nil
	case BackendMongoDB:
		if c.MongoDB.URI == "" {
			return fmt.Errorf("%w: mongodb.uri is required", store.ErrInvalidArgument)
		}
		if c.MongoDB.Database == "" {
			return fmt.Errorf("%w: mongodb.database is required", store.ErrInvalidArgument)
		}
		return nil
	case BackendAerospike:
		if len(c.Aerospike.Hosts) == 0 {
			return fmt.Errorf("%w: aerospike.hosts requires at least one host", store.ErrInvalidArgument)
		}
		if c.Aerospike.Namespace == "" {
			return fmt.Errorf("%w: aerospike.namespace is required", store.ErrInvalidArgument)
		}
		return nil
	case "":
		return fmt.Errorf("%w: store backend is required (memory|sqlite|postgres|pebble|mongodb|aerospike)", store.ErrInvalidArgument)
	default:
		return fmt.Errorf("%w: unknown store backend %q", store.ErrInvalidArgument, c.Backend)
	}
}

// New opens the configured backend with the given clock. opts is variadic so
// every existing call (positional ctx, cfg, now, with no further arguments)
// keeps compiling and behaving exactly as before; see WithLogger for the
// only option New currently accepts.
func New(ctx context.Context, cfg Config, now func() time.Time, opts ...Option) (store.Store, error) {
	if now == nil {
		now = time.Now
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	switch cfg.Backend {
	case BackendMemory:
		return memory.New(memory.WithClock(now)), nil
	case BackendSQLite:
		return sqlite.Open(ctx, cfg.SQLite, sqlite.WithClock(now))
	case BackendPostgres:
		return postgres.New(ctx, cfg.Postgres, postgres.WithClock(now))
	case BackendPebble:
		pebbleOpts := []pebble.Option{pebble.WithClock(now)}
		if o.logger != nil {
			pebbleOpts = append(pebbleOpts, pebble.WithLogger(o.logger))
		}
		return pebble.Open(ctx, cfg.Pebble.toPebble(), pebbleOpts...)
	case BackendMongoDB:
		return mongodb.New(ctx, cfg.MongoDB.toMongoDB(), mongodb.WithClock(now))
	case BackendAerospike:
		aeroOpts := []aerospike.Option{aerospike.WithClock(now)}
		if o.logger != nil {
			aeroOpts = append(aeroOpts, aerospike.WithLogger(o.logger))
		}
		return aerospike.New(ctx, cfg.Aerospike.toAerospike(), aeroOpts...)
	default:
		// Unreachable: Validate rejects every other cfg.Backend value above.
		return nil, fmt.Errorf("%w: unknown store backend %q", store.ErrInvalidArgument, cfg.Backend)
	}
}
