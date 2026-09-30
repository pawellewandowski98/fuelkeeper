// Package factory builds a store.Store from configuration.
package factory

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/postgres"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/sqlite"
)

// Backend names a store implementation.
type Backend string

// Backend values recognized by New.
const (
	BackendMemory   Backend = "memory"
	BackendSQLite   Backend = "sqlite"
	BackendPostgres Backend = "postgres"
	// Plan 4 adds: pebble, mongodb, aerospike.
)

// Config selects and configures a backend.
type Config struct {
	Backend  Backend         `yaml:"backend"`
	SQLite   sqlite.Config   `yaml:"sqlite"`
	Postgres postgres.Config `yaml:"postgres"`
}

// New opens the configured backend with the given clock.
func New(ctx context.Context, cfg Config, now func() time.Time) (store.Store, error) {
	if now == nil {
		now = time.Now
	}
	if cfg.Backend == "" {
		return nil, fmt.Errorf("%w: store backend is required (memory|sqlite|postgres)", store.ErrInvalidArgument)
	}
	switch cfg.Backend {
	case BackendMemory:
		return memory.New(memory.WithClock(now)), nil
	case BackendSQLite:
		return sqlite.Open(ctx, cfg.SQLite, sqlite.WithClock(now))
	case BackendPostgres:
		if cfg.Postgres.DSN == "" {
			return nil, fmt.Errorf("%w: postgres.dsn is required", store.ErrInvalidArgument)
		}
		return postgres.New(ctx, cfg.Postgres, postgres.WithClock(now))
	default:
		return nil, fmt.Errorf("%w: unknown store backend %q", store.ErrInvalidArgument, cfg.Backend)
	}
}
