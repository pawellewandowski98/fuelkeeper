// Package postgres implements store.Store on PostgreSQL using pgx.
//
// Every mutation is a single statement whose WHERE clause re-asserts the row's
// current state, so concurrent writers serialise on the row and the loser simply
// matches zero rows. Batches use unnest() to pair per-op values positionally.
package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

//go:embed schema.sql
var schemaSQL string

// advisoryLockKey serialises schema application across replicas.
const advisoryLockKey int64 = 0x6675656c6b656570 // "fuelkeep"

const maxBatch = 1000

// Config configures New.
type Config struct {
	DSN      string
	MaxConns int32 // 0 = pgxpool default
}

// Option configures New.
type Option func(*Store)

// WithClock injects the clock used for timestamps and lease expiry.
func WithClock(now func() time.Time) Option { return func(s *Store) { s.now = now } }

// Store is a PostgreSQL store.Store.
type Store struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

var _ store.Store = (*Store)(nil)

// New opens a pool, applies the schema (idempotently, under an advisory lock) and pings.
func New(ctx context.Context, cfg Config, opts ...Option) (*Store, error) {
	pc, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse dsn: %w", err)
	}
	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("postgres: open: %w", err)
	}
	s := &Store{pool: pool, now: time.Now}
	for _, o := range opts {
		o(s)
	}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("postgres: acquire for migrate: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		return fmt.Errorf("postgres: advisory lock: %w", err)
	}
	defer func() { _, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", advisoryLockKey) }()
	if _, err := conn.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("postgres: apply schema: %w", err)
	}
	return nil
}

// Ping checks connectivity.
func (s *Store) Ping(ctx context.Context) error { return wrap(s.pool.Ping(ctx)) }

// Capabilities reports Postgres limits.
func (s *Store) Capabilities() store.Caps {
	return store.Caps{MaxConcurrentFlushes: 8, MaxBatch: maxBatch, AtomicBatch: true, MultiProcess: true}
}

// Close closes the pool. Idempotent.
func (s *Store) Close() error {
	if s.pool != nil {
		s.pool.Close()
		s.pool = nil
	}
	return nil
}

// Pool exposes the underlying pool for tests and diagnostics.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// wrap maps transient database errors to store.ErrUnavailable and passes the rest through.
func wrap(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001", "40P01", "55P03", "57014", "08000", "08003", "08006", "53300":
			return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
		}
		return err
	}
	if pgconn.SafeToRetry(err) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
	}
	return err
}

// tx runs fn in a transaction.
func (s *Store) tx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return wrap(err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return wrap(err)
	}
	return wrap(tx.Commit(ctx))
}

// ---- helpers shared by shards.go / rows.go ----

func utc(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.UTC()
}

func hashBytes(hs []store.Outpoint) ([][]byte, []int32) {
	txids := make([][]byte, len(hs))
	vouts := make([]int32, len(hs))
	for i, op := range hs {
		txids[i] = op.TxID[:]
		vouts[i] = int32(op.Vout) //nolint:gosec // vout is bounded by tx output count
	}
	return txids, vouts
}
