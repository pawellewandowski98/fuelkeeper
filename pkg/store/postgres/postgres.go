// Package postgres implements store.Store on PostgreSQL using pgx.
//
// Every batch mutation (Reserve, Release, Spend, Resolve, and RetireShard's
// row cascade) issues one single-row, primary-key point UPDATE per op,
// pipelined as one pgx.Batch inside a single transaction, always in ascending
// (txid, vout) order (see store.LessOutpoint in sort.go). A combined multi-row
// "UPDATE ... FROM unnest(...)" statement lets Postgres's planner pick
// whatever row-lock order it likes; two concurrent batches that touch
// overlapping rows in different orders can then deadlock (each waiting on a
// row the other already holds). Sequencing every batch's own per-row updates
// into the same global lock-acquisition order rules that out: no two
// transactions can ever be waiting on each other in a cycle. Every other
// mutation is a single-row statement whose WHERE clause re-asserts the row's
// current state (and, where relevant, its owner), so a concurrent loser
// simply matches zero rows.
package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

//go:embed schema.sql
var schemaV1 string

// migrations holds each schema version's DDL, in order: migrations[0] is
// version 1 (the current, and so far only, schema). Only versions greater
// than the highest already-applied version (tracked in fuel_schema_version)
// run, and every one of them runs inside the same advisory-locked transaction
// as its own version-tracking insert, so a crash mid-migration never leaves a
// version recorded as applied when it partially isn't (or vice versa).
//
// A future migration that adds an index to a table already large in
// production must NOT go in this slice as-is: Postgres forbids
// CREATE INDEX CONCURRENTLY inside a transaction block, and a plain
// CREATE INDEX here would hold a long lock on that table for the length of
// the migration. Ship the index as a separate, untracked "online" step
// (CREATE INDEX CONCURRENTLY, run standalone, outside migrate) applied
// manually before the version bump that starts relying on it; the tracked
// migration itself should then only contain whatever bookkeeping/constraint
// change depends on the index already existing.
var migrations = []string{schemaV1}

// versionTableDDL creates the schema-version bookkeeping table itself; it is
// not part of migrations because it must exist before we can even ask which
// migrations have already run.
const versionTableDDL = `
CREATE TABLE IF NOT EXISTS fuel_schema_version (
  version    INTEGER PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL
);`

// advisoryLockKey serialises schema migration across replicas racing New at
// the same time.
const advisoryLockKey int64 = 0x6675656c6b656570 // "fuelkeep"

const maxBatch = 1000

// maxConcurrentFlushes is Capabilities().MaxConcurrentFlushes: how many
// Reserve-shaped calls may usefully run in parallel against this backend.
const maxConcurrentFlushes = 8

// defaultMaxConns is used when Config.MaxConns is 0: enough headroom above
// maxConcurrentFlushes for the pool's own background/admin connections
// (migrate, Stats, ListShards, the reaper's Quarantine/ListQuarantined loop,
// ...) without assuming the caller wants to dominate a shared Postgres
// instance's connection budget.
const defaultMaxConns = maxConcurrentFlushes + 4

// Config configures New.
type Config struct {
	DSN      string
	MaxConns int32 // 0 = defaultMaxConns (maxConcurrentFlushes + 4)
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

// buildPoolConfig parses cfg.DSN into a *pgxpool.Config and resolves the
// pool's MaxConns: cfg.MaxConns wins whenever it is set (> 0); otherwise the
// DSN's own pool_max_conns (if it set one) is kept exactly as
// pgxpool.ParseConfig parsed it, and only when the DSN never mentioned
// pool_max_conns does this fall back to defaultMaxConns. This is split out
// from New so a unit test can exercise all three cases without a database.
func buildPoolConfig(cfg Config) (*pgxpool.Config, error) {
	pc, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse dsn: %w", err)
	}
	switch {
	case cfg.MaxConns > 0:
		pc.MaxConns = cfg.MaxConns
	case !dsnSetsPoolMaxConns(cfg.DSN):
		pc.MaxConns = defaultMaxConns
	}
	return pc, nil
}

// dsnSetsPoolMaxConns reports whether dsn itself names pool_max_conns.
// pgxpool.ParseConfig consumes (and deletes) that runtime parameter from its
// own returned Config as it resolves MaxConns, so by the time buildPoolConfig
// has a *pgxpool.Config in hand there is no way to tell "the DSN asked for
// pgxpool's own default (max(4, NumCPU))" apart from "the DSN said nothing at
// all"; parsing dsn a second time, before pgxpool.ParseConfig's side effect,
// recovers that distinction.
func dsnSetsPoolMaxConns(dsn string) bool {
	raw, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return false
	}
	_, ok := raw.RuntimeParams["pool_max_conns"]
	return ok
}

// New opens a pool, applies the schema (idempotently, under an advisory lock) and pings.
func New(ctx context.Context, cfg Config, opts ...Option) (*Store, error) {
	pc, err := buildPoolConfig(cfg)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("postgres: open: %w", err)
	}
	s := &Store{pool: pool, now: time.Now}
	for _, o := range opts {
		o(s)
	}
	if err := s.checkSynchronousCommit(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// checkSynchronousCommit refuses to start against a server whose
// synchronous_commit setting is off. G1 (no fuel output is ever handed to two
// live reservations) depends on an acknowledged commit being durable: with
// synchronous_commit=off, Postgres can report a transaction as committed
// before its WAL record reaches disk, so a crash in that window silently
// forgets a reservation the caller already believes it holds. local,
// remote_write and remote_apply all still fsync (or wait for a replica to
// receive/apply) before acknowledging, so only off is rejected.
func (s *Store) checkSynchronousCommit(ctx context.Context) error {
	var val string
	if err := s.pool.QueryRow(ctx, "SHOW synchronous_commit").Scan(&val); err != nil {
		return fmt.Errorf("postgres: read synchronous_commit: %w", err)
	}
	if val == "off" {
		return fmt.Errorf("%w: postgres: synchronous_commit=off would let a crash forget acknowledged reservations; set it to on for the fuelkeeper role or database", store.ErrInvalidArgument)
	}
	return nil
}

// migrate applies every not-yet-applied schema version inside one transaction
// holding a transaction-scoped advisory lock (pg_advisory_xact_lock), so the
// lock always releases when the transaction ends (commit, rollback, or the
// connection dying) instead of leaking forever the way a session-scoped
// pg_advisory_lock would if the process died between acquiring it and its
// paired unlock call.
func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin migrate: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once Commit has succeeded

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", advisoryLockKey); err != nil {
		return fmt.Errorf("postgres: advisory lock: %w", err)
	}
	if _, err := tx.Exec(ctx, versionTableDDL); err != nil {
		return fmt.Errorf("postgres: create schema version table: %w", err)
	}
	var applied int
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM fuel_schema_version`).Scan(&applied); err != nil {
		return fmt.Errorf("postgres: read schema version: %w", err)
	}
	now := time.Now()
	for i := applied; i < len(migrations); i++ {
		version := i + 1
		if _, err := tx.Exec(ctx, migrations[i]); err != nil {
			return fmt.Errorf("postgres: apply schema v%d: %w", version, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO fuel_schema_version (version, applied_at) VALUES ($1, $2)`, version, now); err != nil {
			return fmt.Errorf("postgres: record schema v%d: %w", version, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit migrate: %w", err)
	}
	return nil
}

// Ping checks connectivity.
func (s *Store) Ping(ctx context.Context) error { return wrap(s.pool.Ping(ctx)) }

// Capabilities reports Postgres limits.
func (s *Store) Capabilities() store.Caps {
	return store.Caps{MaxConcurrentFlushes: maxConcurrentFlushes, MaxBatch: maxBatch, AtomicBatch: true, MultiProcess: true}
}

// Close closes the pool. pgxpool.Pool.Close is itself idempotent (guarded by
// a sync.Once), so Close just delegates to it directly: any store method
// called after Close returns the pool's own closed-pool error
// (puddle.ErrClosedPool, "closed pool") passed through wrap unchanged --
// isTransient does not recognise it, so it is not wrapped as
// store.ErrUnavailable -- never a nil-pointer panic from a manually
// nilled-out field.
func (s *Store) Close() error {
	s.pool.Close()
	return nil
}

// wrap maps transient database errors to store.ErrUnavailable and passes the
// rest through unchanged.
func wrap(err error) error {
	if err == nil {
		return nil
	}
	if isTransient(err) {
		return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
	}
	return err
}

// isTransient reports whether err represents a failure a caller may safely
// retry: connection loss or admin/crash shutdown, serialization or deadlock
// aborts, lock or statement timeouts, a read-only-transaction rejection
// (failover to a replica mid-transaction), or any error whose actual
// server-side effect is unknown (a broken connection, or a failed commit).
// Every write in this package is either idempotent (Reserve) or a checked CAS
// that reports how many rows it actually changed (Release/Spend/Resolve/
// RetireShard/...), so retrying on an "unknown outcome" error is always safe:
// a write that did land is simply reported as already applied (Reserve) or
// matches zero rows the second time (everything else).
func isTransient(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case strings.HasPrefix(pgErr.Code, "08"): // Class 08 — Connection Exception
			return true
		case strings.HasPrefix(pgErr.Code, "53"): // Class 53 — Insufficient Resources
			return true
		}
		switch pgErr.Code {
		case "57P01", // admin_shutdown
			"57P02", // crash_shutdown
			"57P03", // cannot_connect_now
			"57014", // query_canceled (statement_timeout, pg_cancel_backend, ...)
			"40001", // serialization_failure
			"40P01", // deadlock_detected
			"55P03", // lock_not_available (lock_timeout, SKIP LOCKED contention aside)
			"25006": // read_only_sql_transaction (this connection failed over to a replica)
			return true
		}
		return false // a well-formed, non-transient database error: e.g. 23505 unique_violation
	}
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return true
	}
	if pgconn.Timeout(err) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, pgx.ErrTxCommitRollback) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return false
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
	if err := tx.Commit(ctx); err != nil {
		// The commit's server-side outcome is unknown: a connection failure here
		// could mean the transaction committed just before the connection broke, or
		// never reached the server at all. Every write in this package is safe to
		// retry regardless (see isTransient), so a commit failure is always
		// store.ErrUnavailable, independent of the specific error code wrap would
		// otherwise classify it under.
		return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
	}
	return nil
}

// ---- helpers shared by shards.go / rows.go ----

func utc(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.UTC()
}

func outpointArrays(hs []store.Outpoint) ([][]byte, []int32) {
	txids := make([][]byte, len(hs))
	vouts := make([]int32, len(hs))
	for i, op := range hs {
		txids[i] = op.TxID[:]
		vouts[i] = int32(op.Vout) //nolint:gosec // vout is bounded by tx output count
	}
	return txids, vouts
}

// execBatch sends batch on tx and executes and consumes its n queued
// statements in the order they were queued (see store.LessOutpoint in
// sort.go for why that order matters), returning the sum of RowsAffected
// across all of them. Every caller queues plain Exec-style statements (no
// RETURNING); Reserve queues QueryRow-style statements instead, since it
// needs each row's returned expires_at, so it drives its own batch loop.
func execBatch(ctx context.Context, tx pgx.Tx, batch *pgx.Batch, n int) (int, error) {
	br := tx.SendBatch(ctx, batch)
	total := 0
	for i := 0; i < n; i++ {
		tag, err := br.Exec()
		if err != nil {
			_ = br.Close()
			return 0, err
		}
		total += int(tag.RowsAffected())
	}
	return total, br.Close()
}
