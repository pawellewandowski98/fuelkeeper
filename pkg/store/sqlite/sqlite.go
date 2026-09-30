// Package sqlite implements store.Store on SQLite via modernc.org/sqlite (no CGO).
//
// One process, one writer: every mutation runs on a dedicated single-connection
// *sql.DB (MaxOpenConns(1)) inside BEGIN IMMEDIATE, so writes serialise through
// Go's own connection-pool checkout instead of racing SQLite's own locking.
// Every batch mutation (Reserve, Release, Spend, Resolve, and RetireShard's row
// cascade) still issues one single-row, primary-key statement per op, in
// ascending (txid internal bytes, vout) order (see lessOutpoint in rows.go):
// harmless under this single-writer model (there is only ever one write
// transaction in flight, so no two transactions can deadlock against each
// other), but it keeps the on-disk access pattern identical to the Postgres
// backend and costs nothing to keep, including for a future backend that might
// share this code under real row-level locking.
//
// Reads use a separate handle (up to 8 connections): WAL lets them proceed
// concurrently with the single open write transaction.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	sqlite3 "modernc.org/sqlite/lib"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"

	_ "modernc.org/sqlite" // database/sql driver registration
)

const maxBatch = 1000

// Sync is the synchronous pragma level.
type Sync string

const (
	// SyncFull fsyncs every commit; a committed reservation survives power loss. Default.
	SyncFull Sync = "FULL"
	// SyncNormal is faster but may lose the last commits on power loss (WAL mode).
	// Opt in only when the deployment accepts that a committed reservation can revert.
	SyncNormal Sync = "NORMAL"
)

// Config configures Open.
type Config struct {
	Path string
	Sync Sync // "" = SyncFull
}

// Option configures Open.
type Option func(*Store)

// WithClock injects the clock used for timestamps and lease expiry.
func WithClock(now func() time.Time) Option { return func(s *Store) { s.now = now } }

// DSN builds the modernc DSN. Only the _pragma=name(value) form is honoured by
// modernc; mattn-style keys (_journal_mode=WAL) are silently ignored, which is
// why Open reads the pragmas back.
func DSN(path string, sync Sync) string {
	if sync == "" {
		sync = SyncFull
	}
	return "file:" + path +
		"?_pragma=busy_timeout(5000)" +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(" + string(sync) + ")" +
		"&_pragma=foreign_keys(ON)"
}

// Store is a SQLite store.Store.
type Store struct {
	writer    *sql.DB // MaxOpenConns(1): the single writer
	reader    *sql.DB
	now       func() time.Time
	closeOnce sync.Once
	closeErr  error
}

var _ store.Store = (*Store)(nil)

// Pragmas is what Open verified.
type Pragmas struct {
	JournalMode string
	BusyTimeout int
	Synchronous int // 1 NORMAL, 2 FULL
}

// Open opens the database, applies the schema and verifies the pragmas.
func Open(ctx context.Context, cfg Config, opts ...Option) (*Store, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("%w: sqlite path is required", store.ErrInvalidArgument)
	}
	dsn := DSN(cfg.Path, cfg.Sync)
	writer, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open writer: %w", err)
	}
	writer.SetMaxOpenConns(1)
	writer.SetConnMaxLifetime(0)
	reader, err := sql.Open("sqlite", dsn)
	if err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("sqlite: open reader: %w", err)
	}
	reader.SetMaxOpenConns(8)
	s := &Store{writer: writer, reader: reader, now: time.Now}
	for _, o := range opts {
		o(s)
	}
	if err := s.migrate(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	p, err := s.Pragmas(ctx)
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	wantSync := 2
	if cfg.Sync == SyncNormal {
		wantSync = 1
	}
	if p.JournalMode != "wal" || p.BusyTimeout != 5000 || p.Synchronous != wantSync {
		_ = s.Close()
		return nil, fmt.Errorf("sqlite: pragmas not applied (journal_mode=%q busy_timeout=%d synchronous=%d)", p.JournalMode, p.BusyTimeout, p.Synchronous)
	}
	return s, nil
}

// migrate applies every not-yet-applied schema version, tracked via PRAGMA
// user_version, inside one BEGIN IMMEDIATE transaction on the single writer
// connection: a crash mid-migration never leaves user_version bumped without
// its DDL having actually run, or vice versa, since both are ordinary writes
// inside the same transaction (see schema.go).
func (s *Store) migrate(ctx context.Context) error {
	return s.write(ctx, func(c *sql.Conn) error {
		var version int
		if err := c.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
			return fmt.Errorf("sqlite: read schema version: %w", err)
		}
		for i := version; i < len(migrations); i++ {
			if _, err := c.ExecContext(ctx, migrations[i]); err != nil {
				return fmt.Errorf("sqlite: apply schema v%d: %w", i+1, err)
			}
		}
		if version < len(migrations) {
			// PRAGMA user_version does not accept a bound parameter for its value in
			// every SQLite build; len(migrations) is an internal int, not user input,
			// so formatting it into the statement text carries no injection risk.
			if _, err := c.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", len(migrations))); err != nil {
				return fmt.Errorf("sqlite: record schema version: %w", err)
			}
		}
		return nil
	})
}

// Pragmas reads the effective pragmas on the writer connection.
func (s *Store) Pragmas(ctx context.Context) (Pragmas, error) {
	var p Pragmas
	conn, err := s.writer.Conn(ctx)
	if err != nil {
		return p, wrap(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&p.JournalMode); err != nil {
		return p, wrap(err)
	}
	if err := conn.QueryRowContext(ctx, `PRAGMA busy_timeout`).Scan(&p.BusyTimeout); err != nil {
		return p, wrap(err)
	}
	if err := conn.QueryRowContext(ctx, `PRAGMA synchronous`).Scan(&p.Synchronous); err != nil {
		return p, wrap(err)
	}
	p.JournalMode = strings.ToLower(p.JournalMode)
	return p, nil
}

// Ping checks the reader handle.
func (s *Store) Ping(ctx context.Context) error { return wrap(s.reader.PingContext(ctx)) }

// Capabilities reports SQLite limits: one writer, batches bounded, single process.
func (s *Store) Capabilities() store.Caps {
	return store.Caps{MaxConcurrentFlushes: 1, MaxBatch: maxBatch, AtomicBatch: true, MultiProcess: false}
}

// Analyze refreshes planner statistics (tests and operators).
func (s *Store) Analyze(ctx context.Context) error {
	_, err := s.writer.ExecContext(ctx, `ANALYZE`)
	return wrap(err)
}

// ExplainQueryPlan returns the detail lines of EXPLAIN QUERY PLAN for q.
func (s *Store) ExplainQueryPlan(ctx context.Context, q string, args ...any) ([]string, error) {
	rs, err := s.reader.QueryContext(ctx, "EXPLAIN QUERY PLAN "+q, args...)
	if err != nil {
		return nil, wrap(err)
	}
	defer func() { _ = rs.Close() }()
	var out []string
	for rs.Next() {
		var id, parent, notused int
		var detail string
		if err := rs.Scan(&id, &parent, &notused, &detail); err != nil {
			return nil, wrap(err)
		}
		out = append(out, detail)
	}
	return out, wrap(rs.Err())
}

// Close closes both handles. Idempotent (guarded by sync.Once): the handles
// are never nilled out, so a store method called after Close hits
// database/sql's own "sql: database is closed" on a closed *sql.DB, never a
// nil-pointer panic.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		var errs []error
		if err := s.writer.Close(); err != nil {
			errs = append(errs, err)
		}
		if err := s.reader.Close(); err != nil {
			errs = append(errs, err)
		}
		s.closeErr = errors.Join(errs...)
	})
	return s.closeErr
}

// sqliteCoder is satisfied by *modernc.org/sqlite.Error (whose Code method
// returns the underlying C-level SQLite result code) and, in wrap_test.go, by
// a fake used to exercise every code path deterministically: Error's own
// fields are unexported, so a test cannot build one with an arbitrary code
// directly, but it can build any value satisfying this interface.
type sqliteCoder interface{ Code() int }

// isTransient reports whether err represents a failure a caller may safely
// retry. Every write in this package is either idempotent (Reserve) or a
// checked CAS that reports how many rows it actually changed (Release/Spend/
// Resolve/RetireShard/...), so retrying on an "unknown outcome" error is
// always safe.
//
// SQLITE_BUSY and SQLITE_LOCKED (and their extended-result-code variants,
// e.g. SQLITE_BUSY_SNAPSHOT, SQLITE_LOCKED_SHAREDCACHE: masking with & 0xff
// recovers the primary code) mean another connection held a conflicting lock;
// context.DeadlineExceeded and io.EOF/io.ErrUnexpectedEOF mean the caller gave
// up or the connection dropped mid-read. SQLITE_IOERR-class subcodes are not
// classified as retryable here: unlike BUSY/LOCKED they usually indicate a
// disk-level fault (full disk, permission, hardware) that a caller retrying
// immediately will not resolve.
func isTransient(err error) bool {
	var c sqliteCoder
	if errors.As(err, &c) {
		switch c.Code() & 0xff {
		case sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED:
			return true
		}
		return false // a well-formed, non-transient SQLite error: e.g. SQLITE_CONSTRAINT
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	return false
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

// write runs fn inside BEGIN IMMEDIATE on the single writer connection,
// retrying a busy/locked begin a few times. Because writer has exactly one
// connection, at most one write transaction is ever in flight: a second
// concurrent call to write blocks in s.writer.Conn(ctx) until the first's
// connection is released (COMMIT or ROLLBACK), never at the SQL engine level.
// fn therefore never races another write's statements, including the
// read-then-write sequences in shards.go (RenewShard, ReleaseShard,
// RetireShard): nothing else can run between the SELECT and the following
// UPDATE within the same fn.
func (s *Store) write(ctx context.Context, fn func(*sql.Conn) error) error {
	conn, err := s.writer.Conn(ctx)
	if err != nil {
		return wrap(err)
	}
	defer func() { _ = conn.Close() }()
	var begun bool
	for attempt, backoff := 0, 20*time.Millisecond; attempt < 4; attempt, backoff = attempt+1, backoff*2 {
		if _, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err == nil {
			begun = true
			break
		}
		if !errors.Is(wrap(err), store.ErrUnavailable) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	if !begun {
		return wrap(err)
	}
	if err := fn(conn); err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		return wrap(err)
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		return wrap(err)
	}
	return nil
}

// ---- time helpers ----

func fromUS(v sql.NullInt64) time.Time {
	if !v.Valid {
		return time.Time{}
	}
	return time.UnixMicro(v.Int64).UTC()
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}
