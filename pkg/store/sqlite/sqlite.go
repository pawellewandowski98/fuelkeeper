// Package sqlite implements store.Store on SQLite via modernc.org/sqlite (no CGO).
//
// One process, one writer: every mutation runs in a transaction on a
// dedicated single-connection *sql.DB (MaxOpenConns(1), _txlock=immediate),
// serialising writes through Go's own connection-pool checkout. Reads use a
// separate, read-only WAL handle that proceeds concurrently with the single
// open write transaction.
//
// Callers read the clock (s.now()) before queueing a write, not once inside
// it: under contention a write can wait for the sole writer connection to
// free up, so its stamped timestamp is slightly earlier than when it actually
// commits. That is the safe direction -- a lease or reservation computed from
// it runs out slightly sooner than requested, never later.
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

// journalSizeLimit bounds how large the WAL file is allowed to grow before
// SQLite truncates it back down after a checkpoint (PRAGMA
// journal_size_limit): 64 MiB.
const journalSizeLimit = 64 * 1024 * 1024

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

// dsnPathEscaper percent-encodes the characters that are otherwise meaningful
// in a SQLite URI filename (the query-string separators and the escape
// character itself), so a path containing them still names the intended file
// instead of being parsed apart as DSN parameters.
var dsnPathEscaper = strings.NewReplacer("%", "%25", "?", "%3F", "#", "%23")

func normalizeSync(sync Sync) Sync {
	if sync == "" {
		return SyncFull
	}
	return sync
}

func validateSync(sync Sync) error {
	if sync != "" && sync != SyncFull && sync != SyncNormal {
		return fmt.Errorf("%w: sqlite sync %q must be \"\", %q or %q", store.ErrInvalidArgument, sync, SyncFull, SyncNormal)
	}
	return nil
}

func commonPragmas(sync Sync) string {
	return "_pragma=busy_timeout(5000)" +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(" + string(normalizeSync(sync)) + ")" +
		fmt.Sprintf("&_pragma=journal_size_limit(%d)", journalSizeLimit)
}

// DSN builds the writer DSN. _txlock=immediate makes every BeginTx on this
// connection issue BEGIN IMMEDIATE, so a conflicting writer sees SQLITE_BUSY
// up front instead of partway through the transaction.
func DSN(path string, sync Sync) string {
	return "file:" + dsnPathEscaper.Replace(path) + "?_txlock=immediate&" + commonPragmas(sync)
}

// ReaderDSN builds the read-only handle's DSN: the same pragmas as DSN plus
// query_only(1) (a bug can never route a write through the reader), and no
// _txlock, since reads never open a write transaction.
func ReaderDSN(path string, sync Sync) string {
	return "file:" + dsnPathEscaper.Replace(path) + "?" + commonPragmas(sync) + "&_pragma=query_only(1)"
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

// Pragmas is what Open verified: journal_mode/busy_timeout/synchronous/
// journal_size_limit read back on a writer connection, and query_only read
// back on a reader connection (only the reader DSN sets it).
type Pragmas struct {
	JournalMode      string
	BusyTimeout      int
	Synchronous      int // 1 NORMAL, 2 FULL
	JournalSizeLimit int64
	ReaderQueryOnly  bool
}

// Open opens the database, applies the schema and verifies the pragmas.
func Open(ctx context.Context, cfg Config, opts ...Option) (*Store, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("%w: sqlite path is required", store.ErrInvalidArgument)
	}
	if err := validateSync(cfg.Sync); err != nil {
		return nil, err
	}
	writer, err := sql.Open("sqlite", DSN(cfg.Path, cfg.Sync))
	if err != nil {
		return nil, fmt.Errorf("sqlite: open writer: %w", err)
	}
	writer.SetMaxOpenConns(1)
	writer.SetConnMaxLifetime(0)
	reader, err := sql.Open("sqlite", ReaderDSN(cfg.Path, cfg.Sync))
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
	if p.JournalMode != "wal" || p.BusyTimeout != 5000 || p.Synchronous != wantSync ||
		p.JournalSizeLimit != journalSizeLimit || !p.ReaderQueryOnly {
		_ = s.Close()
		return nil, fmt.Errorf("sqlite: pragmas not applied (%+v)", p)
	}
	return s, nil
}

// migrate applies every not-yet-applied schema version, tracked via PRAGMA
// user_version, inside one write transaction: a crash mid-migration never
// leaves user_version bumped without its DDL having actually run, or vice
// versa, since both are ordinary writes inside the same transaction (see
// schema.go).
func (s *Store) migrate(ctx context.Context) error {
	return s.write(ctx, func(c querier) error {
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

// Pragmas reads the effective pragmas back from a writer connection and a
// reader connection.
func (s *Store) Pragmas(ctx context.Context) (Pragmas, error) {
	var p Pragmas
	wconn, err := s.writer.Conn(ctx)
	if err != nil {
		return p, wrapCtx(ctx, err)
	}
	defer func() { _ = wconn.Close() }()
	if err := wconn.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&p.JournalMode); err != nil {
		return p, wrapCtx(ctx, err)
	}
	if err := wconn.QueryRowContext(ctx, `PRAGMA busy_timeout`).Scan(&p.BusyTimeout); err != nil {
		return p, wrapCtx(ctx, err)
	}
	if err := wconn.QueryRowContext(ctx, `PRAGMA synchronous`).Scan(&p.Synchronous); err != nil {
		return p, wrapCtx(ctx, err)
	}
	if err := wconn.QueryRowContext(ctx, `PRAGMA journal_size_limit`).Scan(&p.JournalSizeLimit); err != nil {
		return p, wrapCtx(ctx, err)
	}
	p.JournalMode = strings.ToLower(p.JournalMode)

	rconn, err := s.reader.Conn(ctx)
	if err != nil {
		return p, wrapCtx(ctx, err)
	}
	defer func() { _ = rconn.Close() }()
	var queryOnly int
	if err := rconn.QueryRowContext(ctx, `PRAGMA query_only`).Scan(&queryOnly); err != nil {
		return p, wrapCtx(ctx, err)
	}
	p.ReaderQueryOnly = queryOnly != 0
	return p, nil
}

// Ping checks the reader handle.
func (s *Store) Ping(ctx context.Context) error { return wrapCtx(ctx, s.reader.PingContext(ctx)) }

// Capabilities reports SQLite limits: one writer, batches bounded, single process.
func (s *Store) Capabilities() store.Caps {
	return store.Caps{MaxConcurrentFlushes: 1, MaxBatch: maxBatch, AtomicBatch: true, MultiProcess: false}
}

// Analyze refreshes planner statistics (tests and operators).
func (s *Store) Analyze(ctx context.Context) error {
	_, err := s.writer.ExecContext(ctx, `ANALYZE`)
	return wrapCtx(ctx, err)
}

// Checkpoint runs a PASSIVE WAL checkpoint: it folds as many WAL frames into
// the main database file as it can without blocking a concurrent reader or
// writer, simply doing less if it can't get the locks to do more. Safe to
// call at any time; a maintenance job may call it periodically to bound WAL
// growth between the automatic checkpoints SQLite already runs on its own.
func (s *Store) Checkpoint(ctx context.Context) error {
	_, err := s.writer.ExecContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`)
	return wrapCtx(ctx, err)
}

// ExplainQueryPlan returns the detail lines of EXPLAIN QUERY PLAN for q.
func (s *Store) ExplainQueryPlan(ctx context.Context, q string, args ...any) ([]string, error) {
	rs, err := s.reader.QueryContext(ctx, "EXPLAIN QUERY PLAN "+q, args...)
	if err != nil {
		return nil, wrapCtx(ctx, err)
	}
	defer func() { _ = rs.Close() }()
	var out []string
	for rs.Next() {
		var id, parent, notused int
		var detail string
		if err := rs.Scan(&id, &parent, &notused, &detail); err != nil {
			return nil, wrapCtx(ctx, err)
		}
		out = append(out, detail)
	}
	return out, wrapCtx(ctx, rs.Err())
}

// Close closes both handles, first folding the WAL back into the main
// database file (best effort: closing must proceed even if the checkpoint
// can't complete). Idempotent (guarded by sync.Once); the handles are never
// nilled out, so a store method called after Close hits database/sql's own
// "sql: database is closed" on a closed *sql.DB, never a nil-pointer panic.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		_, _ = s.writer.ExecContext(context.Background(), `PRAGMA wal_checkpoint(TRUNCATE)`)
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

// querier is the subset of *sql.Tx that write's fn and the read helpers in
// rows.go/shards.go need. Using an interface here, rather than *sql.Tx
// directly, is what lets wrap_test.go's fakeCoder-style unit tests stay
// decoupled from the concrete type; every production caller passes a real
// *sql.Tx.
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

var _ querier = (*sql.Tx)(nil)

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
// SQLITE_INTERRUPT means modernc.org/sqlite cut a running statement short
// because its context was done (see wrapCtx, which attributes this to the
// caller's context directly whenever that context is available; this
// classification is the fallback for a path where it isn't). context.
// DeadlineExceeded and io.EOF/io.ErrUnexpectedEOF mean the caller gave up or
// the connection dropped mid-read. SQLITE_IOERR-class subcodes are not
// classified as retryable here: unlike BUSY/LOCKED they usually indicate a
// disk-level fault (full disk, permission, hardware) that a caller retrying
// immediately will not resolve.
func isTransient(err error) bool {
	var c sqliteCoder
	if errors.As(err, &c) {
		switch c.Code() & 0xff {
		case sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED, sqlite3.SQLITE_INTERRUPT:
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

// wrapCtx attributes a failure to ctx when ctx is done: modernc surfaces a
// mid-statement cancellation as SQLITE_INTERRUPT, and database/sql's rollback
// watcher can make Commit return sql.ErrTxDone; both are cancellations. A nil
// err (a successful call racing a context that goes done right after, e.g.
// rs.Err() or a read-only tx.Commit() that both landed cleanly) is never
// turned into a non-nil error: success stays success.
func wrapCtx(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if cerr := ctx.Err(); cerr != nil && !errors.Is(err, cerr) {
		err = fmt.Errorf("%w (%w)", cerr, err)
	}
	return wrap(err)
}

// write runs fn inside one transaction on the single writer *sql.DB
// (_txlock=immediate, so BeginTx issues BEGIN IMMEDIATE). Because writer has
// exactly one connection, at most one write transaction is ever in flight: a
// second concurrent call blocks in BeginTx until the first's connection is
// released, never at the SQL engine level, so fn never races another write's
// statements -- including the read-then-write sequences in shards.go
// (RenewShard, ReleaseShard, RetireShard): nothing else can run between a
// SELECT and its following UPDATE within the same fn.
//
// Using *sql.Tx here, rather than a raw *sql.Conn plus hand-written "BEGIN"/
// "COMMIT"/"ROLLBACK" statements, is what makes this safe under a cancelled
// context: database/sql guarantees that once BeginTx succeeds, either Commit
// or Rollback actually runs against the connection -- via its own background
// watcher issuing a real rollback the moment ctx is done, even when our own
// later Commit/Rollback call short-circuits on the already-cancelled context
// -- before the connection goes back to the pool. A hand-written "COMMIT"
// sent through ExecContext(ctx, ...) has no such guarantee: once ctx is
// cancelled it never reaches the server at all, leaving the connection wedged
// inside an open transaction when it's returned to the pool, so every later
// write then fails with SQLite's non-transient "cannot start a transaction
// within a transaction" -- and because that error isn't transient, callers
// never retry their way out of it either.
func (s *Store) write(ctx context.Context, fn func(querier) error) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return wrapCtx(ctx, err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return wrapCtx(ctx, err)
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return wrapCtx(ctx, err)
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
