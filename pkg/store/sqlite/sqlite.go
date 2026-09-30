// Package sqlite implements store.Store on SQLite via modernc.org/sqlite (no CGO).
//
// One process, one writer: every mutation is queued to a single writer
// goroutine that owns a dedicated single-connection *sql.DB (MaxOpenConns(1),
// _txlock=immediate). The writer group-commits: it takes every write queued
// while the previous commit was in flight, runs each in its own SAVEPOINT
// inside one transaction, and commits once, so N concurrent writes pay for
// one WAL fsync instead of N (see write). Reads use a separate, read-only WAL
// handle that proceeds concurrently with the writer.
//
// Callers read the clock (s.now()) before queueing a write, not once inside
// it: under contention a write can wait for the writer to reach it, so its
// stamped timestamp is slightly earlier than when it actually commits. That
// is the safe direction -- a lease or reservation computed from it runs out
// slightly sooner than requested, never later.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sqlite3 "modernc.org/sqlite/lib"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"

	_ "modernc.org/sqlite" // database/sql driver registration
)

const maxBatch = 1000

// maxGroup bounds how many queued writes one group commit carries, so a burst
// of writers cannot grow a single transaction (and the latency of the first
// write in it) without limit.
const maxGroup = 256

// errClosed is returned by a write issued after Close (matching the other
// backends' closed-store convention: store.ErrUnavailable).
var errClosed = fmt.Errorf("%w: sqlite: store is closed", store.ErrUnavailable)

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
	Path string `yaml:"path"`
	Sync Sync   `yaml:"sync"` // "" = SyncFull
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
//
// path is filepath.Clean'd first: on Linux, Clean already collapses a
// doubled leading slash ("//foo" -> "/foo") along with any other repeated or
// redundant separators, so a caller-supplied path that happens to start with
// "//" (e.g. built by naively joining an empty root with an absolute path)
// names the intended file instead of being parsed as a SQLite URI with a
// (unsupported) authority component ("invalid uri authority").
func DSN(path string, sync Sync) string {
	return "file:" + dsnPathEscaper.Replace(filepath.Clean(path)) + "?_txlock=immediate&" + commonPragmas(sync)
}

// ReaderDSN builds the read-only handle's DSN: the same pragmas as DSN plus
// query_only(1) (a bug can never route a write through the reader), and no
// _txlock, since reads never open a write transaction. See DSN for why path
// is filepath.Clean'd.
func ReaderDSN(path string, sync Sync) string {
	return "file:" + dsnPathEscaper.Replace(filepath.Clean(path)) + "?" + commonPragmas(sync) + "&_pragma=query_only(1)"
}

// Store is a SQLite store.Store.
type Store struct {
	writer    *sql.DB // MaxOpenConns(1): the single writer, used only by writeLoop (and the maintenance helpers)
	reader    *sql.DB
	now       func() time.Time
	writes    chan *writeReq // unbuffered: a request is claimed exactly when writeLoop receives it
	stop      chan struct{}  // closed by Close: writeLoop exits, queued-but-unclaimed writes fail with errClosed
	loopDone  chan struct{}  // closed when writeLoop has exited
	commits   atomic.Uint64  // group commits that reached COMMIT successfully (tests)
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
	reader.SetMaxIdleConns(8) // match MaxOpenConns: don't churn connections under concurrent reads
	s := &Store{
		writer: writer, reader: reader, now: time.Now,
		writes: make(chan *writeReq), stop: make(chan struct{}), loopDone: make(chan struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	go s.writeLoop()
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
	return s.write(ctx, func(ctx context.Context, c querier) error {
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

// Close stops the writer goroutine (after it finishes the group commit it is
// running, if any), then closes both handles, first folding the WAL back into
// the main database file (best effort: closing must proceed even if the
// checkpoint can't complete). Idempotent (guarded by sync.Once); the handles
// are never nilled out, so a write issued after Close fails with errClosed and
// a read hits database/sql's own "sql: database is closed", never a
// nil-pointer panic.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		close(s.stop)
		<-s.loopDone
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

// writeReq is one queued write: fn runs inside the writer's transaction and
// done receives its outcome exactly once.
type writeReq struct {
	ctx  context.Context
	fn   func(context.Context, querier) error
	done chan error // buffered(1): writeLoop never blocks replying
}

// write runs fn as one atomic unit on the single writer and returns once its
// outcome is durable (committed with the configured synchronous level) or
// known to be discarded.
//
// Group commit. Every commit under synchronous=FULL fsyncs the WAL, and with
// one writer those fsyncs serialize: throughput is capped at 1/fsync-latency
// writes per second (≈65/s on a SATA SSD under btrfs, where a WAL fsync costs
// ≈15ms), however small each write is. So write does not open its own
// transaction; it hands fn to writeLoop, which collects every write queued
// while its previous commit was in flight (up to maxGroup), runs them in
// arrival order inside one BEGIN IMMEDIATE transaction, each fn wrapped in its
// own SAVEPOINT, and commits once. Each fn is still atomic and isolated (its
// SAVEPOINT is rolled back on error without disturbing the others, and fns
// run strictly one after another, so a read-then-write inside one fn -- e.g.
// RenewShard, ReleaseShard, RetireShard in shards.go -- never races another
// write), and nobody is acknowledged before the shared COMMIT returns, so
// durability is exactly what it was with one transaction per write. If the
// COMMIT (or a SAVEPOINT statement) fails, every write in the group gets that
// error and none of them is applied.
//
// Cancellation. A write whose ctx is done before writeLoop claims it (the
// unbuffered send below) is never run: it returns the ctx error with nothing
// applied. Once claimed, fn runs under context.WithoutCancel(ctx) and write
// waits for the group's real outcome, which takes at most one group commit:
// a caller's cancellation must not interrupt a statement in the shared
// transaction, since SQLite rolls back the whole transaction -- every other
// caller's write with it -- when a DML statement inside it is interrupted.
// fn must therefore use the ctx it is given, not one captured from its caller.
//
// fn must not call write itself (writeLoop runs one fn at a time).
func (s *Store) write(ctx context.Context, fn func(context.Context, querier) error) error {
	if err := ctx.Err(); err != nil {
		return wrapCtx(ctx, err)
	}
	req := &writeReq{ctx: ctx, fn: fn, done: make(chan error, 1)}
	select {
	case s.writes <- req:
	case <-ctx.Done():
		return wrapCtx(ctx, ctx.Err())
	case <-s.stop:
		return errClosed
	}
	return <-req.done
}

// writeLoop is the single writer: it blocks for one queued write, then drains
// whatever else is already queued (up to maxGroup) and group-commits them.
// Every request it receives is replied to before it looks at the queue again.
func (s *Store) writeLoop() {
	defer close(s.loopDone)
	group := make([]*writeReq, 0, maxGroup)
	errs := make([]error, maxGroup)
	for {
		select {
		case <-s.stop:
			return
		case r := <-s.writes:
			group = append(group[:0], r)
		}
	drain:
		for len(group) < maxGroup {
			select {
			case r := <-s.writes:
				group = append(group, r)
			default:
				break drain
			}
		}
		s.commitGroup(group, errs[:len(group)])
		clear(group) // drop references to finished requests
	}
}

// commitGroup runs group in one transaction and replies to every member.
func (s *Store) commitGroup(group []*writeReq, errs []error) {
	live := 0
	for i, r := range group {
		errs[i] = nil
		if err := r.ctx.Err(); err != nil {
			errs[i] = wrapCtx(r.ctx, err) // expired while queued: never run
			continue
		}
		live++
	}
	var gerr error
	if live > 0 {
		gerr = s.runGroup(group, errs, live > 1)
	}
	for i, r := range group {
		if errs[i] == nil && gerr != nil {
			errs[i] = wrapCtx(r.ctx, gerr)
		}
		r.done <- errs[i]
	}
}

// runGroup runs every member of group whose errs entry is nil inside one
// transaction, recording each fn's own error in errs, and returns an error
// that applies to every member that did not fail on its own (the transaction
// was rolled back, or the COMMIT itself failed). With savepoints false (a
// single live member) fn's error simply rolls the whole transaction back.
//
// A panicking fn is recovered here, not allowed to unwind writeLoop (which
// would take the process down) or to leave the writer connection inside an
// open transaction: the transaction is rolled back, the panicking write gets
// an error naming the panic, and every other write in the group gets
// store.ErrUnavailable wrapping it (none of them was applied, so a retry is
// safe); writeLoop then carries on with the next group.
func (s *Store) runGroup(group []*writeReq, errs []error, savepoints bool) (gerr error) {
	bg := context.Background()
	tx, err := s.writer.BeginTx(bg, nil)
	if err != nil {
		return err
	}
	cur := -1
	defer func() {
		p := recover()
		if p == nil {
			return
		}
		_ = tx.Rollback()
		perr := fmt.Errorf("sqlite: write panicked: %v", p)
		if cur >= 0 {
			errs[cur] = perr
		}
		gerr = fmt.Errorf("%w: sqlite: group transaction rolled back: %w", store.ErrUnavailable, perr)
	}()
	for i, r := range group {
		if errs[i] != nil {
			continue
		}
		cur = i
		ctx := context.WithoutCancel(r.ctx)
		if !savepoints {
			if ferr := r.fn(ctx, tx); ferr != nil {
				_ = tx.Rollback()
				errs[i] = wrapCtx(r.ctx, ferr)
				return nil
			}
			continue
		}
		if _, err := tx.ExecContext(bg, `SAVEPOINT w`); err != nil {
			_ = tx.Rollback()
			return err
		}
		if ferr := r.fn(ctx, tx); ferr != nil {
			errs[i] = wrapCtx(r.ctx, ferr)
			// If fn's failure already aborted the whole transaction (e.g. an
			// I/O error), the savepoint is gone and this fails: the group fails.
			if _, err := tx.ExecContext(bg, `ROLLBACK TO w`); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		if _, err := tx.ExecContext(bg, `RELEASE w`); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return err
	}
	s.commits.Add(1)
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
