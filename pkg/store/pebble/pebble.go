// Package pebble implements store.Store on an embedded CockroachDB Pebble
// LSM tree.
//
// The whole store lives in one process behind one write mutex: every public
// method takes s.mu for its entire body (not just its mutating tail), so
// reads and writes alike observe and produce a fully consistent view without
// relying on Pebble's own MVCC snapshots. Every mutation builds exactly one
// *pebble.Batch and commits it once: the row or shard record, every index key
// that must change with it (is/, ie/, iq/), and every per-pool counter it
// touches are all written in that same batch, so a crash between two
// otherwise-separate writes can never leave an index or counter out of sync
// with the record it describes. Counters are maintained incrementally
// (accumulated as signed deltas per call, then read-modify-written exactly
// once per key per batch — see applyCounters) rather than recomputed by
// scanning, which is what lets Stats answer in O(pool's distinct
// states/statuses) instead of O(rows).
package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pdb "github.com/cockroachdb/pebble"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// maxBatch is Capabilities().MaxBatch: the maximum ops per
// Reserve/Release/Spend/Resolve/GetRows call.
const maxBatch = 1000

// schemaVersion is the current on-disk schema version, stored at
// meta/version. It must not change without a migration path.
const schemaVersion uint32 = 1

// errClosed is returned by every method once the store has been closed.
var errClosed = fmt.Errorf("%w: pebble: store is closed", store.ErrUnavailable)

// Config configures Open. The zero value opens (or creates) the database at
// Path with durable (fsync'd) writes.
type Config struct {
	// Path is the directory Pebble stores its files in. Required.
	Path string
	// NoSync skips fsync on every write batch. The zero value (false) is
	// durable, since G1 (no outpoint in two live reservations) depends on an
	// acknowledged write surviving a crash; set it only for throwaway stores
	// such as tests, where SyncWrites-by-default would otherwise need every
	// caller to opt in to durability instead of opt out of it.
	NoSync bool
}

// Option configures Open.
type Option func(*Store)

// WithClock injects the clock used for timestamps and lease expiry.
func WithClock(now func() time.Time) Option { return func(s *Store) { s.now = now } }

// Store is a Pebble-backed store.Store.
type Store struct {
	db  *pdb.DB
	wo  *pdb.WriteOptions
	now func() time.Time

	mu        sync.Mutex
	closeOnce sync.Once
	closed    atomic.Bool
}

var _ store.Store = (*Store)(nil)

// Open opens (creating if necessary) a Pebble store at cfg.Path.
func Open(ctx context.Context, cfg Config, opts ...Option) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.Path == "" {
		return nil, fmt.Errorf("%w: pebble: path is required", store.ErrInvalidArgument)
	}
	db, err := pdb.Open(cfg.Path, &pdb.Options{})
	if err != nil {
		return nil, fmt.Errorf("%w: pebble: open %s: %w", store.ErrUnavailable, cfg.Path, err)
	}
	wo := pdb.Sync
	if cfg.NoSync {
		wo = pdb.NoSync
	}
	s := &Store{db: db, wo: wo, now: time.Now}
	for _, o := range opts {
		o(s)
	}
	if err := s.ensureSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// ensureSchema writes meta/version on a fresh store, or checks it on an
// existing one.
func (s *Store) ensureSchema() error {
	v, err := s.get(metaVersionKey)
	if err != nil {
		return err
	}
	if v == nil {
		buf := make([]byte, 4)
		beUint32(buf, schemaVersion)
		if err := s.db.Set(metaVersionKey, buf, s.wo); err != nil {
			return wrapErr(err)
		}
		return nil
	}
	if len(v) != 4 {
		return errors.New("pebble: corrupt meta/version record")
	}
	got := deUint32(v)
	if got != schemaVersion {
		return fmt.Errorf("pebble: schema version %d unsupported (this build wants %d)", got, schemaVersion)
	}
	return nil
}

// Ping implements store.Store.
func (s *Store) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return errClosed
	}
	return nil
}

// Capabilities implements store.Store.
func (s *Store) Capabilities() store.Caps {
	return store.Caps{MaxConcurrentFlushes: 1, MaxBatch: maxBatch, AtomicBatch: true, MultiProcess: false}
}

// Close implements store.Store; it is idempotent.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		err = s.db.Close()
	})
	return err
}

// wrapErr maps pebble.ErrClosed and I/O failures to store.ErrUnavailable and
// passes every other error through unchanged. pebble.ErrNotFound must never
// reach here: every caller that can see it (get, and anything built on it)
// translates it to a nil result before returning.
func wrapErr(err error) error {
	if err == nil {
		return nil
	}
	if isTransient(err) {
		return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
	}
	return err
}

// isTransient reports whether err reflects the underlying store being
// unavailable rather than a logic error: the store having been closed out
// from under an in-flight call, or an OS-level I/O failure (disk full,
// permission, EIO, ...) surfaced through Pebble's vfs layer.
func isTransient(err error) bool {
	if errors.Is(err, pdb.ErrClosed) {
		return true
	}
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return true
	}
	var errno syscall.Errno
	return errors.As(err, &errno)
}

// get reads key, returning (nil, nil) when it does not exist.
// pebble.ErrNotFound never escapes this function.
func (s *Store) get(key []byte) ([]byte, error) {
	v, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pdb.ErrNotFound) {
			return nil, nil
		}
		return nil, wrapErr(err)
	}
	out := append([]byte(nil), v...)
	if err := closer.Close(); err != nil {
		return nil, wrapErr(err)
	}
	return out, nil
}

// readCounter reads a counter key, returning 0 when it does not exist.
func (s *Store) readCounter(key []byte) (int64, error) {
	v, err := s.get(key)
	if err != nil {
		return 0, err
	}
	if v == nil {
		return 0, nil
	}
	return deInt64(v), nil
}

// applyCounters reads the current committed value of every counter key that
// accumulated a non-zero delta (under s.mu, so nothing else can be
// concurrently writing it) and writes its new value into b exactly once, so a
// counter is never read-modify-written twice in the same batch even when a
// call touches it from more than one row.
func (s *Store) applyCounters(b *pdb.Batch, deltas map[string]int64) error {
	for k, d := range deltas {
		if d == 0 {
			continue
		}
		key := []byte(k)
		cur, err := s.readCounter(key)
		if err != nil {
			return err
		}
		if err := b.Set(key, encodeInt64(cur+d), nil); err != nil {
			return wrapErr(err)
		}
	}
	return nil
}

// commit applies b and wraps any error as store.ErrUnavailable.
func (s *Store) commit(b *pdb.Batch) error {
	if err := b.Commit(s.wo); err != nil {
		return wrapErr(err)
	}
	return nil
}

// iterate calls fn for every key in [lower, upper) in ascending order, until
// fn returns false or an error. The key/value slices fn receives are only
// valid until the next iterator step, matching pebble.Iterator's own contract.
func (s *Store) iterate(lower, upper []byte, fn func(k, v []byte) (bool, error)) error {
	it, err := s.db.NewIter(&pdb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return wrapErr(err)
	}
	defer func() { _ = it.Close() }()
	for it.First(); it.Valid(); it.Next() {
		cont, ferr := fn(it.Key(), it.Value())
		if ferr != nil {
			return ferr
		}
		if !cont {
			break
		}
	}
	if err := it.Error(); err != nil {
		return wrapErr(err)
	}
	return nil
}

// loadRow reads and decodes a row, returning (nil, nil) when it does not exist.
func (s *Store) loadRow(op store.Outpoint) (*store.Row, error) {
	v, err := s.get(rowKey(op))
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil //nolint:nilnil // "not found" is a valid, non-error result here
	}
	r, err := decodeRow(v)
	if err != nil {
		return nil, wrapErr(err)
	}
	return &r, nil
}

// loadShard reads and decodes a shard, returning (nil, nil) when it does not exist.
func (s *Store) loadShard(pool string, txid chainhash.Hash) (*store.Shard, error) {
	v, err := s.get(shardKey(pool, txid))
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil //nolint:nilnil // "not found" is a valid, non-error result here
	}
	sh, err := decodeShard(v)
	if err != nil {
		return nil, wrapErr(err)
	}
	return &sh, nil
}

// setRow writes next's row key and updates the ie/iq index entries and row
// counters implied by the prev -> next state transition. prev is nil for a
// brand new row (PutShard); next is always non-nil, since rows are never
// deleted, only transitioned between states.
func (s *Store) setRow(b *pdb.Batch, prev, next *store.Row, deltas map[string]int64) error {
	if prev != nil {
		switch prev.State {
		case store.StateReserved:
			if err := b.Delete(expiryKey(prev.Pool, prev.ExpiresAt, prev.Outpoint), nil); err != nil {
				return err
			}
		case store.StateQuarantined:
			if err := b.Delete(quarantineKey(prev.Pool, prev.QuarantinedAt, prev.Outpoint), nil); err != nil {
				return err
			}
		case store.StateAvailable, store.StateSpent, store.StateInvalid:
			// no index entry to remove
		}
		deltas[string(rowCounterKey(prev.Pool, prev.State))]--
	}
	switch next.State {
	case store.StateReserved:
		if err := b.Set(expiryKey(next.Pool, next.ExpiresAt, next.Outpoint), nil, nil); err != nil {
			return err
		}
	case store.StateQuarantined:
		if err := b.Set(quarantineKey(next.Pool, next.QuarantinedAt, next.Outpoint), nil, nil); err != nil {
			return err
		}
	case store.StateAvailable, store.StateSpent, store.StateInvalid:
		// no index entry to add
	}
	deltas[string(rowCounterKey(next.Pool, next.State))]++
	if err := b.Set(rowKey(next.Outpoint), encodeRow(next), nil); err != nil {
		return err
	}
	return nil
}

// setShard writes next's shard key and updates the is/ adoption index and
// shard counters implied by the prev -> next status transition. prev is nil
// for a brand new shard (PutShard). setShard is only used when Status itself
// changes (PutShard, UpdateShard, RetireShard); an Owner/LeaseUntil-only
// change (AcquireShard, RenewShard, ReleaseShard) writes the shard key
// directly instead, since the is/ index and counters don't depend on the
// lease.
func (s *Store) setShard(b *pdb.Batch, prev, next *store.Shard, deltas map[string]int64) error {
	if prev != nil {
		if err := b.Delete(adoptKey(prev.Pool, prev.Status, prev.CreatedAt, prev.TxID), nil); err != nil {
			return err
		}
		deltas[string(shardCounterKey(prev.Pool, prev.Status))]--
	}
	if err := b.Set(adoptKey(next.Pool, next.Status, next.CreatedAt, next.TxID), nil, nil); err != nil {
		return err
	}
	deltas[string(shardCounterKey(next.Pool, next.Status))]++
	if err := b.Set(shardKey(next.Pool, next.TxID), encodeShard(next), nil); err != nil {
		return err
	}
	return nil
}

// writeShardOnly writes next's shard record without touching the is/ index
// or counters, for a lease-only change (Owner/LeaseUntil), whose status is
// unchanged.
func (s *Store) writeShardOnly(next *store.Shard) error {
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	if err := b.Set(shardKey(next.Pool, next.TxID), encodeShard(next), nil); err != nil {
		return wrapErr(err)
	}
	return s.commit(b)
}

func lessShardVal(a, b store.Shard) bool {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.Before(b.CreatedAt)
	}
	return bytes.Compare(a.TxID[:], b.TxID[:]) < 0
}

// beUint32/deUint32 encode/decode meta/version.
func beUint32(buf []byte, v uint32) { binary.BigEndian.PutUint32(buf, v) }
func deUint32(buf []byte) uint32    { return binary.BigEndian.Uint32(buf) }

// encodeInt64/deInt64 encode/decode a per-pool counter, stored little-endian
// (the choice is arbitrary: counter keys are never range-scanned, only
// point-read, so no byte-order-implies-numeric-order property is needed).
func encodeInt64(v int64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(v)) //nolint:gosec // round-trips through deInt64
	return buf
}

func deInt64(buf []byte) int64 {
	return int64(binary.LittleEndian.Uint64(buf)) //nolint:gosec // round-trips through encodeInt64
}
