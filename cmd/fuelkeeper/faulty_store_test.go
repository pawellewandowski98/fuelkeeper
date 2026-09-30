package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// errFaultyStoreClosed is returned by every faultyStore method once Close has
// run, proving that nothing -- a lingering job tick, a delayed mock-treasury
// timer, a pool goroutine that raced shutdown -- reaches the real store after
// runServeConfig believes it is done with it.
var errFaultyStoreClosed = errors.New("faultyStore: used after Close")

// faultyStore wraps a real store.Store for tests: armReserveHold pauses a
// single in-flight Reserve call (simulating a slow flush racing with
// shutdown), and every method -- not just Reserve -- fails once Close has
// run.
type faultyStore struct {
	store.Store

	mu      sync.Mutex
	closed  bool
	hold    chan struct{} // closed by the test to release the paused call
	holding chan struct{} // closed once a call is actually paused
}

func newFaultyStore(inner store.Store) *faultyStore {
	return &faultyStore{Store: inner}
}

// armReserveHold arms a one-shot pause on the next Reserve call: that call
// blocks inside Reserve until release is called. The returned channel closes
// once the call is actually paused, so a test can synchronize on that
// instead of guessing with a sleep.
func (s *faultyStore) armReserveHold() (holding <-chan struct{}, release func()) {
	s.mu.Lock()
	h := make(chan struct{})
	hd := make(chan struct{})
	s.hold, s.holding = h, hd
	s.mu.Unlock()
	var once sync.Once
	return hd, func() { once.Do(func() { close(h) }) }
}

func (s *faultyStore) closedErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errFaultyStoreClosed
	}
	return nil
}

func (s *faultyStore) Ping(ctx context.Context) error {
	if err := s.closedErr(); err != nil {
		return err
	}
	return s.Store.Ping(ctx)
}

func (s *faultyStore) PutShard(ctx context.Context, shard store.Shard, rows []store.Row) error {
	if err := s.closedErr(); err != nil {
		return err
	}
	return s.Store.PutShard(ctx, shard, rows)
}

func (s *faultyStore) GetShard(ctx context.Context, pool string, txid chainhash.Hash) (*store.Shard, error) {
	if err := s.closedErr(); err != nil {
		return nil, err
	}
	return s.Store.GetShard(ctx, pool, txid)
}

func (s *faultyStore) UpdateShard(ctx context.Context, pool string, txid chainhash.Hash, status store.ShardStatus, beef []byte) error {
	if err := s.closedErr(); err != nil {
		return err
	}
	return s.Store.UpdateShard(ctx, pool, txid, status, beef)
}

func (s *faultyStore) RetireShard(ctx context.Context, pool string, txid chainhash.Hash) (int, error) {
	if err := s.closedErr(); err != nil {
		return 0, err
	}
	return s.Store.RetireShard(ctx, pool, txid)
}

func (s *faultyStore) AcquireShard(ctx context.Context, pool, holder string, now, until time.Time, statuses []store.ShardStatus, exclude []chainhash.Hash) (*store.Shard, error) {
	if err := s.closedErr(); err != nil {
		return nil, err
	}
	return s.Store.AcquireShard(ctx, pool, holder, now, until, statuses, exclude)
}

func (s *faultyStore) RenewShard(ctx context.Context, pool string, txid chainhash.Hash, holder string, until time.Time) (store.ShardHead, error) {
	if err := s.closedErr(); err != nil {
		return store.ShardHead{}, err
	}
	return s.Store.RenewShard(ctx, pool, txid, holder, until)
}

func (s *faultyStore) ReleaseShard(ctx context.Context, pool string, txid chainhash.Hash, holder string) error {
	if err := s.closedErr(); err != nil {
		return err
	}
	return s.Store.ReleaseShard(ctx, pool, txid, holder)
}

func (s *faultyStore) ListShards(ctx context.Context, pool string, f store.ShardFilter) ([]store.Shard, error) {
	if err := s.closedErr(); err != nil {
		return nil, err
	}
	return s.Store.ListShards(ctx, pool, f)
}

func (s *faultyStore) ListAvailable(ctx context.Context, pool string, txid chainhash.Hash, fromVout uint32, limit int) ([]store.Row, error) {
	if err := s.closedErr(); err != nil {
		return nil, err
	}
	return s.Store.ListAvailable(ctx, pool, txid, fromVout, limit)
}

func (s *faultyStore) GetRows(ctx context.Context, pool string, ops []store.Outpoint) ([]store.Row, error) {
	if err := s.closedErr(); err != nil {
		return nil, err
	}
	return s.Store.GetRows(ctx, pool, ops)
}

// Reserve is the hot path these tests care about: it observes a one-shot
// hold (see armReserveHold), pausing there -- after the closed check has
// already passed -- before checking closedErr again and delegating. A test
// can cancel the server's context while this call is paused and confirm the
// eventual Reserve still completes successfully instead of racing a
// premature pool shutdown.
func (s *faultyStore) Reserve(ctx context.Context, pool string, ops []store.ReserveOp) ([]store.ReserveOp, error) {
	if err := s.closedErr(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	hold, holding := s.hold, s.holding
	s.hold, s.holding = nil, nil
	s.mu.Unlock()
	if hold != nil {
		close(holding)
		<-hold
	}
	if err := s.closedErr(); err != nil {
		return nil, err
	}
	return s.Store.Reserve(ctx, pool, ops)
}

func (s *faultyStore) Release(ctx context.Context, pool string, token store.Token, ops []store.Outpoint) (int, error) {
	if err := s.closedErr(); err != nil {
		return 0, err
	}
	return s.Store.Release(ctx, pool, token, ops)
}

func (s *faultyStore) Spend(ctx context.Context, pool string, token store.Token, ops []store.Outpoint, txid chainhash.Hash, terminal bool) (int, error) {
	if err := s.closedErr(); err != nil {
		return 0, err
	}
	return s.Store.Spend(ctx, pool, token, ops, txid, terminal)
}

func (s *faultyStore) Quarantine(ctx context.Context, pool string, before time.Time, limit int) (int, error) {
	if err := s.closedErr(); err != nil {
		return 0, err
	}
	return s.Store.Quarantine(ctx, pool, before, limit)
}

func (s *faultyStore) ListQuarantined(ctx context.Context, pool string, before time.Time, after *store.RowCursor, limit int) ([]store.Row, error) {
	if err := s.closedErr(); err != nil {
		return nil, err
	}
	return s.Store.ListQuarantined(ctx, pool, before, after, limit)
}

func (s *faultyStore) Resolve(ctx context.Context, pool string, ops []store.Outpoint, to store.State, spentBy *chainhash.Hash) (int, error) {
	if err := s.closedErr(); err != nil {
		return 0, err
	}
	return s.Store.Resolve(ctx, pool, ops, to, spentBy)
}

func (s *faultyStore) Stats(ctx context.Context, pool string) (store.Stats, error) {
	if err := s.closedErr(); err != nil {
		return store.Stats{}, err
	}
	return s.Store.Stats(ctx, pool)
}

func (s *faultyStore) TryAcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (time.Time, error) {
	if err := s.closedErr(); err != nil {
		return time.Time{}, err
	}
	return s.Store.TryAcquireOrRenew(ctx, name, holder, ttl)
}

func (s *faultyStore) ReleaseLease(ctx context.Context, name, holder string) error {
	if err := s.closedErr(); err != nil {
		return err
	}
	return s.Store.ReleaseLease(ctx, name, holder)
}

// Close marks the store closed -- failing every subsequent call, including a
// second Close -- and releases the real store. Close itself stays idempotent
// (as store.Store documents), so it is never guarded by closedErr.
func (s *faultyStore) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return s.Store.Close()
}

var _ store.Store = (*faultyStore)(nil)
