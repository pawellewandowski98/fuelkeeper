// Package memory is the in-memory reference store used by tests and benchmarks.
// One mutex guards everything; correctness over speed.
package memory

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

var errNotImplemented = errors.New("memory: not implemented")

type shardKey struct {
	pool string
	txid chainhash.Hash
}

type lease struct {
	holder  string
	expires time.Time
}

// Store is an in-memory store.Store.
type Store struct {
	now    func() time.Time
	mu     sync.Mutex
	shards map[shardKey]*store.Shard
	byTxID map[chainhash.Hash]string // txid -> owning pool; enforces global txid uniqueness
	rows   map[store.Outpoint]*store.Row
	leases map[string]lease
}

// Option configures New.
type Option func(*Store)

// WithClock injects the clock used for timestamps and lease expiry.
func WithClock(now func() time.Time) Option { return func(s *Store) { s.now = now } }

// New returns an empty store.
func New(opts ...Option) *Store {
	s := &Store{
		now:    time.Now,
		shards: map[shardKey]*store.Shard{},
		byTxID: map[chainhash.Hash]string{},
		rows:   map[store.Outpoint]*store.Row{},
		leases: map[string]lease{},
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

var _ store.Store = (*Store)(nil)

// Ping implements store.Store.
func (s *Store) Ping(context.Context) error { return nil }

// Capabilities implements store.Store.
func (s *Store) Capabilities() store.Caps {
	return store.Caps{MaxConcurrentFlushes: 1, MaxBatch: 1000, AtomicBatch: true, MultiProcess: false}
}

// Close implements store.Store; it is idempotent.
func (s *Store) Close() error { return nil }

func cloneShard(sh *store.Shard, withBEEF bool) store.Shard {
	c := *sh
	if withBEEF {
		c.BEEF = bytes.Clone(sh.BEEF)
	} else {
		c.BEEF = nil
	}
	return c
}

func cloneRow(r *store.Row) store.Row {
	c := *r
	c.LockingScript = bytes.Clone(r.LockingScript)
	c.UnlockingScript = bytes.Clone(r.UnlockingScript)
	if r.SpentHint != nil {
		h := *r.SpentHint
		c.SpentHint = &h
	}
	if r.SpentBy != nil {
		h := *r.SpentBy
		c.SpentBy = &h
	}
	return c
}

func lessShard(a, b *store.Shard) bool {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.Before(b.CreatedAt)
	}
	return bytes.Compare(a.TxID[:], b.TxID[:]) < 0
}

// ---- shards ----

// PutShard implements store.Store.
func (s *Store) PutShard(_ context.Context, shard store.Shard, rows []store.Row) error {
	if err := store.ValidatePutShard(shard, rows); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := shardKey{shard.Pool, shard.TxID}
	if ex, ok := s.shards[k]; ok {
		if ex.Size != shard.Size || ex.Denomination != shard.Denomination || ex.LockMode != shard.LockMode {
			return store.ErrShardExists
		}
		return nil
	}
	if owner, ok := s.byTxID[shard.TxID]; ok && owner != shard.Pool {
		return store.ErrShardExists
	}
	now := s.now()
	for i := range rows {
		r := cloneRow(&rows[i])
		r.State = store.StateAvailable
		r.ReservedBy = store.Token{}
		r.ReservedAt, r.ExpiresAt, r.QuarantinedAt = time.Time{}, time.Time{}, time.Time{}
		r.SpentHint, r.SpentBy = nil, nil
		r.UpdatedAt = now
		if _, exists := s.rows[r.Outpoint]; !exists {
			s.rows[r.Outpoint] = &r
		}
	}
	sh := cloneShard(&shard, true)
	sh.Owner, sh.LeaseUntil, sh.Version = "", time.Time{}, 1
	sh.CreatedAt, sh.UpdatedAt = now, now
	s.shards[k] = &sh
	s.byTxID[shard.TxID] = shard.Pool
	return nil
}

// GetShard implements store.Store.
func (s *Store) GetShard(_ context.Context, pool string, txid chainhash.Hash) (*store.Shard, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sh, ok := s.shards[shardKey{pool, txid}]
	if !ok {
		return nil, store.ErrNotFound
	}
	c := cloneShard(sh, true)
	return &c, nil
}

// UpdateShard implements store.Store.
func (s *Store) UpdateShard(_ context.Context, pool string, txid chainhash.Hash, status store.ShardStatus, beef []byte) error {
	if err := store.ValidatePool(pool); err != nil {
		return err
	}
	if err := store.ValidateUpdateStatus(status); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sh, ok := s.shards[shardKey{pool, txid}]
	if !ok {
		return store.ErrNotFound
	}
	if sh.Status.Terminal() {
		return store.ErrTerminalShard
	}
	sh.Status = status
	if beef != nil {
		sh.BEEF = bytes.Clone(beef)
	}
	sh.Version++
	sh.UpdatedAt = s.now()
	return nil
}

// RetireShard implements store.Store.
func (s *Store) RetireShard(_ context.Context, pool string, txid chainhash.Hash) (int, error) {
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sh, ok := s.shards[shardKey{pool, txid}]
	if !ok {
		return 0, store.ErrNotFound
	}
	if sh.Status == store.ShardInvalid {
		return 0, nil
	}
	now := s.now()
	sh.Status = store.ShardInvalid
	sh.Owner, sh.LeaseUntil = "", time.Time{}
	sh.Version++
	sh.UpdatedAt = now
	n := 0
	for _, r := range s.rows {
		if r.Pool == pool && r.TxID == txid && !r.State.Terminal() {
			r.State = store.StateInvalid
			r.UpdatedAt = now
			n++
		}
	}
	return n, nil
}

// AcquireShard implements store.Store.
func (s *Store) AcquireShard(_ context.Context, pool, holder string, now, until time.Time, statuses []store.ShardStatus, exclude []chainhash.Hash) (*store.Shard, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateHolder(holder); err != nil {
		return nil, err
	}
	if err := store.ValidateShardStatuses(statuses); err != nil {
		return nil, err
	}
	if err := store.ValidateAcquireUntil(until, now); err != nil {
		return nil, err
	}
	want := map[store.ShardStatus]bool{}
	for _, st := range statuses {
		want[st] = true
	}
	skip := map[chainhash.Hash]bool{}
	for _, h := range exclude {
		skip[h] = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var best *store.Shard
	for k, sh := range s.shards {
		if k.pool != pool || !want[sh.Status] || skip[sh.TxID] {
			continue
		}
		if sh.Owner != "" && sh.LeaseUntil.After(now) {
			continue
		}
		if best == nil || lessShard(sh, best) {
			best = sh
		}
	}
	if best == nil {
		return nil, nil //nolint:nilnil // contract: (nil, nil) means nothing adoptable
	}
	best.Owner = holder
	best.LeaseUntil = until
	best.UpdatedAt = s.now()
	c := cloneShard(best, true)
	return &c, nil
}

// RenewShard implements store.Store.
func (s *Store) RenewShard(_ context.Context, pool string, txid chainhash.Hash, holder string, until time.Time) (store.ShardHead, error) {
	if err := store.ValidatePool(pool); err != nil {
		return store.ShardHead{}, err
	}
	if err := store.ValidateHolder(holder); err != nil {
		return store.ShardHead{}, err
	}
	if err := store.ValidateRenewUntil(until); err != nil {
		return store.ShardHead{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sh, ok := s.shards[shardKey{pool, txid}]
	if !ok {
		return store.ShardHead{}, store.ErrNotFound
	}
	if sh.Owner != holder {
		return store.ShardHead{}, store.ErrNotOwner
	}
	sh.LeaseUntil = until
	return store.ShardHead{Status: sh.Status, Version: sh.Version}, nil
}

// ReleaseShard implements store.Store.
func (s *Store) ReleaseShard(_ context.Context, pool string, txid chainhash.Hash, holder string) error {
	if err := store.ValidatePool(pool); err != nil {
		return err
	}
	if err := store.ValidateHolder(holder); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sh, ok := s.shards[shardKey{pool, txid}]
	if !ok {
		return store.ErrNotFound
	}
	if sh.Owner == holder {
		sh.Owner, sh.LeaseUntil = "", time.Time{}
		sh.UpdatedAt = s.now()
	}
	return nil
}

// ListShards implements store.Store.
func (s *Store) ListShards(_ context.Context, pool string, f store.ShardFilter) ([]store.Shard, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateListShardsFilter(f); err != nil {
		return nil, err
	}
	want := map[store.ShardStatus]bool{}
	for _, st := range f.Statuses {
		want[st] = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.Shard
	for k, sh := range s.shards {
		if k.pool != pool {
			continue
		}
		if len(want) > 0 && !want[sh.Status] {
			continue
		}
		if f.Owner != nil && sh.Owner != *f.Owner {
			continue
		}
		out = append(out, sh)
	}
	sort.Slice(out, func(i, j int) bool { return lessShard(out[i], out[j]) })
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	res := make([]store.Shard, len(out))
	for i, sh := range out {
		res[i] = cloneShard(sh, false)
	}
	return res, nil
}

// ---- rows (implemented in Task 5) ----

// ListAvailable is implemented in Task 5.
func (s *Store) ListAvailable(_ context.Context, pool string, _ chainhash.Hash, _ uint32, _ int) ([]store.Row, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	return nil, errNotImplemented
}

// GetRows is implemented in Task 5.
func (s *Store) GetRows(_ context.Context, pool string, _ []store.Outpoint) ([]store.Row, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	return nil, errNotImplemented
}

// Reserve is implemented in Task 5.
func (s *Store) Reserve(_ context.Context, pool string, _ []store.ReserveOp) ([]store.ReserveOp, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	return nil, errNotImplemented
}

// Release is implemented in Task 5.
func (s *Store) Release(_ context.Context, pool string, _ store.Token, _ []store.Outpoint) (int, error) {
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	return 0, errNotImplemented
}

// Spend is implemented in Task 5.
func (s *Store) Spend(_ context.Context, pool string, _ store.Token, _ []store.Outpoint, _ chainhash.Hash, _ bool) (int, error) {
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	return 0, errNotImplemented
}

// Quarantine is implemented in Task 5.
func (s *Store) Quarantine(_ context.Context, pool string, _ time.Time, _ int) (int, error) {
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	return 0, errNotImplemented
}

// ListQuarantined is implemented in Task 5.
func (s *Store) ListQuarantined(_ context.Context, pool string, _ time.Time, _ int) ([]store.Row, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	return nil, errNotImplemented
}

// Resolve is implemented in Task 5.
func (s *Store) Resolve(_ context.Context, pool string, _ []store.Outpoint, _ store.State, _ *chainhash.Hash) (int, error) {
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	return 0, errNotImplemented
}

// Stats is implemented in Task 5.
func (s *Store) Stats(_ context.Context, pool string) (store.Stats, error) {
	if err := store.ValidatePool(pool); err != nil {
		return store.Stats{}, err
	}
	return store.Stats{}, errNotImplemented
}

// ---- leases ----

// TryAcquireOrRenew implements store.Leaser.
func (s *Store) TryAcquireOrRenew(_ context.Context, name, holder string, ttl time.Duration) (time.Time, error) {
	if err := store.ValidateLease(name, holder, ttl); err != nil {
		return time.Time{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	l, ok := s.leases[name]
	if ok && l.holder != holder && l.expires.After(now) {
		return time.Time{}, nil
	}
	exp := now.Add(ttl)
	s.leases[name] = lease{holder: holder, expires: exp}
	return exp, nil
}

// ReleaseLease implements store.Leaser.
func (s *Store) ReleaseLease(_ context.Context, name, holder string) error {
	if err := store.ValidateLeaseName(name); err != nil {
		return err
	}
	if err := store.ValidateHolder(holder); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if l, ok := s.leases[name]; ok && l.holder == holder {
		delete(s.leases, name)
	}
	return nil
}
