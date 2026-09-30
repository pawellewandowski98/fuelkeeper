package memory

import (
	"bytes"
	"context"
	"sort"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// PutShard implements store.Store.
func (s *Store) PutShard(ctx context.Context, shard store.Shard, rows []store.Row) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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
	now := s.clockNow()
	for i := range rows {
		r := cloneRow(&rows[i])
		r.State = store.StateAvailable
		r.ReservedBy = store.Token{}
		r.ReservedAt, r.ExpiresAt, r.QuarantinedAt = time.Time{}, time.Time{}, time.Time{}
		r.SpentHint, r.SpentBy = nil, nil
		r.UpdatedAt = now
		if _, exists := s.rows[r.Outpoint]; !exists {
			s.rows[r.Outpoint] = &r
			s.rowsByShard[shard.TxID] = append(s.rowsByShard[shard.TxID], r.Outpoint)
			s.addCount(shard.Pool, store.StateAvailable, 1)
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
func (s *Store) GetShard(ctx context.Context, pool string, txid chainhash.Hash) (*store.Shard, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
func (s *Store) UpdateShard(ctx context.Context, pool string, txid chainhash.Hash, status store.ShardStatus, beef []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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
	sh.UpdatedAt = s.clockNow()
	return nil
}

// RetireShard implements store.Store.
func (s *Store) RetireShard(ctx context.Context, pool string, txid chainhash.Hash) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
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
	now := s.clockNow()
	sh.Status = store.ShardInvalid
	sh.Owner, sh.LeaseUntil = "", time.Time{}
	sh.Version++
	sh.UpdatedAt = now
	n := 0
	for _, op := range s.rowsByShard[txid] {
		r, ok := s.rows[op]
		if !ok || r.Pool != pool || r.State.Terminal() {
			continue
		}
		old := r.State
		r.State = store.StateInvalid
		r.UpdatedAt = now
		s.moveCount(pool, old, store.StateInvalid)
		n++
	}
	return n, nil
}

// AcquireShard implements store.Store.
func (s *Store) AcquireShard(ctx context.Context, pool, holder string, now, until time.Time, statuses []store.ShardStatus, exclude []chainhash.Hash) (*store.Shard, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
	// Truncate to microsecond precision once at entry (store.go, "Clocks"): every
	// comparison and stored value below must agree with what ValidateAcquireUntil
	// just validated at truncated precision.
	now = now.Truncate(time.Microsecond)
	until = until.Truncate(time.Microsecond)
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
	best.UpdatedAt = s.clockNow()
	c := cloneShard(best, true)
	return &c, nil
}

// RenewShard implements store.Store.
func (s *Store) RenewShard(ctx context.Context, pool string, txid chainhash.Hash, holder string, until time.Time) (store.ShardHead, error) {
	if err := ctx.Err(); err != nil {
		return store.ShardHead{}, err
	}
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
	sh.LeaseUntil = until.Truncate(time.Microsecond)
	return store.ShardHead{Status: sh.Status, Version: sh.Version}, nil
}

// ReleaseShard implements store.Store.
func (s *Store) ReleaseShard(ctx context.Context, pool string, txid chainhash.Hash, holder string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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
		sh.UpdatedAt = s.clockNow()
	}
	return nil
}

// ListShards implements store.Store.
func (s *Store) ListShards(ctx context.Context, pool string, f store.ShardFilter) ([]store.Shard, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
