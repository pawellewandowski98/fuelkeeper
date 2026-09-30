package pebble

import (
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
	if s.closed.Load() {
		return errClosed
	}

	existing, err := s.loadShard(shard.Pool, shard.TxID)
	if err != nil {
		return err
	}
	if existing != nil {
		if existing.Size != shard.Size || existing.Denomination != shard.Denomination || existing.LockMode != shard.LockMode {
			return store.ErrShardExists
		}
		return nil
	}
	ownerPool, err := s.get(txidPoolKey(shard.TxID))
	if err != nil {
		return err
	}
	if ownerPool != nil && string(ownerPool) != shard.Pool {
		return store.ErrShardExists
	}

	now := s.now().Truncate(time.Microsecond)
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	deltas := map[string]int64{}

	for i := range rows {
		ex, lerr := s.loadRow(rows[i].Outpoint)
		if lerr != nil {
			return lerr
		}
		if ex != nil {
			continue // already exists (e.g. a partial prior write); PutShard never touches it
		}
		r := rows[i]
		r.State = store.StateAvailable
		r.ReservedBy = store.Token{}
		r.ReservedAt, r.ExpiresAt, r.QuarantinedAt = time.Time{}, time.Time{}, time.Time{}
		r.SpentHint, r.SpentBy = nil, nil
		r.UpdatedAt = now
		if err := s.setRow(b, nil, &r, deltas); err != nil {
			return wrapErr(err)
		}
	}

	sh := shard
	sh.Owner, sh.LeaseUntil, sh.Version = "", time.Time{}, 1
	sh.CreatedAt, sh.UpdatedAt = now, now
	if err := s.setShard(b, nil, &sh, deltas); err != nil {
		return wrapErr(err)
	}
	if err := b.Set(txidPoolKey(shard.TxID), []byte(shard.Pool), nil); err != nil {
		return wrapErr(err)
	}
	if err := s.applyCounters(b, deltas); err != nil {
		return err
	}
	return s.commit(b)
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
	if s.closed.Load() {
		return nil, errClosed
	}
	sh, err := s.loadShard(pool, txid)
	if err != nil {
		return nil, err
	}
	if sh == nil {
		return nil, store.ErrNotFound
	}
	return sh, nil
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
	if s.closed.Load() {
		return errClosed
	}
	sh, err := s.loadShard(pool, txid)
	if err != nil {
		return err
	}
	if sh == nil {
		return store.ErrNotFound
	}
	if sh.Status.Terminal() {
		return store.ErrTerminalShard
	}
	prev := *sh
	next := *sh
	next.Status = status
	if beef != nil {
		next.BEEF = beef
	}
	next.Version++
	next.UpdatedAt = s.now().Truncate(time.Microsecond)

	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	deltas := map[string]int64{}
	if err := s.setShard(b, &prev, &next, deltas); err != nil {
		return wrapErr(err)
	}
	if err := s.applyCounters(b, deltas); err != nil {
		return err
	}
	return s.commit(b)
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
	if s.closed.Load() {
		return 0, errClosed
	}
	sh, err := s.loadShard(pool, txid)
	if err != nil {
		return 0, err
	}
	if sh == nil {
		return 0, store.ErrNotFound
	}
	if sh.Status == store.ShardInvalid {
		return 0, nil
	}

	now := s.now().Truncate(time.Microsecond)
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	deltas := map[string]int64{}

	prevShard := *sh
	nextShard := *sh
	nextShard.Status = store.ShardInvalid
	nextShard.Owner, nextShard.LeaseUntil = "", time.Time{}
	nextShard.Version++
	nextShard.UpdatedAt = now
	if err := s.setShard(b, &prevShard, &nextShard, deltas); err != nil {
		return 0, wrapErr(err)
	}

	lower := rowPrefix(txid)
	upper := prefixEnd(lower)
	var ops []store.Outpoint
	if err := s.iterate(lower, upper, func(k, _ []byte) (bool, error) {
		var op store.Outpoint
		op.TxID = txid
		op.Vout = deUint32(k[len(lower):])
		ops = append(ops, op)
		return true, nil
	}); err != nil {
		return 0, err
	}

	n := 0
	for _, op := range ops {
		r, lerr := s.loadRow(op)
		if lerr != nil {
			return 0, lerr
		}
		if r == nil || r.Pool != pool || r.State.Terminal() {
			continue
		}
		prevRow := *r
		nextRow := *r
		nextRow.State = store.StateInvalid
		nextRow.UpdatedAt = now
		if err := s.setRow(b, &prevRow, &nextRow, deltas); err != nil {
			return 0, wrapErr(err)
		}
		n++
	}
	if err := s.applyCounters(b, deltas); err != nil {
		return 0, err
	}
	if err := s.commit(b); err != nil {
		return 0, err
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
	now = now.Truncate(time.Microsecond)
	until = until.Truncate(time.Microsecond)
	skip := map[chainhash.Hash]bool{}
	for _, h := range exclude {
		skip[h] = true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return nil, errClosed
	}

	var best *store.Shard
	for _, status := range statuses {
		prefix := adoptPrefix(pool, status)
		upper := prefixEnd(prefix)
		err := s.iterate(prefix, upper, func(k, _ []byte) (bool, error) {
			var txid chainhash.Hash
			copy(txid[:], k[len(prefix)+8:])
			if skip[txid] {
				return true, nil
			}
			sh, lerr := s.loadShard(pool, txid)
			if lerr != nil {
				return false, lerr
			}
			if sh == nil {
				return true, nil // stale index entry; defensively skip
			}
			if sh.Owner != "" && sh.LeaseUntil.After(now) {
				return true, nil
			}
			if best == nil || lessShardVal(*sh, *best) {
				best = sh
			}
			return false, nil // the index is sorted by (created_at, txid); the first eligible entry is this status's earliest
		})
		if err != nil {
			return nil, wrapErr(err)
		}
	}
	if best == nil {
		return nil, nil //nolint:nilnil // contract: (nil, nil) means nothing adoptable
	}

	next := *best
	next.Owner = holder
	next.LeaseUntil = until
	next.UpdatedAt = s.now().Truncate(time.Microsecond)
	if err := s.writeShardOnly(&next); err != nil {
		return nil, err
	}
	return &next, nil
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
	if s.closed.Load() {
		return store.ShardHead{}, errClosed
	}
	sh, err := s.loadShard(pool, txid)
	if err != nil {
		return store.ShardHead{}, err
	}
	if sh == nil {
		return store.ShardHead{}, store.ErrNotFound
	}
	if sh.Owner != holder {
		return store.ShardHead{}, store.ErrNotOwner
	}
	next := *sh
	next.LeaseUntil = until.Truncate(time.Microsecond)
	if err := s.writeShardOnly(&next); err != nil {
		return store.ShardHead{}, err
	}
	return store.ShardHead{Status: next.Status, Version: next.Version}, nil
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
	if s.closed.Load() {
		return errClosed
	}
	sh, err := s.loadShard(pool, txid)
	if err != nil {
		return err
	}
	if sh == nil {
		return store.ErrNotFound
	}
	if sh.Owner != holder {
		return nil
	}
	next := *sh
	next.Owner, next.LeaseUntil = "", time.Time{}
	next.UpdatedAt = s.now().Truncate(time.Microsecond)
	return s.writeShardOnly(&next)
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return nil, errClosed
	}
	want := map[store.ShardStatus]bool{}
	for _, st := range f.Statuses {
		want[st] = true
	}
	prefix := shardPrefix(pool)
	upper := prefixEnd(prefix)
	var out []store.Shard
	err := s.iterate(prefix, upper, func(_, v []byte) (bool, error) {
		sh, derr := decodeShard(v)
		if derr != nil {
			return false, derr
		}
		if len(want) > 0 && !want[sh.Status] {
			return true, nil
		}
		if f.Owner != nil && sh.Owner != *f.Owner {
			return true, nil
		}
		sh.BEEF = nil
		out = append(out, sh)
		return true, nil
	})
	if err != nil {
		return nil, wrapErr(err)
	}
	sort.Slice(out, func(i, j int) bool { return lessShardVal(out[i], out[j]) })
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}
