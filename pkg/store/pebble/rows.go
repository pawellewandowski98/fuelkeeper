package pebble

import (
	"context"
	"time"

	pdb "github.com/cockroachdb/pebble"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// clearReservation zeroes the five provenance fields Release and Resolve(to
// available) clear (see store.go, "Provenance").
func clearReservation(r *store.Row) {
	r.ReservedBy = store.Token{}
	r.ReservedAt, r.ExpiresAt, r.QuarantinedAt = time.Time{}, time.Time{}, time.Time{}
	r.SpentHint = nil
}

// matchesNothingBefore reports whether before -- Quarantine's and
// ListQuarantined's comparison bound, not stored data (store.go, "Clocks") --
// can only ever match nothing: the zero time, the exact Unix epoch instant,
// or any earlier time. Every stored expires_at/quarantined_at is strictly
// after the epoch (store.ValidateTime enforces this on write), so none of
// these can ever be less than a real stored value.
func matchesNothingBefore(before time.Time) bool {
	return !before.After(unixEpoch)
}

// ListAvailable implements store.Store.
func (s *Store) ListAvailable(ctx context.Context, pool string, txid chainhash.Hash, fromVout uint32, limit int) (_ []store.Row, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateLimit(limit); err != nil {
		return nil, err
	}
	g := s.lock()
	defer g.unlock(&err)
	if s.closed.Load() {
		return nil, errClosed
	}
	lower := rowKey(store.Outpoint{TxID: txid, Vout: fromVout})
	upper := prefixEnd(rowPrefix(txid))
	out := make([]store.Row, 0, limit)
	err = s.iterate(lower, upper, func(_, v []byte) (bool, error) {
		r, derr := decodeRow(v)
		if derr != nil {
			return false, derr
		}
		if r.Pool != pool || r.State != store.StateAvailable {
			return true, nil
		}
		out = append(out, r)
		return len(out) < limit, nil
	})
	if err != nil {
		return nil, wrapErr(err)
	}
	return out, nil
}

// GetRows implements store.Store.
func (s *Store) GetRows(ctx context.Context, pool string, ops []store.Outpoint) (_ []store.Row, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateOutpoints(ops, maxBatch); err != nil {
		return nil, err
	}
	g := s.lock()
	defer g.unlock(&err)
	if s.closed.Load() {
		return nil, errClosed
	}
	out := make([]store.Row, 0, len(ops))
	for _, op := range ops {
		r, err := s.loadRow(op)
		if err != nil {
			return nil, err
		}
		if r != nil && r.Pool == pool {
			out = append(out, *r)
		}
	}
	return out, nil
}

// Reserve implements store.Store.
func (s *Store) Reserve(ctx context.Context, pool string, ops []store.ReserveOp) (_ []store.ReserveOp, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateReserveOps(ops, maxBatch); err != nil {
		return nil, err
	}
	g := s.lock()
	defer g.unlock(&err)
	if s.closed.Load() {
		return nil, errClosed
	}
	now := s.now().Truncate(time.Microsecond)
	sorted := store.SortReserveOps(ops)

	// Above reserveSeekThreshold ops, resolving every row via its own
	// DB.Get point lookup (as below) does one B-tree/SSTable descent per op;
	// since sorted's outpoints are already in the same ascending order as
	// their row keys (rowKey's encoding preserves LessOutpoint's ordering),
	// a single forward iterator walking sorted once (loadRowsSorted) instead
	// resolves the whole batch in one pass. Below the threshold, a handful of
	// point Gets skips the iterator's setup cost and is simply faster; see
	// BenchmarkReserveRowLookup for the crossover.
	var preloaded map[store.Outpoint]*store.Row
	if len(sorted) > reserveSeekThreshold {
		outs := make([]store.Outpoint, len(sorted))
		for i, op := range sorted {
			outs[i] = op.Outpoint
		}
		var err error
		preloaded, err = s.loadRowsSorted(outs)
		if err != nil {
			return nil, err
		}
	}

	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	deltas := map[string]int64{}
	applied := make([]store.ReserveOp, 0, len(sorted))
	for _, op := range sorted {
		var r *store.Row
		if preloaded != nil {
			r = preloaded[op.Outpoint] // nil for a missing row, exactly like loadRow's nil-on-missing result
		} else {
			var err error
			r, err = s.loadRow(op.Outpoint)
			if err != nil {
				return nil, err
			}
		}
		if r == nil || r.Pool != pool {
			continue
		}
		switch {
		case r.State == store.StateAvailable:
			prev := *r
			next := *r
			next.State = store.StateReserved
			next.ReservedBy = op.Token
			next.ReservedAt = now
			next.ExpiresAt = op.ExpiresAt.Truncate(time.Microsecond)
			next.UpdatedAt = now
			if err := s.setRow(b, &prev, &next, deltas); err != nil {
				return nil, wrapErr(err)
			}
			op.ExpiresAt = next.ExpiresAt // report the stored (truncated) expiry
			applied = append(applied, op)
		case r.State == store.StateReserved && r.ReservedBy == op.Token:
			op.ExpiresAt = r.ExpiresAt // replay: report the stored expiry, do not touch UpdatedAt
			applied = append(applied, op)
		}
	}
	if err := s.applyCounters(b, deltas); err != nil {
		return nil, err
	}
	if err := g.commit(b); err != nil {
		return nil, err
	}
	return applied, nil
}

// mutateReserved applies mutate to every op's row currently reserved(token),
// stamping UpdatedAt and writing exactly one batch for the whole call.
func (s *Store) mutateReserved(pool string, token store.Token, ops []store.Outpoint, mutate func(next *store.Row, now time.Time)) (_ int, err error) {
	if err := store.ValidateOutpoints(ops, maxBatch); err != nil {
		return 0, err
	}
	if token.IsZero() {
		return 0, store.ErrInvalidArgument
	}
	g := s.lock()
	defer g.unlock(&err)
	if s.closed.Load() {
		return 0, errClosed
	}
	now := s.now().Truncate(time.Microsecond)
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	deltas := map[string]int64{}
	n, err := s.mutateReservedInto(b, deltas, nil, pool, token, ops, now, mutate)
	if err != nil {
		return 0, err
	}
	if err := s.applyCounters(b, deltas); err != nil {
		return 0, err
	}
	if err := g.commit(b); err != nil {
		return 0, err
	}
	return n, nil
}

// mutateReservedInto is mutateReserved's body: it stages every change in b
// and deltas (s.mu must be held) and returns how many rows it moved. When
// staged is non-nil it maps outpoints already rewritten earlier in the same,
// not yet applied, batch to their staged rows -- b is not readable through
// s.db until it is applied -- and is updated with every row this call
// rewrites, so a later call staging into the same batch sees them.
func (s *Store) mutateReservedInto(b *pdb.Batch, deltas map[string]int64, staged map[store.Outpoint]*store.Row, pool string, token store.Token, ops []store.Outpoint, now time.Time, mutate func(next *store.Row, now time.Time)) (int, error) {
	n := 0
	for _, op := range store.SortOutpoints(ops) {
		r, ok := staged[op]
		if !ok {
			var err error
			if r, err = s.loadRow(op); err != nil {
				return 0, err
			}
		}
		if r == nil || r.Pool != pool || r.State != store.StateReserved || r.ReservedBy != token {
			continue
		}
		prev := *r
		next := *r
		mutate(&next, now)
		next.UpdatedAt = now
		if err := s.setRow(b, &prev, &next, deltas); err != nil {
			return 0, wrapErr(err)
		}
		if staged != nil {
			staged[op] = &next
		}
		n++
	}
	return n, nil
}

// Release implements store.Store.
func (s *Store) Release(ctx context.Context, pool string, token store.Token, ops []store.Outpoint) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	return s.mutateReserved(pool, token, ops, func(next *store.Row, _ time.Time) {
		next.State = store.StateAvailable
		clearReservation(next)
	})
}

// Spend implements store.Store.
func (s *Store) Spend(ctx context.Context, pool string, token store.Token, ops []store.Outpoint, txid chainhash.Hash, terminal bool) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	if err := store.ValidateSpend(txid, terminal); err != nil {
		return 0, err
	}
	return s.mutateReserved(pool, token, ops, spendMutation(txid, terminal))
}

// spendMutation is Spend's row mutation, shared with SpendBatch.
func spendMutation(txid chainhash.Hash, terminal bool) func(next *store.Row, now time.Time) {
	return func(next *store.Row, now time.Time) {
		if terminal {
			h := txid
			next.State = store.StateSpent
			next.SpentBy = &h
			return
		}
		next.State = store.StateQuarantined
		next.QuarantinedAt = now
		if txid != (chainhash.Hash{}) {
			h := txid
			next.SpentHint = &h
		}
	}
}

// SpendBatch implements store.SpendBatcher: every request is staged, in
// slice order, into one Pebble batch under one hold of s.mu and committed
// once through the guard (one WAL append, one fsync shared with any other
// concurrent commit). A request reads the rows earlier requests in the same
// batch already rewrote from the staged overlay, so it sees exactly what
// sequential Spend calls would have left behind.
func (s *Store) SpendBatch(ctx context.Context, pool string, reqs []store.SpendRequest) (_ []int, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateSpendBatch(reqs, maxBatch); err != nil {
		return nil, err
	}
	g := s.lock()
	defer g.unlock(&err)
	if s.closed.Load() {
		return nil, errClosed
	}
	now := s.now().Truncate(time.Microsecond)
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	deltas := map[string]int64{}
	staged := map[store.Outpoint]*store.Row{}
	counts := make([]int, len(reqs))
	for i, r := range reqs {
		n, err := s.mutateReservedInto(b, deltas, staged, pool, r.Token, r.Ops, now, spendMutation(r.TxID, r.Terminal))
		if err != nil {
			return nil, err
		}
		counts[i] = n
	}
	if err := s.applyCounters(b, deltas); err != nil {
		return nil, err
	}
	if err := g.commit(b); err != nil {
		return nil, err
	}
	return counts, nil
}

// Quarantine implements store.Store. before is a comparison bound, not
// stored data (store.go, "Clocks"): the zero time, the exact Unix epoch
// instant, or an earlier time all match nothing, since every stored
// expires_at is strictly after the epoch, so this returns 0, nil for them
// without querying rather than ErrInvalidArgument (matchesNothingBefore).
// Checking the time.Time value itself here, before it is ever encoded, also
// sidesteps a trap in the key encoding below: us() maps a time to an
// *unsigned* microsecond count, so a pre-epoch before's negative Unix time
// would otherwise wrap around to a huge value and match everything instead
// of nothing.
func (s *Store) Quarantine(ctx context.Context, pool string, before time.Time, limit int) (_ int, err error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	if err := store.ValidateLimit(limit); err != nil {
		return 0, err
	}
	if matchesNothingBefore(before) {
		return 0, nil
	}
	before = before.Truncate(time.Microsecond)
	g := s.lock()
	defer g.unlock(&err)
	if s.closed.Load() {
		return 0, errClosed
	}

	prefix := expiryPrefix(pool)
	upper := upperBoundBefore(prefix, before)
	var due []store.Outpoint
	if err := s.iterate(prefix, upper, func(k, _ []byte) (bool, error) {
		tail := k[len(prefix)+8:]
		var op store.Outpoint
		copy(op.TxID[:], tail[:32])
		op.Vout = deUint32(tail[32:36])
		due = append(due, op)
		return len(due) < limit, nil
	}); err != nil {
		return 0, wrapErr(err)
	}

	now := s.now().Truncate(time.Microsecond)
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	deltas := map[string]int64{}
	n := 0
	for _, op := range due {
		r, err := s.loadRow(op)
		if err != nil {
			return 0, err
		}
		if r == nil || r.State != store.StateReserved || !r.ExpiresAt.Before(before) {
			continue // re-check the predicate: a concurrent Release/Reserve replay may have already moved this row
		}
		prev := *r
		next := *r
		next.State = store.StateQuarantined
		next.QuarantinedAt = now
		next.UpdatedAt = now
		if err := s.setRow(b, &prev, &next, deltas); err != nil {
			return 0, wrapErr(err)
		}
		n++
	}
	if err := s.applyCounters(b, deltas); err != nil {
		return 0, err
	}
	if err := g.commit(b); err != nil {
		return 0, err
	}
	return n, nil
}

// ListQuarantined implements store.Store. before is a comparison bound, not
// stored data (store.go, "Clocks"): a before at or before the Unix epoch
// matches nothing (see Quarantine's doc comment for why, and for the key-
// encoding trap this sidesteps) and returns an empty page, nil, without
// querying, rather than ErrInvalidArgument. An after cursor at or past
// before also matches nothing, but needs no separate check here: the range
// [nextKey(quarantineKey(pool, after.At, after.Outpoint)), upperBoundBefore(before))
// is already empty by construction whenever after is not strictly before
// before.
func (s *Store) ListQuarantined(ctx context.Context, pool string, before time.Time, after *store.RowCursor, limit int) (_ []store.Row, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateLimit(limit); err != nil {
		return nil, err
	}
	if matchesNothingBefore(before) {
		return nil, nil
	}
	before = before.Truncate(time.Microsecond)
	g := s.lock()
	defer g.unlock(&err)
	if s.closed.Load() {
		return nil, errClosed
	}

	prefix := quarantinePrefix(pool)
	upper := upperBoundBefore(prefix, before)
	lower := prefix
	if after != nil {
		lower = nextKey(quarantineKey(pool, after.At, after.Outpoint))
	}
	out := make([]store.Row, 0, limit)
	err = s.iterate(lower, upper, func(k, _ []byte) (bool, error) {
		tail := k[len(prefix)+8:]
		var op store.Outpoint
		copy(op.TxID[:], tail[:32])
		op.Vout = deUint32(tail[32:36])
		r, lerr := s.loadRow(op)
		if lerr != nil {
			return false, lerr
		}
		if r == nil || r.State != store.StateQuarantined {
			return true, nil
		}
		out = append(out, *r)
		return len(out) < limit, nil
	})
	if err != nil {
		return nil, wrapErr(err)
	}
	return out, nil
}

// Resolve implements store.Store.
func (s *Store) Resolve(ctx context.Context, pool string, ops []store.Outpoint, to store.State, spentBy *chainhash.Hash) (_ int, err error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	if err := store.ValidateOutpoints(ops, maxBatch); err != nil {
		return 0, err
	}
	if err := store.ValidateResolve(to, spentBy); err != nil {
		return 0, err
	}
	g := s.lock()
	defer g.unlock(&err)
	if s.closed.Load() {
		return 0, errClosed
	}
	now := s.now().Truncate(time.Microsecond)
	b := s.db.NewBatch()
	defer func() { _ = b.Close() }()
	deltas := map[string]int64{}
	n := 0
	for _, op := range store.SortOutpoints(ops) {
		r, err := s.loadRow(op)
		if err != nil {
			return 0, err
		}
		if r == nil || r.Pool != pool || r.State != store.StateQuarantined {
			continue
		}
		prev := *r
		next := *r
		if to == store.StateAvailable {
			next.State = store.StateAvailable
			clearReservation(&next)
		} else {
			h := *spentBy
			next.State = store.StateSpent
			next.SpentBy = &h
		}
		next.UpdatedAt = now
		if err := s.setRow(b, &prev, &next, deltas); err != nil {
			return 0, wrapErr(err)
		}
		n++
	}
	if err := s.applyCounters(b, deltas); err != nil {
		return 0, err
	}
	if err := g.commit(b); err != nil {
		return 0, err
	}
	return n, nil
}

// Stats implements store.Store.
func (s *Store) Stats(ctx context.Context, pool string) (_ store.Stats, err error) {
	if err := ctx.Err(); err != nil {
		return store.Stats{}, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return store.Stats{}, err
	}
	g := s.lock()
	defer g.unlock(&err)
	if s.closed.Load() {
		return store.Stats{}, errClosed
	}
	var st store.Stats
	for state := store.StateAvailable; state <= store.StateInvalid; state++ {
		n, err := s.readCounter(rowCounterKey(pool, state))
		if err != nil {
			return store.Stats{}, err
		}
		switch state {
		case store.StateAvailable:
			st.Available = n
		case store.StateReserved:
			st.Reserved = n
		case store.StateQuarantined:
			st.Quarantined = n
		case store.StateSpent:
			st.Spent = n
		case store.StateInvalid:
			st.Invalid = n
		}
	}
	for status := store.ShardUnmined; status <= store.ShardInvalid; status++ {
		n, err := s.readCounter(shardCounterKey(pool, status))
		if err != nil {
			return store.Stats{}, err
		}
		switch status {
		case store.ShardUnmined:
			st.ShardsUnmined = n
		case store.ShardMined:
			st.ShardsMined = n
		case store.ShardRetired:
			st.ShardsRetired = n
		case store.ShardInvalid:
			st.ShardsInvalid = n
		}
	}
	return st, nil
}
