package memory

import (
	"bytes"
	"context"
	"sort"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// ListAvailable implements store.Store.
func (s *Store) ListAvailable(ctx context.Context, pool string, txid chainhash.Hash, fromVout uint32, limit int) ([]store.Row, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateLimit(limit); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []store.Row
	for _, op := range s.rowsByShard[txid] {
		r, ok := s.rows[op]
		if !ok || r.Pool != pool || r.State != store.StateAvailable || r.Vout < fromVout {
			continue
		}
		out = append(out, cloneRow(r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Vout < out[j].Vout })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// GetRows implements store.Store.
func (s *Store) GetRows(ctx context.Context, pool string, ops []store.Outpoint) ([]store.Row, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateOutpoints(ops, s.Capabilities().MaxBatch); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]store.Row, 0, len(ops))
	for _, op := range ops {
		if r, ok := s.rows[op]; ok && r.Pool == pool {
			out = append(out, cloneRow(r))
		}
	}
	return out, nil
}

// Reserve implements store.Store.
func (s *Store) Reserve(ctx context.Context, pool string, ops []store.ReserveOp) ([]store.ReserveOp, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateReserveOps(ops, s.Capabilities().MaxBatch); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clockNow()
	applied := make([]store.ReserveOp, 0, len(ops))
	for _, op := range ops {
		r, ok := s.rows[op.Outpoint]
		if !ok || r.Pool != pool {
			continue
		}
		switch {
		case r.State == store.StateAvailable:
			r.State = store.StateReserved
			r.ReservedBy = op.Token
			r.ReservedAt = now
			r.ExpiresAt = op.ExpiresAt.Truncate(time.Microsecond)
			r.UpdatedAt = now
			op.ExpiresAt = r.ExpiresAt // report the stored (truncated) expiry
			applied = append(applied, op)
		case r.State == store.StateReserved && r.ReservedBy == op.Token:
			op.ExpiresAt = r.ExpiresAt // report the stored expiry on replay
			applied = append(applied, op)
		}
	}
	return applied, nil
}

// mutateReserved applies fn to every op row in state reserved under token.
func (s *Store) mutateReserved(pool string, token store.Token, ops []store.Outpoint, fn func(r *store.Row, now time.Time)) (int, error) {
	if err := store.ValidateOutpoints(ops, s.Capabilities().MaxBatch); err != nil {
		return 0, err
	}
	if token.IsZero() {
		return 0, store.ErrInvalidArgument
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clockNow()
	n := 0
	for _, op := range ops {
		r, ok := s.rows[op]
		if !ok || r.Pool != pool || r.State != store.StateReserved || r.ReservedBy != token {
			continue
		}
		fn(r, now)
		r.UpdatedAt = now
		n++
	}
	return n, nil
}

func clearReservation(r *store.Row) {
	r.ReservedBy = store.Token{}
	r.ReservedAt, r.ExpiresAt, r.QuarantinedAt = time.Time{}, time.Time{}, time.Time{}
	r.SpentHint = nil
}

// Release implements store.Store.
func (s *Store) Release(ctx context.Context, pool string, token store.Token, ops []store.Outpoint) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	return s.mutateReserved(pool, token, ops, func(r *store.Row, _ time.Time) {
		r.State = store.StateAvailable
		clearReservation(r)
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
	return s.mutateReserved(pool, token, ops, func(r *store.Row, now time.Time) {
		if terminal {
			h := txid
			r.State = store.StateSpent
			r.SpentBy = &h
			return
		}
		r.State = store.StateQuarantined
		r.QuarantinedAt = now
		if txid != (chainhash.Hash{}) {
			h := txid
			r.SpentHint = &h
		}
	})
}

// Quarantine implements store.Store.
//
// The select-then-update below is safe only because it runs entirely under s.mu:
// nothing can observe or mutate a row between the scan and the write. A backend
// without one global lock must re-check "state = reserved AND expires_at <
// before" inside the very same conditional write that flips the row to
// quarantined (e.g. UPDATE ... WHERE state = 1 AND expires_at < $before, or a
// SELECT ... FOR UPDATE / locking CTE that re-validates the predicate before
// committing); otherwise a concurrent Release or Reserve replay on the same row
// could be silently clobbered.
func (s *Store) Quarantine(ctx context.Context, pool string, before time.Time, limit int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	if err := store.ValidateLimit(limit); err != nil {
		return 0, err
	}
	// Truncate to microsecond precision (store.go, "Clocks"): every stored
	// expires_at is already truncated on write, so comparing against a raw,
	// sub-microsecond-precise `before` could treat a value only a few hundred
	// nanoseconds past expiry as if it were a full microsecond past it.
	before = before.Truncate(time.Microsecond)
	s.mu.Lock()
	defer s.mu.Unlock()
	var due []*store.Row
	for _, r := range s.rows {
		if r.Pool == pool && r.State == store.StateReserved && r.ExpiresAt.Before(before) {
			due = append(due, r)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if !due[i].ExpiresAt.Equal(due[j].ExpiresAt) {
			return due[i].ExpiresAt.Before(due[j].ExpiresAt)
		}
		if c := bytes.Compare(due[i].TxID[:], due[j].TxID[:]); c != 0 {
			return c < 0
		}
		return due[i].Vout < due[j].Vout
	})
	if len(due) > limit {
		due = due[:limit]
	}
	now := s.clockNow()
	for _, r := range due {
		r.State = store.StateQuarantined
		r.QuarantinedAt = now
		r.UpdatedAt = now
	}
	return len(due), nil
}

// quarantineCursorLess orders by (At, txid internal bytes, Outpoint.Vout)
// ascending: the tuple ListQuarantined pages over.
func quarantineCursorLess(a, b store.RowCursor) bool {
	if !a.At.Equal(b.At) {
		return a.At.Before(b.At)
	}
	if c := bytes.Compare(a.Outpoint.TxID[:], b.Outpoint.TxID[:]); c != 0 {
		return c < 0
	}
	return a.Outpoint.Vout < b.Outpoint.Vout
}

// ListQuarantined implements store.Store.
func (s *Store) ListQuarantined(ctx context.Context, pool string, before time.Time, after *store.RowCursor, limit int) ([]store.Row, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateLimit(limit); err != nil {
		return nil, err
	}
	// Truncate to microsecond precision: see the matching comment in Quarantine.
	before = before.Truncate(time.Microsecond)
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []store.Row
	for _, r := range s.rows {
		if r.Pool != pool || r.State != store.StateQuarantined || !r.QuarantinedAt.Before(before) {
			continue
		}
		if after != nil && !quarantineCursorLess(*after, r.QuarantineCursor()) {
			continue // not strictly greater than the cursor
		}
		out = append(out, cloneRow(r))
	}
	sort.Slice(out, func(i, j int) bool {
		return quarantineCursorLess(out[i].QuarantineCursor(), out[j].QuarantineCursor())
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// Resolve implements store.Store.
func (s *Store) Resolve(ctx context.Context, pool string, ops []store.Outpoint, to store.State, spentBy *chainhash.Hash) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	if err := store.ValidateOutpoints(ops, s.Capabilities().MaxBatch); err != nil {
		return 0, err
	}
	if err := store.ValidateResolve(to, spentBy); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clockNow()
	n := 0
	for _, op := range ops {
		r, ok := s.rows[op]
		if !ok || r.Pool != pool || r.State != store.StateQuarantined {
			continue
		}
		if to == store.StateAvailable {
			r.State = store.StateAvailable
			clearReservation(r)
		} else {
			h := *spentBy
			r.State = store.StateSpent
			r.SpentBy = &h
		}
		r.UpdatedAt = now
		n++
	}
	return n, nil
}

// Stats implements store.Store.
func (s *Store) Stats(ctx context.Context, pool string) (store.Stats, error) {
	if err := ctx.Err(); err != nil {
		return store.Stats{}, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return store.Stats{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var st store.Stats
	for _, r := range s.rows {
		if r.Pool != pool {
			continue
		}
		switch r.State {
		case store.StateAvailable:
			st.Available++
		case store.StateReserved:
			st.Reserved++
		case store.StateQuarantined:
			st.Quarantined++
		case store.StateSpent:
			st.Spent++
		case store.StateInvalid:
			st.Invalid++
		}
	}
	for k, sh := range s.shards {
		if k.pool != pool {
			continue
		}
		switch sh.Status {
		case store.ShardUnmined:
			st.ShardsUnmined++
		case store.ShardMined:
			st.ShardsMined++
		case store.ShardRetired:
			st.ShardsRetired++
		case store.ShardInvalid:
			st.ShardsInvalid++
		}
	}
	return st, nil
}
