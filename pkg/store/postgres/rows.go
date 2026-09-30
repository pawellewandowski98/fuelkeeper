package postgres

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/jackc/pgx/v5"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// lessOutpoint orders outpoints by their txid's 32 internal bytes, then vout.
// Reserve, updateReserved (Release/Spend) and Resolve each touch their ops as
// individual single-row, primary-key point UPDATEs, pipelined in this order as
// one pgx.Batch inside one transaction: two concurrent batches that touch
// overlapping rows then always request their locks in the same global order,
// which rules out the deadlock a combined multi-row "UPDATE ... FROM
// unnest(...)" statement can hit whenever Postgres's planner does not happen
// to honour the array's input order (observed in practice under
// ReserveExclusivityConcurrent / ReserveReleaseCycleConcurrent).
func lessOutpoint(a, b store.Outpoint) bool {
	if c := bytes.Compare(a.TxID[:], b.TxID[:]); c != 0 {
		return c < 0
	}
	return a.Vout < b.Vout
}

func sortedOutpoints(ops []store.Outpoint) []store.Outpoint {
	out := make([]store.Outpoint, len(ops))
	copy(out, ops)
	sort.Slice(out, func(i, j int) bool { return lessOutpoint(out[i], out[j]) })
	return out
}

func sortedReserveOps(ops []store.ReserveOp) []store.ReserveOp {
	out := make([]store.ReserveOp, len(ops))
	copy(out, ops)
	sort.Slice(out, func(i, j int) bool { return lessOutpoint(out[i].Outpoint, out[j].Outpoint) })
	return out
}

// rowCols is qualified with the "r" alias so it stays unambiguous in queries
// that join fuel_rows against an unnest(...) key CTE (GetRows, Reserve).
const rowCols = `r.txid, r.vout, r.pool, r.satoshis, r.locking_script, r.unlocking_script, r.state, r.reserved_by, r.reserved_at, r.expires_at, r.quarantined_at, r.spent_hint, r.spent_by, r.updated_at`

func scanRow(sc rowScanner) (store.Row, error) {
	var (
		r                  store.Row
		txid, rb, hint, sb []byte
		vout               int32
		sats               int64
		state              int16
		ra, ea, qa         *time.Time
		up                 time.Time
	)
	if err := sc.Scan(&txid, &vout, &r.Pool, &sats, &r.LockingScript, &r.UnlockingScript, &state, &rb, &ra, &ea, &qa, &hint, &sb, &up); err != nil {
		return store.Row{}, err
	}
	copy(r.TxID[:], txid)
	r.Vout = uint32(vout)        //nolint:gosec // non-negative by construction
	r.Satoshis = uint64(sats)    //nolint:gosec // non-negative by construction
	r.State = store.State(state) //nolint:gosec // small enum
	copy(r.ReservedBy[:], rb)
	r.ReservedAt, r.ExpiresAt, r.QuarantinedAt = utc(ra), utc(ea), utc(qa)
	if len(hint) == 32 {
		h, _ := chainhash.NewHash(hint)
		r.SpentHint = h
	}
	if len(sb) == 32 {
		h, _ := chainhash.NewHash(sb)
		r.SpentBy = h
	}
	r.UpdatedAt = up.UTC()
	return r, nil
}

func (s *Store) queryRows(ctx context.Context, sql string, args ...any) ([]store.Row, error) {
	rs, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, wrap(err)
	}
	defer rs.Close()
	var out []store.Row
	for rs.Next() {
		r, err := scanRow(rs)
		if err != nil {
			return nil, wrap(err)
		}
		out = append(out, r)
	}
	return out, wrap(rs.Err())
}

// ListAvailable implements store.Store.
func (s *Store) ListAvailable(ctx context.Context, pool string, txid chainhash.Hash, fromVout uint32, limit int) ([]store.Row, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateLimit(limit); err != nil {
		return nil, err
	}
	return s.queryRows(ctx, `
		SELECT `+rowCols+` FROM fuel_rows r
		 WHERE r.pool = $1 AND r.txid = $2 AND r.state = 0 AND r.vout >= $3
		 ORDER BY r.vout LIMIT $4`, pool, txid[:], int32(fromVout), limit) //nolint:gosec // bounded
}

// GetRows implements store.Store.
func (s *Store) GetRows(ctx context.Context, pool string, ops []store.Outpoint) ([]store.Row, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateOutpoints(ops, maxBatch); err != nil {
		return nil, err
	}
	txids, vouts := hashBytes(ops)
	return s.queryRows(ctx, `
		SELECT `+rowCols+` FROM fuel_rows r
		  JOIN unnest($2::bytea[], $3::int4[]) AS k(txid, vout) ON r.txid = k.txid AND r.vout = k.vout
		 WHERE r.pool = $1`, pool, txids, vouts)
}

// Reserve implements store.Store.
func (s *Store) Reserve(ctx context.Context, pool string, ops []store.ReserveOp) ([]store.ReserveOp, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateReserveOps(ops, maxBatch); err != nil {
		return nil, err
	}
	now := s.now()
	sorted := sortedReserveOps(ops)

	batch := &pgx.Batch{}
	for _, op := range sorted {
		// updated_at is stamped only when the row actually transitions from
		// available (state = 0); a same-token replay of an already-reserved row
		// leaves it alone (see store.go "Idempotency").
		batch.Queue(`
			UPDATE fuel_rows
			   SET state = 1,
			       reserved_by = $4,
			       reserved_at = CASE WHEN state = 0 THEN $5 ELSE reserved_at END,
			       expires_at  = CASE WHEN state = 0 THEN $6 ELSE expires_at END,
			       updated_at  = CASE WHEN state = 0 THEN $5 ELSE updated_at END
			 WHERE pool = $1 AND txid = $2 AND vout = $3
			   AND (state = 0 OR (state = 1 AND reserved_by = $4))
			RETURNING expires_at`,
			pool, op.TxID[:], int32(op.Vout), op.Token[:], now, op.ExpiresAt) //nolint:gosec // bounded
	}

	applied := make([]store.ReserveOp, 0, len(ops))
	err := s.tx(ctx, func(tx pgx.Tx) error {
		br := tx.SendBatch(ctx, batch)
		for _, op := range sorted {
			var exp time.Time
			err := br.QueryRow().Scan(&exp)
			switch {
			case errors.Is(err, pgx.ErrNoRows):
				continue // predicate did not match: not applied
			case err != nil:
				_ = br.Close()
				return err
			}
			applied = append(applied, store.ReserveOp{Outpoint: op.Outpoint, Token: op.Token, ExpiresAt: exp.UTC()})
		}
		return br.Close()
	})
	if err != nil {
		return nil, err
	}
	return applied, nil
}

// updateReserved runs setClause against rows in state reserved under token, one
// row at a time in sorted order (see lessOutpoint).
func (s *Store) updateReserved(ctx context.Context, pool string, token store.Token, ops []store.Outpoint, setClause string, extra ...any) (int, error) {
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	if err := store.ValidateOutpoints(ops, maxBatch); err != nil {
		return 0, err
	}
	if token.IsZero() {
		return 0, store.ErrInvalidArgument
	}
	sorted := sortedOutpoints(ops)
	now := s.now()
	sqlText := `UPDATE fuel_rows SET ` + setClause + ` WHERE pool = $1 AND txid = $2 AND vout = $3 AND state = 1 AND reserved_by = $4`
	batch := &pgx.Batch{}
	for _, op := range sorted {
		args := append([]any{pool, op.TxID[:], int32(op.Vout), token[:], now}, extra...) //nolint:gosec // bounded
		batch.Queue(sqlText, args...)
	}
	n := 0
	err := s.tx(ctx, func(tx pgx.Tx) error {
		br := tx.SendBatch(ctx, batch)
		for range sorted {
			tag, err := br.Exec()
			if err != nil {
				_ = br.Close()
				return err
			}
			n += int(tag.RowsAffected())
		}
		return br.Close()
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// Release implements store.Store.
func (s *Store) Release(ctx context.Context, pool string, token store.Token, ops []store.Outpoint) (int, error) {
	return s.updateReserved(ctx, pool, token, ops,
		`state = 0, reserved_by = NULL, reserved_at = NULL, expires_at = NULL, quarantined_at = NULL, spent_hint = NULL, updated_at = $5`)
}

// Spend implements store.Store.
func (s *Store) Spend(ctx context.Context, pool string, token store.Token, ops []store.Outpoint, txid chainhash.Hash, terminal bool) (int, error) {
	if terminal {
		return s.updateReserved(ctx, pool, token, ops, `state = 3, spent_by = $6, updated_at = $5`, txid[:])
	}
	return s.updateReserved(ctx, pool, token, ops, `state = 2, quarantined_at = $5, spent_hint = $6, updated_at = $5`, txid[:])
}

// Quarantine implements store.Store.
func (s *Store) Quarantine(ctx context.Context, pool string, before time.Time, limit int) (int, error) {
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	if err := store.ValidateLimit(limit); err != nil {
		return 0, err
	}
	tag, err := s.pool.Exec(ctx, `
		WITH c AS (
		  SELECT txid, vout FROM fuel_rows
		   WHERE pool = $1 AND state = 1 AND expires_at < $2
		   ORDER BY expires_at, txid, vout
		   LIMIT $3
		   FOR UPDATE SKIP LOCKED)
		UPDATE fuel_rows r SET state = 2, quarantined_at = $4, updated_at = $4
		  FROM c
		 WHERE r.txid = c.txid AND r.vout = c.vout AND r.state = 1`, pool, before, limit, s.now())
	if err != nil {
		return 0, wrap(err)
	}
	return int(tag.RowsAffected()), nil
}

// ListQuarantined implements store.Store.
func (s *Store) ListQuarantined(ctx context.Context, pool string, before time.Time, after *store.RowCursor, limit int) ([]store.Row, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateLimit(limit); err != nil {
		return nil, err
	}
	var cAt *time.Time
	var cTx []byte
	var cVout int32
	if after != nil {
		at := after.At
		cAt, cTx, cVout = &at, after.Outpoint.TxID[:], int32(after.Outpoint.Vout) //nolint:gosec // bounded
	}
	return s.queryRows(ctx, `
		SELECT `+rowCols+` FROM fuel_rows r
		 WHERE r.pool = $1 AND r.state = 2 AND r.quarantined_at < $2
		   AND ($3::timestamptz IS NULL OR (r.quarantined_at, r.txid, r.vout) > ($3, $4::bytea, $5::int4))
		 ORDER BY r.quarantined_at, r.txid, r.vout LIMIT $6`, pool, before, cAt, cTx, cVout, limit)
}

// Resolve implements store.Store.
func (s *Store) Resolve(ctx context.Context, pool string, ops []store.Outpoint, to store.State, spentBy *chainhash.Hash) (int, error) {
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	if err := store.ValidateOutpoints(ops, maxBatch); err != nil {
		return 0, err
	}
	if err := store.ValidateResolve(to, spentBy); err != nil {
		return 0, err
	}
	sorted := sortedOutpoints(ops)
	now := s.now()
	var set string
	var extra []any
	if to == store.StateAvailable {
		set = `state = 0, reserved_by = NULL, reserved_at = NULL, expires_at = NULL, quarantined_at = NULL, spent_hint = NULL, updated_at = $4`
	} else {
		set = `state = 3, spent_by = $5, updated_at = $4`
		extra = []any{spentBy[:]}
	}
	sqlText := `UPDATE fuel_rows SET ` + set + ` WHERE pool = $1 AND txid = $2 AND vout = $3 AND state = 2`
	batch := &pgx.Batch{}
	for _, op := range sorted {
		args := append([]any{pool, op.TxID[:], int32(op.Vout), now}, extra...) //nolint:gosec // bounded
		batch.Queue(sqlText, args...)
	}
	n := 0
	err := s.tx(ctx, func(tx pgx.Tx) error {
		br := tx.SendBatch(ctx, batch)
		for range sorted {
			tag, err := br.Exec()
			if err != nil {
				_ = br.Close()
				return err
			}
			n += int(tag.RowsAffected())
		}
		return br.Close()
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// Stats implements store.Store.
func (s *Store) Stats(ctx context.Context, pool string) (store.Stats, error) {
	if err := store.ValidatePool(pool); err != nil {
		return store.Stats{}, err
	}
	var st store.Stats
	rs, err := s.pool.Query(ctx, `SELECT state, count(*) FROM fuel_rows WHERE pool = $1 GROUP BY state`, pool)
	if err != nil {
		return st, wrap(err)
	}
	for rs.Next() {
		var state int16
		var n int64
		if err := rs.Scan(&state, &n); err != nil {
			rs.Close()
			return st, wrap(err)
		}
		switch store.State(state) { //nolint:gosec // small enum
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
	rs.Close()
	if err := rs.Err(); err != nil {
		return st, wrap(err)
	}
	rs, err = s.pool.Query(ctx, `SELECT status, count(*) FROM fuel_shards WHERE pool = $1 GROUP BY status`, pool)
	if err != nil {
		return st, wrap(err)
	}
	defer rs.Close()
	for rs.Next() {
		var status int16
		var n int64
		if err := rs.Scan(&status, &n); err != nil {
			return st, wrap(err)
		}
		switch store.ShardStatus(status) { //nolint:gosec // small enum
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
	return st, wrap(rs.Err())
}
