package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/jackc/pgx/v5"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// Reserve, updateReserved (Release/Spend) and Resolve each touch their ops as
// individual single-row, primary-key point UPDATEs, pipelined via
// store.SortReserveOps/store.SortOutpoints in ascending (txid internal bytes,
// vout) order as one pgx.Batch inside one transaction: two concurrent batches
// that touch overlapping rows then always request their locks in the same
// global order, which rules out the deadlock a combined multi-row "UPDATE ...
// FROM unnest(...)" statement can hit whenever Postgres's planner does not
// happen to honour the array's input order (observed in practice under
// ReserveExclusivityConcurrent / ReserveReleaseCycleConcurrent).

// rowCols is qualified with the "r" alias so it stays unambiguous in queries
// that join fuel_rows against an unnest(...) key CTE (GetRows, Reserve).
const rowCols = `r.txid, r.vout, r.pool, r.satoshis, r.locking_script, r.unlocking_script, r.state, r.reserved_by, r.reserved_at, r.expires_at, r.quarantined_at, r.spent_hint, r.spent_by, r.updated_at`

// listQuarantinedSQL and listQuarantinedAfterSQL are ListQuarantined's two
// variants (see its doc comment for why they are separate statements, not one
// with an optional-cursor branch).
const listQuarantinedSQL = `
	SELECT ` + rowCols + ` FROM fuel_rows r
	 WHERE r.pool = $1 AND r.state = 2 AND r.quarantined_at < $2
	 ORDER BY r.quarantined_at, r.txid, r.vout LIMIT $3`

const listQuarantinedAfterSQL = `
	SELECT ` + rowCols + ` FROM fuel_rows r
	 WHERE r.pool = $1 AND r.state = 2 AND r.quarantined_at < $2
	   AND (r.quarantined_at, r.txid, r.vout) > ($3, $4, $5)
	 ORDER BY r.quarantined_at, r.txid, r.vout LIMIT $6`

// listAvailableSQL is ListAvailable's statement: pool = $1, txid = $2, fromVout = $3, limit = $4.
const listAvailableSQL = `
	SELECT ` + rowCols + ` FROM fuel_rows r
	 WHERE r.pool = $1 AND r.txid = $2 AND r.state = 0 AND r.vout >= $3
	 ORDER BY r.vout LIMIT $4`

// getRowsSQL is GetRows's statement: pool = $1, txids = $2, vouts = $3.
const getRowsSQL = `
	SELECT ` + rowCols + ` FROM fuel_rows r
	  JOIN unnest($2::bytea[], $3::int4[]) AS k(txid, vout) ON r.txid = k.txid AND r.vout = k.vout
	 WHERE r.pool = $1`

// reserveRowSQL is Reserve's per-row statement: pool = $1, txid = $2, vout =
// $3, token = $4, now = $5, expiresAt = $6. updated_at is stamped only when
// the row actually transitions from available (state = 0); a same-token
// replay of an already-reserved row leaves it alone (see store.go
// "Idempotency"). Unlike updateReserved/Resolve's per-row statements, this one
// needs no IS TRUE guard: state appears only inside an OR (state = 0 OR
// (state = 1 AND ...)), so the WHERE clause never implies a single fixed
// value of state, and neither fuel_rows_avail nor fuel_rows_expiry's partial
// predicate can ever be proved implied (confirmed by EXPLAIN below).
const reserveRowSQL = `
	UPDATE fuel_rows
	   SET state = 1,
	       reserved_by = $4,
	       reserved_at = CASE WHEN state = 0 THEN $5 ELSE reserved_at END,
	       expires_at  = CASE WHEN state = 0 THEN $6 ELSE expires_at END,
	       updated_at  = CASE WHEN state = 0 THEN $5 ELSE updated_at END
	 WHERE pool = $1 AND txid = $2 AND vout = $3
	   AND (state = 0 OR (state = 1 AND reserved_by = $4))
	RETURNING expires_at`

// updateReservedWhereSQL is the WHERE clause shared by every updateReserved
// (Release/Spend) per-row statement: pool = $1, txid = $2, vout = $3, token =
// $4; each set clause (releaseSetClause / spendTerminalSetClause /
// spendHintSetClause) references $5 (now) and beyond.
//
// (state = 1) IS TRUE, not state = 1: this predicate targets exactly one row
// via its primary key (txid, vout), but a plain "state = 1" is *exactly* the
// partial predicate on fuel_rows_expiry (pool, expires_at, txid, vout) WHERE
// state = 1. Postgres's partial-index-match check is a syntactic proof, not a
// cost comparison: once it can prove the WHERE clause implies state = 1, the
// index becomes an eligible plan, and a generic (parameter-value-blind) plan
// -- which every backend connection settles into after a handful of
// executions of the same statement text -- routinely picks it whenever
// ANALYZE last ran with few reserved rows, turning an O(log n) PK lookup into
// an O(reserved rows in pool) scan (measured locally: Release p50 ~132ms
// unguarded vs ~50ms guarded, 8 workers x 256 ops, ~5000 reserved rows,
// ANALYZE taken at 0 reserved; see explain_test.go for the EXPLAIN proof).
// Wrapping the comparison in IS TRUE (a no-op given state is NOT NULL)
// changes its shape just enough that the proof no longer holds, so the
// partial index is never considered and the PK is used unconditionally.
const updateReservedWhereSQL = ` WHERE pool = $1 AND txid = $2 AND vout = $3 AND (state = 1) IS TRUE AND reserved_by = $4`

const releaseSetClause = `state = 0, reserved_by = NULL, reserved_at = NULL, expires_at = NULL, quarantined_at = NULL, spent_hint = NULL, updated_at = $5`

const spendTerminalSetClause = `state = 3, spent_by = $6, updated_at = $5`

const spendHintSetClause = `state = 2, quarantined_at = $5, spent_hint = $6, updated_at = $5`

// releaseRowSQL, spendTerminalRowSQL and spendHintRowSQL are the exact
// statements updateReserved sends for Release and Spend's two modes.
const releaseRowSQL = `UPDATE fuel_rows SET ` + releaseSetClause + updateReservedWhereSQL

const spendTerminalRowSQL = `UPDATE fuel_rows SET ` + spendTerminalSetClause + updateReservedWhereSQL

const spendHintRowSQL = `UPDATE fuel_rows SET ` + spendHintSetClause + updateReservedWhereSQL

// quarantineSQL is Quarantine's statement: pool = $1, before = $2, limit =
// $3, now = $4.
const quarantineSQL = `
	WITH c AS (
	  SELECT txid, vout FROM fuel_rows
	   WHERE pool = $1 AND state = 1 AND expires_at < $2
	   ORDER BY expires_at, txid, vout
	   LIMIT $3
	   FOR UPDATE SKIP LOCKED)
	UPDATE fuel_rows r SET state = 2, quarantined_at = $4, updated_at = $4
	  FROM c
	 WHERE r.txid = c.txid AND r.vout = c.vout AND r.state = 1`

// resolveWhereSQL is the WHERE clause shared by both Resolve per-row
// statements: pool = $1, txid = $2, vout = $3.
//
// (state = 2) IS TRUE, not state = 2: see the longer comment on
// updateReservedWhereSQL. This predicate exactly matches fuel_rows_quar's
// partial condition, so a plain "state = 2" is eligible for that index once
// Postgres settles on a generic plan for this statement text, even though the
// primary key already gives an O(log n) lookup for this exact (pool, txid, vout).
const resolveWhereSQL = ` WHERE pool = $1 AND txid = $2 AND vout = $3 AND (state = 2) IS TRUE`

const resolveAvailableSetClause = `state = 0, reserved_by = NULL, reserved_at = NULL, expires_at = NULL, quarantined_at = NULL, spent_hint = NULL, updated_at = $4`

const resolveSpentSetClause = `state = 3, spent_by = $5, updated_at = $4`

// resolveAvailableRowSQL and resolveSpentRowSQL are the exact statements
// Resolve sends for its two targets.
const resolveAvailableRowSQL = `UPDATE fuel_rows SET ` + resolveAvailableSetClause + resolveWhereSQL

const resolveSpentRowSQL = `UPDATE fuel_rows SET ` + resolveSpentSetClause + resolveWhereSQL

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
	return s.queryRows(ctx, listAvailableSQL, pool, txid[:], int32(fromVout), limit) //nolint:gosec // bounded
}

// GetRows implements store.Store.
func (s *Store) GetRows(ctx context.Context, pool string, ops []store.Outpoint) ([]store.Row, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateOutpoints(ops, maxBatch); err != nil {
		return nil, err
	}
	txids, vouts := outpointArrays(ops)
	return s.queryRows(ctx, getRowsSQL, pool, txids, vouts)
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
	sorted := store.SortReserveOps(ops)

	batch := &pgx.Batch{}
	for _, op := range sorted {
		batch.Queue(reserveRowSQL, pool, op.TxID[:], int32(op.Vout), op.Token[:], now, op.ExpiresAt) //nolint:gosec // bounded
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

// updateReserved runs sqlText against rows in state reserved under token, one
// row at a time in sorted order (see store.LessOutpoint). sqlText must be one of
// releaseRowSQL, spendTerminalRowSQL or spendHintRowSQL.
func (s *Store) updateReserved(ctx context.Context, pool string, token store.Token, ops []store.Outpoint, sqlText string, extra ...any) (int, error) {
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	if err := store.ValidateOutpoints(ops, maxBatch); err != nil {
		return 0, err
	}
	if token.IsZero() {
		return 0, store.ErrInvalidArgument
	}
	sorted := store.SortOutpoints(ops)
	now := s.now()
	batch := &pgx.Batch{}
	for _, op := range sorted {
		args := append([]any{pool, op.TxID[:], int32(op.Vout), token[:], now}, extra...) //nolint:gosec // bounded
		batch.Queue(sqlText, args...)
	}
	var n int
	err := s.tx(ctx, func(tx pgx.Tx) error {
		var err error
		n, err = execBatch(ctx, tx, batch, len(sorted))
		return err
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// Release implements store.Store.
func (s *Store) Release(ctx context.Context, pool string, token store.Token, ops []store.Outpoint) (int, error) {
	return s.updateReserved(ctx, pool, token, ops, releaseRowSQL)
}

// Spend implements store.Store.
func (s *Store) Spend(ctx context.Context, pool string, token store.Token, ops []store.Outpoint, txid chainhash.Hash, terminal bool) (int, error) {
	if terminal {
		return s.updateReserved(ctx, pool, token, ops, spendTerminalRowSQL, txid[:])
	}
	return s.updateReserved(ctx, pool, token, ops, spendHintRowSQL, txid[:])
}

// Quarantine implements store.Store.
func (s *Store) Quarantine(ctx context.Context, pool string, before time.Time, limit int) (int, error) {
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	if err := store.ValidateLimit(limit); err != nil {
		return 0, err
	}
	tag, err := s.pool.Exec(ctx, quarantineSQL, pool, before, limit, s.now())
	if err != nil {
		return 0, wrap(err)
	}
	return int(tag.RowsAffected()), nil
}

// ListQuarantined implements store.Store.
//
// Two separate statements, not one with an "after IS NULL OR ..." branch: the
// fuel_rows_quar index now carries (quarantined_at, txid, vout) as trailing
// key columns exactly matching the ORDER BY, so a row-value cursor comparison
// against all three lets Postgres seek the index directly to the cursor and
// scan forward from there (see EXPLAIN in explain_test.go). An optional-
// parameter idiom folded into one statement routinely defeats that: the
// planner can no longer prove which branch will run, so it falls back to
// scanning from the start of the index and filtering, or adds an explicit
// Sort.
func (s *Store) ListQuarantined(ctx context.Context, pool string, before time.Time, after *store.RowCursor, limit int) ([]store.Row, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateLimit(limit); err != nil {
		return nil, err
	}
	if after == nil {
		return s.queryRows(ctx, listQuarantinedSQL, pool, before, limit)
	}
	return s.queryRows(ctx, listQuarantinedAfterSQL,
		pool, before, after.At, after.Outpoint.TxID[:], int32(after.Outpoint.Vout), limit) //nolint:gosec // bounded
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
	sorted := store.SortOutpoints(ops)
	now := s.now()
	sqlText := resolveAvailableRowSQL
	var extra []any
	if to != store.StateAvailable {
		sqlText = resolveSpentRowSQL
		extra = []any{spentBy[:]}
	}
	batch := &pgx.Batch{}
	for _, op := range sorted {
		args := append([]any{pool, op.TxID[:], int32(op.Vout), now}, extra...) //nolint:gosec // bounded
		batch.Queue(sqlText, args...)
	}
	var n int
	err := s.tx(ctx, func(tx pgx.Tx) error {
		var err error
		n, err = execBatch(ctx, tx, batch, len(sorted))
		return err
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
