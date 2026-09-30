package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// Reserve, updateReserved (Release/Spend) and Resolve process their ops one
// row at a time via store.SortReserveOps/store.SortOutpoints (see its doc for
// why); harmless but free to keep under this backend's single-writer model.

const rowCols = `txid, vout, pool, satoshis, locking_script, unlocking_script, state, reserved_by, reserved_at, expires_at, quarantined_at, spent_hint, spent_by, updated_at`

// listAvailableSQL is ListAvailable's statement: pool = ?, txid = ?, fromVout = ?, limit = ?.
const listAvailableSQL = `
	SELECT ` + rowCols + ` FROM fuel_rows
	 WHERE pool = ? AND txid = ? AND state = 0 AND vout >= ?
	 ORDER BY vout LIMIT ?`

// reserveRowSQL is Reserve's per-row statement: token = ?, now = ?, expiresAt
// = ?, now = ?, pool = ?, txid = ?, vout = ?, token = ?. updated_at is
// stamped only when the row actually transitions from available (state = 0);
// a same-token replay of an already-reserved row leaves it alone (see
// store.go "Idempotency").
const reserveRowSQL = `
	UPDATE fuel_rows
	   SET state = 1,
	       reserved_by = ?,
	       reserved_at = CASE WHEN state = 0 THEN ? ELSE reserved_at END,
	       expires_at  = CASE WHEN state = 0 THEN ? ELSE expires_at END,
	       updated_at  = CASE WHEN state = 0 THEN ? ELSE updated_at END
	 WHERE pool = ? AND txid = ? AND vout = ?
	   AND (state = 0 OR (state = 1 AND reserved_by = ?))
	RETURNING expires_at`

// updateReservedWhereSQL is the WHERE clause shared by every updateReserved
// (Release/Spend) per-row statement: pool = ?, txid = ?, vout = ?, token = ?.
// Unlike Postgres, this backend needs no "(state = 1) IS TRUE" rewrite: SQLite
// prefers an exact primary-key match over a partial index outright (pinned by
// explain_test.go), so a plain "state = 1" carries no plan-cliff risk here.
const updateReservedWhereSQL = ` WHERE pool = ? AND txid = ? AND vout = ? AND state = 1 AND reserved_by = ?`

const releaseSetClause = `state = 0, reserved_by = NULL, reserved_at = NULL, expires_at = NULL, quarantined_at = NULL, spent_hint = NULL, updated_at = ?`

const spendTerminalSetClause = `state = 3, spent_by = ?, updated_at = ?`

const spendHintSetClause = `state = 2, quarantined_at = ?, spent_hint = ?, updated_at = ?`

// releaseRowSQL, spendTerminalRowSQL and spendHintRowSQL are the exact
// statements updateReserved sends for Release and Spend's two modes.
const releaseRowSQL = `UPDATE fuel_rows SET ` + releaseSetClause + updateReservedWhereSQL

const spendTerminalRowSQL = `UPDATE fuel_rows SET ` + spendTerminalSetClause + updateReservedWhereSQL

const spendHintRowSQL = `UPDATE fuel_rows SET ` + spendHintSetClause + updateReservedWhereSQL

// quarantineCandidateSQL is Quarantine's candidate select: pool = ?, before = ?, limit = ?.
const quarantineCandidateSQL = `
	SELECT txid, vout FROM fuel_rows
	 WHERE pool = ? AND state = 1 AND expires_at < ?
	 ORDER BY expires_at, txid, vout LIMIT ?`

// quarantineSQL is Quarantine's statement: quarantined_at = ?, updated_at =
// ?, then quarantineCandidateSQL's own args (pool = ?, before = ?, limit =
// ?). The row-value IN clause updates exactly the candidate set the select
// alone would return, atomically, without a separate read-then-write step:
// harmless here since the whole statement already runs inside this backend's
// single write transaction.
const quarantineSQL = `
	UPDATE fuel_rows SET state = 2, quarantined_at = ?, updated_at = ?
	 WHERE (txid, vout) IN (` + quarantineCandidateSQL + `)`

// listQuarantinedSQL and listQuarantinedAfterSQL are ListQuarantined's two
// variants (see its doc comment in store.go for why they are separate
// statements, not one with an optional-cursor branch): the row-value cursor
// comparison in the "after" variant lets SQLite seek fuel_rows_quar directly
// to the cursor and scan forward, instead of scanning from the start of the
// index and filtering.
const listQuarantinedSQL = `
	SELECT ` + rowCols + ` FROM fuel_rows
	 WHERE pool = ? AND state = 2 AND quarantined_at < ?
	 ORDER BY quarantined_at, txid, vout LIMIT ?`

const listQuarantinedAfterSQL = `
	SELECT ` + rowCols + ` FROM fuel_rows
	 WHERE pool = ? AND state = 2 AND quarantined_at < ?
	   AND (quarantined_at, txid, vout) > (?, ?, ?)
	 ORDER BY quarantined_at, txid, vout LIMIT ?`

// resolveWhereSQL is the WHERE clause shared by both Resolve per-row
// statements: pool = ?, txid = ?, vout = ?. See updateReservedWhereSQL for
// why no "(state = 2) IS TRUE" rewrite is needed here.
const resolveWhereSQL = ` WHERE pool = ? AND txid = ? AND vout = ? AND state = 2`

const resolveAvailableSetClause = `state = 0, reserved_by = NULL, reserved_at = NULL, expires_at = NULL, quarantined_at = NULL, spent_hint = NULL, updated_at = ?`

const resolveSpentSetClause = `state = 3, spent_by = ?, updated_at = ?`

// resolveAvailableRowSQL and resolveSpentRowSQL are the exact statements
// Resolve sends for its two targets.
const resolveAvailableRowSQL = `UPDATE fuel_rows SET ` + resolveAvailableSetClause + resolveWhereSQL

const resolveSpentRowSQL = `UPDATE fuel_rows SET ` + resolveSpentSetClause + resolveWhereSQL

func scanRow(sc rowScanner) (store.Row, error) {
	var (
		r             store.Row
		txid, rb      []byte
		hint, sb      []byte
		sats, updated int64
		state         int
		ra, ea, qa    sql.NullInt64
	)
	if err := sc.Scan(&txid, &r.Vout, &r.Pool, &sats, &r.LockingScript, &r.UnlockingScript, &state, &rb, &ra, &ea, &qa, &hint, &sb, &updated); err != nil {
		return store.Row{}, err
	}
	copy(r.TxID[:], txid)
	r.Satoshis = uint64(sats)    //nolint:gosec // stored from uint64
	r.State = store.State(state) //nolint:gosec // small enum
	copy(r.ReservedBy[:], rb)
	r.ReservedAt, r.ExpiresAt, r.QuarantinedAt = fromUS(ra), fromUS(ea), fromUS(qa)
	if len(hint) == 32 {
		h, _ := chainhash.NewHash(hint)
		r.SpentHint = h
	}
	if len(sb) == 32 {
		h, _ := chainhash.NewHash(sb)
		r.SpentBy = h
	}
	r.UpdatedAt = time.UnixMicro(updated).UTC()
	return r, nil
}

func (s *Store) queryRows(ctx context.Context, q string, args ...any) ([]store.Row, error) {
	rs, err := s.reader.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, wrapCtx(ctx, err)
	}
	defer func() { _ = rs.Close() }()
	var out []store.Row
	for rs.Next() {
		r, err := scanRow(rs)
		if err != nil {
			return nil, wrapCtx(ctx, err)
		}
		out = append(out, r)
	}
	return out, wrapCtx(ctx, rs.Err())
}

// ListAvailable implements store.Store.
func (s *Store) ListAvailable(ctx context.Context, pool string, txid chainhash.Hash, fromVout uint32, limit int) ([]store.Row, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateLimit(limit); err != nil {
		return nil, err
	}
	return s.queryRows(ctx, listAvailableSQL, pool, txid[:], fromVout, limit)
}

// GetRows implements store.Store.
func (s *Store) GetRows(ctx context.Context, pool string, ops []store.Outpoint) ([]store.Row, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateOutpoints(ops, maxBatch); err != nil {
		return nil, err
	}
	stmt, err := s.reader.PrepareContext(ctx, `SELECT `+rowCols+` FROM fuel_rows WHERE pool=? AND txid=? AND vout=?`)
	if err != nil {
		return nil, wrapCtx(ctx, err)
	}
	defer func() { _ = stmt.Close() }()
	out := make([]store.Row, 0, len(ops))
	for _, op := range ops {
		r, err := scanRow(stmt.QueryRowContext(ctx, pool, op.TxID[:], op.Vout))
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, wrapCtx(ctx, err)
		}
		out = append(out, r)
	}
	return out, nil
}

// Reserve implements store.Store.
func (s *Store) Reserve(ctx context.Context, pool string, ops []store.ReserveOp) ([]store.ReserveOp, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateReserveOps(ops, maxBatch); err != nil {
		return nil, err
	}
	now := s.now().UnixMicro()
	sorted := store.SortReserveOps(ops)
	applied := make([]store.ReserveOp, 0, len(ops))
	err := s.write(ctx, func(c querier) error {
		stmt, err := c.PrepareContext(ctx, reserveRowSQL)
		if err != nil {
			return err
		}
		defer func() { _ = stmt.Close() }()
		for _, op := range sorted {
			var exp int64
			err := stmt.QueryRowContext(ctx, op.Token[:], now, op.ExpiresAt.UnixMicro(), now, pool, op.TxID[:], op.Vout, op.Token[:]).Scan(&exp)
			if errors.Is(err, sql.ErrNoRows) {
				continue // predicate did not match: not applied
			}
			if err != nil {
				return err
			}
			applied = append(applied, store.ReserveOp{Outpoint: op.Outpoint, Token: op.Token, ExpiresAt: time.UnixMicro(exp).UTC()})
		}
		return nil
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
	n := 0
	err := s.write(ctx, func(c querier) error {
		n = 0
		stmt, err := c.PrepareContext(ctx, sqlText)
		if err != nil {
			return err
		}
		defer func() { _ = stmt.Close() }()
		for _, op := range sorted {
			args := append(append([]any{}, extra...), pool, op.TxID[:], op.Vout, token[:])
			res, err := stmt.ExecContext(ctx, args...)
			if err != nil {
				return err
			}
			k, _ := res.RowsAffected()
			n += int(k)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// Release implements store.Store.
func (s *Store) Release(ctx context.Context, pool string, token store.Token, ops []store.Outpoint) (int, error) {
	return s.updateReserved(ctx, pool, token, ops, releaseRowSQL, s.now().UnixMicro())
}

// Spend implements store.Store.
func (s *Store) Spend(ctx context.Context, pool string, token store.Token, ops []store.Outpoint, txid chainhash.Hash, terminal bool) (int, error) {
	if err := store.ValidateSpend(txid, terminal); err != nil {
		return 0, err
	}
	now := s.now().UnixMicro()
	if terminal {
		return s.updateReserved(ctx, pool, token, ops, spendTerminalRowSQL, txid[:], now)
	}
	// A zero txid is only a client-supplied hint, not an oracle-proven fact (see
	// ValidateSpend): bind NULL rather than 32 zero bytes so SpentHint scans back
	// as nil, not a non-nil zero hash.
	var hint []byte
	if txid != (chainhash.Hash{}) {
		hint = txid[:]
	}
	return s.updateReserved(ctx, pool, token, ops, spendHintRowSQL, now, hint, now)
}

// Quarantine implements store.Store.
func (s *Store) Quarantine(ctx context.Context, pool string, before time.Time, limit int) (int, error) {
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	if err := store.ValidateLimit(limit); err != nil {
		return 0, err
	}
	now := s.now().UnixMicro()
	n := 0
	err := s.write(ctx, func(c querier) error {
		res, err := c.ExecContext(ctx, quarantineSQL, now, now, pool, before.UnixMicro(), limit)
		if err != nil {
			return err
		}
		k, _ := res.RowsAffected()
		n = int(k)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// ListQuarantined implements store.Store.
func (s *Store) ListQuarantined(ctx context.Context, pool string, before time.Time, after *store.RowCursor, limit int) ([]store.Row, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateLimit(limit); err != nil {
		return nil, err
	}
	if after == nil {
		return s.queryRows(ctx, listQuarantinedSQL, pool, before.UnixMicro(), limit)
	}
	return s.queryRows(ctx, listQuarantinedAfterSQL,
		pool, before.UnixMicro(), after.At.UnixMicro(), after.Outpoint.TxID[:], after.Outpoint.Vout, limit)
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
	now := s.now().UnixMicro()
	sqlText := resolveAvailableRowSQL
	extra := []any{now}
	if to != store.StateAvailable {
		sqlText = resolveSpentRowSQL
		extra = []any{spentBy[:], now}
	}
	n := 0
	err := s.write(ctx, func(c querier) error {
		n = 0
		stmt, err := c.PrepareContext(ctx, sqlText)
		if err != nil {
			return err
		}
		defer func() { _ = stmt.Close() }()
		for _, op := range sorted {
			args := append(append([]any{}, extra...), pool, op.TxID[:], op.Vout)
			res, err := stmt.ExecContext(ctx, args...)
			if err != nil {
				return err
			}
			k, _ := res.RowsAffected()
			n += int(k)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// countGroupBy runs a "SELECT <key>, count(*) FROM <table> WHERE pool = ?
// GROUP BY <key>" style query on q (a *sql.Tx, so Stats' two counts share one
// snapshot) and returns the counts keyed by the integer column value.
func countGroupBy(ctx context.Context, q querier, query, pool string) (map[int]int64, error) {
	rs, err := q.QueryContext(ctx, query, pool)
	if err != nil {
		return nil, wrapCtx(ctx, err)
	}
	defer func() { _ = rs.Close() }()
	out := make(map[int]int64)
	for rs.Next() {
		var key int
		var n int64
		if err := rs.Scan(&key, &n); err != nil {
			return nil, wrapCtx(ctx, err)
		}
		out[key] = n
	}
	return out, wrapCtx(ctx, rs.Err())
}

// Stats implements store.Store. Both counts run inside one read-only
// transaction on the reader, so they reflect a single consistent snapshot
// instead of two independent reads that a concurrent write could straddle.
func (s *Store) Stats(ctx context.Context, pool string) (store.Stats, error) {
	if err := store.ValidatePool(pool); err != nil {
		return store.Stats{}, err
	}
	tx, err := s.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return store.Stats{}, wrapCtx(ctx, err)
	}
	defer func() { _ = tx.Rollback() }()

	var st store.Stats
	rowCounts, err := countGroupBy(ctx, tx, `SELECT state, count(*) FROM fuel_rows WHERE pool = ? GROUP BY state`, pool)
	if err != nil {
		return st, err
	}
	st.Available = rowCounts[int(store.StateAvailable)]
	st.Reserved = rowCounts[int(store.StateReserved)]
	st.Quarantined = rowCounts[int(store.StateQuarantined)]
	st.Spent = rowCounts[int(store.StateSpent)]
	st.Invalid = rowCounts[int(store.StateInvalid)]

	shardCounts, err := countGroupBy(ctx, tx, `SELECT status, count(*) FROM fuel_shards WHERE pool = ? GROUP BY status`, pool)
	if err != nil {
		return st, err
	}
	st.ShardsUnmined = shardCounts[int(store.ShardUnmined)]
	st.ShardsMined = shardCounts[int(store.ShardMined)]
	st.ShardsRetired = shardCounts[int(store.ShardRetired)]
	st.ShardsInvalid = shardCounts[int(store.ShardInvalid)]
	return st, wrapCtx(ctx, tx.Commit())
}
