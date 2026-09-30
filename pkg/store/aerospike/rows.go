package aerospike

import (
	"context"
	"fmt"
	"sort"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// lessOutpointBytes orders outpoints by (txid internal bytes, vout), matching
// store.LessOutpoint.
func lessOutpointBytes(a, b store.Outpoint) bool { return store.LessOutpoint(a, b) }

// ListAvailable implements store.Store. ak encodes exactly "the available
// rows of this shard" (see bins.go), so the whole discovery is one equality
// query; the fromVout lower bound is applied server-side via FilterExpression
// and the (small, per-shard) result is sorted client-side since Aerospike
// query results are unordered.
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
	if s.closed.Load() {
		return nil, errClosed
	}

	filterExpr := as.ExpGreaterEq(as.ExpIntBin(binVout), as.ExpIntVal(int64(fromVout)))
	recs, err := s.queryEqual(ctx, s.rowsSet, binAK, akFor(pool, txid), filterExpr)
	if err != nil {
		return nil, err
	}
	rows, err := rowsFromRecords(recs)
	if err != nil {
		return nil, err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Vout < rows[j].Vout })
	if limit < len(rows) {
		rows = rows[:limit]
	}
	return rows, nil
}

func rowsFromRecords(recs []*as.Record) ([]store.Row, error) {
	out := make([]store.Row, len(recs))
	for i, rec := range recs {
		r, err := rowFromRecord(rec)
		if err != nil {
			return nil, err
		}
		out[i] = r
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
	if err := store.ValidateOutpoints(ops, maxBatch); err != nil {
		return nil, err
	}
	if s.closed.Load() {
		return nil, errClosed
	}

	keys := make([]*as.Key, len(ops))
	for i, op := range ops {
		k, err := s.rowKey(op)
		if err != nil {
			return nil, err
		}
		keys[i] = k
	}
	recs, aerr := s.client.BatchGet(s.readBatchPolicy(ctx), keys)
	if aerr != nil {
		return nil, wrap(aerr)
	}
	out := make([]store.Row, 0, len(ops))
	for _, rec := range recs {
		if rec == nil {
			continue
		}
		r, err := rowFromRecord(rec)
		if err != nil {
			return nil, err
		}
		if r.Pool != pool {
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

// Reserve implements store.Store. Each op is its own per-record CAS: a
// filter expression admits the write only when the row is available or
// already reserved under this very token (a replay), and the ra/ea/ua bins
// are themselves written by an expression that reads the record's own
// pre-write state to pick "set to the new value" on the available edge or
// "leave unchanged" on a replay (store.go, "Idempotency") -- all inside one
// atomic per-record operate. Aerospike returns the per-record results in
// input order, so no follow-up read (as mongodb needs) is required to learn
// which ops applied.
func (s *Store) Reserve(ctx context.Context, pool string, ops []store.ReserveOp) ([]store.ReserveOp, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateReserveOps(ops, maxBatch); err != nil {
		return nil, err
	}
	if s.closed.Load() {
		return nil, errClosed
	}

	now := s.now().Truncate(time.Microsecond)
	sorted := store.SortReserveOps(ops)

	records := make([]as.BatchRecordIfc, len(sorted))
	for i, op := range sorted {
		key, kerr := s.rowKey(op.Outpoint)
		if kerr != nil {
			return nil, kerr
		}
		bwp := s.batchWritePolicy(ctx)
		bwp.FilterExpression = reserveFilter(pool, op.Token)
		records[i] = as.NewBatchWrite(bwp, key, reserveOps(pool, op.Token, now, op.ExpiresAt.Truncate(time.Microsecond))...)
	}
	if aerr := s.client.BatchOperate(s.batchPolicy(ctx), records); aerr != nil {
		return nil, fmt.Errorf("%w: %w", store.ErrUnavailable, aerr)
	}

	applied, err := classifyBatch(records, types.FILTERED_OUT, types.KEY_NOT_FOUND_ERROR)
	if err != nil {
		return nil, err
	}
	out := make([]store.ReserveOp, 0, len(applied))
	for _, i := range applied {
		br := records[i].BatchRec()
		op := sorted[i]
		ea := int64(0)
		if br.Record != nil {
			ea = toInt64(br.Record.Bins[binEA])
		}
		out = append(out, store.ReserveOp{Outpoint: op.Outpoint, Token: op.Token, ExpiresAt: fromUS(ea)})
	}
	return out, nil
}

// reserveFilter is the CAS predicate for Reserve: available, or already
// reserved under this very token (a replay).
func reserveFilter(pool string, tok store.Token) *as.Expression {
	isAvail := as.ExpEq(as.ExpIntBin(binState), as.ExpIntVal(int64(store.StateAvailable)))
	isSameToken := as.ExpAnd(
		as.ExpEq(as.ExpIntBin(binState), as.ExpIntVal(int64(store.StateReserved))),
		as.ExpEq(as.ExpBlobBin(binRB), as.ExpBlobVal(tok[:])),
	)
	return as.ExpAnd(as.ExpEq(as.ExpStringBin(binPool), as.ExpStringVal(pool)), as.ExpOr(isAvail, isSameToken))
}

// reserveOps builds the per-record write for the available->reserved edge
// (or a same-token replay, which the ExpCond below leaves untouched).
func reserveOps(pool string, tok store.Token, now, exp time.Time) []*as.Operation {
	isAvail := as.ExpEq(as.ExpIntBin(binState), as.ExpIntVal(int64(store.StateAvailable)))
	raExpr := as.ExpCond(isAvail, as.ExpIntVal(us(now)), as.ExpIntBin(binRA))
	eaExpr := as.ExpCond(isAvail, as.ExpIntVal(us(exp)), as.ExpIntBin(binEA))
	uaExpr := as.ExpCond(isAvail, as.ExpIntVal(us(now)), as.ExpIntBin(binUA))
	return []*as.Operation{
		as.ExpWriteOp(binRA, raExpr, as.ExpWriteFlagDefault),
		as.ExpWriteOp(binEA, eaExpr, as.ExpWriteFlagDefault),
		as.ExpWriteOp(binUA, uaExpr, as.ExpWriteFlagDefault),
		as.PutOp(as.NewBin(binState, int64(store.StateReserved))),
		as.PutOp(as.NewBin(binRB, tok[:])),
		as.PutOp(as.NewBin(binPST, pstFor(pool, store.StateReserved))),
		as.PutOp(as.NewBin(binAK, nil)),
		as.GetBinOp(binEA),
	}
}

// Release implements store.Store.
func (s *Store) Release(ctx context.Context, pool string, token store.Token, ops []store.Outpoint) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	if err := store.ValidateOutpoints(ops, maxBatch); err != nil {
		return 0, err
	}
	if token.IsZero() {
		return 0, store.ErrInvalidArgument
	}
	if s.closed.Load() {
		return 0, errClosed
	}

	now := s.now().Truncate(time.Microsecond)
	sorted := store.SortOutpoints(ops)
	filterExpr := as.ExpAnd(
		as.ExpEq(as.ExpStringBin(binPool), as.ExpStringVal(pool)),
		as.ExpEq(as.ExpIntBin(binState), as.ExpIntVal(int64(store.StateReserved))),
		as.ExpEq(as.ExpBlobBin(binRB), as.ExpBlobVal(token[:])),
	)
	return s.mutateRowsCount(ctx, sorted, filterExpr, func(op store.Outpoint) []*as.Operation {
		return releaseOrResolveAvailableOps(pool, op, now)
	})
}

// releaseOrResolveAvailableOps is the write both Release and Resolve-to-
// available use: return to available, restoring ak and clearing every
// provenance field (store.go, "Provenance": "Only Release, or Resolve to
// available, clears all five").
func releaseOrResolveAvailableOps(pool string, op store.Outpoint, now time.Time) []*as.Operation {
	return []*as.Operation{
		as.PutOp(as.NewBin(binState, int64(store.StateAvailable))),
		as.PutOp(as.NewBin(binPST, pstFor(pool, store.StateAvailable))),
		as.PutOp(as.NewBin(binAK, akFor(pool, op.TxID))),
		as.PutOp(as.NewBin(binUA, us(now))),
		as.PutOp(as.NewBin(binRB, nil)),
		as.PutOp(as.NewBin(binRA, nil)),
		as.PutOp(as.NewBin(binEA, nil)),
		as.PutOp(as.NewBin(binQA, nil)),
		as.PutOp(as.NewBin(binHint, nil)),
	}
}

// Spend implements store.Store. ea/ra/rb are never removed here: store.go's
// "Provenance" rule keeps them across every transition that is not a return
// to available, and the ea NUMERIC index is only ever queried (Quarantine)
// together with a state==reserved FilterExpression, so a value left behind
// by an earlier reservation cannot produce a wrong result there, only a
// larger index (see bins.go).
func (s *Store) Spend(ctx context.Context, pool string, token store.Token, ops []store.Outpoint, txid chainhash.Hash, terminal bool) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	if err := store.ValidateOutpoints(ops, maxBatch); err != nil {
		return 0, err
	}
	if token.IsZero() {
		return 0, store.ErrInvalidArgument
	}
	if err := store.ValidateSpend(txid, terminal); err != nil {
		return 0, err
	}
	if s.closed.Load() {
		return 0, errClosed
	}

	now := s.now().Truncate(time.Microsecond)
	sorted := store.SortOutpoints(ops)
	filterExpr := as.ExpAnd(
		as.ExpEq(as.ExpStringBin(binPool), as.ExpStringVal(pool)),
		as.ExpEq(as.ExpIntBin(binState), as.ExpIntVal(int64(store.StateReserved))),
		as.ExpEq(as.ExpBlobBin(binRB), as.ExpBlobVal(token[:])),
	)
	return s.mutateRowsCount(ctx, sorted, filterExpr, func(store.Outpoint) []*as.Operation {
		if terminal {
			return []*as.Operation{
				as.PutOp(as.NewBin(binState, int64(store.StateSpent))),
				as.PutOp(as.NewBin(binSB, txid[:])),
				as.PutOp(as.NewBin(binPST, pstFor(pool, store.StateSpent))),
				as.PutOp(as.NewBin(binUA, us(now))),
			}
		}
		spendOps := []*as.Operation{
			as.PutOp(as.NewBin(binState, int64(store.StateQuarantined))),
			as.PutOp(as.NewBin(binQA, us(now))),
			as.PutOp(as.NewBin(binPST, pstFor(pool, store.StateQuarantined))),
			as.PutOp(as.NewBin(binUA, us(now))),
		}
		if txid != (chainhash.Hash{}) {
			spendOps = append(spendOps, as.PutOp(as.NewBin(binHint, txid[:])))
		}
		return spendOps
	})
}

// Quarantine implements store.Store: due rows are discovered by a range
// query on the ea NUMERIC index (gated by state == reserved), sorted
// client-side (Aerospike query results are unordered) and capped at limit,
// then moved one at a time via BatchOperate, each record's own filter
// re-checking state and expiry so a row a concurrent Release/Reserve-replay
// already moved away from reserved is simply skipped.
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
	if s.closed.Load() {
		return 0, errClosed
	}

	beforeUS := us(before.Truncate(time.Microsecond))
	stateReservedExpr := as.ExpAnd(
		as.ExpEq(as.ExpStringBin(binPool), as.ExpStringVal(pool)),
		as.ExpEq(as.ExpIntBin(binState), as.ExpIntVal(int64(store.StateReserved))),
	)
	stmt := as.NewStatement(s.ns, s.rowsSet)
	stmt.Filter = as.NewRangeFilter(binEA, 0, beforeUS-1)
	qp := s.queryPolicy(ctx)
	qp.FilterExpression = stateReservedExpr
	rs, aerr := s.client.Query(qp, stmt)
	if aerr != nil {
		return 0, wrap(aerr)
	}
	recs, err := collectRecords(rs)
	if err != nil {
		return 0, err
	}
	rows, err := rowsFromRecords(recs)
	if err != nil {
		return 0, err
	}
	sort.Slice(rows, func(i, j int) bool { return lessQuarantineCandidate(rows[i], rows[j]) })
	if limit < len(rows) {
		rows = rows[:limit]
	}
	if len(rows) == 0 {
		return 0, nil
	}

	now := s.now().Truncate(time.Microsecond)
	mutateFilter := as.ExpAnd(
		as.ExpEq(as.ExpIntBin(binState), as.ExpIntVal(int64(store.StateReserved))),
		as.ExpLess(as.ExpIntBin(binEA), as.ExpIntVal(beforeUS)),
	)
	return s.mutateRowsCount(ctx, rowOutpoints(rows), mutateFilter, func(store.Outpoint) []*as.Operation {
		return []*as.Operation{
			as.PutOp(as.NewBin(binState, int64(store.StateQuarantined))),
			as.PutOp(as.NewBin(binQA, us(now))),
			as.PutOp(as.NewBin(binPST, pstFor(pool, store.StateQuarantined))),
			as.PutOp(as.NewBin(binUA, us(now))),
		}
	})
}

// rowOutpoints extracts the outpoints of rows, in order.
func rowOutpoints(rows []store.Row) []store.Outpoint {
	out := make([]store.Outpoint, len(rows))
	for i, r := range rows {
		out[i] = r.Outpoint
	}
	return out
}

// lessQuarantineCandidate orders Quarantine's own candidate selection by
// (expires_at, txid internal bytes, vout).
func lessQuarantineCandidate(a, b store.Row) bool {
	if !a.ExpiresAt.Equal(b.ExpiresAt) {
		return a.ExpiresAt.Before(b.ExpiresAt)
	}
	return lessOutpointBytes(a.Outpoint, b.Outpoint)
}

// lessQuarantinedOrder orders ListQuarantined by
// (quarantined_at, txid internal bytes, vout).
func lessQuarantinedOrder(a, b store.Row) bool {
	if !a.QuarantinedAt.Equal(b.QuarantinedAt) {
		return a.QuarantinedAt.Before(b.QuarantinedAt)
	}
	return lessOutpointBytes(a.Outpoint, b.Outpoint)
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
	if s.closed.Load() {
		return nil, errClosed
	}

	beforeUS := us(before.Truncate(time.Microsecond))
	var afterUS int64
	var afterOp store.Outpoint
	hasAfter := after != nil
	if hasAfter {
		afterUS = us(after.At.Truncate(time.Microsecond))
		afterOp = after.Outpoint
	}

	filterExpr := as.ExpAnd(
		as.ExpEq(as.ExpStringBin(binPool), as.ExpStringVal(pool)),
		as.ExpEq(as.ExpIntBin(binState), as.ExpIntVal(int64(store.StateQuarantined))),
	)
	stmt := as.NewStatement(s.ns, s.rowsSet)
	stmt.Filter = as.NewRangeFilter(binQA, afterUS, beforeUS-1)
	qp := s.queryPolicy(ctx)
	qp.FilterExpression = filterExpr
	rs, aerr := s.client.Query(qp, stmt)
	if aerr != nil {
		return nil, wrap(aerr)
	}
	recs, err := collectRecords(rs)
	if err != nil {
		return nil, err
	}
	rows, err := rowsFromRecords(recs)
	if err != nil {
		return nil, err
	}

	if hasAfter {
		filtered := rows[:0]
		for _, r := range rows {
			cur := store.RowCursor{At: r.QuarantinedAt, Outpoint: r.Outpoint}
			if quarantineCursorAfter(cur, store.RowCursor{At: fromUS(afterUS), Outpoint: afterOp}) {
				filtered = append(filtered, r)
			}
		}
		rows = filtered
	}

	sort.Slice(rows, func(i, j int) bool { return lessQuarantinedOrder(rows[i], rows[j]) })
	if limit < len(rows) {
		rows = rows[:limit]
	}
	return rows, nil
}

// quarantineCursorAfter reports whether cur is strictly after ref in the
// (quarantined_at, txid internal bytes, vout) order.
func quarantineCursorAfter(cur, ref store.RowCursor) bool {
	if !cur.At.Equal(ref.At) {
		return cur.At.After(ref.At)
	}
	return lessOutpointBytes(ref.Outpoint, cur.Outpoint)
}

// Resolve implements store.Store.
func (s *Store) Resolve(ctx context.Context, pool string, ops []store.Outpoint, to store.State, spentBy *chainhash.Hash) (int, error) {
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
	if s.closed.Load() {
		return 0, errClosed
	}

	now := s.now().Truncate(time.Microsecond)
	sorted := store.SortOutpoints(ops)
	filterExpr := as.ExpAnd(
		as.ExpEq(as.ExpStringBin(binPool), as.ExpStringVal(pool)),
		as.ExpEq(as.ExpIntBin(binState), as.ExpIntVal(int64(store.StateQuarantined))),
	)
	if to == store.StateAvailable {
		return s.mutateRowsCount(ctx, sorted, filterExpr, func(op store.Outpoint) []*as.Operation {
			return releaseOrResolveAvailableOps(pool, op, now)
		})
	}
	return s.mutateRowsCount(ctx, sorted, filterExpr, func(store.Outpoint) []*as.Operation {
		return []*as.Operation{
			as.PutOp(as.NewBin(binState, int64(store.StateSpent))),
			as.PutOp(as.NewBin(binSB, spentBy[:])),
			as.PutOp(as.NewBin(binPST, pstFor(pool, store.StateSpent))),
			as.PutOp(as.NewBin(binUA, us(now))),
		}
	})
}

// Stats implements store.Store. Every count is its own secondary-index
// equality query with bin data suppressed: O(rows)/O(shards) in the pool,
// documented as such since Aerospike has no covering "count" aggregate.
func (s *Store) Stats(ctx context.Context, pool string) (store.Stats, error) {
	if err := ctx.Err(); err != nil {
		return store.Stats{}, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return store.Stats{}, err
	}
	if s.closed.Load() {
		return store.Stats{}, errClosed
	}

	var st store.Stats
	var err error
	if st.Available, err = s.countEqual(ctx, s.rowsSet, binPST, pstFor(pool, store.StateAvailable)); err != nil {
		return store.Stats{}, err
	}
	if st.Reserved, err = s.countEqual(ctx, s.rowsSet, binPST, pstFor(pool, store.StateReserved)); err != nil {
		return store.Stats{}, err
	}
	if st.Quarantined, err = s.countEqual(ctx, s.rowsSet, binPST, pstFor(pool, store.StateQuarantined)); err != nil {
		return store.Stats{}, err
	}
	if st.Spent, err = s.countEqual(ctx, s.rowsSet, binPST, pstFor(pool, store.StateSpent)); err != nil {
		return store.Stats{}, err
	}
	if st.Invalid, err = s.countEqual(ctx, s.rowsSet, binPST, pstFor(pool, store.StateInvalid)); err != nil {
		return store.Stats{}, err
	}
	if st.ShardsUnmined, err = s.countEqual(ctx, s.shardsSet, binPS, psFor(pool, store.ShardUnmined)); err != nil {
		return store.Stats{}, err
	}
	if st.ShardsMined, err = s.countEqual(ctx, s.shardsSet, binPS, psFor(pool, store.ShardMined)); err != nil {
		return store.Stats{}, err
	}
	if st.ShardsRetired, err = s.countEqual(ctx, s.shardsSet, binPS, psFor(pool, store.ShardRetired)); err != nil {
		return store.Stats{}, err
	}
	if st.ShardsInvalid, err = s.countEqual(ctx, s.shardsSet, binPS, psFor(pool, store.ShardInvalid)); err != nil {
		return store.Stats{}, err
	}
	return st, nil
}
