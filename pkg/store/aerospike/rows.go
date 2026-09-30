package aerospike

import (
	"context"
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
	if err := validatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateLimit(limit); err != nil {
		return nil, err
	}
	if s.closed.Load() {
		return nil, errClosed
	}

	filterExpr := as.ExpGreaterEq(as.ExpIntBin(binVout), as.ExpIntVal(int64(fromVout)))
	recs, err := s.queryEqual(ctx, s.rowsSet, binAK, s.akFor(pool, txid), filterExpr)
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
	if err := validatePool(pool); err != nil {
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
	if err := validatePool(pool); err != nil {
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
		records[i] = as.NewBatchWrite(bwp, key, s.reserveOps(pool, op.Token, now, op.ExpiresAt.Truncate(time.Microsecond))...)
	}
	if err := s.batchOperate(ctx, records); err != nil {
		return nil, err
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
func (s *Store) reserveOps(pool string, tok store.Token, now, exp time.Time) []*as.Operation {
	isAvail := as.ExpEq(as.ExpIntBin(binState), as.ExpIntVal(int64(store.StateAvailable)))
	raExpr := as.ExpCond(isAvail, as.ExpIntVal(us(now)), as.ExpIntBin(binRA))
	eaExpr := as.ExpCond(isAvail, as.ExpIntVal(us(exp)), as.ExpIntBin(binEA))
	uaExpr := as.ExpCond(isAvail, as.ExpIntVal(us(now)), as.ExpIntBin(binUA))
	// rx mirrors ea while reserved; on a replay it already holds ea.
	rxExpr := as.ExpCond(isAvail, as.ExpIntVal(us(exp)), as.ExpIntBin(binEA))
	return []*as.Operation{
		as.ExpWriteOp(binRA, raExpr, as.ExpWriteFlagDefault),
		as.ExpWriteOp(binEA, eaExpr, as.ExpWriteFlagDefault),
		as.ExpWriteOp(binRX, rxExpr, as.ExpWriteFlagDefault),
		as.ExpWriteOp(binUA, uaExpr, as.ExpWriteFlagDefault),
		as.PutOp(as.NewBin(binState, int64(store.StateReserved))),
		as.PutOp(as.NewBin(binRB, tok[:])),
		as.PutOp(as.NewBin(binPST, s.pstFor(pool, store.StateReserved))),
		as.PutOp(as.NewBin(binAK, nil)),
		as.GetBinOp(binEA),
	}
}

// Release implements store.Store.
func (s *Store) Release(ctx context.Context, pool string, token store.Token, ops []store.Outpoint) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := validatePool(pool); err != nil {
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
		return s.releaseOrResolveAvailableOps(pool, op, now)
	})
}

// releaseOrResolveAvailableOps is the write both Release and Resolve-to-
// available use: return to available, restoring ak and clearing every
// provenance field (store.go, "Provenance": "Only Release, or Resolve to
// available, clears all five").
func (s *Store) releaseOrResolveAvailableOps(pool string, op store.Outpoint, now time.Time) []*as.Operation {
	return []*as.Operation{
		as.PutOp(as.NewBin(binState, int64(store.StateAvailable))),
		as.PutOp(as.NewBin(binPST, s.pstFor(pool, store.StateAvailable))),
		as.PutOp(as.NewBin(binAK, s.akFor(pool, op.TxID))),
		as.PutOp(as.NewBin(binUA, us(now))),
		as.PutOp(as.NewBin(binRB, nil)),
		as.PutOp(as.NewBin(binRA, nil)),
		as.PutOp(as.NewBin(binEA, nil)),
		as.PutOp(as.NewBin(binQA, nil)),
		as.PutOp(as.NewBin(binHint, nil)),
		as.PutOp(as.NewBin(binRX, nil)),
		as.PutOp(as.NewBin(binQX, nil)),
	}
}

// Spend implements store.Store. ea/ra/rb are kept for provenance (store.go
// "Provenance"); only the index-only rx bin is removed, so the row leaves
// Quarantine's discovery index in the same atomic write (see bins.go).
func (s *Store) Spend(ctx context.Context, pool string, token store.Token, ops []store.Outpoint, txid chainhash.Hash, terminal bool) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := validatePool(pool); err != nil {
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
				as.PutOp(as.NewBin(binPST, s.pstFor(pool, store.StateSpent))),
				as.PutOp(as.NewBin(binUA, us(now))),
				as.PutOp(as.NewBin(binRX, nil)),
			}
		}
		spendOps := []*as.Operation{
			as.PutOp(as.NewBin(binState, int64(store.StateQuarantined))),
			as.PutOp(as.NewBin(binQA, us(now))),
			as.PutOp(as.NewBin(binQX, us(now))),
			as.PutOp(as.NewBin(binRX, nil)),
			as.PutOp(as.NewBin(binPST, s.pstFor(pool, store.StateQuarantined))),
			as.PutOp(as.NewBin(binUA, us(now))),
		}
		if txid != (chainhash.Hash{}) {
			spendOps = append(spendOps, as.PutOp(as.NewBin(binHint, txid[:])))
		}
		return spendOps
	})
}

// Quarantine implements store.Store: due rows are discovered by a range
// query on the rx NUMERIC index, which only holds rows currently reserved
// (see bins.go), so terminal rows never cost anything here. Aerospike query
// results are unordered, so the limit smallest (expires_at, txid, vout)
// candidates are kept in a bounded max-heap while streaming (O(limit)
// memory, and only txid/vout/ea are transferred), then moved via one
// BatchOperate whose per-record filter re-checks state and expiry, so a row
// a concurrent Release/Spend already moved away from reserved is skipped.
//
// The limit cannot be pushed into the query (MaxRecords) without breaking
// the contract's pick order: the server returns an arbitrary subset.
//
// A before at or before the Unix epoch, or a zero before, matches nothing
// (every stored expiry is strictly after the epoch; see store.ValidateTime)
// and returns 0 without querying.
func (s *Store) Quarantine(ctx context.Context, pool string, before time.Time, limit int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := validatePool(pool); err != nil {
		return 0, err
	}
	if err := store.ValidateLimit(limit); err != nil {
		return 0, err
	}
	if s.closed.Load() {
		return 0, errClosed
	}

	beforeUS := us(before.Truncate(time.Microsecond))
	if beforeUS <= 1 {
		return 0, nil // matches nothing: no expiry is <= the epoch
	}
	stmt := as.NewStatement(s.ns, s.rowsSet, binTxID, binVout, binEA)
	stmt.Filter = as.NewRangeFilter(binRX, 1, beforeUS-1)
	qp := s.queryPolicy(ctx)
	qp.FilterExpression = as.ExpAnd(
		s.tenantExpr(),
		as.ExpEq(as.ExpStringBin(binPool), as.ExpStringVal(pool)),
		as.ExpEq(as.ExpIntBin(binState), as.ExpIntVal(int64(store.StateReserved))),
	)
	top := newBoundedTop(limit, lessQuarantineCandidate)
	var derr error
	err := s.query(qp, stmt, func(rec *as.Record) {
		if derr != nil {
			return
		}
		op, err := outpointFromBins(rec.Bins)
		if err != nil {
			derr = err
			return
		}
		top.push(store.Row{Outpoint: op, ExpiresAt: fromUS(toInt64(rec.Bins[binEA]))})
	})
	if err != nil {
		return 0, err
	}
	if derr != nil {
		return 0, derr
	}
	rows := top.sorted()
	if len(rows) == 0 {
		return 0, nil
	}

	now := s.now().Truncate(time.Microsecond)
	mutateFilter := as.ExpAnd(
		as.ExpEq(as.ExpStringBin(binPool), as.ExpStringVal(pool)),
		as.ExpEq(as.ExpIntBin(binState), as.ExpIntVal(int64(store.StateReserved))),
		as.ExpLess(as.ExpIntBin(binEA), as.ExpIntVal(beforeUS)),
	)
	return s.mutateRowsCount(ctx, rowOutpoints(rows), mutateFilter, func(store.Outpoint) []*as.Operation {
		return []*as.Operation{
			as.PutOp(as.NewBin(binState, int64(store.StateQuarantined))),
			as.PutOp(as.NewBin(binQA, us(now))),
			as.PutOp(as.NewBin(binQX, us(now))),
			as.PutOp(as.NewBin(binRX, nil)),
			as.PutOp(as.NewBin(binPST, s.pstFor(pool, store.StateQuarantined))),
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

// ListQuarantined implements store.Store. Discovery is a two-phase read, the
// same shape Quarantine already uses for its own discovery: a range query on
// the qx NUMERIC index (which only holds rows currently quarantined),
// projected to just the bins needed to order and identify a candidate --
// txid, vout, qx -- keeps the top limit candidates in (quarantined_at, txid
// internal bytes, vout) order in a bounded heap while streaming, starting at
// the cursor's quarantined_at. Only those selected limit keys are then
// fetched in full, by one BatchGet.
//
// Without the projection, this query (unlike Quarantine's) used to name no
// bins at all, so it transferred a full record -- including both scripts --
// for every row in [after, before), on every single page, not just the
// limit rows the page returns: paging a backlog of n quarantined rows with
// page size limit cost O(n) full-record transfers per page and
// O(n^2/limit) overall for a full walk, instead of O(limit) full records
// per page and O(n) overall.
//
// The fetch phase (fetchQuarantinedRows) re-checks each candidate against
// its own current record and drops any that changed between the two phases
// (Resolved away, or re-quarantined at a different instant) rather than
// returning stale or mismatched data, so a page can come back shorter than
// limit even when more rows exist.
//
// A before at or before the epoch, or a cursor at or after before, matches
// nothing and returns an empty page without querying.
func (s *Store) ListQuarantined(ctx context.Context, pool string, before time.Time, after *store.RowCursor, limit int) ([]store.Row, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateLimit(limit); err != nil {
		return nil, err
	}
	if s.closed.Load() {
		return nil, errClosed
	}

	beforeUS := us(before.Truncate(time.Microsecond))
	var afterUS int64 = 1
	var ref *store.RowCursor
	if after != nil {
		afterUS = max(afterUS, us(after.At.Truncate(time.Microsecond)))
		ref = &store.RowCursor{At: fromUS(us(after.At.Truncate(time.Microsecond))), Outpoint: after.Outpoint}
	}
	if beforeUS <= 1 || afterUS >= beforeUS {
		return []store.Row{}, nil // matches nothing
	}

	stmt := as.NewStatement(s.ns, s.rowsSet, binTxID, binVout, binQX)
	stmt.Filter = as.NewRangeFilter(binQX, afterUS, beforeUS-1)
	qp := s.queryPolicy(ctx)
	qp.FilterExpression = as.ExpAnd(
		s.tenantExpr(),
		as.ExpEq(as.ExpStringBin(binPool), as.ExpStringVal(pool)),
		as.ExpEq(as.ExpIntBin(binState), as.ExpIntVal(int64(store.StateQuarantined))),
	)
	// Candidates carry only Outpoint and QuarantinedAt -- exactly what
	// lessQuarantinedOrder and quarantineCursorAfter need -- not a full Row;
	// the full Row for each of the limit kept candidates is fetched below.
	top := newBoundedTop(limit, lessQuarantinedOrder)
	var derr error
	err := s.query(qp, stmt, func(rec *as.Record) {
		if derr != nil {
			return
		}
		op, err := outpointFromBins(rec.Bins)
		if err != nil {
			derr = err
			return
		}
		cand := store.Row{Outpoint: op, QuarantinedAt: fromUS(toInt64(rec.Bins[binQX]))}
		if ref != nil && !quarantineCursorAfter(store.RowCursor{At: cand.QuarantinedAt, Outpoint: cand.Outpoint}, *ref) {
			return
		}
		top.push(cand)
	})
	if err != nil {
		return nil, err
	}
	if derr != nil {
		return nil, derr
	}
	candidates := top.sorted()
	if len(candidates) == 0 {
		return []store.Row{}, nil
	}
	return s.fetchQuarantinedRows(ctx, pool, candidates)
}

// fetchQuarantinedRows fetches the full row for each of candidates by key in
// one BatchGet, and keeps only the ones that are still exactly what
// discovery saw: same pool, state still quarantined, and QuarantinedAt
// unchanged from the qx value discovery read.
//
// BatchGet reads each key's CURRENT record, not a snapshot from discovery:
// a candidate that Resolved back to available between the two phases would
// otherwise come back with State == available and a zero QuarantinedAt
// (Resolve clears it), corrupting a reaper's oldest-age gauge and its resume
// cursor; one that was re-quarantined at a different instant in that same
// window would come back with a QuarantinedAt that no longer matches the
// position candidates was sorted into, breaking the documented
// (quarantined_at, txid, vout) page order. Dropping any such row instead of
// returning it is safe: ListQuarantined's contract allows a page to come
// back shorter than limit.
//
// The client returns records in candidates' own order, with a nil slot for
// any key not found (never expected here -- this package never deletes rows
// -- but skipped defensively, matching GetRows).
func (s *Store) fetchQuarantinedRows(ctx context.Context, pool string, candidates []store.Row) ([]store.Row, error) {
	keys := make([]*as.Key, len(candidates))
	for i, c := range candidates {
		k, err := s.rowKey(c.Outpoint)
		if err != nil {
			return nil, err
		}
		keys[i] = k
	}
	recs, aerr := s.client.BatchGet(s.readBatchPolicy(ctx), keys)
	if aerr != nil {
		return nil, wrap(aerr)
	}
	out := make([]store.Row, 0, len(candidates))
	for i, rec := range recs {
		if rec == nil {
			continue
		}
		r, err := rowFromRecord(rec)
		if err != nil {
			return nil, err
		}
		if r.Pool != pool || r.State != store.StateQuarantined || !r.QuarantinedAt.Equal(candidates[i].QuarantinedAt) {
			continue // changed between discovery and fetch: drop, don't return stale/mismatched data
		}
		out = append(out, r)
	}
	return out, nil
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
	if err := validatePool(pool); err != nil {
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
			return s.releaseOrResolveAvailableOps(pool, op, now)
		})
	}
	return s.mutateRowsCount(ctx, sorted, filterExpr, func(store.Outpoint) []*as.Operation {
		return []*as.Operation{
			as.PutOp(as.NewBin(binState, int64(store.StateSpent))),
			as.PutOp(as.NewBin(binSB, spentBy[:])),
			as.PutOp(as.NewBin(binPST, s.pstFor(pool, store.StateSpent))),
			as.PutOp(as.NewBin(binUA, us(now))),
			as.PutOp(as.NewBin(binQX, nil)),
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
	if err := validatePool(pool); err != nil {
		return store.Stats{}, err
	}
	if s.closed.Load() {
		return store.Stats{}, errClosed
	}

	var st store.Stats
	var err error
	if st.Available, err = s.countEqual(ctx, s.rowsSet, binPST, s.pstFor(pool, store.StateAvailable)); err != nil {
		return store.Stats{}, err
	}
	if st.Reserved, err = s.countEqual(ctx, s.rowsSet, binPST, s.pstFor(pool, store.StateReserved)); err != nil {
		return store.Stats{}, err
	}
	if st.Quarantined, err = s.countEqual(ctx, s.rowsSet, binPST, s.pstFor(pool, store.StateQuarantined)); err != nil {
		return store.Stats{}, err
	}
	if st.Spent, err = s.countEqual(ctx, s.rowsSet, binPST, s.pstFor(pool, store.StateSpent)); err != nil {
		return store.Stats{}, err
	}
	if st.Invalid, err = s.countEqual(ctx, s.rowsSet, binPST, s.pstFor(pool, store.StateInvalid)); err != nil {
		return store.Stats{}, err
	}
	if st.ShardsUnmined, err = s.countEqual(ctx, s.shardsSet, binPS, s.psFor(pool, store.ShardUnmined)); err != nil {
		return store.Stats{}, err
	}
	if st.ShardsMined, err = s.countEqual(ctx, s.shardsSet, binPS, s.psFor(pool, store.ShardMined)); err != nil {
		return store.Stats{}, err
	}
	if st.ShardsRetired, err = s.countEqual(ctx, s.shardsSet, binPS, s.psFor(pool, store.ShardRetired)); err != nil {
		return store.Stats{}, err
	}
	if st.ShardsInvalid, err = s.countEqual(ctx, s.shardsSet, binPS, s.psFor(pool, store.ShardInvalid)); err != nil {
		return store.Stats{}, err
	}
	return st, nil
}
