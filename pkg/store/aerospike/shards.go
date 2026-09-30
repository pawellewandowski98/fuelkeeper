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

// PutShard implements store.Store. Rows are inserted before the shard record
// (see the package doc comment): a visible shard therefore implies its rows
// exist, even though this backend has no multi-record atomicity.
func (s *Store) PutShard(ctx context.Context, shard store.Shard, rows []store.Row) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := store.ValidatePutShard(shard, rows); err != nil {
		return err
	}
	if s.closed.Load() {
		return errClosed
	}

	shardKey, err := s.shardKey(shard.Pool, shard.TxID)
	if err != nil {
		return err
	}

	existing, aerr := s.client.Get(s.readPolicy(ctx), shardKey)
	switch {
	case aerr == nil:
		cur, derr := shardFromRecord(existing, false)
		if derr != nil {
			return derr
		}
		if cur.Size != shard.Size || cur.Denomination != shard.Denomination || cur.LockMode != shard.LockMode {
			return store.ErrShardExists
		}
		return nil // idempotent replay
	case isKeyNotFound(aerr):
		// fall through: create it
	default:
		return wrap(aerr)
	}

	// Reserve the txid globally across pools before writing anything else.
	txidKey, err := s.txidKey(shard.TxID)
	if err != nil {
		return err
	}
	if aerr := s.client.Put(s.createPolicy(ctx), txidKey, as.BinMap{binPool: shard.Pool}); aerr != nil {
		if !aerr.Matches(types.KEY_EXISTS_ERROR) {
			return wrap(aerr)
		}
		owner, gerr := s.client.Get(s.readPolicy(ctx), txidKey, binPool)
		if gerr != nil {
			return wrap(gerr)
		}
		if stringBin(owner.Bins, binPool) != shard.Pool {
			return store.ErrShardExists
		}
		// Same pool: a concurrent/partial PutShard is in flight; continue.
	}

	now := s.now().Truncate(time.Microsecond)
	rowRecords := make([]as.BatchRecordIfc, len(rows))
	for i := range rows {
		r := rows[i]
		r.State = store.StateAvailable
		r.ReservedBy = store.Token{}
		r.ReservedAt, r.ExpiresAt, r.QuarantinedAt = time.Time{}, time.Time{}, time.Time{}
		r.SpentHint, r.SpentBy = nil, nil
		r.UpdatedAt = now
		key, kerr := s.rowKey(r.Outpoint)
		if kerr != nil {
			return kerr
		}
		bins := rowBins(r)
		ops := make([]*as.Operation, 0, len(bins))
		for name, val := range bins {
			ops = append(ops, as.PutOp(as.NewBin(name, val)))
		}
		rowRecords[i] = as.NewBatchWrite(s.batchCreatePolicy(ctx), key, ops...)
	}
	if len(rowRecords) > 0 {
		if aerr := s.client.BatchOperate(s.batchPolicy(ctx), rowRecords); aerr != nil {
			return fmt.Errorf("%w: %w", store.ErrUnavailable, aerr)
		}
		if _, cerr := classifyBatch(rowRecords, types.KEY_EXISTS_ERROR); cerr != nil {
			return cerr
		}
		// every failure was a tolerated duplicate key: rows already exist from a
		// partial prior write (PutShard never touches an existing row).
	}

	sd := shardBins(&store.Shard{
		Pool: shard.Pool, TxID: shard.TxID, Size: shard.Size, Denomination: shard.Denomination,
		LockMode: shard.LockMode, Status: shard.Status, BEEF: shard.BEEF, CreatedAt: now, UpdatedAt: now,
	})
	if aerr := s.client.Put(s.createPolicy(ctx), shardKey, sd); aerr != nil {
		if !aerr.Matches(types.KEY_EXISTS_ERROR) {
			return wrap(aerr)
		}
		// A concurrent, identical PutShard just landed: compare and settle.
		cur, gerr := s.client.Get(s.readPolicy(ctx), shardKey)
		if gerr != nil {
			return wrap(gerr)
		}
		curShard, derr := shardFromRecord(cur, false)
		if derr != nil {
			return derr
		}
		if curShard.Size != shard.Size || curShard.Denomination != shard.Denomination || curShard.LockMode != shard.LockMode {
			return store.ErrShardExists
		}
	}
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
	if s.closed.Load() {
		return nil, errClosed
	}
	key, err := s.shardKey(pool, txid)
	if err != nil {
		return nil, err
	}
	rec, aerr := s.client.Get(s.readPolicy(ctx), key)
	if aerr != nil {
		if isKeyNotFound(aerr) {
			return nil, store.ErrNotFound
		}
		return nil, wrap(aerr)
	}
	sh, err := shardFromRecord(rec, true)
	if err != nil {
		return nil, err
	}
	return &sh, nil
}

// UpdateShard implements store.Store. It never touches the shard's lease.
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
	if s.closed.Load() {
		return errClosed
	}
	key, err := s.shardKey(pool, txid)
	if err != nil {
		return err
	}
	now := s.now().Truncate(time.Microsecond)

	wp := s.writePolicy(ctx)
	wp.FilterExpression = as.ExpOr(
		as.ExpEq(as.ExpIntBin(binStatus), as.ExpIntVal(int64(store.ShardUnmined))),
		as.ExpEq(as.ExpIntBin(binStatus), as.ExpIntVal(int64(store.ShardMined))),
	)
	ops := []*as.Operation{
		as.PutOp(as.NewBin(binStatus, int64(status))),
		as.PutOp(as.NewBin(binPS, psFor(pool, status))),
		as.PutOp(as.NewBin(binUA, us(now))),
		as.AddOp(as.NewBin(binVer, int64(1))),
	}
	if beef != nil {
		ops = append(ops, as.PutOp(as.NewBin(binBEEF, beef)))
	}
	_, aerr := s.client.Operate(wp, key, ops...)
	switch {
	case aerr == nil:
		return nil
	case isKeyNotFound(aerr):
		return store.ErrNotFound
	case isFiltered(aerr):
		return store.ErrTerminalShard
	default:
		return wrap(aerr)
	}
}

// RetireShard implements store.Store.
func (s *Store) RetireShard(ctx context.Context, pool string, txid chainhash.Hash) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	if s.closed.Load() {
		return 0, errClosed
	}
	key, err := s.shardKey(pool, txid)
	if err != nil {
		return 0, err
	}
	now := s.now().Truncate(time.Microsecond)

	wp := s.writePolicy(ctx)
	wp.FilterExpression = as.ExpNotEq(as.ExpIntBin(binStatus), as.ExpIntVal(int64(store.ShardInvalid)))
	ops := []*as.Operation{
		as.PutOp(as.NewBin(binStatus, int64(store.ShardInvalid))),
		as.PutOp(as.NewBin(binPS, psFor(pool, store.ShardInvalid))),
		as.PutOp(as.NewBin(binOwner, "")),
		as.PutOp(as.NewBin(binLU, int64(0))),
		as.PutOp(as.NewBin(binUA, us(now))),
		as.AddOp(as.NewBin(binVer, int64(1))),
	}
	_, aerr := s.client.Operate(wp, key, ops...)
	switch {
	case aerr == nil:
		// fall through to the row cascade below
	case isKeyNotFound(aerr):
		return 0, store.ErrNotFound
	case isFiltered(aerr):
		return 0, nil // already invalid: idempotent no-op
	default:
		return 0, wrap(aerr)
	}

	candidates, err := s.retireCascadeCandidates(ctx, pool, txid)
	if err != nil {
		return 0, err
	}
	if len(candidates) == 0 {
		return 0, nil
	}

	cascadeFilter := as.ExpAnd(
		as.ExpEq(as.ExpStringBin(binPool), as.ExpStringVal(pool)),
		as.ExpOr(
			as.ExpEq(as.ExpIntBin(binState), as.ExpIntVal(int64(store.StateAvailable))),
			as.ExpEq(as.ExpIntBin(binState), as.ExpIntVal(int64(store.StateReserved))),
			as.ExpEq(as.ExpIntBin(binState), as.ExpIntVal(int64(store.StateQuarantined))),
		),
	)
	return s.mutateRowsCount(ctx, candidates, cascadeFilter, func(store.Outpoint) []*as.Operation {
		return []*as.Operation{
			as.PutOp(as.NewBin(binState, int64(store.StateInvalid))),
			as.PutOp(as.NewBin(binPST, pstFor(pool, store.StateInvalid))),
			as.PutOp(as.NewBin(binUA, us(now))),
			as.PutOp(as.NewBin(binAK, nil)), // no-op unless the row was available
		}
	})
}

// retireCascadeCandidates discovers every non-terminal row of (pool, txid):
// available rows are found directly via the shard-scoped ak index; reserved
// and quarantined rows have no shard-scoped index, so every reserved/
// quarantined row of the whole pool is scanned and filtered client-side by
// txid (documented O(pool rows), matching Stats).
func (s *Store) retireCascadeCandidates(ctx context.Context, pool string, txid chainhash.Hash) ([]store.Outpoint, error) {
	seen := make(map[store.Outpoint]struct{})
	var out []store.Outpoint
	add := func(op store.Outpoint) {
		if _, dup := seen[op]; dup {
			return
		}
		seen[op] = struct{}{}
		out = append(out, op)
	}

	avail, err := s.queryEqual(ctx, s.rowsSet, binAK, akFor(pool, txid), nil)
	if err != nil {
		return nil, err
	}
	for _, rec := range avail {
		op, derr := outpointFromBins(rec.Bins)
		if derr != nil {
			return nil, derr
		}
		add(op)
	}

	for _, st := range []store.State{store.StateReserved, store.StateQuarantined} {
		recs, qerr := s.queryEqual(ctx, s.rowsSet, binPST, pstFor(pool, st), nil)
		if qerr != nil {
			return nil, qerr
		}
		for _, rec := range recs {
			op, derr := outpointFromBins(rec.Bins)
			if derr != nil {
				return nil, derr
			}
			if op.TxID != txid {
				continue
			}
			add(op)
		}
	}
	return out, nil
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
	if s.closed.Load() {
		return nil, errClosed
	}

	now = now.Truncate(time.Microsecond)
	until = until.Truncate(time.Microsecond)
	excluded := make(map[chainhash.Hash]struct{}, len(exclude))
	for _, h := range exclude {
		excluded[h] = struct{}{}
	}

	var candidates []store.Shard
	for _, status := range statuses {
		recs, err := s.queryEqual(ctx, s.shardsSet, binPS, psFor(pool, status), nil)
		if err != nil {
			return nil, err
		}
		for _, rec := range recs {
			sh, derr := shardFromRecord(rec, false)
			if derr != nil {
				return nil, derr
			}
			if _, skip := excluded[sh.TxID]; skip {
				continue
			}
			if sh.Owner != "" && sh.LeaseUntil.After(now) {
				continue // held, unexpired
			}
			candidates = append(candidates, sh)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if !candidates[i].CreatedAt.Equal(candidates[j].CreatedAt) {
			return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
		}
		return lessOutpointBytes(store.Outpoint{TxID: candidates[i].TxID}, store.Outpoint{TxID: candidates[j].TxID})
	})

	storeNow := s.now().Truncate(time.Microsecond)
	for _, cand := range candidates {
		key, err := s.shardKey(pool, cand.TxID)
		if err != nil {
			return nil, err
		}
		wp := s.writePolicy(ctx)
		wp.FilterExpression = as.ExpAnd(
			as.ExpEq(as.ExpStringBin(binPool), as.ExpStringVal(pool)),
			as.ExpOr(
				as.ExpEq(as.ExpStringBin(binOwner), as.ExpStringVal("")),
				as.ExpLessEq(as.ExpIntBin(binLU), as.ExpIntVal(us(now))),
			),
		)
		ops := []*as.Operation{
			as.PutOp(as.NewBin(binOwner, holder)),
			as.PutOp(as.NewBin(binLU, us(until))),
			as.PutOp(as.NewBin(binUA, us(storeNow))),
			as.GetOp(),
		}
		rec, aerr := s.client.Operate(wp, key, ops...)
		switch {
		case aerr == nil:
			sh, derr := shardFromRecord(rec, true)
			if derr != nil {
				return nil, derr
			}
			return &sh, nil
		case isFiltered(aerr), isKeyNotFound(aerr):
			continue // lost the race for this candidate; try the next
		default:
			return nil, wrap(aerr)
		}
	}
	return nil, nil //nolint:nilnil // contract: (nil, nil) means nothing adoptable
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
	if s.closed.Load() {
		return store.ShardHead{}, errClosed
	}
	key, err := s.shardKey(pool, txid)
	if err != nil {
		return store.ShardHead{}, err
	}
	until = until.Truncate(time.Microsecond)

	wp := s.writePolicy(ctx)
	wp.FilterExpression = as.ExpEq(as.ExpStringBin(binOwner), as.ExpStringVal(holder))
	rec, aerr := s.client.Operate(wp, key, as.PutOp(as.NewBin(binLU, us(until))), as.GetOp())
	if aerr == nil {
		sh, derr := shardFromRecord(rec, false)
		if derr != nil {
			return store.ShardHead{}, derr
		}
		return store.ShardHead{Status: sh.Status, Version: sh.Version}, nil
	}
	if isKeyNotFound(aerr) {
		return store.ShardHead{}, store.ErrNotFound
	}
	if !isFiltered(aerr) {
		return store.ShardHead{}, wrap(aerr)
	}
	if _, gerr := s.client.Get(s.readPolicy(ctx), key, binPool); gerr != nil {
		if isKeyNotFound(gerr) {
			return store.ShardHead{}, store.ErrNotFound
		}
		return store.ShardHead{}, wrap(gerr)
	}
	return store.ShardHead{}, store.ErrNotOwner
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
	if s.closed.Load() {
		return errClosed
	}
	key, err := s.shardKey(pool, txid)
	if err != nil {
		return err
	}
	now := s.now().Truncate(time.Microsecond)

	wp := s.writePolicy(ctx)
	wp.FilterExpression = as.ExpEq(as.ExpStringBin(binOwner), as.ExpStringVal(holder))
	ops := []*as.Operation{
		as.PutOp(as.NewBin(binOwner, "")),
		as.PutOp(as.NewBin(binLU, int64(0))),
		as.PutOp(as.NewBin(binUA, us(now))),
	}
	_, aerr := s.client.Operate(wp, key, ops...)
	if aerr == nil {
		return nil
	}
	if isKeyNotFound(aerr) {
		return store.ErrNotFound
	}
	if !isFiltered(aerr) {
		return wrap(aerr)
	}
	// Exists, but holder isn't the owner: a legitimate no-op. Confirm existence
	// so an unknown shard still reports ErrNotFound.
	_, gerr := s.client.Get(s.readPolicy(ctx), key)
	if gerr != nil {
		if isKeyNotFound(gerr) {
			return store.ErrNotFound
		}
		return wrap(gerr)
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
	if s.closed.Load() {
		return nil, errClosed
	}

	statuses := f.Statuses
	if len(statuses) == 0 {
		statuses = []store.ShardStatus{store.ShardUnmined, store.ShardMined, store.ShardRetired, store.ShardInvalid}
	}

	var out []store.Shard
	for _, status := range statuses {
		recs, err := s.queryEqual(ctx, s.shardsSet, binPS, psFor(pool, status), nil)
		if err != nil {
			return nil, err
		}
		for _, rec := range recs {
			sh, derr := shardFromRecord(rec, false)
			if derr != nil {
				return nil, derr
			}
			if f.Owner != nil && sh.Owner != *f.Owner {
				continue
			}
			out = append(out, sh)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return lessOutpointBytes(store.Outpoint{TxID: out[i].TxID}, store.Outpoint{TxID: out[j].TxID})
	})
	if f.Limit > 0 && f.Limit < len(out) {
		out = out[:f.Limit]
	}
	return out, nil
}
