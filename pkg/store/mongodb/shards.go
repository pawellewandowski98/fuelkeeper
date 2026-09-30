package mongodb

import (
	"context"
	"encoding/hex"
	"errors"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// PutShard implements store.Store. Backends without multi-record atomicity
// write rows first and the shard record last (see store.go), so rows are
// inserted before the shard document below.
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
	octx, cancel := s.op(ctx)
	defer cancel()

	id := shardID(shard.Pool, shard.TxID)

	var existing shardDoc
	err := s.shards.FindOne(octx, bson.D{{Key: "_id", Value: id}}).Decode(&existing)
	switch {
	case err == nil:
		if !sameShardParams(existing, shard) {
			return store.ErrShardExists
		}
		return nil // idempotent replay
	case errors.Is(err, mongo.ErrNoDocuments):
		// fall through: create it
	default:
		return wrap(err)
	}

	// Reserve the txid globally across pools before writing anything else.
	txidHex := hex.EncodeToString(shard.TxID[:])
	if _, err := s.txids.InsertOne(octx, bson.D{{Key: "_id", Value: txidHex}, {Key: "p", Value: shard.Pool}}); err != nil {
		if !mongo.IsDuplicateKeyError(err) {
			return wrap(err)
		}
		var owner struct {
			Pool string `bson:"p"`
		}
		if gerr := s.txids.FindOne(octx, bson.D{{Key: "_id", Value: txidHex}}).Decode(&owner); gerr != nil {
			return wrap(gerr)
		}
		if owner.Pool != shard.Pool {
			return store.ErrShardExists
		}
		// Same pool: a concurrent/partial PutShard is in flight; continue.
	}

	now := s.now().Truncate(time.Microsecond)
	docs := make([]any, len(rows))
	for i := range rows {
		r := rows[i]
		r.State = store.StateAvailable
		r.ReservedBy = store.Token{}
		r.ReservedAt, r.ExpiresAt, r.QuarantinedAt = time.Time{}, time.Time{}, time.Time{}
		r.SpentHint, r.SpentBy = nil, nil
		r.UpdatedAt = now
		docs[i] = newRowDoc(&r)
	}
	if _, err := s.rows.InsertMany(octx, docs, options.InsertMany().SetOrdered(false)); err != nil {
		if !allDuplicateKey(err) {
			return wrap(err)
		}
		// every failure was a duplicate key: rows already exist from a partial
		// prior write (PutShard never touches an existing row).
	}

	sd := newShardDoc(&store.Shard{
		Pool: shard.Pool, TxID: shard.TxID, Size: shard.Size, Denomination: shard.Denomination,
		LockMode: shard.LockMode, Status: shard.Status, Owner: "", LeaseUntil: time.Time{},
		Version: 1, BEEF: shard.BEEF, CreatedAt: now, UpdatedAt: now,
	})
	if _, err := s.shards.InsertOne(octx, sd); err != nil {
		if !mongo.IsDuplicateKeyError(err) {
			return wrap(err)
		}
		// A concurrent, identical PutShard just landed: compare and settle.
		var cur shardDoc
		if gerr := s.shards.FindOne(octx, bson.D{{Key: "_id", Value: id}}).Decode(&cur); gerr != nil {
			return wrap(gerr)
		}
		if !sameShardParams(cur, shard) {
			return store.ErrShardExists
		}
	}
	return nil
}

func sameShardParams(d shardDoc, shard store.Shard) bool {
	return int(d.Size) == shard.Size && uint64(d.Denom) == shard.Denomination && //nolint:gosec // stored from uint64
		store.LockMode(d.Mode) == shard.LockMode //nolint:gosec // small enum
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
	octx, cancel := s.op(ctx)
	defer cancel()

	var d shardDoc
	err := s.shards.FindOne(octx, bson.D{{Key: "_id", Value: shardID(pool, txid)}}).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, wrap(err)
	}
	sh, err := toShard(d, true)
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
	octx, cancel := s.op(ctx)
	defer cancel()

	id := shardID(pool, txid)
	now := s.now().Truncate(time.Microsecond)
	set := bson.D{{Key: "s", Value: int32(status)}, {Key: "ua", Value: us(now)}}
	if beef != nil {
		set = append(set, bson.E{Key: "b", Value: beef})
	}
	filter := bson.D{
		{Key: "_id", Value: id},
		{Key: "s", Value: bson.D{{Key: "$in", Value: bson.A{int32(store.ShardUnmined), int32(store.ShardMined)}}}},
	}
	update := bson.D{{Key: "$set", Value: set}, {Key: "$inc", Value: bson.D{{Key: "ver", Value: int64(1)}}}}
	res, err := s.shards.UpdateOne(octx, filter, update)
	if err != nil {
		return wrap(err)
	}
	if res.MatchedCount > 0 {
		return nil
	}
	err = s.shards.FindOne(octx, bson.D{{Key: "_id", Value: id}}).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return store.ErrNotFound
	}
	if err != nil {
		return wrap(err)
	}
	return store.ErrTerminalShard
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
	octx, cancel := s.op(ctx)
	defer cancel()

	id := shardID(pool, txid)
	now := s.now().Truncate(time.Microsecond)

	filter := bson.D{{Key: "_id", Value: id}, {Key: "s", Value: bson.D{{Key: "$ne", Value: int32(store.ShardInvalid)}}}}
	update := bson.D{
		{Key: "$set", Value: bson.D{{Key: "s", Value: int32(store.ShardInvalid)}, {Key: "o", Value: ""}, {Key: "ua", Value: us(now)}}},
		{Key: "$unset", Value: bson.D{{Key: "lu", Value: ""}}},
		{Key: "$inc", Value: bson.D{{Key: "ver", Value: int64(1)}}},
	}
	res, err := s.shards.UpdateOne(octx, filter, update)
	if err != nil {
		return 0, wrap(err)
	}
	if res.MatchedCount == 0 {
		err = s.shards.FindOne(octx, bson.D{{Key: "_id", Value: id}}).Err()
		if errors.Is(err, mongo.ErrNoDocuments) {
			return 0, store.ErrNotFound
		}
		if err != nil {
			return 0, wrap(err)
		}
		return 0, nil // already invalid: idempotent no-op
	}

	cascadeFilter := bson.D{
		{Key: "p", Value: pool},
		{Key: "sh", Value: txid[:]},
		{Key: "st", Value: bson.D{{Key: "$in", Value: bson.A{
			int32(store.StateAvailable), int32(store.StateReserved), int32(store.StateQuarantined),
		}}}},
	}
	cascadeUpdate := bson.D{{Key: "$set", Value: bson.D{{Key: "st", Value: int32(store.StateInvalid)}, {Key: "ua", Value: us(now)}}}}
	ures, err := s.rows.UpdateMany(octx, cascadeFilter, cascadeUpdate)
	if err != nil {
		return 0, wrap(err)
	}
	return int(ures.ModifiedCount), nil
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
	octx, cancel := s.op(ctx)
	defer cancel()

	now = now.Truncate(time.Microsecond)
	until = until.Truncate(time.Microsecond)

	sts := make(bson.A, len(statuses))
	for i, st := range statuses {
		sts[i] = int32(st)
	}
	ex := make(bson.A, len(exclude))
	for i, h := range exclude {
		ex[i] = h[:]
	}

	filter := bson.D{
		{Key: "p", Value: pool},
		{Key: "s", Value: bson.D{{Key: "$in", Value: sts}}},
		{Key: "t", Value: bson.D{{Key: "$nin", Value: ex}}},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "o", Value: ""}},
			bson.D{{Key: "lu", Value: bson.D{{Key: "$exists", Value: false}}}},
			bson.D{{Key: "lu", Value: bson.D{{Key: "$lte", Value: us(now)}}}},
		}},
	}
	storeNow := s.now().Truncate(time.Microsecond)
	update := bson.D{{Key: "$set", Value: bson.D{
		{Key: "o", Value: holder},
		{Key: "lu", Value: us(until)},
		{Key: "ua", Value: us(storeNow)},
	}}}
	opts := options.FindOneAndUpdate().
		SetSort(bson.D{{Key: "ca", Value: 1}, {Key: "t", Value: 1}}).
		SetReturnDocument(options.After)

	var d shardDoc
	err := s.shards.FindOneAndUpdate(octx, filter, update, opts).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil //nolint:nilnil // contract: (nil, nil) means nothing adoptable
	}
	if err != nil {
		return nil, wrap(err)
	}
	sh, err := toShard(d, true)
	if err != nil {
		return nil, err
	}
	return &sh, nil
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
	octx, cancel := s.op(ctx)
	defer cancel()

	id := shardID(pool, txid)
	until = until.Truncate(time.Microsecond)
	filter := bson.D{{Key: "_id", Value: id}, {Key: "o", Value: holder}}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "lu", Value: us(until)}}}}
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)

	var d shardDoc
	err := s.shards.FindOneAndUpdate(octx, filter, update, opts).Decode(&d)
	if err == nil {
		return store.ShardHead{Status: store.ShardStatus(d.Status), Version: d.Version}, nil //nolint:gosec // small enum
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		return store.ShardHead{}, wrap(err)
	}
	var exists shardDoc
	gerr := s.shards.FindOne(octx, bson.D{{Key: "_id", Value: id}}).Decode(&exists)
	if errors.Is(gerr, mongo.ErrNoDocuments) {
		return store.ShardHead{}, store.ErrNotFound
	}
	if gerr != nil {
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
	octx, cancel := s.op(ctx)
	defer cancel()

	id := shardID(pool, txid)
	now := s.now().Truncate(time.Microsecond)
	filter := bson.D{{Key: "_id", Value: id}, {Key: "o", Value: holder}}
	update := bson.D{
		{Key: "$set", Value: bson.D{{Key: "o", Value: ""}, {Key: "ua", Value: us(now)}}},
		{Key: "$unset", Value: bson.D{{Key: "lu", Value: ""}}},
	}
	res, err := s.shards.UpdateOne(octx, filter, update)
	if err != nil {
		return wrap(err)
	}
	if res.MatchedCount == 1 {
		return nil
	}
	err = s.shards.FindOne(octx, bson.D{{Key: "_id", Value: id}}).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return store.ErrNotFound
	}
	if err != nil {
		return wrap(err)
	}
	return nil // exists, but holder isn't the owner: a legitimate no-op
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
	octx, cancel := s.op(ctx)
	defer cancel()

	filter := bson.D{{Key: "p", Value: pool}}
	if len(f.Statuses) > 0 {
		sts := make(bson.A, len(f.Statuses))
		for i, st := range f.Statuses {
			sts[i] = int32(st)
		}
		filter = append(filter, bson.E{Key: "s", Value: bson.D{{Key: "$in", Value: sts}}})
	}
	if f.Owner != nil {
		filter = append(filter, bson.E{Key: "o", Value: *f.Owner})
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "ca", Value: 1}, {Key: "t", Value: 1}}).
		SetProjection(bson.D{{Key: "b", Value: 0}})
	if f.Limit > 0 {
		opts = opts.SetLimit(int64(f.Limit))
	}
	cur, err := s.shards.Find(octx, filter, opts)
	if err != nil {
		return nil, wrap(err)
	}
	defer func() { _ = cur.Close(octx) }()
	var docs []shardDoc
	if err := cur.All(octx, &docs); err != nil {
		return nil, wrap(err)
	}
	out := make([]store.Shard, len(docs))
	for i, d := range docs {
		sh, err := toShard(d, false)
		if err != nil {
			return nil, err
		}
		out[i] = sh
	}
	return out, nil
}
