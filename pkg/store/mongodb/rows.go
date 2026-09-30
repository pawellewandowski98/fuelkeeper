package mongodb

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// idDoc decodes just the _id of a fuel_rows document.
type idDoc struct {
	ID []byte `bson:"_id"`
}

// reserveProbe is the projection Reserve reads back to classify which ops
// actually applied: a replay under a different token never becomes state
// reserved under this call's token, so comparing rb decides membership.
type reserveProbe struct {
	ID []byte `bson:"_id"`
	RB []byte `bson:"rb"`
	EA int64  `bson:"ea"`
}

func decodeAll[T any](ctx context.Context, cur *mongo.Cursor) ([]T, error) {
	defer func() { _ = cur.Close(ctx) }()
	var out []T
	if err := cur.All(ctx, &out); err != nil {
		return nil, wrap(err)
	}
	return out, nil
}

// ListAvailable implements store.Store. Rows of one txid share rowID's
// 32-byte txid prefix and sort by vout within it (see docs.go), so the sh
// equality plus the _id lower bound stays within this shard.
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
	octx, cancel := s.op(ctx)
	defer cancel()

	filter := bson.D{
		{Key: "p", Value: pool},
		{Key: "sh", Value: txid[:]},
		{Key: "st", Value: int32(store.StateAvailable)},
		{Key: "_id", Value: bson.D{{Key: "$gte", Value: rowID(store.Outpoint{TxID: txid, Vout: fromVout})}}},
	}
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(limit))
	cur, err := s.rows.Find(octx, filter, opts)
	if err != nil {
		return nil, wrap(err)
	}
	docs, err := decodeAll[rowDoc](octx, cur)
	if err != nil {
		return nil, err
	}
	return rowsFromDocs(docs)
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
	octx, cancel := s.op(ctx)
	defer cancel()

	ids := make(bson.A, len(ops))
	for i, op := range ops {
		ids[i] = rowID(op)
	}
	filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}, {Key: "p", Value: pool}}
	cur, err := s.rows.Find(octx, filter)
	if err != nil {
		return nil, wrap(err)
	}
	docs, err := decodeAll[rowDoc](octx, cur)
	if err != nil {
		return nil, err
	}
	return rowsFromDocs(docs)
}

func rowsFromDocs(docs []rowDoc) ([]store.Row, error) {
	out := make([]store.Row, len(docs))
	for i, d := range docs {
		r, err := toRow(d)
		if err != nil {
			return nil, err
		}
		out[i] = r
	}
	return out, nil
}

// Reserve implements store.Store. Every op is its own single-document
// conditional update (available -> reserved, or a same-token replay of an
// already-reserved row), sent as one unordered BulkWrite; the aggregation
// pipeline update lets a single statement both decide (via $cond on the
// document's own current st) whether this is a fresh reservation or a
// replay, and, for a replay, leave ra/ea/ua untouched (see store.go
// "Idempotency"). Because BulkWrite gives no reliable per-op result under a
// partial failure, which ops actually ended up reserved(token) is decided by
// a follow-up read, not by the write's own result.
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
	octx, cancel := s.op(ctx)
	defer cancel()

	now := s.now().Truncate(time.Microsecond)
	sorted := store.SortReserveOps(ops)

	models := make([]mongo.WriteModel, len(sorted))
	ids := make(bson.A, len(sorted))
	for i, op := range sorted {
		id := rowID(op.Outpoint)
		ids[i] = id
		exp := us(op.ExpiresAt.Truncate(time.Microsecond))
		tok := op.Token

		isAvail := bson.D{{Key: "$eq", Value: bson.A{"$st", int32(store.StateAvailable)}}}
		filter := bson.D{
			{Key: "_id", Value: id},
			{Key: "p", Value: pool},
			{Key: "$or", Value: bson.A{
				bson.D{{Key: "st", Value: int32(store.StateAvailable)}},
				bson.D{{Key: "st", Value: int32(store.StateReserved)}, {Key: "rb", Value: tok[:]}},
			}},
		}
		update := mongo.Pipeline{
			bson.D{{Key: "$set", Value: bson.D{
				{Key: "ra", Value: bson.D{{Key: "$cond", Value: bson.A{isAvail, us(now), "$ra"}}}},
				{Key: "ea", Value: bson.D{{Key: "$cond", Value: bson.A{isAvail, exp, "$ea"}}}},
				{Key: "ua", Value: bson.D{{Key: "$cond", Value: bson.A{isAvail, us(now), "$ua"}}}},
				{Key: "st", Value: int32(store.StateReserved)},
				{Key: "rb", Value: tok[:]},
			}}},
		}
		models[i] = mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update)
	}

	if _, err := s.rows.BulkWrite(octx, models, options.BulkWrite().SetOrdered(false)); err != nil {
		// The write's own outcome is now in doubt (an unordered BulkWrite gives no
		// reliable "which ops landed" under a partial/network failure): report the
		// whole call as unavailable rather than guess via the classification read
		// below. Reserve is idempotent per (outpoint, token), so a retry is safe.
		// The double %w keeps a cancellation reachable via
		// errors.Is(err, context.Canceled) / context.DeadlineExceeded even though
		// the call is unconditionally classified as ErrUnavailable.
		return nil, fmt.Errorf("%w: %w", store.ErrUnavailable, err)
	}

	filter := bson.D{
		{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}},
		{Key: "p", Value: pool},
		{Key: "st", Value: int32(store.StateReserved)},
	}
	projection := bson.D{{Key: "_id", Value: 1}, {Key: "rb", Value: 1}, {Key: "ea", Value: 1}}
	// Hint _id_: an _id $in always uses the primary-key index in practice, but
	// pinning it explicitly means a future index cannot win this query by
	// planner heuristics instead (see RetireShard's cascade filter for why an
	// _id-stable scan matters here: this read-back must see every row this
	// call's own BulkWrite just touched, not skip or double-count one whose st
	// a concurrent write also moved).
	cur, err := s.rows.Find(octx, filter, options.Find().SetProjection(projection).SetHint("_id_"))
	if err != nil {
		return nil, wrap(err)
	}
	probes, err := decodeAll[reserveProbe](octx, cur)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]reserveProbe, len(probes))
	for _, p := range probes {
		byID[string(p.ID)] = p
	}

	applied := make([]store.ReserveOp, 0, len(sorted))
	for _, op := range sorted {
		p, ok := byID[string(rowID(op.Outpoint))]
		if !ok || !bytes.Equal(p.RB, op.Token[:]) {
			continue
		}
		applied = append(applied, store.ReserveOp{Outpoint: op.Outpoint, Token: op.Token, ExpiresAt: fromUS(p.EA)})
	}
	return applied, nil
}

// bulkMutate runs models as one unordered BulkWrite against fuel_rows and
// returns the number of documents actually modified. Every model's own
// filter re-asserts the row's current state (and, where relevant, its
// reservation token), so this is safe to call with any subset of ops.
func (s *Store) bulkMutate(ctx context.Context, models []mongo.WriteModel) (int, error) {
	res, err := s.rows.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return 0, wrap(err)
	}
	return int(res.ModifiedCount), nil
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
	octx, cancel := s.op(ctx)
	defer cancel()

	now := s.now().Truncate(time.Microsecond)
	sorted := store.SortOutpoints(ops)
	models := make([]mongo.WriteModel, len(sorted))
	for i, op := range sorted {
		filter := bson.D{
			{Key: "_id", Value: rowID(op)}, {Key: "p", Value: pool},
			{Key: "st", Value: int32(store.StateReserved)}, {Key: "rb", Value: token[:]},
		}
		update := bson.D{
			{Key: "$set", Value: bson.D{{Key: "st", Value: int32(store.StateAvailable)}, {Key: "ua", Value: us(now)}}},
			{Key: "$unset", Value: clearProvenance()},
		}
		models[i] = mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update)
	}
	return s.bulkMutate(octx, models)
}

// clearProvenance is the $unset document Release and Resolve-to-available
// use to drop all five provenance fields (see store.go "Provenance").
func clearProvenance() bson.D {
	return bson.D{
		{Key: "rb", Value: ""}, {Key: "ra", Value: ""}, {Key: "ea", Value: ""},
		{Key: "qa", Value: ""}, {Key: "h", Value: ""},
	}
}

// Spend implements store.Store.
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
	octx, cancel := s.op(ctx)
	defer cancel()

	now := s.now().Truncate(time.Microsecond)
	var set bson.D
	if terminal {
		set = bson.D{{Key: "st", Value: int32(store.StateSpent)}, {Key: "sb", Value: txid[:]}, {Key: "ua", Value: us(now)}}
	} else {
		set = bson.D{{Key: "st", Value: int32(store.StateQuarantined)}, {Key: "qa", Value: us(now)}, {Key: "ua", Value: us(now)}}
		if txid != (chainhash.Hash{}) {
			set = append(set, bson.E{Key: "h", Value: txid[:]})
		}
	}
	sorted := store.SortOutpoints(ops)
	models := make([]mongo.WriteModel, len(sorted))
	for i, op := range sorted {
		filter := bson.D{
			{Key: "_id", Value: rowID(op)}, {Key: "p", Value: pool},
			{Key: "st", Value: int32(store.StateReserved)}, {Key: "rb", Value: token[:]},
		}
		update := bson.D{{Key: "$set", Value: set}}
		models[i] = mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update)
	}
	return s.bulkMutate(octx, models)
}

// Quarantine implements store.Store: the due rows are discovered by an
// indexed read (rows_expiry), then moved one at a time via an unordered
// BulkWrite whose own filter re-checks st/ea, so a row a concurrent
// Release/Reserve-replay already moved away from reserved is simply skipped.
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
	octx, cancel := s.op(ctx)
	defer cancel()

	beforeUS := us(before.Truncate(time.Microsecond))
	filter := bson.D{
		{Key: "p", Value: pool},
		{Key: "st", Value: int32(store.StateReserved)},
		{Key: "ea", Value: bson.D{{Key: "$lt", Value: beforeUS}}},
	}
	opts := options.Find().
		SetSort(bson.D{{Key: "ea", Value: 1}, {Key: "_id", Value: 1}}).
		SetLimit(int64(limit)).
		SetProjection(bson.D{{Key: "_id", Value: 1}})
	cur, err := s.rows.Find(octx, filter, opts)
	if err != nil {
		return 0, wrap(err)
	}
	due, err := decodeAll[idDoc](octx, cur)
	if err != nil {
		return 0, err
	}
	if len(due) == 0 {
		return 0, nil
	}

	now := s.now().Truncate(time.Microsecond)
	models := make([]mongo.WriteModel, len(due))
	for i, d := range due {
		f := bson.D{
			{Key: "_id", Value: d.ID},
			{Key: "st", Value: int32(store.StateReserved)},
			{Key: "ea", Value: bson.D{{Key: "$lt", Value: beforeUS}}},
		}
		u := bson.D{{Key: "$set", Value: bson.D{
			{Key: "st", Value: int32(store.StateQuarantined)}, {Key: "qa", Value: us(now)}, {Key: "ua", Value: us(now)},
		}}}
		models[i] = mongo.NewUpdateOneModel().SetFilter(f).SetUpdate(u)
	}
	return s.bulkMutate(octx, models)
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
	octx, cancel := s.op(ctx)
	defer cancel()

	filter := bson.D{
		{Key: "p", Value: pool},
		{Key: "st", Value: int32(store.StateQuarantined)},
		{Key: "qa", Value: bson.D{{Key: "$lt", Value: us(before.Truncate(time.Microsecond))}}},
	}
	if after != nil {
		afterUS := us(after.At.Truncate(time.Microsecond))
		afterID := rowID(after.Outpoint)
		filter = append(filter, bson.E{Key: "$or", Value: bson.A{
			bson.D{{Key: "qa", Value: bson.D{{Key: "$gt", Value: afterUS}}}},
			bson.D{{Key: "qa", Value: afterUS}, {Key: "_id", Value: bson.D{{Key: "$gt", Value: afterID}}}},
		}})
	}
	opts := options.Find().SetSort(bson.D{{Key: "qa", Value: 1}, {Key: "_id", Value: 1}}).SetLimit(int64(limit))
	cur, err := s.rows.Find(octx, filter, opts)
	if err != nil {
		return nil, wrap(err)
	}
	docs, err := decodeAll[rowDoc](octx, cur)
	if err != nil {
		return nil, err
	}
	return rowsFromDocs(docs)
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
	octx, cancel := s.op(ctx)
	defer cancel()

	now := s.now().Truncate(time.Microsecond)
	var update bson.D
	if to == store.StateAvailable {
		update = bson.D{
			{Key: "$set", Value: bson.D{{Key: "st", Value: int32(store.StateAvailable)}, {Key: "ua", Value: us(now)}}},
			{Key: "$unset", Value: clearProvenance()},
		}
	} else {
		update = bson.D{{Key: "$set", Value: bson.D{
			{Key: "st", Value: int32(store.StateSpent)}, {Key: "sb", Value: spentBy[:]}, {Key: "ua", Value: us(now)},
		}}}
	}
	sorted := store.SortOutpoints(ops)
	models := make([]mongo.WriteModel, len(sorted))
	for i, op := range sorted {
		filter := bson.D{{Key: "_id", Value: rowID(op)}, {Key: "p", Value: pool}, {Key: "st", Value: int32(store.StateQuarantined)}}
		models[i] = mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update)
	}
	return s.bulkMutate(octx, models)
}

// Stats implements store.Store. Its two aggregations ($match{p} + $group by
// state/status) are covered by rows_pool_state_v2 ({p, st, sh}) and
// shards_adopt ({p, s, ca, t}) respectively -- both start with the exact
// {p, <grouped field>} prefix the pipeline needs -- so in practice this is
// already an index-only scan (see indexes_test.go's TestExplainUsesIndexes,
// "Stats"). That is incidental to how those indexes are shaped for their own
// primary queries, not something Stats is specifically optimized for; if a
// future index change ever made this a FETCH-per-document scan instead, that
// would be an acceptable trade for Stats specifically (an operational/
// monitoring query, not a per-row hot path), not worth a dedicated index.
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
	octx, cancel := s.op(ctx)
	defer cancel()

	var st store.Stats
	rowCounts, err := aggregateCounts(octx, s.rows, pool, "st")
	if err != nil {
		return store.Stats{}, err
	}
	for _, c := range rowCounts {
		switch store.State(c.Key) { //nolint:gosec // small enum
		case store.StateAvailable:
			st.Available = c.N
		case store.StateReserved:
			st.Reserved = c.N
		case store.StateQuarantined:
			st.Quarantined = c.N
		case store.StateSpent:
			st.Spent = c.N
		case store.StateInvalid:
			st.Invalid = c.N
		}
	}

	shardCounts, err := aggregateCounts(octx, s.shards, pool, "s")
	if err != nil {
		return store.Stats{}, err
	}
	for _, c := range shardCounts {
		switch store.ShardStatus(c.Key) { //nolint:gosec // small enum
		case store.ShardUnmined:
			st.ShardsUnmined = c.N
		case store.ShardMined:
			st.ShardsMined = c.N
		case store.ShardRetired:
			st.ShardsRetired = c.N
		case store.ShardInvalid:
			st.ShardsInvalid = c.N
		}
	}
	return st, nil
}

type countBucket struct {
	Key int32 `bson:"_id"`
	N   int64 `bson:"n"`
}

func aggregateCounts(ctx context.Context, coll *mongo.Collection, pool, field string) ([]countBucket, error) {
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.D{{Key: "p", Value: pool}}}},
		bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$" + field}, {Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}}}}},
	}
	cur, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, wrap(err)
	}
	return decodeAll[countBucket](ctx, cur)
}
