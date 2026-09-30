// Package mongodb implements store.Store on MongoDB without relying on
// multi-document transactions: every mutation is a per-document conditional
// update (a compare-and-set on the document's current state), and batches
// that touch several documents (Reserve, Release, Spend, Resolve, Quarantine)
// are classified by a follow-up read rather than by a shared transaction. A
// standalone mongod therefore works: the package never opens a session or a
// transaction.
//
// Every write uses write concern "majority" with journal acknowledgment and
// read preference primary, so an acknowledged write is durable and every read
// observes it. There is deliberately no TTL index on fuel_rows: a row's
// lifecycle is driven entirely by explicit state transitions (Reserve,
// Release, Spend, Quarantine, Resolve, RetireShard), never by MongoDB
// expiring a document out from under a live reservation.
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// maxBatch is Capabilities().MaxBatch: the maximum ops per
// Reserve/Release/Spend/Resolve/GetRows call.
const maxBatch = 1000

// maxConcurrentFlushes is Capabilities().MaxConcurrentFlushes.
const maxConcurrentFlushes = 8

// defaultMaxPoolSize is used when Config.MaxPoolSize is 0.
const defaultMaxPoolSize = 64

// defaultOpTimeout is used when Config.OpTimeout is <= 0.
const defaultOpTimeout = 5 * time.Second

// schemaVersion is the current schema version, stored in fuel_meta. New
// refuses to open against a database whose recorded version is higher than
// this build understands. See ensureIndexes/migrateSchema for the v1 -> v2
// migration this version bump drives.
const schemaVersion int64 = 2

// errClosed is returned by every method once the store has been closed.
var errClosed = fmt.Errorf("%w: mongodb: store is closed", store.ErrUnavailable)

// Config configures New.
type Config struct {
	// URI is the mongodb:// connection string. Required.
	URI string
	// Database is the database name this store uses. Required. Tests use a
	// fresh, randomly-named database per store so subtests stay isolated.
	Database string
	// MaxPoolSize is the driver's per-client connection pool size. 0 means 64.
	MaxPoolSize uint64
	// OpTimeout bounds every individual operation against the server,
	// independent of the caller's own context deadline. 0 means 5s. Reserve
	// and RetireShard each spend two or three round-trips (a write, then a
	// follow-up read to classify what landed; RetireShard also spends a
	// preceding existence check) out of this single budget, not one, so a
	// deployment doing cross-region majority writes -- where one round-trip
	// alone can approach typical inter-region RTTs -- needs a larger value
	// than the 5s default to avoid spurious ErrUnavailable under otherwise
	// normal replication latency.
	OpTimeout time.Duration
}

// Option configures New.
type Option func(*Store)

// WithClock injects the clock used for timestamps and lease expiry.
func WithClock(now func() time.Time) Option { return func(s *Store) { s.now = now } }

// Store is a MongoDB-backed store.Store.
type Store struct {
	client *mongo.Client
	db     *mongo.Database
	rows   *mongo.Collection
	shards *mongo.Collection
	txids  *mongo.Collection
	leases *mongo.Collection
	meta   *mongo.Collection

	now  func() time.Time
	opTO time.Duration

	closed atomic.Bool
	once   sync.Once
}

var _ store.Store = (*Store)(nil)

// New connects to MongoDB, applies write concern majority + journal and read
// preference primary, ensures the fuel_rows/fuel_shards indexes exist and
// checks/records the schema version in fuel_meta.
func New(ctx context.Context, cfg Config, opts ...Option) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.URI == "" {
		return nil, fmt.Errorf("%w: mongodb: URI is required", store.ErrInvalidArgument)
	}
	if cfg.Database == "" {
		return nil, fmt.Errorf("%w: mongodb: Database is required", store.ErrInvalidArgument)
	}
	maxPoolSize := cfg.MaxPoolSize
	if maxPoolSize == 0 {
		maxPoolSize = defaultMaxPoolSize
	}
	opTO := cfg.OpTimeout
	if opTO <= 0 {
		opTO = defaultOpTimeout
	}

	journal := true
	clientOpts := options.Client().
		ApplyURI(cfg.URI).
		SetMaxPoolSize(maxPoolSize).
		SetWriteConcern(&writeconcern.WriteConcern{W: "majority", Journal: &journal}).
		SetReadPreference(readpref.Primary())

	host := connectHost(cfg.URI)
	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("mongodb: connect %s: %w", host, wrap(err))
	}

	s := &Store{client: client, now: time.Now, opTO: opTO}
	for _, o := range opts {
		o(s)
	}
	s.db = client.Database(cfg.Database)
	s.rows = s.db.Collection("fuel_rows")
	s.shards = s.db.Collection("fuel_shards")
	s.txids = s.db.Collection("fuel_shard_txids")
	s.leases = s.db.Collection("fuel_leases")
	s.meta = s.db.Collection("fuel_meta")

	// Setup (Ping/ensureCollections/ensureIndexes/ensureSchema) runs under the
	// caller's own ctx, not s.opTO: New can involve a burst of collection and
	// index creation across many concurrently-opened stores (as the
	// conformance suite does), which can legitimately take longer than a
	// single steady-state operation's budget. s.opTO only starts bounding
	// individual calls once the store is open.
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("mongodb: connect %s: %w", host, wrap(err))
	}
	if err := s.ensureCollections(ctx); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	if err := s.ensureIndexes(ctx); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	if err := s.ensureSchema(ctx); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	return s, nil
}

// connectHost extracts just the host(s) portion of a mongodb:// URI for use
// in connection-error messages, without ever including embedded credentials
// (net/url.URL.Host never includes userinfo). It falls back to a generic
// label rather than failing New over a cosmetic detail if the URI cannot be
// parsed as a URL (which New's own connection attempt will report anyway).
func connectHost(uri string) string {
	u, err := url.Parse(uri)
	if err != nil || u.Host == "" {
		return "mongodb"
	}
	return u.Host
}

// ensureCollections creates every collection this package uses, explicitly
// and up front, ignoring "already exists" (code 48): MongoDB otherwise
// creates a collection implicitly on first write, and when many stores open
// concurrently (as New's own tests and the conformance suite do), that
// implicit creation can land as extra, contended work interleaved with the
// first real operation against a brand-new collection such as fuel_leases
// (TryAcquireOrRenew's first findAndModify), tipping it past OpTimeout. Doing
// it here, before any index build or real operation, keeps that cost out of
// the timing-sensitive path.
func (s *Store) ensureCollections(ctx context.Context) error {
	for _, name := range []string{"fuel_rows", "fuel_shards", "fuel_shard_txids", "fuel_leases", "fuel_meta"} {
		if err := s.db.CreateCollection(ctx, name); err != nil && !isNamespaceExists(err) {
			return wrap(err)
		}
	}
	return nil
}

// isNamespaceExists reports whether err is the server's "NamespaceExists"
// error (code 48), returned by createCollection when another concurrent
// New() already created it.
func isNamespaceExists(err error) bool {
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		return ce.Code == 48
	}
	var se mongo.ServerError
	if errors.As(err, &se) {
		return se.HasErrorCode(48)
	}
	return false
}

// ensureIndexes creates every index this package relies on, idempotently:
// creating an index that already exists with the same key/options is a no-op.
//
// # Schema v1 -> v2 migration
//
// v1's rows_expiry ({p, ea}, partial on state=reserved) could not provide the
// {ea, _id} sort Quarantine's discovery scan needs, forcing a blocking SORT
// over every reserved row; v1's rows_pool_state ({p, st}) could not bound
// RetireShard's cascade update (filtered on p, sh and st IN [...]) by shard,
// forcing a scan over every row in the pool's given states. v2 replaces both
// with rows_expiry_v2 ({p, ea, _id}, same partial filter -- covers Quarantine's
// discovery scan, including its {_id: 1} projection, with no SORT stage) and
// rows_pool_state_v2 ({p, st, sh} -- lets RetireShard's cascade seek directly
// to one shard's rows per state, and still covers Stats' {p}-match/{st}-group
// aggregation the same way {p, st} did).
//
// Because changing an index's key pattern under the same name makes the
// server reject index creation with IndexKeySpecsConflict, v2's indexes are
// created here under new names rather than by redefining rows_expiry/
// rows_pool_state in place; migrateSchema (called from ensureSchema once the
// stored schema version is confirmed to be v1) then drops the old-named v1
// indexes, which by that point nothing references any more.
func (s *Store) ensureIndexes(ctx context.Context) error {
	rowsIdx := []mongo.IndexModel{
		{
			// ListAvailable: {p, sh, st: available, _id >= rowID(txid, fromVout)} sort _id.
			Keys: bson.D{{Key: "p", Value: 1}, {Key: "sh", Value: 1}, {Key: "_id", Value: 1}},
			Options: options.Index().SetName("rows_avail").
				SetPartialFilterExpression(bson.D{{Key: "st", Value: int32(store.StateAvailable)}}),
		},
		{
			// Quarantine discovery: {p, st: reserved, ea < before} sort {ea, _id},
			// projected to {_id} only -- a covered scan, see the migration note above.
			Keys: bson.D{{Key: "p", Value: 1}, {Key: "ea", Value: 1}, {Key: "_id", Value: 1}},
			Options: options.Index().SetName("rows_expiry_v2").
				SetPartialFilterExpression(bson.D{{Key: "st", Value: int32(store.StateReserved)}}),
		},
		{
			// ListQuarantined: {p, st: quarantined, qa < before, [cursor]} sort {qa, _id}.
			Keys: bson.D{{Key: "p", Value: 1}, {Key: "qa", Value: 1}, {Key: "_id", Value: 1}},
			Options: options.Index().SetName("rows_quar").
				SetPartialFilterExpression(bson.D{{Key: "st", Value: int32(store.StateQuarantined)}}),
		},
		{
			// Stats' {p}-match/{st}-group aggregation, and RetireShard's cascade
			// update ({p, sh, st IN [...]}): st before sh so the {p, st} prefix
			// alone still covers Stats, while adding sh lets the cascade bound
			// each of its (at most 3) st branches to one shard instead of the
			// whole pool. See the migration note above.
			Keys:    bson.D{{Key: "p", Value: 1}, {Key: "st", Value: 1}, {Key: "sh", Value: 1}},
			Options: options.Index().SetName("rows_pool_state_v2"),
		},
	}
	if _, err := s.rows.Indexes().CreateMany(ctx, rowsIdx); err != nil {
		return wrap(err)
	}

	shardsIdx := []mongo.IndexModel{
		{
			// AcquireShard's candidate scan: {p, s: {$in}, [lease-free], t: {$nin}} sort {ca, t}.
			Keys:    bson.D{{Key: "p", Value: 1}, {Key: "s", Value: 1}, {Key: "ca", Value: 1}, {Key: "t", Value: 1}},
			Options: options.Index().SetName("shards_adopt"),
		},
	}
	if _, err := s.shards.Indexes().CreateMany(ctx, shardsIdx); err != nil {
		return wrap(err)
	}
	return nil
}

// dropIndexIfExists drops the named index from coll if present, and is a
// no-op if it is not (checked via a list rather than by matching the drop
// error's code, so it does not depend on the server's exact "index not
// found" error shape).
func (s *Store) dropIndexIfExists(ctx context.Context, coll *mongo.Collection, name string) error {
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		return wrap(err)
	}
	var idxs []bson.M
	if err := cur.All(ctx, &idxs); err != nil {
		return wrap(err)
	}
	for _, idx := range idxs {
		if n, _ := idx["name"].(string); n == name {
			if err := coll.Indexes().DropOne(ctx, name); err != nil {
				return wrap(err)
			}
			return nil
		}
	}
	return nil
}

// migrateSchema upgrades a database recorded at schema version from (< the
// current schemaVersion) in place. It is safe to call redundantly (a second
// New against an already-migrated database never reaches here, since
// ensureSchema only calls it when from < schemaVersion) and safe to retry
// after a partial failure (dropIndexIfExists is idempotent, and the version
// bump below only ever moves forward).
func (s *Store) migrateSchema(ctx context.Context, from int64) error {
	if from < 2 {
		// v2's rows_expiry_v2/rows_pool_state_v2 were already created by
		// ensureIndexes, above ensureSchema in New; it is now safe to drop the
		// v1-named indexes they replace.
		for _, name := range []string{"rows_expiry", "rows_pool_state"} {
			if err := s.dropIndexIfExists(ctx, s.rows, name); err != nil {
				return err
			}
		}
	}
	filter := bson.D{{Key: "_id", Value: "schema"}, {Key: "v", Value: bson.D{{Key: "$lt", Value: schemaVersion}}}}
	update := bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: schemaVersion}}}}
	if _, err := s.meta.UpdateOne(ctx, filter, update); err != nil {
		return wrap(err)
	}
	return nil
}

// ensureSchema atomically creates the fuel_meta "schema" record on a fresh
// database ($setOnInsert leaves an existing record untouched), migrates an
// older recorded version forward (see migrateSchema), and refuses to open
// against a database whose recorded version is newer than this build
// understands.
func (s *Store) ensureSchema(ctx context.Context) error {
	filter := bson.D{{Key: "_id", Value: "schema"}}
	update := bson.D{{Key: "$setOnInsert", Value: bson.D{{Key: "v", Value: schemaVersion}}}}
	opts := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)
	var d metaDoc
	if err := s.meta.FindOneAndUpdate(ctx, filter, update, opts).Decode(&d); err != nil {
		return wrap(err)
	}
	if d.Version < schemaVersion {
		if err := s.migrateSchema(ctx, d.Version); err != nil {
			return err
		}
	}
	if d.Version > schemaVersion {
		return fmt.Errorf("mongodb: schema version %d unsupported (this build wants %d)", d.Version, schemaVersion)
	}
	return nil
}

// Rows exposes the fuel_rows collection for tests (index/explain assertions).
func (s *Store) Rows() *mongo.Collection { return s.rows }

// Ping implements store.Store.
func (s *Store) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed.Load() {
		return errClosed
	}
	octx, cancel := s.op(ctx)
	defer cancel()
	return wrap(s.client.Ping(octx, readpref.Primary()))
}

// Capabilities implements store.Store.
func (s *Store) Capabilities() store.Caps {
	return store.Caps{MaxConcurrentFlushes: maxConcurrentFlushes, MaxBatch: maxBatch, AtomicBatch: false, MultiProcess: true}
}

// Close implements store.Store; it is idempotent. The client is never nilled
// out: every method continues to check s.closed and returns errClosed instead
// of risking a nil-pointer dereference on a racing caller.
func (s *Store) Close() error {
	var err error
	s.once.Do(func() {
		s.closed.Store(true)
		err = s.client.Disconnect(context.Background())
	})
	return err
}

// op bounds a single operation against the server by s.opTO, independent of
// (but still respecting) the caller's own context deadline.
func (s *Store) op(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.opTO)
}

// notPrimaryOrShutdownCodes are server codes signalling that the node
// handling a request stopped being primary, or is shutting down, at some
// point around the write: the write may or may not have reached (or been
// applied by) a primary that was still primary at the time, so the outcome
// is in doubt -- exactly like a timeout -- never a permanent rejection.
//
//	10107 NotWritablePrimary
//	13435 NotPrimaryNoSecondaryOk
//	13436 NotPrimaryOrSecondary
//	  189 PrimarySteppedDown
//	   91 ShutdownInProgress
//	11600 InterruptedAtShutdown
//	11602 InterruptedDueToReplStateChange
var notPrimaryOrShutdownCodes = map[int]bool{
	10107: true, 13435: true, 13436: true, 189: true, 91: true, 11600: true, 11602: true,
}

// wrap maps transient/infrastructure errors to store.ErrUnavailable and
// passes everything else through unchanged, in particular duplicate-key
// errors, which callers classify themselves (PutShard, TryAcquireOrRenew).
//
// Every mutation in this package is either a per-document CAS or, for a
// batch, individually idempotent (see store.go "Idempotency"), so treating an
// in-doubt write as ErrUnavailable rather than a permanent error is always
// safe for a caller to retry.
func wrap(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, mongo.ErrClientDisconnected) {
		return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
	}
	if mongo.IsTimeout(err) || mongo.IsNetworkError(err) {
		return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
	}
	var sse topology.ServerSelectionError
	if errors.As(err, &sse) {
		return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
	}

	// A BulkWriteException/WriteException write-concern error (MaxTimeMSExpired,
	// wtimeout, UnsatisfiableWriteConcern, ...) means the write itself may have
	// applied but its "majority" acknowledgment is unconfirmed. mongo.IsTimeout
	// only recognizes this for WriteException's MaxTimeMSExpired (code 50) --
	// never for BulkWriteException, and never for WriteException's other
	// write-concern codes such as 64 (wtimeout) -- so both are handled
	// explicitly here regardless of the specific code, matching how Reserve's
	// own BulkWrite path already treats every error as unavailable.
	var bwe mongo.BulkWriteException
	if errors.As(err, &bwe) && bwe.WriteConcernError != nil {
		return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
	}
	var we mongo.WriteException
	if errors.As(err, &we) && we.WriteConcernError != nil {
		return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
	}

	// RetryableWriteError/TransientTransactionError labels are the driver's own
	// signal that the server considers the operation safe (and expected) to
	// retry.
	var le mongo.LabeledError
	if errors.As(err, &le) && (le.HasErrorLabel("RetryableWriteError") || le.HasErrorLabel("TransientTransactionError")) {
		return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
	}

	// Not-primary / stepping-down / shutting-down codes, wherever they appear:
	// a bare CommandError, or nested inside a WriteException/BulkWriteException
	// as one of its WriteErrors or its WriteConcernError. ServerError.ErrorCodes
	// already flattens all of those into one list for any of this package's
	// error shapes (CommandError, WriteError, WriteException, BulkWriteException
	// all implement it).
	var se mongo.ServerError
	if errors.As(err, &se) {
		for _, code := range se.ErrorCodes() {
			if notPrimaryOrShutdownCodes[code] {
				return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
			}
		}
	}
	return err
}

// isDupCode reports whether code is one of the server's duplicate-key codes
// (mirrors the set mongo.IsDuplicateKeyError checks).
func isDupCode(code int) bool {
	switch code {
	case 11000, 11001, 12582:
		return true
	default:
		return false
	}
}

// allDuplicateKey reports whether err is a bulk/insert-many failure whose
// every individual write error is a duplicate key, safe to ignore wholesale
// (PutShard's unordered row insert: a row that already exists from a partial
// prior write is expected, not an error).
func allDuplicateKey(err error) bool {
	var bwe mongo.BulkWriteException
	if errors.As(err, &bwe) {
		if len(bwe.WriteErrors) == 0 {
			return false
		}
		for _, we := range bwe.WriteErrors {
			if !isDupCode(we.Code) {
				return false
			}
		}
		return true
	}
	var we mongo.WriteException
	if errors.As(err, &we) {
		if len(we.WriteErrors) == 0 {
			return false
		}
		for _, e := range we.WriteErrors {
			if !isDupCode(e.Code) {
				return false
			}
		}
		return true
	}
	return false
}
