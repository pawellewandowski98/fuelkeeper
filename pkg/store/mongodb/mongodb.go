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
// this build understands.
const schemaVersion int64 = 1

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
	// independent of the caller's own context deadline. 0 means 5s.
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

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, wrap(err)
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

	// Setup (Ping/ensureIndexes/ensureSchema) runs under the caller's own ctx,
	// not s.opTO: New can involve a burst of index builds across many
	// concurrently-opened stores (as the conformance suite does), which can
	// legitimately take longer than a single steady-state operation's budget.
	// s.opTO only starts bounding individual calls once the store is open.
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, wrap(err)
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

// ensureIndexes creates every index this package relies on, idempotently:
// creating an index that already exists with the same key/options is a no-op.
func (s *Store) ensureIndexes(ctx context.Context) error {
	rowsIdx := []mongo.IndexModel{
		{
			// ListAvailable: {p, sh, st: available, _id >= rowID(txid, fromVout)} sort _id.
			Keys: bson.D{{Key: "p", Value: 1}, {Key: "sh", Value: 1}, {Key: "_id", Value: 1}},
			Options: options.Index().SetName("rows_avail").
				SetPartialFilterExpression(bson.D{{Key: "st", Value: int32(store.StateAvailable)}}),
		},
		{
			// Quarantine: {p, st: reserved, ea < before} sort {ea, _id}.
			Keys: bson.D{{Key: "p", Value: 1}, {Key: "ea", Value: 1}},
			Options: options.Index().SetName("rows_expiry").
				SetPartialFilterExpression(bson.D{{Key: "st", Value: int32(store.StateReserved)}}),
		},
		{
			// ListQuarantined: {p, st: quarantined, qa < before, [cursor]} sort {qa, _id}.
			Keys: bson.D{{Key: "p", Value: 1}, {Key: "qa", Value: 1}, {Key: "_id", Value: 1}},
			Options: options.Index().SetName("rows_quar").
				SetPartialFilterExpression(bson.D{{Key: "st", Value: int32(store.StateQuarantined)}}),
		},
		{
			// Stats, RetireShard's discovery scans.
			Keys:    bson.D{{Key: "p", Value: 1}, {Key: "st", Value: 1}},
			Options: options.Index().SetName("rows_pool_state"),
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

// ensureSchema atomically creates the fuel_meta "schema" record on a fresh
// database ($setOnInsert leaves an existing record untouched) and refuses to
// open against a database whose recorded version is newer than this build
// understands.
func (s *Store) ensureSchema(ctx context.Context) error {
	filter := bson.D{{Key: "_id", Value: "schema"}}
	update := bson.D{{Key: "$setOnInsert", Value: bson.D{{Key: "v", Value: schemaVersion}}}}
	opts := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)
	var d metaDoc
	if err := s.meta.FindOneAndUpdate(ctx, filter, update, opts).Decode(&d); err != nil {
		return wrap(err)
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

// wrap maps transient/infrastructure errors to store.ErrUnavailable and
// passes everything else through unchanged, in particular duplicate-key
// errors, which callers classify themselves (PutShard, TryAcquireOrRenew).
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
