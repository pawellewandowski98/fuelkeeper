// Package aerospike implements store.Store on Aerospike using per-record
// compare-and-set: every mutation is a single-record (or per-record, inside a
// batch) conditional write guarded by a filter expression re-asserting the
// record's current state, mirroring the postgres/mongodb backends' CAS
// discipline without relying on Aerospike's multi-record transactions.
//
// # Consistency mode
//
// Aerospike's generation counter is bumped by any writer regardless of intent,
// so it cannot distinguish "nobody touched this record" from "somebody wrote
// back the same value": every CAS here is therefore a filter-expression
// predicate on the record's own bins (its state, its reservation token, ...),
// never a generation check. Every write policy in this package also sets
// MaxRetries = 0: the client's built-in retry would resend a write whose
// first attempt actually succeeded but whose response was lost, and a
// retried non-idempotent CAS write would then be misread as "filtered out"
// (lost the race) when it in fact already applied. Reserve and friends are
// idempotent at the store.Store layer instead (see store.go, "Idempotency"),
// so callers retry ErrUnavailable at that layer, not inside this client.
//
// Production deployments of this backend must run against a namespace
// configured for strong consistency (strong-consistency true), so that a
// successful, COMMIT_ALL write is linearizable across the cluster; the
// conformance suite and the tests in this package run against a single-node
// Community Edition namespace instead, which is trivially "strongly
// consistent" by having only one copy of the data, not because CE supports
// the SC namespace mode.
//
// # Schema
//
// Every set name is prefixed by Config.SetPrefix so parallel test stores
// sharing one namespace stay isolated:
//
//   - <prefix>rows: keyed by the outpoint's 36 raw bytes (32-byte txid,
//     big-endian uint32 vout). Presence-encoded index bins (ak, present iff
//     state == available; pst, the "pool|state" string, always present) let
//     ListAvailable and Stats/RetireShard run as ordinary secondary-index
//     equality queries instead of a full scan, without Aerospike supporting a
//     Mongo-style partial index: see bins.go.
//   - <prefix>shards: keyed by "pool|txidhex".
//   - <prefix>txids: keyed by txidhex, enforcing a shard's global uniqueness
//     across pools (CREATE_ONLY insert; a losing insert under a different
//     pool is ErrShardExists).
//   - <prefix>leases: keyed by lease name.
//   - <prefix>meta: a single "schema" record recording the schema version.
package aerospike

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// maxBatch is Capabilities().MaxBatch: the maximum ops per
// Reserve/Release/Spend/Resolve/GetRows call.
const maxBatch = 1000

// maxConcurrentFlushes is Capabilities().MaxConcurrentFlushes.
const maxConcurrentFlushes = 8

// defaultPoolSize is used when Config.PoolSize is 0.
const defaultPoolSize = 128

// defaultTimeout is used when Config.Timeout is <= 0: the default per-
// operation TotalTimeout, before any caller context deadline narrows it.
const defaultTimeout = 3 * time.Second

// defaultPort is used for a Config.Hosts entry with no explicit port.
const defaultPort = 3000

// schemaVersion is the current schema version, stored in the meta set. New
// refuses to open against a store whose recorded version is higher than this
// build understands.
const schemaVersion int64 = 1

// indexBuildWait bounds how long New waits for a newly-created secondary
// index to finish building.
const indexBuildWait = 30 * time.Second

// errClosed is returned by every method once the store has been closed.
var errClosed = fmt.Errorf("%w: aerospike: store is closed", store.ErrUnavailable)

// Config configures New.
type Config struct {
	// Hosts are "host:port" (or bare "host", defaulting to port 3000) seed
	// addresses. Required.
	Hosts []string
	// Namespace is the Aerospike namespace this store uses. Required.
	// Production deployments must configure it for strong consistency; see
	// the package doc comment.
	Namespace string
	// SetPrefix is prepended to every set name this package uses, so tests
	// can isolate parallel stores sharing one namespace. May be empty.
	SetPrefix string
	// Timeout bounds every individual operation against the server
	// (TotalTimeout), independent of the caller's own context deadline,
	// unless that deadline is sooner. 0 means 3s.
	Timeout time.Duration
	// PoolSize is the client's per-node connection pool size. 0 means 128.
	PoolSize int
}

// Option configures New.
type Option func(*Store)

// WithClock injects the clock used for timestamps and lease expiry.
func WithClock(now func() time.Time) Option { return func(s *Store) { s.now = now } }

// Store is an Aerospike-backed store.Store.
type Store struct {
	client *as.Client
	hosts  []*as.Host // kept for Truncate, which reconnects independently (see Truncate)
	ns     string
	prefix string

	rowsSet   string
	shardsSet string
	txidsSet  string
	leasesSet string
	metaSet   string

	timeout time.Duration
	now     func() time.Time

	closed atomic.Bool
	once   sync.Once
}

var _ store.Store = (*Store)(nil)

// New connects to Aerospike, ensures the secondary indexes this package
// relies on exist, and checks/records the schema version in the meta set.
func New(ctx context.Context, cfg Config, opts ...Option) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(cfg.Hosts) == 0 {
		return nil, fmt.Errorf("%w: aerospike: at least one host is required", store.ErrInvalidArgument)
	}
	if cfg.Namespace == "" {
		return nil, fmt.Errorf("%w: aerospike: Namespace is required", store.ErrInvalidArgument)
	}
	poolSize := cfg.PoolSize
	if poolSize == 0 {
		poolSize = defaultPoolSize
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	hosts, err := parseHosts(cfg.Hosts)
	if err != nil {
		return nil, err
	}

	cp := as.NewClientPolicy()
	cp.ConnectionQueueSize = poolSize
	cp.IdleTimeout = 55 * time.Second
	cp.Timeout = timeout
	cp.FailIfNotConnected = true

	client, aerr := as.NewClientWithPolicyAndHost(cp, hosts...)
	if aerr != nil {
		return nil, wrap(aerr)
	}

	s := &Store{
		client:    client,
		hosts:     hosts,
		ns:        cfg.Namespace,
		prefix:    cfg.SetPrefix,
		rowsSet:   cfg.SetPrefix + "rows",
		shardsSet: cfg.SetPrefix + "shards",
		txidsSet:  cfg.SetPrefix + "txids",
		leasesSet: cfg.SetPrefix + "leases",
		metaSet:   cfg.SetPrefix + "meta",
		timeout:   timeout,
		now:       time.Now,
	}
	for _, o := range opts {
		o(s)
	}

	if !client.IsConnected() {
		client.Close()
		return nil, fmt.Errorf("%w: aerospike: not connected", store.ErrUnavailable)
	}
	if err := s.ensureIndexes(ctx); err != nil {
		client.Close()
		return nil, err
	}
	if err := s.ensureSchema(ctx); err != nil {
		client.Close()
		return nil, err
	}
	return s, nil
}

// parseHosts parses "host:port" (or bare "host", defaulting to defaultPort)
// entries into *as.Host values.
func parseHosts(raw []string) ([]*as.Host, error) {
	hosts := make([]*as.Host, 0, len(raw))
	for _, h := range raw {
		name, portStr, err := net.SplitHostPort(h)
		if err != nil {
			// No port supplied at all.
			hosts = append(hosts, as.NewHost(h, defaultPort))
			continue
		}
		port, perr := strconv.Atoi(portStr)
		if perr != nil {
			return nil, fmt.Errorf("%w: aerospike: invalid host %q: %w", store.ErrInvalidArgument, h, perr)
		}
		hosts = append(hosts, as.NewHost(name, port))
	}
	return hosts, nil
}

// indexSpec is one secondary index this package relies on.
type indexSpec struct {
	set       string
	indexName string
	binName   string
	idxType   as.IndexType
}

func (s *Store) indexSpecs() []indexSpec {
	p := s.prefix
	return []indexSpec{
		{s.rowsSet, p + "rows_ak", binAK, as.STRING},
		{s.rowsSet, p + "rows_pst", binPST, as.STRING},
		{s.rowsSet, p + "rows_ea", binEA, as.NUMERIC},
		{s.rowsSet, p + "rows_qa", binQA, as.NUMERIC},
		{s.shardsSet, p + "shards_ps", binPS, as.STRING},
	}
}

// ensureIndexes creates every secondary index this package relies on,
// idempotently: INDEX_FOUND (an identical index already exists) is not an
// error. It waits for each newly-created index to finish building.
func (s *Store) ensureIndexes(ctx context.Context) error {
	wp := as.NewWritePolicy(0, 0)
	wp.TotalTimeout = s.totalTimeout(ctx)
	for _, spec := range s.indexSpecs() {
		task, aerr := s.client.CreateIndex(wp, s.ns, spec.set, spec.indexName, spec.binName, spec.idxType)
		if aerr != nil {
			if aerr.Matches(types.INDEX_FOUND) {
				continue
			}
			return wrap(aerr)
		}
		if task == nil {
			continue
		}
		select {
		case aerr := <-task.OnComplete():
			if aerr != nil {
				return wrap(aerr)
			}
		case <-time.After(indexBuildWait):
			return fmt.Errorf("%w: aerospike: index %s did not finish building within %s", store.ErrUnavailable, spec.indexName, indexBuildWait)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// ensureSchema atomically creates the meta "schema" record on a fresh store
// (CREATE_ONLY leaves an existing record untouched) and refuses to open
// against a store whose recorded version is newer than this build
// understands.
func (s *Store) ensureSchema(ctx context.Context) error {
	key, err := s.metaKey()
	if err != nil {
		return err
	}
	wp := s.createPolicy(ctx)
	if aerr := s.client.Put(wp, key, as.BinMap{binSchema: schemaVersion}); aerr != nil {
		if !aerr.Matches(types.KEY_EXISTS_ERROR) {
			return wrap(aerr)
		}
		rec, gerr := s.client.Get(s.readPolicy(ctx), key, binSchema)
		if gerr != nil {
			return wrap(gerr)
		}
		v := toInt64(rec.Bins[binSchema])
		if v > schemaVersion {
			return fmt.Errorf("aerospike: schema version %d unsupported (this build wants %d)", v, schemaVersion)
		}
	}
	return nil
}

// Truncate removes every record this store owns (its own uniquely-prefixed
// sets) and drops the secondary indexes it created, for test cleanup between
// isolated stores sharing one namespace: a namespace's secondary-index count
// is capped server-side, so a long-running suite that opens many isolated
// stores (RunSuite alone opens dozens per run) must drop each store's
// indexes on cleanup rather than merely truncating their data, or the
// namespace eventually hits that cap.
//
// Truncate opens its own short-lived connection rather than reusing s.client:
// callers register it via t.Cleanup alongside RunSuite's own
// t.Cleanup(Close), and t.Cleanup runs in LIFO order, so Close (registered
// after isolatedStore returns) always runs before Truncate (registered
// inside isolatedStore, earlier). Reusing s.client here would then always
// operate on an already-closed client and silently do nothing.
func (s *Store) Truncate() {
	cp := as.NewClientPolicy()
	cp.Timeout = s.timeout
	client, aerr := as.NewClientWithPolicyAndHost(cp, s.hosts...)
	if aerr != nil {
		return
	}
	defer client.Close()

	ip := as.NewInfoPolicy()
	for _, set := range []string{s.rowsSet, s.shardsSet, s.txidsSet, s.leasesSet, s.metaSet} {
		_ = client.Truncate(ip, s.ns, set, nil)
	}
	wp := as.NewWritePolicy(0, 0)
	for _, spec := range s.indexSpecs() {
		_ = client.DropIndex(wp, s.ns, spec.set, spec.indexName)
	}
}

// Ping implements store.Store.
func (s *Store) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed.Load() {
		return errClosed
	}
	if !s.client.IsConnected() {
		return fmt.Errorf("%w: aerospike: not connected", store.ErrUnavailable)
	}
	return nil
}

// Capabilities implements store.Store.
func (s *Store) Capabilities() store.Caps {
	return store.Caps{MaxConcurrentFlushes: maxConcurrentFlushes, MaxBatch: maxBatch, AtomicBatch: false, MultiProcess: true}
}

// Close implements store.Store; it is idempotent. The client is never nilled
// out: every method continues to check s.closed and returns errClosed instead
// of risking a nil-pointer dereference on a racing caller.
func (s *Store) Close() error {
	s.once.Do(func() {
		s.closed.Store(true)
		s.client.Close()
	})
	return nil
}

// totalTimeout bounds a single operation: Config.Timeout, unless the
// caller's own context deadline is sooner.
func (s *Store) totalTimeout(ctx context.Context) time.Duration {
	d := s.timeout
	if dl, ok := ctx.Deadline(); ok {
		if rem := time.Until(dl); rem < d {
			if rem < time.Millisecond {
				rem = time.Millisecond
			}
			d = rem
		}
	}
	return d
}

// writePolicy is the policy for every single-record CAS write in this
// package: no client-side retries (see the package doc comment),
// COMMIT_ALL durability, and UPDATE_ONLY (the record must already exist;
// PutShard's inserts use createPolicy instead).
func (s *Store) writePolicy(ctx context.Context) *as.WritePolicy {
	wp := as.NewWritePolicy(0, 0)
	wp.MaxRetries = 0
	wp.CommitLevel = as.COMMIT_ALL
	wp.RecordExistsAction = as.UPDATE_ONLY
	wp.TotalTimeout = s.totalTimeout(ctx)
	return wp
}

// createPolicy is writePolicy with RecordExistsAction = CREATE_ONLY, for
// PutShard's initial row/shard/txid inserts.
func (s *Store) createPolicy(ctx context.Context) *as.WritePolicy {
	wp := s.writePolicy(ctx)
	wp.RecordExistsAction = as.CREATE_ONLY
	return wp
}

// batchWritePolicy is the per-record policy for a BatchOperate write: no
// retries at the batch level (see batchPolicy), COMMIT_ALL, UPDATE_ONLY.
// FilterExpression is set per call site.
func (s *Store) batchWritePolicy(_ context.Context) *as.BatchWritePolicy {
	bwp := as.NewBatchWritePolicy()
	bwp.CommitLevel = as.COMMIT_ALL
	bwp.RecordExistsAction = as.UPDATE_ONLY
	return bwp
}

// batchCreatePolicy is batchWritePolicy with RecordExistsAction =
// CREATE_ONLY, for PutShard's row inserts.
func (s *Store) batchCreatePolicy(ctx context.Context) *as.BatchWritePolicy {
	bwp := s.batchWritePolicy(ctx)
	bwp.RecordExistsAction = as.CREATE_ONLY
	return bwp
}

// batchPolicy is the parent policy for every BatchOperate write call: no
// retries (a retried non-idempotent per-record CAS would misreport a
// success as a loss).
func (s *Store) batchPolicy(ctx context.Context) *as.BatchPolicy {
	bp := as.NewWriteBatchPolicy()
	bp.MaxRetries = 0
	bp.TotalTimeout = s.totalTimeout(ctx)
	return bp
}

// readBatchPolicy is the parent policy for a read-only BatchOperate/BatchGet
// call (GetRows): reads may retry once.
func (s *Store) readBatchPolicy(ctx context.Context) *as.BatchPolicy {
	bp := as.NewBatchPolicy()
	bp.MaxRetries = 1
	bp.TotalTimeout = s.totalTimeout(ctx)
	return bp
}

// queryPolicy is the policy for every secondary-index query.
func (s *Store) queryPolicy(ctx context.Context) *as.QueryPolicy {
	qp := as.NewQueryPolicy()
	qp.MaxRetries = 1
	qp.TotalTimeout = s.totalTimeout(ctx)
	return qp
}

// readPolicy is the policy for a single-record read.
func (s *Store) readPolicy(ctx context.Context) *as.BasePolicy {
	rp := as.NewPolicy()
	rp.MaxRetries = 1
	rp.TotalTimeout = s.totalTimeout(ctx)
	return rp
}

// wrap maps transient/infrastructure result codes to store.ErrUnavailable and
// passes everything else through unchanged, in particular FILTERED_OUT and
// KEY_EXISTS_ERROR, which callers classify themselves.
func wrap(aerr as.Error) error {
	if aerr == nil {
		return nil
	}
	if aerr.Matches(
		types.TIMEOUT,
		types.NETWORK_ERROR,
		types.NO_AVAILABLE_CONNECTIONS_TO_NODE,
		types.SERVER_NOT_AVAILABLE,
		types.INVALID_NODE_ERROR,
	) {
		return fmt.Errorf("%w: %w", store.ErrUnavailable, aerr)
	}
	return aerr
}

// isFiltered reports whether err is exactly a FILTERED_OUT result: the
// record existed but this call's filter expression rejected it (a CAS loss).
func isFiltered(aerr as.Error) bool {
	return aerr != nil && aerr.Matches(types.FILTERED_OUT)
}

// isKeyNotFound reports whether err is a missing-record result.
func isKeyNotFound(aerr as.Error) bool {
	return aerr != nil && aerr.Matches(types.KEY_NOT_FOUND_ERROR)
}

// joinPoolTxID renders "pool|txidhex", used for shard keys and the ak index bin.
func joinPoolTxID(pool, txidHex string) string { return pool + "|" + txidHex }

// classifyBatch inspects a completed BatchOperate call's per-record results.
// A record whose error matches one of tolerate is silently skipped (missed,
// not an error): FILTERED_OUT/KEY_NOT_FOUND_ERROR for a CAS mutate,
// KEY_EXISTS_ERROR for PutShard's idempotent row inserts. A per-record
// InDoubt result, or a transient TIMEOUT/NETWORK_ERROR, aborts classification
// of the WHOLE batch with ErrUnavailable instead of guessing whether that
// one non-idempotent write actually landed: every mutation in this package
// is idempotent at the store.Store layer (see the package doc comment), so
// the caller can always safely replay the whole call.
func classifyBatch(records []as.BatchRecordIfc, tolerate ...types.ResultCode) ([]int, error) {
	applied := make([]int, 0, len(records))
	for i, r := range records {
		br := r.BatchRec()
		if br.Err == nil {
			applied = append(applied, i)
			continue
		}
		if br.InDoubt || br.Err.Matches(types.TIMEOUT, types.NETWORK_ERROR) {
			return nil, fmt.Errorf("%w: %w", store.ErrUnavailable, br.Err)
		}
		if len(tolerate) > 0 && br.Err.Matches(tolerate...) {
			continue
		}
		return nil, wrap(br.Err)
	}
	return applied, nil
}

// batchOperateRows runs one BatchOperate write over ops, building each
// record's filter expression and operations from build(op), and returns the
// raw per-record results for the caller to classify. A non-nil top-level
// error (a cluster/network-level failure, distinct from any per-record
// result) is always reported as ErrUnavailable: Reserve and friends are
// idempotent per (outpoint, token) at the store.Store layer, so the caller
// can always safely retry the whole call.
func (s *Store) batchOperateRows(ctx context.Context, ops []store.Outpoint, build func(op store.Outpoint) (*as.Expression, []*as.Operation)) ([]as.BatchRecordIfc, error) {
	records := make([]as.BatchRecordIfc, len(ops))
	for i, op := range ops {
		key, err := s.rowKey(op)
		if err != nil {
			return nil, err
		}
		filterExpr, opsList := build(op)
		bwp := s.batchWritePolicy(ctx)
		bwp.FilterExpression = filterExpr
		records[i] = as.NewBatchWrite(bwp, key, opsList...)
	}
	if aerr := s.client.BatchOperate(s.batchPolicy(ctx), records); aerr != nil {
		return nil, fmt.Errorf("%w: %w", store.ErrUnavailable, aerr)
	}
	return records, nil
}

// mutateRowsCount runs batchOperateRows with a single shared filter
// expression and per-op operations, and returns the count of records
// actually changed (Release/Spend/Resolve/Quarantine's apply step, and
// RetireShard's row cascade).
func (s *Store) mutateRowsCount(ctx context.Context, ops []store.Outpoint, filterExpr *as.Expression, opsFn func(op store.Outpoint) []*as.Operation) (int, error) {
	if len(ops) == 0 {
		return 0, nil
	}
	records, err := s.batchOperateRows(ctx, ops, func(op store.Outpoint) (*as.Expression, []*as.Operation) {
		return filterExpr, opsFn(op)
	})
	if err != nil {
		return 0, err
	}
	applied, err := classifyBatch(records, types.FILTERED_OUT, types.KEY_NOT_FOUND_ERROR)
	if err != nil {
		return 0, err
	}
	return len(applied), nil
}

// collectRecords drains a Recordset into a slice, closing it when done.
func collectRecords(rs *as.Recordset) ([]*as.Record, error) {
	defer func() { _ = rs.Close() }()
	var out []*as.Record
	for rec, err := range rs.Records() {
		if err != nil {
			return nil, wrap(err)
		}
		out = append(out, rec)
	}
	return out, nil
}

// queryEqual runs a secondary-index equality query against set/bin, gated by
// an optional additional filterExpr (nil for none), and returns the matching
// records with full bin data.
func (s *Store) queryEqual(ctx context.Context, set, bin string, value any, filterExpr *as.Expression) ([]*as.Record, error) {
	stmt := as.NewStatement(s.ns, set)
	stmt.Filter = as.NewEqualFilter(bin, value)
	qp := s.queryPolicy(ctx)
	qp.FilterExpression = filterExpr
	rs, aerr := s.client.Query(qp, stmt)
	if aerr != nil {
		return nil, wrap(aerr)
	}
	return collectRecords(rs)
}

// countEqual runs a secondary-index equality query against set/bin and
// returns how many records matched, without transferring their bin data.
func (s *Store) countEqual(ctx context.Context, set, bin string, value any) (int64, error) {
	stmt := as.NewStatement(s.ns, set)
	stmt.Filter = as.NewEqualFilter(bin, value)
	qp := s.queryPolicy(ctx)
	qp.IncludeBinData = false
	rs, aerr := s.client.Query(qp, stmt)
	if aerr != nil {
		return 0, wrap(aerr)
	}
	defer func() { _ = rs.Close() }()
	var n int64
	for _, err := range rs.Records() {
		if err != nil {
			return 0, wrap(err)
		}
		n++
	}
	return n, nil
}
