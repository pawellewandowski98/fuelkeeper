// Package aerospike implements store.Store on Aerospike using per-record
// compare-and-set: every mutation is a single-record (or per-record, inside a
// batch) conditional write guarded by a filter expression re-asserting the
// record's current state, mirroring the postgres/mongodb backends' CAS
// discipline without relying on Aerospike's multi-record transactions.
//
// # Consistency mode (operator note)
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
// Production deployments MUST run against a namespace configured with
// strong-consistency true (Enterprise Edition). In availability (AP) mode a
// node failure or network partition can let two sides of the cluster accept
// conflicting CAS writes to the same record, and a migration can resurrect
// an older copy: the "one row, one owner" invariant (G1) does not survive
// that. New therefore reads the namespace's strong-consistency info property
// on every node and refuses to start unless it is true.
// Config.AllowAvailabilityMode is the explicit opt-out for single-node
// development and test servers (Community Edition cannot run SC); it logs a
// loud warning.
//
// Recommended SC namespace settings:
//
//   - strong-consistency true, with a roster covering every node;
//   - replication-factor 2 or more (replication-factor 1 survives no node
//     loss: every partition on a failed node becomes unavailable, which
//     surfaces as ErrUnavailable rather than as wrong answers);
//   - commit-to-device true on the storage engine if the deployment must
//     survive a simultaneous crash of every replica of a partition without
//     losing acknowledged writes;
//   - default-ttl 0 (rows and shards never expire; this package never sets a
//     record TTL).
//
// This package never deletes production records: ReleaseLease is a guarded
// update, not a delete. The only delete is Truncate, a test helper, and it
// uses a tenant filter expression plus DurableDelete (on an SC namespace)
// so no expunge is ever needed.
//
// # Deadlines and cancellation
//
// The Aerospike Go client does not observe context cancellation: a context
// is consulted before each call and for its deadline, which narrows the
// per-operation TotalTimeout. A context WITHOUT a deadline that is cancelled
// mid-call does not interrupt the call; it runs to completion or to
// Config.Timeout (default 3s), whichever comes first.
//
// # Schema
//
// Set names are Config.SetPrefix + {rows, shards, txids, leases, meta}; they
// are fixed per deployment (Aerospike never frees a set name once created,
// and a namespace caps them at ~4K). Stores that share sets are isolated by
// Config.KeyPrefix instead: when non-empty it prefixes every record key and
// every indexed string value ("<KeyPrefix>|..."), and every record carries
// a "tn" (tenant) bin holding it, so two stores with different prefixes
// never see each other's records and Truncate can delete exactly one
// tenant's records.
//
//   - rows: keyed by KeyPrefix bytes followed by the outpoint's 36 raw bytes
//     (32-byte txid, big-endian uint32 vout). Presence-encoded index bins
//     (ak, present iff state == available; rx, present iff reserved; qx,
//     present iff quarantined; pst, the "pool|state" string, always present)
//     let ListAvailable, Quarantine, ListQuarantined and Stats run as
//     secondary-index queries that only ever touch rows currently in the
//     relevant state, without Aerospike supporting a Mongo-style partial
//     index: see bins.go.
//   - shards: keyed by "pool|txidhex". The record also carries the shard's
//     vout list ("vo"), which RetireShard uses to address every row of the
//     shard by key instead of discovering rows by state.
//   - txids: keyed by txidhex, enforcing a shard's global uniqueness
//     across pools (CREATE_ONLY insert; a losing insert under a different
//     pool is ErrShardExists).
//   - leases: keyed by lease name.
//   - meta: a single "schema" record recording the schema version.
//
// Every secondary index is named Config.SetPrefix + one of the names below
// (see indexSpecs, the single source of truth New's ensureIndexes builds
// from):
//
//   - rows_ak (rows set, bin ak, string): ListAvailable's per-shard scan of
//     available rows.
//   - rows_pst (rows set, bin pst, string): Stats' per-(pool, state) counts.
//   - rows_rx (rows set, bin rx, numeric): Quarantine's discovery scan of
//     reserved rows whose expiry is due.
//   - rows_qx (rows set, bin qx, numeric): ListQuarantined's paging scan of
//     quarantined rows.
//   - shards_ps (shards set, bin ps, string): ListShards' per-(pool, status)
//     scan.
//   - <set>_tn (one per set in {rows, shards, txids, leases, meta}, bin tn,
//     string): created only when Config.KeyPrefix is set (production
//     records carry no tn bin at all), and used only by Truncate to delete
//     exactly one tenant's records from each set.
package aerospike

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

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

// maxKeyPrefix bounds Config.KeyPrefix's length in bytes.
const maxKeyPrefix = 64

// schemaVersion is the current schema version, stored in the meta set. New
// refuses to open against a store whose recorded version differs: version 2
// added the vo/rx/qx/tn bins and the rx/qx indexes (replacing ea/qa).
const schemaVersion int64 = 2

// indexBuildWait bounds how long New waits for a secondary index to finish
// building (its own, or one a concurrent New created).
const indexBuildWait = 30 * time.Second

// errClosed is returned by every method once the store has been closed.
var errClosed = fmt.Errorf("%w: aerospike: store is closed", store.ErrUnavailable)

// Config configures New.
type Config struct {
	// Hosts are "host:port" (or bare "host", defaulting to port 3000) seed
	// addresses. Required.
	Hosts []string
	// Namespace is the Aerospike namespace this store uses. Required. It
	// must be configured with strong-consistency true unless
	// AllowAvailabilityMode is set; see the package doc comment.
	Namespace string
	// SetPrefix is prepended to every set name (and secondary-index name)
	// this package uses. It is a per-deployment constant: Aerospike never
	// frees a set name, so never generate it per store. May be empty.
	SetPrefix string
	// KeyPrefix, when non-empty, isolates this store from every other store
	// sharing the same sets: it prefixes every record key and indexed value
	// and is stored in each record's tn bin (see the package doc). It must
	// be valid UTF-8, at most 64 bytes, and contain neither '|' nor NUL.
	// Empty (the default) means plain keys.
	KeyPrefix string
	// AllowAvailabilityMode lets New open a namespace that is not in
	// strong-consistency mode. Development and tests only: see the package
	// doc comment. New logs a warning when it is used.
	AllowAvailabilityMode bool
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

// WithLogger sets the logger New uses for its availability-mode warning.
// Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option { return func(s *Store) { s.log = l } }

// Store is an Aerospike-backed store.Store.
type Store struct {
	client *as.Client
	hosts  []*as.Host // kept for Truncate, which reconnects independently (see Truncate)
	ns     string

	setPrefix string
	keyPrefix string // Config.KeyPrefix
	tenant    string // keyPrefix + "|", or "" when keyPrefix is empty

	rowsSet   string
	shardsSet string
	txidsSet  string
	leasesSet string
	metaSet   string

	// strongConsistency records whether New found the namespace in SC mode.
	// Truncate's deletes are durable exactly then: Community Edition (the
	// only AP servers AllowAvailabilityMode admits in practice) rejects
	// DurableDelete as ENTERPRISE_ONLY, and tombstones buy nothing in AP.
	strongConsistency bool

	timeout time.Duration
	now     func() time.Time
	log     *slog.Logger

	// queryHook, when non-nil, observes every secondary-index query's
	// returned records. Tests only.
	queryHook func(set string, recs []*as.Record)

	closed atomic.Bool
	once   sync.Once
}

var _ store.Store = (*Store)(nil)

// validateKeyPrefix checks Config.KeyPrefix.
func validateKeyPrefix(p string) error {
	switch {
	case len(p) > maxKeyPrefix:
		return fmt.Errorf("%w: aerospike: KeyPrefix longer than %d bytes", store.ErrInvalidArgument, maxKeyPrefix)
	case !utf8.ValidString(p):
		return fmt.Errorf("%w: aerospike: KeyPrefix must be valid UTF-8", store.ErrInvalidArgument)
	case strings.ContainsAny(p, "|\x00"):
		return fmt.Errorf("%w: aerospike: KeyPrefix must not contain '|' or NUL", store.ErrInvalidArgument)
	}
	return nil
}

// validatePool checks a pool name for this package's own boundary, on top
// of store.ValidatePool's generic checks: it must not contain '|'.
//
// '|' is this package's own field separator for every "[KeyPrefix|]pool|..."
// key and secondary-index value (shardID, pstFor, psFor: see bins.go). A
// pool name containing '|' would let two distinct logical stores collide on
// the same underlying Aerospike sets: a store with no KeyPrefix using pool
// "tX|main" derives exactly the same shardID/pst/ps/ak values as a store
// with KeyPrefix "tX" using pool "main" ("tX|main|..." either way). The
// top-level FuelKeeper config already rejects '|' in configured pool names,
// but this package cannot assume every caller goes through that config, so
// it enforces the same rule itself, in depth, with ErrInvalidArgument.
func validatePool(pool string) error {
	if err := store.ValidatePool(pool); err != nil {
		return err
	}
	if strings.Contains(pool, "|") {
		return fmt.Errorf("%w: aerospike: pool name must not contain '|'", store.ErrInvalidArgument)
	}
	return nil
}

// New connects to Aerospike, checks the namespace's consistency mode,
// ensures the secondary indexes this package relies on exist, and
// checks/records the schema version in the meta set.
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
	if err := validateKeyPrefix(cfg.KeyPrefix); err != nil {
		return nil, err
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

	tenant := ""
	if cfg.KeyPrefix != "" {
		tenant = cfg.KeyPrefix + "|"
	}
	s := &Store{
		client:    client,
		hosts:     hosts,
		ns:        cfg.Namespace,
		setPrefix: cfg.SetPrefix,
		keyPrefix: cfg.KeyPrefix,
		tenant:    tenant,
		rowsSet:   cfg.SetPrefix + "rows",
		shardsSet: cfg.SetPrefix + "shards",
		txidsSet:  cfg.SetPrefix + "txids",
		leasesSet: cfg.SetPrefix + "leases",
		metaSet:   cfg.SetPrefix + "meta",
		timeout:   timeout,
		now:       time.Now,
		log:       slog.Default(),
	}
	for _, o := range opts {
		o(s)
	}

	if !client.IsConnected() {
		client.Close()
		return nil, fmt.Errorf("%w: aerospike: not connected", store.ErrUnavailable)
	}
	if err := s.checkConsistency(cfg.AllowAvailabilityMode); err != nil {
		client.Close()
		return nil, err
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

// ErrNotStrongConsistency is returned by New for a namespace that is not in
// strong-consistency mode, unless Config.AllowAvailabilityMode is set.
var ErrNotStrongConsistency = errors.New("aerospike: namespace is not in strong-consistency mode")

// infoNode is the subset of *as.Node's info API checkConsistency needs. It
// exists so checkConsistency's own all-nodes decision logic (checkNodesSC)
// can be unit-tested against a fake info responder instead of requiring a
// live multi-node cluster; *as.Node satisfies it as-is.
type infoNode interface {
	RequestInfo(policy *as.InfoPolicy, name ...string) (map[string]string, as.Error)
	GetName() string
}

// checkConsistency reads "namespace/<ns>" from every node and requires
// strong-consistency=true on all of them, unless allowAP.
func (s *Store) checkConsistency(allowAP bool) error {
	nodes := s.client.GetNodes()
	if len(nodes) == 0 {
		return fmt.Errorf("%w: aerospike: no cluster nodes", store.ErrUnavailable)
	}
	infoNodes := make([]infoNode, len(nodes))
	for i, n := range nodes {
		infoNodes[i] = n
	}
	sc, err := s.checkNodesSC(infoNodes, allowAP)
	if err != nil {
		return err
	}
	s.strongConsistency = sc
	return nil
}

// checkNodesSC queries every node in nodes for the namespace's
// strong-consistency setting and reports whether ALL of them have it
// enabled, never just whether the first (or first non-conforming) one does:
// stopping at the first availability-mode node -- as an earlier version of
// this function did -- makes the result, and which node ends up named in
// the log/error, depend on node iteration order, and silently skips
// checking every node after it (including one that might itself fail to
// respond or fail to parse). Every node is always queried and every
// non-SC node is always collected, regardless of allowAP or of how many
// non-SC nodes are found.
//
// A hard failure (RequestInfo error, or a response that fails to parse) is
// always a hard error, even when allowAP is set: those are infrastructure/
// configuration problems, not an availability-mode opt-out.
func (s *Store) checkNodesSC(nodes []infoNode, allowAP bool) (bool, error) {
	cmd := "namespace/" + s.ns
	var apNodes []string
	for _, n := range nodes {
		info, aerr := n.RequestInfo(as.NewInfoPolicy(), cmd)
		if aerr != nil {
			return false, wrap(aerr)
		}
		sc, err := parseStrongConsistency(info[cmd])
		if err != nil {
			return false, fmt.Errorf("aerospike: node %s namespace %q: %w", n.GetName(), s.ns, err)
		}
		if !sc {
			apNodes = append(apNodes, n.GetName())
		}
	}
	if len(apNodes) == 0 {
		return true, nil
	}
	if !allowAP {
		return false, fmt.Errorf("%w: %q on node(s) %s (set strong-consistency true, or Config.AllowAvailabilityMode for development only)",
			ErrNotStrongConsistency, s.ns, strings.Join(apNodes, ", "))
	}
	s.log.Warn("aerospike: namespace is NOT in strong-consistency mode on some nodes and AllowAvailabilityMode is set: "+
		"reservation exclusivity is NOT guaranteed across node failures or partitions; never use this in production",
		"namespace", s.ns, "ap_nodes", apNodes)
	return false, nil
}

// parseStrongConsistency extracts strong-consistency from a "namespace/<ns>"
// info response ("k=v;k=v;..."). An empty response means the namespace does
// not exist.
func parseStrongConsistency(resp string) (bool, error) {
	if strings.TrimSpace(resp) == "" {
		return false, errors.New("namespace not found")
	}
	for _, kv := range strings.Split(resp, ";") {
		k, v, ok := strings.Cut(kv, "=")
		if ok && k == "strong-consistency" {
			return v == "true", nil
		}
	}
	return false, errors.New("strong-consistency property missing from namespace info")
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

// indexSpecs lists every secondary index. Names are fixed per SetPrefix, so
// they are created once per deployment and shared by every KeyPrefix. The
// tn indexes exist only for Truncate and are only created when a KeyPrefix
// is in use (production records carry no tn bin).
func (s *Store) indexSpecs() []indexSpec {
	p := s.setPrefix
	specs := []indexSpec{
		{s.rowsSet, p + "rows_ak", binAK, as.STRING},
		{s.rowsSet, p + "rows_pst", binPST, as.STRING},
		{s.rowsSet, p + "rows_rx", binRX, as.NUMERIC},
		{s.rowsSet, p + "rows_qx", binQX, as.NUMERIC},
		{s.shardsSet, p + "shards_ps", binPS, as.STRING},
	}
	if s.keyPrefix != "" {
		for _, set := range s.allSets() {
			specs = append(specs, indexSpec{set, set + "_tn", binTN, as.STRING})
		}
	}
	return specs
}

func (s *Store) allSets() []string {
	return []string{s.rowsSet, s.shardsSet, s.txidsSet, s.leasesSet, s.metaSet}
}

// sindexEntry is one parsed entry of a sindex-list info response.
type sindexEntry struct {
	name, set, bin, typ, state string
}

// parseSindexList parses a "sindex-list:ns=<ns>" info response.
func parseSindexList(resp string) []sindexEntry {
	var out []sindexEntry
	for _, item := range strings.Split(resp, ";") {
		if strings.TrimSpace(item) == "" {
			continue
		}
		var e sindexEntry
		for _, kv := range strings.Split(item, ":") {
			k, v, _ := strings.Cut(kv, "=")
			switch k {
			case "indexname":
				e.name = v
			case "set":
				e.set = v
			case "bin":
				e.bin = v
			case "type":
				e.typ = strings.ToLower(v)
			case "state":
				e.state = v
			}
		}
		out = append(out, e)
	}
	return out
}

func idxTypeName(t as.IndexType) string {
	if t == as.NUMERIC {
		return "numeric"
	}
	return "string"
}

// matchExistingIndex resolves an INDEX_FOUND result: the existing index
// (same name, or another name on the same set/bin) must index the same set
// and bin with the same type. It reports whether that index is readable.
func matchExistingIndex(entries []sindexEntry, spec indexSpec) (ready bool, err error) {
	want := idxTypeName(spec.idxType)
	for _, e := range entries {
		if e.name != spec.indexName && (e.set != spec.set || e.bin != spec.binName) {
			continue
		}
		if e.set != spec.set || e.bin != spec.binName || e.typ != want {
			return false, fmt.Errorf("aerospike: existing index %q is set=%s bin=%s type=%s; want set=%s bin=%s type=%s",
				e.name, e.set, e.bin, e.typ, spec.set, spec.binName, want)
		}
		return e.state == "RW", nil
	}
	return false, nil
}

// ensureIndexes creates every secondary index this package relies on,
// idempotently. INDEX_FOUND is accepted only when the existing index covers
// the same set/bin with the same type; New then waits (bounded) for it to
// become readable, since a concurrent New may still be building it.
func (s *Store) ensureIndexes(ctx context.Context) error {
	for _, spec := range s.indexSpecs() {
		wp := as.NewWritePolicy(0, 0)
		wp.TotalTimeout = s.totalTimeout(ctx)
		task, aerr := s.client.CreateIndex(wp, s.ns, spec.set, spec.indexName, spec.binName, spec.idxType)
		if aerr != nil {
			if !isCode(aerr, types.INDEX_FOUND) {
				return wrap(aerr)
			}
			if err := s.awaitExistingIndex(ctx, spec); err != nil {
				return err
			}
			continue
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

func (s *Store) awaitExistingIndex(ctx context.Context, spec indexSpec) error {
	deadline := time.Now().Add(indexBuildWait)
	cmd := "sindex-list:ns=" + s.ns
	for {
		nodes := s.client.GetNodes()
		if len(nodes) == 0 {
			return fmt.Errorf("%w: aerospike: no cluster nodes", store.ErrUnavailable)
		}
		allReady := true
		for _, n := range nodes {
			info, aerr := n.RequestInfo(as.NewInfoPolicy(), cmd)
			if aerr != nil {
				return wrap(aerr)
			}
			ready, err := matchExistingIndex(parseSindexList(info[cmd]), spec)
			if err != nil {
				return err
			}
			allReady = allReady && ready
		}
		if allReady {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: aerospike: index %s not readable within %s", store.ErrUnavailable, spec.indexName, indexBuildWait)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// ensureSchema atomically creates the meta "schema" record on a fresh store
// (CREATE_ONLY leaves an existing record untouched) and refuses to open
// against a store whose recorded version is not this build's.
func (s *Store) ensureSchema(ctx context.Context) error {
	key, err := s.metaKey()
	if err != nil {
		return err
	}
	wp := s.createPolicy(ctx)
	if aerr := s.client.Put(wp, key, s.withTenant(as.BinMap{binSchema: schemaVersion})); aerr != nil {
		if !isCode(aerr, types.KEY_EXISTS_ERROR) {
			return wrap(aerr)
		}
		rec, gerr := s.client.Get(s.readPolicy(ctx), key, binSchema)
		if gerr != nil {
			return wrap(gerr)
		}
		v := toInt64(rec.Bins[binSchema])
		if v != schemaVersion {
			return fmt.Errorf("aerospike: schema version %d unsupported (this build wants %d)", v, schemaVersion)
		}
	}
	return nil
}

// Truncate deletes every record of this store's KeyPrefix from every set
// (test cleanup for stores sharing fixed sets) and returns the first error.
// It refuses to run without a KeyPrefix, since the sets are shared. Records
// are found through the tn indexes and deleted with a tn filter expression,
// so another tenant's record can never be removed; on a strong-consistency
// namespace the deletes are durable (DurableDelete), so no expunge is ever
// needed. (Community Edition rejects DurableDelete; an availability-mode
// namespace gets plain deletes.)
// Indexes and set names are shared and are left alone.
//
// Truncate opens its own short-lived connection rather than reusing s.client:
// callers register it via t.Cleanup alongside RunSuite's own
// t.Cleanup(Close), and t.Cleanup runs in LIFO order, so Close (registered
// after isolatedStore returns) always runs before Truncate (registered
// inside isolatedStore, earlier).
func (s *Store) Truncate() error {
	if s.keyPrefix == "" {
		return fmt.Errorf("%w: aerospike: Truncate requires a KeyPrefix", store.ErrInvalidArgument)
	}
	cp := as.NewClientPolicy()
	cp.Timeout = s.timeout
	client, aerr := as.NewClientWithPolicyAndHost(cp, s.hosts...)
	if aerr != nil {
		return wrap(aerr)
	}
	defer client.Close()

	for _, set := range s.allSets() {
		if err := s.truncateSet(client, set); err != nil {
			return fmt.Errorf("aerospike: truncate %s: %w", set, err)
		}
	}
	return nil
}

func (s *Store) truncateSet(client *as.Client, set string) error {
	stmt := as.NewStatement(s.ns, set)
	stmt.Filter = as.NewEqualFilter(binTN, s.keyPrefix)
	qp := as.NewQueryPolicy()
	qp.IncludeBinData = false
	qp.TotalTimeout = 30 * time.Second
	rs, aerr := client.Query(qp, stmt)
	if aerr != nil {
		return wrap(aerr)
	}
	var keys []*as.Key
	for rec, err := range rs.Records() {
		if err != nil {
			_ = rs.Close()
			return wrap(err)
		}
		keys = append(keys, rec.Key)
	}
	_ = rs.Close()

	dp := as.NewBatchDeletePolicy()
	dp.FilterExpression = s.tenantExpr()
	dp.DurableDelete = s.strongConsistency
	dp.CommitLevel = as.COMMIT_ALL
	for len(keys) > 0 {
		n := min(len(keys), maxBatch)
		bp := as.NewWriteBatchPolicy()
		bp.MaxRetries = 0
		bp.TotalTimeout = 30 * time.Second
		recs, berr := client.BatchDelete(bp, dp, keys[:n])
		if berr != nil {
			return wrap(berr)
		}
		for _, r := range recs {
			switch r.ResultCode {
			case types.OK, types.KEY_NOT_FOUND_ERROR, types.FILTERED_OUT:
			default:
				return resultErr(r.Err, r.ResultCode)
			}
		}
		keys = keys[n:]
	}
	return nil
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
// caller's own context deadline is sooner. The client never observes
// cancellation itself (see the package doc, "Deadlines and cancellation").
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

// transientCodes are the result codes that mean "the cluster could not
// serve this right now; the same call may succeed later". Anything else is
// permanent (a bad request, or a CAS loss the caller classifies).
var transientCodes = map[types.ResultCode]struct{}{
	types.TIMEOUT:                          {},
	types.NETWORK_ERROR:                    {},
	types.NO_AVAILABLE_CONNECTIONS_TO_NODE: {},
	types.SERVER_NOT_AVAILABLE:             {},
	types.INVALID_NODE_ERROR:               {},
	types.KEY_BUSY:                         {},
	types.XDR_KEY_BUSY:                     {},
	types.DEVICE_OVERLOAD:                  {},
	types.PARTITION_UNAVAILABLE:            {},
	types.MAX_RETRIES_EXCEEDED:             {},
	types.SERVER_MEM_ERROR:                 {},
	types.BATCH_QUEUES_FULL:                {},
	types.BATCH_MAX_REQUESTS_EXCEEDED:      {},
	types.NO_RESPONSE:                      {},
	types.QUERY_TIMEOUT:                    {},
	types.MAX_ERROR_RATE:                   {},
	types.CLUSTER_KEY_MISMATCH:             {},
	types.INDEX_NOTREADABLE:                {},
}

// outerCode returns the result code of the outermost AerospikeError in the
// chain. Classification never looks deeper: the client attaches earlier
// (e.g. pooled-connection retry) errors underneath the one that actually
// decided the call, and a code buried underneath must not change whether
// the call looks retryable.
//
// It calls the error's own As method rather than errors.As: that copies the
// outermost link's fields for both *AerospikeError and the client's
// (unexported) constant error type, and never walks the chain.
func outerCode(err error) (types.ResultCode, bool) {
	a, ok := err.(interface{ As(target any) bool })
	if !ok {
		return 0, false
	}
	ae := &as.AerospikeError{}
	if !a.As(ae) {
		return 0, false
	}
	return ae.ResultCode, true
}

// isCode reports whether aerr's outermost result code is one of codes.
func isCode(aerr as.Error, codes ...types.ResultCode) bool {
	if aerr == nil {
		return false
	}
	c, ok := outerCode(aerr)
	if !ok {
		return false
	}
	for _, want := range codes {
		if c == want {
			return true
		}
	}
	return false
}

// isTransient reports whether aerr is an infrastructure failure worth
// retrying: any in-doubt error, or an outermost transient result code.
func isTransient(aerr as.Error) bool {
	if aerr == nil {
		return false
	}
	if aerr.IsInDoubt() {
		return true
	}
	c, ok := outerCode(aerr)
	if !ok {
		return false
	}
	_, t := transientCodes[c]
	return t
}

// wrap maps transient/infrastructure failures (isTransient) to
// store.ErrUnavailable and passes everything else through unchanged, in
// particular FILTERED_OUT and KEY_EXISTS_ERROR, which callers classify
// themselves.
func wrap(aerr as.Error) error {
	if aerr == nil {
		return nil
	}
	if isTransient(aerr) {
		return fmt.Errorf("%w: %w", store.ErrUnavailable, aerr)
	}
	return aerr
}

// resultErr turns a per-record (batch) result into a classified error.
func resultErr(err as.Error, code types.ResultCode) error {
	if err == nil {
		err = &as.AerospikeError{ResultCode: code}
	}
	return wrap(err)
}

// isFiltered reports whether err is exactly a FILTERED_OUT result: the
// record existed but this call's filter expression rejected it (a CAS loss).
func isFiltered(aerr as.Error) bool { return isCode(aerr, types.FILTERED_OUT) }

// isKeyNotFound reports whether err is a missing-record result.
func isKeyNotFound(aerr as.Error) bool { return isCode(aerr, types.KEY_NOT_FOUND_ERROR) }

// joinPoolTxID renders "pool|txidhex", used for shard keys and the ak index bin.
func joinPoolTxID(pool, txidHex string) string { return pool + "|" + txidHex }

// classifyBatch inspects a completed BatchOperate call's per-record results.
// Only ResultCode == OK counts as applied. A record whose code is one of
// tolerate is silently skipped (missed, not an error): FILTERED_OUT/
// KEY_NOT_FOUND_ERROR for a CAS mutate, KEY_EXISTS_ERROR for PutShard's
// idempotent row inserts. A per-record in-doubt or transient result aborts
// classification of the WHOLE batch with ErrUnavailable instead of guessing
// whether that one non-idempotent write actually landed: every mutation in
// this package is idempotent at the store.Store layer (see the package doc
// comment), so the caller can always safely replay the whole call.
func classifyBatch(records []as.BatchRecordIfc, tolerate ...types.ResultCode) ([]int, error) {
	applied := make([]int, 0, len(records))
	for i, r := range records {
		br := r.BatchRec()
		if br.ResultCode == types.OK {
			applied = append(applied, i)
			continue
		}
		_, transientCode := transientCodes[br.ResultCode]
		if br.InDoubt || transientCode || isTransient(br.Err) {
			return nil, fmt.Errorf("%w: %w", store.ErrUnavailable, resultErr(br.Err, br.ResultCode))
		}
		tolerated := false
		for _, c := range tolerate {
			if br.ResultCode == c {
				tolerated = true
				break
			}
		}
		if tolerated {
			continue
		}
		return nil, resultErr(br.Err, br.ResultCode)
	}
	return applied, nil
}

// batchOperate runs one BatchOperate write and classifies a top-level error
// exactly like any other error (wrap): transient failures become
// ErrUnavailable, permanent ones pass through.
func (s *Store) batchOperate(ctx context.Context, records []as.BatchRecordIfc) error {
	if aerr := s.client.BatchOperate(s.batchPolicy(ctx), records); aerr != nil {
		return wrap(aerr)
	}
	return nil
}

// batchOperateRows runs one BatchOperate write over ops, building each
// record's filter expression and operations from build(op), and returns the
// raw per-record results for the caller to classify.
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
	if err := s.batchOperate(ctx, records); err != nil {
		return nil, err
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

// query runs stmt with qp and streams every record to fn, reporting the
// records to queryHook when set.
func (s *Store) query(qp *as.QueryPolicy, stmt *as.Statement, fn func(*as.Record)) error {
	rs, aerr := s.client.Query(qp, stmt)
	if aerr != nil {
		return wrap(aerr)
	}
	defer func() { _ = rs.Close() }()
	var seen []*as.Record
	for rec, err := range rs.Records() {
		if err != nil {
			return wrap(err)
		}
		if s.queryHook != nil {
			seen = append(seen, rec)
		}
		fn(rec)
	}
	if s.queryHook != nil {
		s.queryHook(stmt.SetName, seen)
	}
	return nil
}

// queryEqual runs a secondary-index equality query against set/bin, gated by
// an optional additional filterExpr (nil for none), and returns the matching
// records with the named bins (every bin when binNames is empty).
func (s *Store) queryEqual(ctx context.Context, set, bin string, value any, filterExpr *as.Expression, binNames ...string) ([]*as.Record, error) {
	stmt := as.NewStatement(s.ns, set, binNames...)
	stmt.Filter = as.NewEqualFilter(bin, value)
	qp := s.queryPolicy(ctx)
	qp.FilterExpression = filterExpr
	var out []*as.Record
	err := s.query(qp, stmt, func(rec *as.Record) { out = append(out, rec) })
	return out, err
}

// countEqual runs a secondary-index equality query against set/bin and
// returns how many records matched, without transferring their bin data.
func (s *Store) countEqual(ctx context.Context, set, bin string, value any) (int64, error) {
	stmt := as.NewStatement(s.ns, set)
	stmt.Filter = as.NewEqualFilter(bin, value)
	qp := s.queryPolicy(ctx)
	qp.IncludeBinData = false
	var n int64
	err := s.query(qp, stmt, func(*as.Record) { n++ })
	return n, err
}
