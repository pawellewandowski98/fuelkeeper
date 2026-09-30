// Package client is a Go client for the FuelKeeper HTTP API (pkg/api): typed
// wrappers over reserve/release/spend/pools, a per-pool LRU BEEF cache with
// singleflight-deduplicated fetches, the admin endpoints, (in beef.go)
// helpers that turn a reservation into go-sdk createAction inputs, and (in
// settle.go) Settle, which maps a consumer's own broadcast outcome to the
// right spend/release/hint call.
package client

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
)

const (
	// defaultTimeout bounds a single HTTP round trip made by the default
	// client (Client.hc), including connection setup and reading the full
	// response body (net/http's Client.Timeout covers both). A slow link or
	// an unusually large BEEF blob may need a longer bound; pass a custom
	// *http.Client via WithHTTPClient to change it.
	defaultTimeout = 10 * time.Second
	// maxIdleConnsPerHost and maxIdleConns raise the default transport's
	// keep-alive pool (cloned from http.DefaultTransport, which caps both far
	// lower) well past a single-server client's own needs, so a load
	// generator that reuses one Client for many concurrent calls to one
	// FuelKeeper host (e.g. fuelbench) does not thrash connections. Pass a
	// custom *http.Client via WithHTTPClient for an even larger pool, a
	// different proxy, or per-host tuning across several FuelKeeper hosts.
	maxIdleConnsPerHost = 256
	maxIdleConns        = 512
	// defaultUserAgent is sent unless overridden with WithUserAgent.
	defaultUserAgent = "fuelkeeper-client/1"

	// acceptJSON and acceptOctetStream are the "Accept" header values sent on
	// JSON calls and the BEEF call respectively.
	acceptJSON        = "application/json"
	acceptOctetStream = "application/octet-stream"

	// maxJSONBytes bounds how much of a 2xx JSON response body doJSON will
	// read, so a misbehaving server cannot make the client buffer an
	// unbounded amount of memory.
	maxJSONBytes = 4 << 20 // 4 MiB
	// maxBEEFBytes bounds how much of a BEEF response body BEEF will read.
	maxBEEFBytes = 32 << 20 // 32 MiB

	// maxBEEFCacheEntriesPerPool bounds each pool's BEEF cache: once a pool
	// has this many cached shards, caching another evicts the least recently
	// used one.
	maxBEEFCacheEntriesPerPool = 64
)

// ReserveRequest asks a pool for fuel; see wire.ReserveRequest.
type ReserveRequest struct {
	Count    int
	MinCount int           // 0 = Count
	TTL      time.Duration // 0 = pool default
}

// beefEntry is one cached BEEF blob and its known shard version, held as the
// Value of an element in beefLRU[pool] (front = most recently used).
type beefEntry struct {
	pool    string
	txid    string
	version int64
	bytes   []byte
}

// Observer is a per-call hook invoked after every HTTP request a Client
// makes (a cache hit that never reaches the network does not invoke it): for
// timing/telemetry, e.g. fuelbench or fueldemo. method and path identify the
// endpoint (path excludes the query string, to keep cardinality low); status
// is the resulting HTTP status code, or 0 if no response was ever received
// (a transport failure); d is the request's wall-clock duration, from just
// before it was sent to just after its body was fully read or the failure
// was known; err is the error the call is returning (nil on success). An
// Observer must return quickly and must not call back into the Client that
// invoked it, to avoid deadlocking that Client.
type Observer func(method, path string, status int, d time.Duration, err error)

// Client is a FuelKeeper HTTP API client. It is safe for concurrent use.
type Client struct {
	base      *url.URL
	hc        *http.Client
	apiKey    string
	userAgent string
	observer  Observer

	mu      sync.Mutex
	beefIdx map[string]*list.Element // "pool/txid" -> element in beefLRU[pool]
	beefLRU map[string]*list.List    // pool -> LRU list of *beefEntry
	misses  int
	sf      singleflight.Group // dedupes concurrent fetches of the same pool/txid@version
}

// Option configures a Client constructed by New.
type Option func(*Client)

// WithAPIKey sets the bearer API key sent as "Authorization: Bearer <key>"
// with every request.
func WithAPIKey(key string) Option {
	return func(c *Client) { c.apiKey = key }
}

// WithHTTPClient overrides the default *http.Client: a 10 s timeout over a
// transport cloned from http.DefaultTransport (keeping its dial and TLS
// handshake timeouts, proxy support and the rest of its defaults) with
// MaxIdleConnsPerHost raised to 256 and MaxIdleConns to 512. A load generator
// driving far more concurrent requests at one FuelKeeper host than that --
// fuelbench, say -- should pass its own *http.Client with a larger pool
// still (and no per-request Timeout, if it wants requests bounded by ctx
// alone).
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.hc = hc }
}

// WithUserAgent overrides the default "User-Agent" header value.
func WithUserAgent(ua string) Option {
	return func(c *Client) { c.userAgent = ua }
}

// WithObserver sets a per-call hook invoked after every HTTP request (see
// Observer). Only one observer may be set; a later WithObserver replaces an
// earlier one.
func WithObserver(fn Observer) Option {
	return func(c *Client) { c.observer = fn }
}

// New builds a Client for the FuelKeeper server at baseURL, which must be an
// absolute URL with a scheme and host (e.g. "https://fuel.example.com").
func New(baseURL string, opts ...Option) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("client: invalid base url %q: %w", baseURL, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("client: base url %q must be absolute (scheme and host required)", baseURL)
	}
	c := &Client{
		base: u,
		hc: &http.Client{
			Timeout:   defaultTimeout,
			Transport: newDefaultTransport(),
		},
		userAgent: defaultUserAgent,
		beefIdx:   make(map[string]*list.Element),
		beefLRU:   make(map[string]*list.List),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// newDefaultTransport clones http.DefaultTransport -- keeping its dial and
// TLS handshake timeouts, proxy support and the rest of its defaults -- and
// only raises the idle-connection pool limits (see WithHTTPClient).
func newDefaultTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone() // http.DefaultTransport is always *http.Transport
	t.MaxIdleConnsPerHost = maxIdleConnsPerHost
	t.MaxIdleConns = maxIdleConns
	return t
}

// newRequest builds an HTTP request against path (joined onto the base URL)
// with rawQuery as its query string, JSON-encoding body when non-nil and
// setting the Accept header to accept.
func (c *Client) newRequest(ctx context.Context, method, path, rawQuery, accept string, body any) (*http.Request, error) {
	u := c.base.JoinPath(path)
	u.RawQuery = rawQuery
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("client: encode request body: %w", err)
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), r)
	if err != nil {
		return nil, fmt.Errorf("client: build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	req.Header.Set("User-Agent", c.userAgent)
	return req, nil
}

// do sends req and returns the raw response: a transport failure (dial,
// timeout, caller cancellation) is mapped by wrapTransportErr, never returned
// as-is. The caller is responsible for closing resp.Body.
func (c *Client) do(ctx context.Context, req *http.Request) (*http.Response, error) {
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, wrapTransportErr(ctx, err)
	}
	return resp, nil
}

// readErrorBody reads at most maxErrorBodyBytes of resp.Body and closes it,
// for building an *APIError.
func readErrorBody(resp *http.Response) []byte {
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
	return body
}

// notify invokes the configured Observer (if any) for one completed request:
// status is 0 when no response was ever received.
func (c *Client) notify(method, path string, status int, start time.Time, err error) {
	if c.observer == nil {
		return
	}
	c.observer(method, path, status, time.Since(start), err)
}

// statusOf extracts the HTTP status err carries as an *APIError, or 0 for a
// transport failure (which never reached a response).
func statusOf(err error) int {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Status
	}
	return 0
}

// doJSON sends a JSON request and, on a 2xx response, decodes the body into
// into (which may be nil to discard it). A non-2xx response is mapped to
// *APIError. A failure reading or decoding a 2xx body (e.g. a connection
// dropped mid-response) goes through wrapTransportErr like any other
// transport failure, since the client cannot tell that apart from the server
// having gone away. The response body is always closed before doJSON
// returns. The configured Observer (see WithObserver), if any, is notified
// once per call.
func (c *Client) doJSON(ctx context.Context, method, path, rawQuery string, body, into any) (err error) {
	start := time.Now()
	status := 0
	defer func() { c.notify(method, path, status, start, err) }()

	req, err := c.newRequest(ctx, method, path, rawQuery, acceptJSON, body)
	if err != nil {
		return err
	}
	resp, err := c.do(ctx, req)
	if err != nil {
		status = statusOf(err)
		return err
	}
	status = resp.StatusCode
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err = newAPIError(resp.StatusCode, readErrorBody(resp), resp.Header)
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if into == nil {
		return nil
	}
	if decErr := json.NewDecoder(io.LimitReader(resp.Body, maxJSONBytes)).Decode(into); decErr != nil {
		err = wrapTransportErr(ctx, decErr)
		return err
	}
	return nil
}

// getRaw sends a GET request (Accept: application/octet-stream) to path and
// returns the response for the caller to read directly (the BEEF endpoint's
// body may be large, which doJSON should not buffer). A non-2xx response is
// mapped to *APIError and its body is closed here; on success the caller
// owns resp.Body and must close it.
func (c *Client) getRaw(ctx context.Context, path string) (*http.Response, error) {
	req, err := c.newRequest(ctx, http.MethodGet, path, "", acceptOctetStream, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, newAPIError(resp.StatusCode, readErrorBody(resp), resp.Header)
	}
	return resp, nil
}

// poolPath builds "/v1/pools/{pool}" + suffix, URL-escaping pool.
func poolPath(poolName, suffix string) string {
	return "/v1/pools/" + url.PathEscape(poolName) + suffix
}

// Reserve asks pool for fuel outputs.
func (c *Client) Reserve(ctx context.Context, pool string, req ReserveRequest) (*wire.ReserveResponse, error) {
	wreq := wire.ReserveRequest{
		Count:      req.Count,
		MinCount:   req.MinCount,
		TTLSeconds: int(req.TTL / time.Second),
	}
	var out wire.ReserveResponse
	if err := c.doJSON(ctx, http.MethodPost, poolPath(pool, "/reserve"), "", wreq, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// retryConfig holds ReserveWithRetry's options.
type retryConfig struct {
	maxAttempts int // 0 = unbounded (retry until ctx is done)
}

// RetryOption configures ReserveWithRetry.
type RetryOption func(*retryConfig)

// WithMaxAttempts caps ReserveWithRetry at n attempts (including the first),
// regardless of how much of ctx's deadline remains.
func WithMaxAttempts(n int) RetryOption {
	return func(c *retryConfig) { c.maxAttempts = n }
}

const (
	// retryBaseDelay is ReserveWithRetry's first backoff wait, absent a
	// server-given Retry-After.
	retryBaseDelay = 50 * time.Millisecond
	// retryMaxDelay caps the exponential backoff.
	retryMaxDelay = 2 * time.Second
	// retryJitterFactor scales each backoff wait by a random factor in
	// [1-retryJitterFactor, 1+retryJitterFactor].
	retryJitterFactor = 0.2
)

// ReserveWithRetry calls Reserve, retrying while the error is IsRetryable:
// waiting the server-given Retry-After (also jittered by +/-20%, so many
// clients hitting the same Retry-After do not all retry in lockstep) when the
// error carries one, or otherwise an exponential backoff starting at
// retryBaseDelay and doubling up to retryMaxDelay with +/-20% jitter. Either
// wait is capped to ctx's remaining time, so ReserveWithRetry never sleeps
// past ctx's deadline. It gives up once a non-retryable error is returned or
// (with WithMaxAttempts) once the attempt cap is reached, returning that
// error; if ctx ends first (while waiting to retry), it returns
// errors.Join(ctx.Err(), lastErr) so the last API error is not lost.
func (c *Client) ReserveWithRetry(ctx context.Context, pool string, req ReserveRequest, opts ...RetryOption) (*wire.ReserveResponse, error) {
	var cfg retryConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	delay := retryBaseDelay
	var lastErr error
	for attempt := 1; ; attempt++ {
		if cErr := ctx.Err(); cErr != nil {
			return nil, errors.Join(cErr, lastErr)
		}
		res, err := c.Reserve(ctx, pool, req)
		if err == nil {
			return res, nil
		}
		lastErr = err
		if !IsRetryable(err) {
			return nil, err
		}
		if cfg.maxAttempts > 0 && attempt >= cfg.maxAttempts {
			return nil, err
		}
		wait := jitter(delay)
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.RetryAfter > 0 {
			wait = jitter(time.Duration(apiErr.RetryAfter) * time.Second)
		}
		delay = min(delay*2, retryMaxDelay)

		// If ctx's own deadline is the limiting factor, block on ctx.Done()
		// directly instead of racing a separately-scheduled timer against
		// it: two independent timers set for "the same" instant have no
		// guaranteed firing order, so a timer merely capped to
		// time.Until(deadline) can still fire microseconds before ctx is
		// actually marked done, letting the loop slip through to one more
		// Reserve call against an already-expiring ctx -- which fails with
		// its own context-flavored transport error mid-request and would
		// silently lose lastErr. Waiting on ctx.Done() itself has no such
		// race: it only ever unblocks once ctx.Err() is already set.
		if deadline, ok := ctx.Deadline(); ok {
			if remaining := time.Until(deadline); remaining <= wait {
				<-ctx.Done()
				return nil, errors.Join(ctx.Err(), lastErr)
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, errors.Join(ctx.Err(), lastErr)
		case <-timer.C:
		}
	}
}

// jitter scales d by a random factor in [1-retryJitterFactor, 1+retryJitterFactor].
func jitter(d time.Duration) time.Duration {
	factor := 1 + retryJitterFactor*(2*rand.Float64()-1)
	return time.Duration(float64(d) * factor)
}

// Release returns reserved outpoints under token to pool, reporting how many
// changed. A consumer settling its own broadcast's outcome should call
// Settle instead of calling this directly: Release must never be called for
// an outpoint whose broadcast was rejected as a double-spend (see Settle) --
// a spent output released back to the pool would be handed to every later
// consumer, each failing the same way in turn.
func (c *Client) Release(ctx context.Context, pool, token string, outpoints []string) (int, error) {
	req := wire.ReleaseRequest{Token: token, Outpoints: outpoints}
	var out wire.CountResponse
	if err := c.doJSON(ctx, http.MethodPost, poolPath(pool, "/release"), "", req, &out); err != nil {
		return 0, err
	}
	return out.Released, nil
}

// Spend reports outpoints under token spent in txid, reporting how many
// changed. A consumer settling its own broadcast's outcome should call
// Settle instead of calling this directly: Spend must never be called for a
// rejection that names no competing transaction -- under spend_policy:
// trust a Spend is terminal the moment the server accepts it, so an
// invalid-but-not-double-spent output would be burned rather than returned
// to the pool for reuse (see Settle).
func (c *Client) Spend(ctx context.Context, pool, token string, outpoints []string, txid string) (int, error) {
	req := wire.SpendRequest{Token: token, Outpoints: outpoints, TxID: txid}
	var out wire.CountResponse
	if err := c.doJSON(ctx, http.MethodPost, poolPath(pool, "/spend"), "", req, &out); err != nil {
		return 0, err
	}
	return out.Accepted, nil
}

// Pools returns every configured pool with its live stats.
func (c *Client) Pools(ctx context.Context) ([]wire.Pool, error) {
	var out []wire.Pool
	if err := c.doJSON(ctx, http.MethodGet, "/v1/pools", "", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Pool returns one configured pool with its live stats.
func (c *Client) Pool(ctx context.Context, pool string) (*wire.Pool, error) {
	var out wire.Pool
	if err := c.doJSON(ctx, http.MethodGet, poolPath(pool, ""), "", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// beefCacheKey identifies a cached BEEF by pool and txid.
func beefCacheKey(pool, txid string) string { return pool + "/" + txid }

// parseETagVersion extracts the shard version from an ETag of the form
// `"v<n>"` (as api's beef handler sets it), also accepting the weak form
// `W/"v<n>"`. It returns 0 if the value cannot be parsed.
func parseETagVersion(etag string) int64 {
	s := strings.TrimPrefix(etag, "W/")
	s = strings.Trim(s, `"`)
	s = strings.TrimPrefix(s, "v")
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// cacheLookup returns a defensive copy (see bytes.Clone; item 4: callers must
// not be able to corrupt the cache by mutating what they got back) of the
// cached BEEF for pool/txid when its cached version is at least minVersion,
// and marks the entry most recently used.
func (c *Client) cacheLookup(pool, txid string, minVersion int64) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.beefIdx[beefCacheKey(pool, txid)]
	if !ok {
		return nil, false
	}
	entry, _ := elem.Value.(*beefEntry)
	if entry.version < minVersion {
		return nil, false
	}
	c.beefLRU[pool].MoveToFront(elem)
	return bytes.Clone(entry.bytes), true
}

// cacheStore records a freshly fetched BEEF for pool/txid at version, unless
// a version already cached is at least as new: a fetch for an older version
// that happens to complete after a newer one must never clobber it. Each
// pool's cache is bounded to maxBEEFCacheEntriesPerPool entries, evicting the
// least recently used one once that bound would be exceeded.
func (c *Client) cacheStore(pool, txid string, version int64, body []byte) {
	key := beefCacheKey(pool, txid)
	stored := bytes.Clone(body)
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.beefIdx[key]; ok {
		entry, _ := elem.Value.(*beefEntry)
		if version < entry.version {
			return
		}
		entry.version = version
		entry.bytes = stored
		c.beefLRU[pool].MoveToFront(elem)
		return
	}
	lru, ok := c.beefLRU[pool]
	if !ok {
		lru = list.New()
		c.beefLRU[pool] = lru
	}
	elem := lru.PushFront(&beefEntry{pool: pool, txid: txid, version: version, bytes: stored})
	c.beefIdx[key] = elem
	if lru.Len() > maxBEEFCacheEntriesPerPool {
		oldest := lru.Back()
		lru.Remove(oldest)
		oldestEntry, _ := oldest.Value.(*beefEntry)
		delete(c.beefIdx, beefCacheKey(oldestEntry.pool, oldestEntry.txid))
	}
}

// BEEF returns shard's BEEF bytes, serving a defensive copy from the
// client's per-pool LRU cache when the cached version is at least
// shard.Version and otherwise fetching a fresh copy. Concurrent BEEF calls
// for the same pool, txid and shard.Version are deduplicated into a single
// HTTP request via singleflight (see fetchBEEF): each caller still returns
// as soon as its own ctx is done, even while the shared fetch (which is not
// tied to any single caller's ctx) keeps running for other callers still
// waiting on it.
func (c *Client) BEEF(ctx context.Context, pool string, shard wire.Shard) ([]byte, error) {
	if b, ok := c.cacheLookup(pool, shard.TxID, shard.Version); ok {
		return b, nil
	}
	key := beefCacheKey(pool, shard.TxID) + "@" + strconv.FormatInt(shard.Version, 10)
	resCh := c.sf.DoChan(key, func() (any, error) {
		return c.fetchBEEF(pool, shard)
	})
	select {
	case res := <-resCh:
		if res.Err != nil {
			return nil, res.Err
		}
		b, _ := res.Val.([]byte)
		return bytes.Clone(b), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// fetchBEEF is BEEF's singleflight-deduplicated leader: it runs detached
// from any single caller's ctx (c.hc's own Timeout bounds it instead),
// because one caller giving up must not abort the fetch for every other
// caller still waiting on the same shard and version. The configured
// Observer (see WithObserver), if any, is notified once per call.
func (c *Client) fetchBEEF(pool string, shard wire.Shard) (body []byte, err error) {
	path := poolPath(pool, "/beef/"+url.PathEscape(shard.TxID))
	start := time.Now()
	status := 0
	defer func() { c.notify(http.MethodGet, path, status, start, err) }()

	ctx := context.Background()
	resp, err := c.getRaw(ctx, path)
	if err != nil {
		status = statusOf(err)
		return nil, err
	}
	status = resp.StatusCode
	defer func() { _ = resp.Body.Close() }()
	body, err = io.ReadAll(io.LimitReader(resp.Body, maxBEEFBytes))
	if err != nil {
		err = wrapTransportErr(ctx, err)
		return nil, err
	}
	version := parseETagVersion(resp.Header.Get("ETag"))
	c.mu.Lock()
	c.misses++
	c.mu.Unlock()
	c.cacheStore(pool, shard.TxID, version, body)
	return body, nil
}

// BEEFCacheMisses returns how many BEEF calls have gone to the server rather
// than being served from the cache (concurrent calls deduplicated into one
// fetch count once). It is meant for tests and diagnostics.
func (c *Client) BEEFCacheMisses() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.misses
}

// Shards is the admin view of pool's shards, optionally filtered by status
// ("unmined"|"mined"|"retired"|"invalid"; "" for no filter) and capped at
// limit (0 for the server default).
func (c *Client) Shards(ctx context.Context, pool, status string, limit int) ([]wire.ShardInfo, error) {
	q := url.Values{}
	if status != "" {
		q.Set("status", status)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var out []wire.ShardInfo
	if err := c.doJSON(ctx, http.MethodGet, poolPath(pool, "/shards"), q.Encode(), nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Quarantined is the admin view of pool's quarantined rows, capped at limit
// (0 for the server default).
func (c *Client) Quarantined(ctx context.Context, pool string, limit int) ([]wire.Row, error) {
	q := url.Values{}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var out []wire.Row
	if err := c.doJSON(ctx, http.MethodGet, poolPath(pool, "/quarantined"), q.Encode(), nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Requeue moves quarantined outpoints of pool back to available, reporting
// how many changed.
func (c *Client) Requeue(ctx context.Context, pool string, outpoints []string) (int, error) {
	req := wire.OutpointsRequest{Outpoints: outpoints}
	var out wire.CountResponse
	if err := c.doJSON(ctx, http.MethodPost, poolPath(pool, "/quarantined/requeue"), "", req, &out); err != nil {
		return 0, err
	}
	return out.Requeued, nil
}

// RetireShard marks txid's shard invalid and invalidates its non-terminal
// rows, reporting how many rows were invalidated.
func (c *Client) RetireShard(ctx context.Context, pool, txid string) (int, error) {
	var out wire.CountResponse
	path := poolPath(pool, "/shards/"+url.PathEscape(txid)+"/retire")
	if err := c.doJSON(ctx, http.MethodPost, path, "", nil, &out); err != nil {
		return 0, err
	}
	return out.Retired, nil
}
