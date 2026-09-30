// Package client is a Go client for the FuelKeeper HTTP API (pkg/api): typed
// wrappers over reserve/release/spend/pools, a per-shard BEEF cache, the admin
// endpoints, and (in beef.go) helpers that turn a reservation into go-sdk
// createAction inputs.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
)

const (
	// defaultTimeout bounds a single HTTP round trip made by the default
	// client (Client.hc), including connection setup.
	defaultTimeout = 10 * time.Second
	// maxIdleConnsPerHost and idleConnTimeout tune the default transport's
	// keep-alive pool for a client that talks to one FuelKeeper server
	// repeatedly (every Reserve/Release/Spend/BEEF call).
	maxIdleConnsPerHost = 64
	idleConnTimeout     = 90 * time.Second
	// defaultUserAgent is sent unless overridden with WithUserAgent.
	defaultUserAgent = "fuelkeeper-client/1"
)

// ReserveRequest asks a pool for fuel; see wire.ReserveRequest.
type ReserveRequest struct {
	Count    int
	MinCount int           // 0 = Count
	TTL      time.Duration // 0 = pool default
}

// cachedBEEF is one pool+txid's cached BEEF bytes at a known shard version.
type cachedBEEF struct {
	version int64
	bytes   []byte
}

// Client is a FuelKeeper HTTP API client. It is safe for concurrent use.
type Client struct {
	base      *url.URL
	hc        *http.Client
	apiKey    string
	userAgent string

	mu     sync.Mutex
	beef   map[string]cachedBEEF // "pool/txid" -> cached BEEF
	misses int
}

// Option configures a Client constructed by New.
type Option func(*Client)

// WithAPIKey sets the bearer API key sent as "Authorization: Bearer <key>"
// with every request.
func WithAPIKey(key string) Option {
	return func(c *Client) { c.apiKey = key }
}

// WithHTTPClient overrides the default *http.Client (a 10 s timeout over a
// keep-alive transport with MaxIdleConnsPerHost 64 and IdleConnTimeout 90 s).
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.hc = hc }
}

// WithUserAgent overrides the default "User-Agent" header value.
func WithUserAgent(ua string) Option {
	return func(c *Client) { c.userAgent = ua }
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
			Timeout: defaultTimeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: maxIdleConnsPerHost,
				IdleConnTimeout:     idleConnTimeout,
			},
		},
		userAgent: defaultUserAgent,
		beef:      make(map[string]cachedBEEF),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// newRequest builds an HTTP request against path (joined onto the base URL)
// with rawQuery as its query string, JSON-encoding body when non-nil.
func (c *Client) newRequest(ctx context.Context, method, path, rawQuery string, body any) (*http.Request, error) {
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

// doJSON sends a JSON request and, on a 2xx response, decodes the body into
// into (which may be nil to discard it). A non-2xx response is mapped to
// *APIError. The response body is always closed before doJSON returns.
func (c *Client) doJSON(ctx context.Context, method, path, rawQuery string, body, into any) error {
	req, err := c.newRequest(ctx, method, path, rawQuery, body)
	if err != nil {
		return err
	}
	resp, err := c.do(ctx, req)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return newAPIError(resp.StatusCode, readErrorBody(resp), resp.Header)
	}
	defer func() { _ = resp.Body.Close() }()
	if into == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		return fmt.Errorf("client: decode response: %w", err)
	}
	return nil
}

// getRaw sends a GET request to path and returns the response for the caller
// to read directly (the BEEF endpoint may return a large binary body that
// doJSON should not buffer). A non-2xx response is mapped to *APIError and
// its body is closed here; on success the caller owns resp.Body and must
// close it.
func (c *Client) getRaw(ctx context.Context, path string) (*http.Response, error) {
	req, err := c.newRequest(ctx, http.MethodGet, path, "", nil)
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

// Release returns reserved outpoints under token to pool, reporting how many
// changed.
func (c *Client) Release(ctx context.Context, pool, token string, outpoints []string) (int, error) {
	req := wire.ReleaseRequest{Token: token, Outpoints: outpoints}
	var out wire.CountResponse
	if err := c.doJSON(ctx, http.MethodPost, poolPath(pool, "/release"), "", req, &out); err != nil {
		return 0, err
	}
	return out.Released, nil
}

// Spend reports outpoints under token spent in txid, reporting how many
// changed.
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
// `"v<n>"` (as api's beef handler sets it), returning 0 if it cannot be
// parsed.
func parseETagVersion(etag string) int64 {
	s := strings.Trim(etag, `"`)
	s = strings.TrimPrefix(s, "v")
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// BEEF returns shard's BEEF bytes, serving from the client's in-memory cache
// when the cached version is at least shard.Version and otherwise fetching
// and caching a fresh copy. Cache lookups are keyed by pool and shard.TxID.
func (c *Client) BEEF(ctx context.Context, pool string, shard wire.Shard) ([]byte, error) {
	key := beefCacheKey(pool, shard.TxID)
	c.mu.Lock()
	cached, ok := c.beef[key]
	c.mu.Unlock()
	if ok && cached.version >= shard.Version {
		return cached.bytes, nil
	}
	resp, err := c.getRaw(ctx, poolPath(pool, "/beef/"+url.PathEscape(shard.TxID)))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("client: read beef body: %w", err)
	}
	version := parseETagVersion(resp.Header.Get("ETag"))
	c.mu.Lock()
	c.beef[key] = cachedBEEF{version: version, bytes: body}
	c.misses++
	c.mu.Unlock()
	return body, nil
}

// BEEFCacheMisses returns how many BEEF calls have gone to the server rather
// than being served from the cache. It is meant for tests and diagnostics.
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
