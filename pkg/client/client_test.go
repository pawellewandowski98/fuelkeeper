package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api"
	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func startAPI(t *testing.T, keys []string) (*httptest.Server, *memory.Store, *storetest.Clock) { //nolint:unparam // the clock is a general-purpose helper result; unused here, but a caller exercising expiry may want it
	t.Helper()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	pc.MinTTL = time.Second
	require.NoError(t, pc.Validate())
	ic := pool.DefaultInstanceConfig()
	ic.InstanceID = "client-test"
	ic.AdoptTimeout = 50 * time.Millisecond
	p := pool.NewPool(st, pc, ic, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	srv := httptest.NewServer(api.NewHandler(api.Deps{Pools: []api.PoolService{p}, Store: st, APIKeys: keys}))
	t.Cleanup(func() { srv.Close(); cancel(); <-done })
	return srv, st, clk
}

func TestClientFlow(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := startAPI(t, []string{"k1"})
	sh, _ := storetest.MintShard(t, st, "main", 4, store.ShardMined)

	c, err := client.New(srv.URL, client.WithAPIKey("k1"))
	require.NoError(t, err)

	res, err := c.Reserve(ctx, "main", client.ReserveRequest{Count: 2, TTL: 30 * time.Second})
	require.NoError(t, err)
	require.Len(t, res.Outputs, 2)
	assert.Equal(t, 30, res.TTLSeconds)
	assert.Equal(t, sh.TxID.String(), res.Shards[0].TxID)

	n, err := c.Release(ctx, "main", res.Token, []string{res.Outputs[0].Outpoint})
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	n, err = c.Spend(ctx, "main", res.Token, []string{res.Outputs[1].Outpoint}, storetest.NewTxID().String())
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	pools, err := c.Pools(ctx)
	require.NoError(t, err)
	require.Len(t, pools, 1)
	assert.Equal(t, int64(3), pools[0].Stats.Available)
	one, err := c.Pool(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, uint64(50), one.Denomination)

	beef, err := c.BEEF(ctx, "main", res.Shards[0])
	require.NoError(t, err)
	assert.Equal(t, sh.BEEF, beef)
	beef2, err := c.BEEF(ctx, "main", res.Shards[0])
	require.NoError(t, err)
	assert.Equal(t, beef, beef2)
	assert.Equal(t, 1, c.BEEFCacheMisses(), "second call served from cache")
	newer := res.Shards[0]
	newer.Version++
	_, err = c.BEEF(ctx, "main", newer)
	require.NoError(t, err)
	assert.Equal(t, 2, c.BEEFCacheMisses(), "a newer version bypasses the cache")

	shards, err := c.Shards(ctx, "main", "mined", 10)
	require.NoError(t, err)
	require.Len(t, shards, 1)
	q, err := c.Quarantined(ctx, "main", 10)
	require.NoError(t, err)
	require.Len(t, q, 1)
	rq, err := c.Requeue(ctx, "main", []string{q[0].Outpoint})
	require.NoError(t, err)
	assert.Equal(t, 1, rq)
	rt, err := c.RetireShard(ctx, "main", sh.TxID.String())
	require.NoError(t, err)
	assert.Equal(t, 4, rt)
}

func TestClientErrors(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := startAPI(t, []string{"k1"})

	unauth, err := client.New(srv.URL)
	require.NoError(t, err)
	_, err = unauth.Pools(ctx)
	require.ErrorIs(t, err, client.ErrUnauthorized)

	c, err := client.New(srv.URL, client.WithAPIKey("k1"))
	require.NoError(t, err)

	_, err = c.Reserve(ctx, "main", client.ReserveRequest{Count: 1})
	require.ErrorIs(t, err, client.ErrExhausted)
	var apiErr *client.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, 503, apiErr.Status)
	assert.Equal(t, 1, apiErr.RetryAfter)
	assert.True(t, client.IsRetryable(err))

	storetest.MintShard(t, st, "main", 1, store.ShardMined)
	_, err = c.Reserve(ctx, "main", client.ReserveRequest{Count: 3, MinCount: 2})
	require.ErrorIs(t, err, client.ErrInsufficient)
	assert.True(t, client.IsRetryable(err))

	_, err = c.Reserve(ctx, "nope", client.ReserveRequest{Count: 1})
	require.ErrorIs(t, err, client.ErrNotFound)
	assert.False(t, client.IsRetryable(err))

	_, err = c.Reserve(ctx, "main", client.ReserveRequest{Count: 0})
	require.ErrorIs(t, err, client.ErrBadRequest)
	assert.False(t, client.IsRetryable(err), "a 400 is never retryable")

	_, err = client.New("://bad")
	require.Error(t, err)

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = c.Pools(cctx)
	require.ErrorIs(t, err, context.Canceled, "caller cancellation is surfaced as the context error, not ErrUnavailable")
	assert.False(t, client.IsRetryable(err), "caller cancellation is never retryable")

	down, err := client.New("http://127.0.0.1:1")
	require.NoError(t, err)
	_, err = down.Pools(ctx)
	require.ErrorIs(t, err, client.ErrUnavailable)
	assert.True(t, client.IsRetryable(err))
}

// TestIsRetryableClientSideTimeout covers the bug where the client's own
// http.Client.Timeout firing (as opposed to the caller's ctx ending) was
// misclassified as non-retryable: the resulting error wraps ErrUnavailable
// (via wrapTransportErr, since the caller's own ctx is not done) but also
// happens to wrap context.DeadlineExceeded (from the http.Client's internal
// per-request deadline), which IsRetryable must not treat as caller
// cancellation.
func TestIsRetryableClientSideTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode([]wire.Pool{})
	}))
	defer srv.Close()

	c, err := client.New(srv.URL, client.WithHTTPClient(&http.Client{Timeout: 50 * time.Millisecond}))
	require.NoError(t, err)

	_, err = c.Pools(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, client.ErrUnavailable)
	assert.True(t, client.IsRetryable(err), "the client's own request timeout is a transport failure, not caller cancellation")
}

// TestAPIErrorUnavailableWithoutJSONBody covers a 5xx response whose body is
// not a wire.Error (e.g. an HTML page from a proxy in front of FuelKeeper):
// it must still be classified as ErrUnavailable and retryable.
func TestAPIErrorUnavailableWithoutJSONBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("<html><body>bad gateway</body></html>"))
	}))
	defer srv.Close()

	c, err := client.New(srv.URL)
	require.NoError(t, err)

	_, err = c.Pools(context.Background())
	require.Error(t, err)
	var apiErr *client.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusBadGateway, apiErr.Status)
	assert.Equal(t, "http_502", apiErr.Code)
	require.ErrorIs(t, err, client.ErrUnavailable)
	assert.True(t, client.IsRetryable(err))
}

// TestBEEFSingleflightDedup covers concurrent BEEF calls for the same pool,
// txid and version being deduplicated into exactly one HTTP request.
func TestBEEFSingleflightDedup(t *testing.T) {
	var reqs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reqs.Add(1)
		time.Sleep(50 * time.Millisecond) // widen the window so every goroutine below joins the same in-flight fetch
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte("beef-bytes"))
	}))
	defer srv.Close()

	c, err := client.New(srv.URL)
	require.NoError(t, err)

	const n = 16
	var wg sync.WaitGroup
	results := make([][]byte, n)
	errs := make([]error, n)
	shard := wire.Shard{TxID: "abc123", Version: 1}
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = c.BEEF(context.Background(), "main", shard)
		}(i)
	}
	wg.Wait()

	for i := range n {
		require.NoError(t, errs[i])
		assert.Equal(t, []byte("beef-bytes"), results[i])
	}
	assert.Equal(t, int32(1), reqs.Load(), "16 concurrent BEEF calls for the same shard should make exactly one request")
	assert.Equal(t, 1, c.BEEFCacheMisses())
}

// TestBEEFCacheReturnsDefensiveCopy covers a caller mutating a returned BEEF
// slice being unable to corrupt what the cache (or another caller) sees.
func TestBEEFCacheReturnsDefensiveCopy(t *testing.T) {
	ctx := context.Background()
	srv, st, _ := startAPI(t, nil)
	sh, _ := storetest.MintShard(t, st, "main", 1, store.ShardMined)
	c, err := client.New(srv.URL)
	require.NoError(t, err)

	shardRef := wire.Shard{TxID: sh.TxID.String(), Version: sh.Version}
	b1, err := c.BEEF(ctx, "main", shardRef)
	require.NoError(t, err)
	original := append([]byte(nil), b1...)
	for i := range b1 {
		b1[i] ^= 0xFF
	}

	b2, err := c.BEEF(ctx, "main", shardRef)
	require.NoError(t, err)
	assert.Equal(t, original, b2, "mutating a returned BEEF slice must not corrupt the cache")
}

// TestBEEFCacheNeverRegresses covers an older fetch completing after a newer
// one (a race the coalescer's version-suffixed singleflight key does not by
// itself prevent, since the two are different keys) without clobbering the
// newer cache entry; it also exercises weak-ETag parsing (item 5): a broken
// weak-ETag parse would make the "new" fetch look older than it is, letting
// the slow "old" fetch win and forcing an extra request on the final read.
func TestBEEFCacheNeverRegresses(t *testing.T) {
	var reqs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if reqs.Add(1) == 1 {
			time.Sleep(100 * time.Millisecond) // the older fetch finishes last
			w.Header().Set("ETag", `"v1"`)
			_, _ = w.Write([]byte("old-beef"))
			return
		}
		w.Header().Set("ETag", `W/"v2"`) // weak ETag
		_, _ = w.Write([]byte("new-beef"))
	}))
	defer srv.Close()

	c, err := client.New(srv.URL)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = c.BEEF(context.Background(), "main", wire.Shard{TxID: "abc", Version: 1})
	}()
	time.Sleep(20 * time.Millisecond) // let the slow "old" request start first
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = c.BEEF(context.Background(), "main", wire.Shard{TxID: "abc", Version: 2})
	}()
	wg.Wait()

	got, err := c.BEEF(context.Background(), "main", wire.Shard{TxID: "abc", Version: 2})
	require.NoError(t, err)
	assert.Equal(t, []byte("new-beef"), got, "the newer fetch must win even though the older one completed later")
	assert.Equal(t, int32(2), reqs.Load(), "the third call should be served from cache, not a new request")
}

// TestBEEFCacheEvictsLRUPerPool covers each pool's BEEF cache being bounded
// to 64 entries, evicting the least recently used one once exceeded.
func TestBEEFCacheEvictsLRUPerPool(t *testing.T) {
	var reqs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reqs.Add(1)
		w.Header().Set("ETag", `"v1"`)
		_, _ = w.Write([]byte("beef"))
	}))
	defer srv.Close()

	c, err := client.New(srv.URL)
	require.NoError(t, err)

	const n = 65
	txids := make([]string, n)
	for i := range txids {
		txids[i] = fmt.Sprintf("txid-%d", i)
		_, err := c.BEEF(context.Background(), "main", wire.Shard{TxID: txids[i], Version: 1})
		require.NoError(t, err)
	}
	require.Equal(t, int32(n), reqs.Load())

	_, err = c.BEEF(context.Background(), "main", wire.Shard{TxID: txids[0], Version: 1})
	require.NoError(t, err)
	assert.Equal(t, int32(n+1), reqs.Load(), "the least recently used entry (inserted first, never re-touched) should have been evicted")

	before := reqs.Load()
	_, err = c.BEEF(context.Background(), "main", wire.Shard{TxID: txids[n-1], Version: 1})
	require.NoError(t, err)
	assert.Equal(t, before, reqs.Load(), "a recently inserted entry should still be cached")
}

// TestReserveWithRetry covers retrying a retryable Reserve error (503) until
// the server succeeds.
func TestReserveWithRetry(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(wire.Error{Code: "exhausted", Message: "no fuel"})
			return
		}
		_ = json.NewEncoder(w).Encode(wire.ReserveResponse{Token: "tok", TTLSeconds: 30})
	}))
	defer srv.Close()

	c, err := client.New(srv.URL)
	require.NoError(t, err)

	res, err := c.ReserveWithRetry(context.Background(), "main", client.ReserveRequest{Count: 1})
	require.NoError(t, err)
	assert.Equal(t, "tok", res.Token)
	assert.Equal(t, int32(3), n.Load(), "the third attempt should have succeeded")
}

// TestReserveWithRetryMaxAttempts covers WithMaxAttempts capping retries and
// returning the last error once exhausted.
func TestReserveWithRetryMaxAttempts(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(wire.Error{Code: "exhausted", Message: "no fuel"})
	}))
	defer srv.Close()

	c, err := client.New(srv.URL)
	require.NoError(t, err)

	_, err = c.ReserveWithRetry(context.Background(), "main", client.ReserveRequest{Count: 1}, client.WithMaxAttempts(2))
	require.ErrorIs(t, err, client.ErrExhausted)
	assert.Equal(t, int32(2), n.Load(), "WithMaxAttempts(2) should try exactly twice")
}
