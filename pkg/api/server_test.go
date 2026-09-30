package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api"
	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// env is a running FuelKeeper API server over a real pool.Pool and memory
// store, for end-to-end tests that pin the wire format.
type env struct {
	t    *testing.T
	srv  *httptest.Server
	st   *memory.Store
	clk  *storetest.Clock
	pool *pool.Pool
	keys []string
}

func newEnv(t *testing.T, keys []string) *env {
	t.Helper()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	pc.MinTTL = time.Second
	require.NoError(t, pc.Validate())
	ic := pool.DefaultInstanceConfig()
	ic.InstanceID = "api-test"
	ic.AdoptTimeout = 50 * time.Millisecond
	require.NoError(t, ic.Validate())
	p := pool.NewPool(st, pc, ic, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()

	h := api.NewHandler(api.Deps{
		Pools:        []api.PoolService{p},
		Store:        st,
		APIKeys:      keys,
		OpenAPI:      []byte("openapi: 3.0.3\n"),
		MaxBodyBytes: 4096,
	})
	srv := httptest.NewServer(h)
	t.Cleanup(func() {
		srv.Close()
		cancel()
		<-done
	})
	return &env{t: t, srv: srv, st: st, clk: clk, pool: p, keys: keys}
}

// do issues an HTTP request against the running server. body is JSON-encoded
// unless it is already a string or []byte (for tests that need to send
// deliberately malformed payloads); a non-nil into decodes a 2xx response
// body. The returned response's Body is fully buffered and re-readable, so
// callers may also inspect it directly (e.g. for an error envelope).
func (e *env) do(t *testing.T, method, path string, body, into any) *http.Response {
	t.Helper()
	var reader io.Reader
	switch v := body.(type) {
	case nil:
	case string:
		reader = strings.NewReader(v)
	case []byte:
		reader = bytes.NewReader(v)
	default:
		b, err := json.Marshal(v)
		require.NoError(t, err)
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, e.srv.URL+path, reader)
	require.NoError(t, err)
	if reader != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if len(e.keys) > 0 {
		req.Header.Set("Authorization", "Bearer "+e.keys[0])
	}
	resp, err := e.srv.Client().Do(req)
	require.NoError(t, err)
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	resp.Body = io.NopCloser(bytes.NewReader(data))
	if into != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		require.NoError(t, json.Unmarshal(data, into), "body: %s", data)
	}
	return resp
}

// decodeErr reads and closes resp.Body as a wire.Error.
func decodeErr(t *testing.T, resp *http.Response) wire.Error {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var e wire.Error
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&e))
	return e
}

// mintPuzzleShard inserts a shard of n real hash-puzzle rows (denomination
// 50), unlike storetest.MintShard's placeholder scripts: the API's wire
// format bakes in real script lengths (35-byte lock, 33-byte unlock), so the
// end-to-end tests need the real thing.
func mintPuzzleShard(t *testing.T, st store.Store, poolName string, n int, status store.ShardStatus) (store.Shard, []store.Row) { //nolint:unparam // poolName is a general-purpose helper param; every call site here happens to pass "main"
	t.Helper()
	txid := storetest.NewTxID()
	seed := fuel.NewSeed()
	rows := make([]store.Row, n)
	for i := range rows {
		pre := fuel.Preimage(seed, uint32(i)) //nolint:gosec // n is always small in tests
		rows[i] = store.Row{
			Outpoint:        store.Outpoint{TxID: txid, Vout: uint32(i)}, //nolint:gosec // n is always small in tests
			Pool:            poolName,
			Satoshis:        50,
			LockingScript:   fuel.PuzzleLock(fuel.PuzzleHash(pre)),
			UnlockingScript: fuel.PuzzleUnlock(pre),
		}
	}
	shard := store.Shard{
		Pool:         poolName,
		TxID:         txid,
		Size:         n,
		Denomination: 50,
		LockMode:     store.LockPuzzle,
		Status:       status,
		BEEF:         append([]byte("beef-"), txid[:4]...),
	}
	require.NoError(t, st.PutShard(context.Background(), shard, rows))
	return shard, rows
}

func TestReserveReleaseSpendFlow(t *testing.T) {
	e := newEnv(t, []string{"k1"})
	sh, _ := mintPuzzleShard(t, e.st, "main", 5, store.ShardMined)

	var reserved wire.ReserveResponse
	resp := e.do(t, http.MethodPost, "/v1/pools/main/reserve", wire.ReserveRequest{Count: 3, TTLSeconds: 60}, &reserved)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	assert.Len(t, reserved.Token, 22)
	assert.Equal(t, 60, reserved.TTLSeconds)
	assert.Equal(t, "puzzle", reserved.LockMode)
	assert.Nil(t, reserved.Constraints)
	require.Len(t, reserved.Outputs, 3)
	require.Len(t, reserved.Shards, 1)
	assert.Equal(t, sh.TxID.String(), reserved.Shards[0].TxID)
	assert.Equal(t, "/v1/pools/main/beef/"+sh.TxID.String(), reserved.Shards[0].BEEFURL)

	outpoints := make([]string, 0, len(reserved.Outputs))
	for _, o := range reserved.Outputs {
		assert.Equal(t, o.TxID+":"+strconv.FormatUint(uint64(o.Vout), 10), o.Outpoint)
		assert.Equal(t, uint64(50), o.Satoshis)
		assert.Equal(t, "puzzle", o.Unlock.Mode)
		assert.Equal(t, 33, o.Unlock.Length)
		assert.Len(t, o.LockingScript, 70)
		outpoints = append(outpoints, o.Outpoint)
	}

	var released wire.CountResponse
	resp = e.do(t, http.MethodPost, "/v1/pools/main/release", wire.ReleaseRequest{
		Token: reserved.Token, Outpoints: outpoints[:2],
	}, &released)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, 2, released.Released)

	var spent wire.CountResponse
	txid := storetest.NewTxID()
	resp = e.do(t, http.MethodPost, "/v1/pools/main/spend", wire.SpendRequest{
		Token: reserved.Token, Outpoints: outpoints[2:], TxID: txid.String(),
	}, &spent)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, 1, spent.Accepted)

	var pools []wire.Pool
	resp = e.do(t, http.MethodGet, "/v1/pools", nil, &pools)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	require.Len(t, pools, 1)
	assert.Equal(t, int64(4), pools[0].Stats.Available)
	assert.Equal(t, int64(1), pools[0].Stats.Quarantined)
	assert.Equal(t, int64(1), pools[0].Stats.Shards.Mined)

	var p wire.Pool
	resp = e.do(t, http.MethodGet, "/v1/pools/main", nil, &p)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, "main", p.Name)
	assert.Equal(t, 100, p.MaxCount)
}

func TestErrorsAndStatusCodes(t *testing.T) {
	e := newEnv(t, []string{"k1"})

	resp := e.do(t, http.MethodGet, "/v1/pools/nope", nil, nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "unknown_pool", decodeErr(t, resp).Code)

	resp = e.do(t, http.MethodPost, "/v1/pools/main/reserve", wire.ReserveRequest{Count: 1}, nil)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Equal(t, "1", resp.Header.Get("Retry-After"))
	errResp := decodeErr(t, resp)
	assert.Equal(t, "exhausted", errResp.Code)
	assert.Equal(t, 1, errResp.RetryAfter)

	mintPuzzleShard(t, e.st, "main", 2, store.ShardMined)
	resp = e.do(t, http.MethodPost, "/v1/pools/main/reserve", wire.ReserveRequest{Count: 5, MinCount: 3}, nil)
	require.Equal(t, http.StatusConflict, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = e.do(t, http.MethodPost, "/v1/pools/main/reserve", wire.ReserveRequest{Count: 0}, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = e.do(t, http.MethodPost, "/v1/pools/main/reserve", "{not json", nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = e.do(t, http.MethodPost, "/v1/pools/main/reserve", `{"count":1,"bogus":true}`, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = e.do(t, http.MethodPost, "/v1/pools/main/release",
		wire.ReleaseRequest{Token: "not-a-token", Outpoints: []string{"bad-outpoint"}}, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = e.do(t, http.MethodPost, "/v1/pools/main/spend", wire.SpendRequest{
		Token:     store.NewToken().String(),
		Outpoints: []string{store.Outpoint{TxID: storetest.NewTxID(), Vout: 0}.String()},
		TxID:      "not-a-txid",
	}, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	many := make([]string, 200)
	for i := range many {
		many[i] = store.Outpoint{TxID: storetest.NewTxID(), Vout: uint32(i)}.String() //nolint:gosec // i is small
	}
	resp = e.do(t, http.MethodPost, "/v1/pools/main/release",
		wire.ReleaseRequest{Token: store.NewToken().String(), Outpoints: many}, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = e.do(t, http.MethodGet, "/v1/nope", nil, nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "not_found", decodeErr(t, resp).Code)

	resp = e.do(t, http.MethodDelete, "/v1/pools", nil, nil)
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func TestBEEFEndpointCaching(t *testing.T) {
	e := newEnv(t, nil)
	mined, _ := mintPuzzleShard(t, e.st, "main", 1, store.ShardMined)
	unmined, _ := mintPuzzleShard(t, e.st, "main", 1, store.ShardUnmined)

	resp := e.do(t, http.MethodGet, "/v1/pools/main/beef/"+mined.TxID.String(), nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
	assert.Equal(t, `"v1"`, resp.Header.Get("ETag"))
	assert.Equal(t, "mined", resp.Header.Get("X-Shard-Status"))
	assert.Contains(t, resp.Header.Get("Cache-Control"), "immutable")
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, mined.BEEF, body)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		e.srv.URL+"/v1/pools/main/beef/"+mined.TxID.String(), nil)
	require.NoError(t, err)
	req.Header.Set("If-None-Match", `"v1"`)
	resp, err = e.srv.Client().Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotModified, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = e.do(t, http.MethodGet, "/v1/pools/main/beef/"+unmined.TxID.String(), nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))
	assert.Equal(t, "unmined", resp.Header.Get("X-Shard-Status"))
	require.NoError(t, resp.Body.Close())

	resp = e.do(t, http.MethodGet, "/v1/pools/main/beef/"+storetest.NewTxID().String(), nil, nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func TestAuthHealthAndOpenAPI(t *testing.T) {
	e := newEnv(t, []string{"k1"})

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, e.srv.URL+"/v1/pools", nil)
	require.NoError(t, err)
	resp, err := e.srv.Client().Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.NotEmpty(t, resp.Header.Get("WWW-Authenticate"))
	require.NoError(t, resp.Body.Close())

	resp = e.do(t, http.MethodGet, "/v1/pools", nil, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	for _, path := range []string{"/healthz", "/readyz", "/metrics", "/openapi.yaml"} {
		req, err = http.NewRequestWithContext(context.Background(), http.MethodGet, e.srv.URL+path, nil)
		require.NoError(t, err)
		resp, err = e.srv.Client().Do(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode, path)
		require.NoError(t, resp.Body.Close())
	}

	var ready wire.Ready
	resp = e.do(t, http.MethodGet, "/readyz", nil, &ready)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, 1, ready.Pools)
	assert.Equal(t, 0, ready.PoolsReady)

	mintPuzzleShard(t, e.st, "main", 1, store.ShardMined)
	resp = e.do(t, http.MethodPost, "/v1/pools/main/reserve", wire.ReserveRequest{Count: 1}, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = e.do(t, http.MethodGet, "/readyz", nil, &ready)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, 1, ready.PoolsReady)
}

func TestAdminEndpoints(t *testing.T) {
	e := newEnv(t, []string{"k1"})
	sh, rows := mintPuzzleShard(t, e.st, "main", 3, store.ShardMined)

	tok := store.NewToken()
	storetest.MustReserve(t, e.st, "main", rows[:2], tok, e.clk.Now().Add(time.Minute))
	hint := storetest.NewTxID()
	n, err := e.st.Spend(context.Background(), "main", tok, storetest.Ops(rows[:1]), hint, false)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	var shards []wire.ShardInfo
	resp := e.do(t, http.MethodGet, "/v1/pools/main/shards?status=mined", nil, &shards)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	require.Len(t, shards, 1)
	assert.Equal(t, sh.TxID.String(), shards[0].TxID)

	var quarantined []wire.Row
	resp = e.do(t, http.MethodGet, "/v1/pools/main/quarantined", nil, &quarantined)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	require.Len(t, quarantined, 1)
	assert.Equal(t, hint.String(), quarantined[0].SpentHint)

	var requeued wire.CountResponse
	resp = e.do(t, http.MethodPost, "/v1/pools/main/quarantined/requeue",
		wire.OutpointsRequest{Outpoints: []string{quarantined[0].Outpoint}}, &requeued)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, 1, requeued.Requeued)

	var retired wire.CountResponse
	resp = e.do(t, http.MethodPost, "/v1/pools/main/shards/"+sh.TxID.String()+"/retire", nil, &retired)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, 3, retired.Retired)

	resp = e.do(t, http.MethodGet, "/v1/pools/main/shards?status=bogus", nil, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	mintPuzzleShard(t, e.st, "main", 1, store.ShardMined)
	mintPuzzleShard(t, e.st, "main", 1, store.ShardMined)
	mintPuzzleShard(t, e.st, "main", 1, store.ShardUnmined)
	var limited []wire.ShardInfo
	resp = e.do(t, http.MethodGet, "/v1/pools/main/shards?limit=2", nil, &limited)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	assert.Len(t, limited, 2)
}

// blockingStatsStore blocks Stats until its ctx is done, to exercise the
// request-timeout middleware without depending on real I/O latency.
type blockingStatsStore struct {
	store.Store
}

func (b *blockingStatsStore) Stats(ctx context.Context, _ string) (store.Stats, error) {
	<-ctx.Done()
	return store.Stats{}, ctx.Err()
}

func TestRequestTimeout(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	base := memory.New(memory.WithClock(clk.Now))
	bs := &blockingStatsStore{Store: base}
	pc := pool.DefaultPoolConfig("main", 50)
	pc.MinTTL = time.Second
	require.NoError(t, pc.Validate())
	ic := pool.DefaultInstanceConfig()
	ic.InstanceID = "api-timeout-test"
	ic.AdoptTimeout = 50 * time.Millisecond
	require.NoError(t, ic.Validate())
	p := pool.NewPool(bs, pc, ic, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	h := api.NewHandler(api.Deps{
		Pools:          []api.PoolService{p},
		Store:          bs,
		RequestTimeout: 50 * time.Millisecond,
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/v1/pools", nil)
	require.NoError(t, err)
	start := time.Now()
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	assert.Less(t, time.Since(start), time.Second)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Equal(t, "unavailable", decodeErr(t, resp).Code)
}
