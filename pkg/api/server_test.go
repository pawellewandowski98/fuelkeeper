package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-sdk/chainhash"

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

	// duplicate outpoints
	dup := store.Outpoint{TxID: storetest.NewTxID(), Vout: 0}.String()
	resp = e.do(t, http.MethodPost, "/v1/pools/main/release",
		wire.ReleaseRequest{Token: store.NewToken().String(), Outpoints: []string{dup, dup}}, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "bad_request", decodeErr(t, resp).Code)

	// trailing data after the JSON value
	resp = e.do(t, http.MethodPost, "/v1/pools/main/reserve", `{"count":1}{"count":2}`, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// ttl_seconds large enough to overflow int64 once converted to a Duration
	resp = e.do(t, http.MethodPost, "/v1/pools/main/reserve",
		fmt.Sprintf(`{"count":1,"ttl_seconds":%d}`, int64(math.MaxInt64)), nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = e.do(t, http.MethodGet, "/v1/nope", nil, nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "not_found", decodeErr(t, resp).Code)

	resp = e.do(t, http.MethodDelete, "/v1/pools", nil, nil)
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	assert.Equal(t, "method_not_allowed", decodeErr(t, resp).Code)
}

// TestStrictTxIDParsing guards against chainhash.NewHashFromHex's lenient
// parsing (it zero-pads short input) silently accepting a malformed txid
// wherever the API takes one.
func TestStrictTxIDParsing(t *testing.T) {
	e := newEnv(t, []string{"k1"})
	mintPuzzleShard(t, e.st, "main", 1, store.ShardMined)

	short := "abc"
	resp := e.do(t, http.MethodGet, "/v1/pools/main/beef/"+short, nil, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	upper := strings.Repeat("A", 64)
	resp = e.do(t, http.MethodGet, "/v1/pools/main/beef/"+upper, nil, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = e.do(t, http.MethodPost, "/v1/pools/main/spend", wire.SpendRequest{
		Token:     store.NewToken().String(),
		Outpoints: []string{store.Outpoint{TxID: storetest.NewTxID(), Vout: 0}.String()},
		TxID:      short,
	}, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = e.do(t, http.MethodPost, "/v1/pools/main/shards/"+short+"/retire", nil, nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

// TestAdminRoutesUnknownPool checks that every admin endpoint answers
// unknown_pool, not a store-level not_found or a panic, for a pool that was
// never configured.
func TestAdminRoutesUnknownPool(t *testing.T) {
	e := newEnv(t, []string{"k1"})
	reqTxID := storetest.NewTxID().String()

	cases := []struct {
		method, path string
		body         any
	}{
		{http.MethodGet, "/v1/pools/nope/shards", nil},
		{http.MethodPost, "/v1/pools/nope/shards/" + reqTxID + "/retire", nil},
		{http.MethodGet, "/v1/pools/nope/quarantined", nil},
		{http.MethodPost, "/v1/pools/nope/quarantined/requeue",
			wire.OutpointsRequest{Outpoints: []string{store.Outpoint{TxID: storetest.NewTxID(), Vout: 0}.String()}}},
	}
	for _, tc := range cases {
		resp := e.do(t, tc.method, tc.path, tc.body, nil)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode, tc.path)
		assert.Equal(t, "unknown_pool", decodeErr(t, resp).Code, tc.path)
	}
}

// TestIfNoneMatchForms exercises RFC 9110 §13.1.2's weak comparison: a weak
// tag, a comma-separated list and the wildcard must all satisfy If-None-Match.
func TestIfNoneMatchForms(t *testing.T) {
	e := newEnv(t, nil)
	mined, _ := mintPuzzleShard(t, e.st, "main", 1, store.ShardMined)
	path := "/v1/pools/main/beef/" + mined.TxID.String()

	for _, hdr := range []string{`W/"v1"`, `"v0", "v1"`, `*`} {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, e.srv.URL+path, nil)
		require.NoError(t, err)
		req.Header.Set("If-None-Match", hdr)
		resp, err := e.srv.Client().Do(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotModified, resp.StatusCode, hdr)
		require.NoError(t, resp.Body.Close())
	}
}

// TestAllowHeaderAndOptions checks that a known route's Allow header is
// correct on both a 405 and an OPTIONS 204, and that the root mux (not just
// /v1/) answers unknown paths and wrong methods with the JSON envelope.
func TestAllowHeaderAndOptions(t *testing.T) {
	e := newEnv(t, []string{"k1"})

	req, err := http.NewRequestWithContext(context.Background(), http.MethodOptions, e.srv.URL+"/v1/pools", nil)
	require.NoError(t, err)
	resp, err := e.srv.Client().Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, "GET, HEAD", resp.Header.Get("Allow"))
	require.NoError(t, resp.Body.Close())

	resp = e.do(t, http.MethodDelete, "/v1/pools", nil, nil)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	assert.Equal(t, "GET, HEAD", resp.Header.Get("Allow"))
	assert.Equal(t, "method_not_allowed", decodeErr(t, resp).Code)

	req, err = http.NewRequestWithContext(context.Background(), http.MethodPost, e.srv.URL+"/healthz", nil)
	require.NoError(t, err)
	resp, err = e.srv.Client().Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	var envelope wire.Error
	require.NoError(t, json.Unmarshal(body, &envelope))
	assert.Equal(t, "method_not_allowed", envelope.Code)

	req, err = http.NewRequestWithContext(context.Background(), http.MethodGet, e.srv.URL+"/nope", nil)
	require.NoError(t, err)
	resp, err = e.srv.Client().Do(req)
	require.NoError(t, err)
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, json.Unmarshal(body, &envelope))
	assert.Equal(t, "not_found", envelope.Code)
}

// TestCountResponseExplicitZero checks that CountResponse's three unused
// fields are present (and 0), not omitted, so a client need not treat a
// missing key as "unknown".
func TestCountResponseExplicitZero(t *testing.T) {
	e := newEnv(t, []string{"k1"})
	sh, _ := mintPuzzleShard(t, e.st, "main", 1, store.ShardMined)

	retire := func() []byte {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
			e.srv.URL+"/v1/pools/main/shards/"+sh.TxID.String()+"/retire", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer k1")
		resp, err := e.srv.Client().Do(req)
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode)
		return body
	}

	first := retire()
	assert.Contains(t, string(first), `"released":0`)
	assert.Contains(t, string(first), `"accepted":0`)
	assert.Contains(t, string(first), `"requeued":0`)
	assert.Contains(t, string(first), `"retired":1`)

	// A retired shard is a store-level no-op to retire again.
	second := retire()
	assert.Contains(t, string(second), `"retired":0`)
}

// dummyPoolService is a minimal PoolService for tests that only need "at
// least one pool is configured" without exercising any pool behaviour.
type dummyPoolService struct{ name string }

func (d dummyPoolService) Config() pool.Config { return pool.Config{Name: d.name, MaxCount: 10} }

func (d dummyPoolService) Reserve(context.Context, pool.ReserveRequest) (*pool.Reservation, error) {
	return nil, nil //nolint:nilnil // unused by these tests
}

func (d dummyPoolService) Release(context.Context, store.Token, []store.Outpoint) (int, error) {
	return 0, nil
}

func (d dummyPoolService) Spend(context.Context, store.Token, []store.Outpoint, chainhash.Hash) (int, error) {
	return 0, nil
}

func (d dummyPoolService) Stats(context.Context) (store.Stats, error) { return store.Stats{}, nil }

func (d dummyPoolService) BEEF(context.Context, chainhash.Hash) ([]byte, int64, store.ShardStatus, error) {
	return nil, 0, 0, nil
}

func (d dummyPoolService) Ready(context.Context) bool { return false }

// pingFailingStore always fails Ping with a configured error, to check that
// readyz never leaks the underlying detail.
type pingFailingStore struct {
	store.Store
	err error
}

func (p *pingFailingStore) Ping(context.Context) error { return p.err }

func TestReadyzUnavailablePaths(t *testing.T) {
	h := api.NewHandler(api.Deps{Store: memory.New()})
	srv := httptest.NewServer(h)
	defer srv.Close()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/readyz", nil)
	require.NoError(t, err)
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	var ready wire.Ready
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&ready))
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, "no pools configured", ready.Status)

	sensitive := fmt.Errorf("%w: dial tcp 10.1.2.3:5432: password authentication failed", store.ErrUnavailable)
	failing := &pingFailingStore{Store: memory.New(), err: sensitive}
	h2 := api.NewHandler(api.Deps{Pools: []api.PoolService{dummyPoolService{name: "main"}}, Store: failing})
	srv2 := httptest.NewServer(h2)
	defer srv2.Close()
	req2, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv2.URL+"/readyz", nil)
	require.NoError(t, err)
	resp2, err := srv2.Client().Do(req2)
	require.NoError(t, err)
	body, err := io.ReadAll(resp2.Body)
	require.NoError(t, err)
	require.NoError(t, resp2.Body.Close())
	assert.Equal(t, http.StatusServiceUnavailable, resp2.StatusCode)
	assert.NotContains(t, string(body), "10.1.2.3")
	assert.NotContains(t, string(body), "password")
	var ready2 wire.Ready
	require.NoError(t, json.Unmarshal(body, &ready2))
	assert.Equal(t, "store unavailable", ready2.Status)
	assert.Equal(t, "store ping failed", ready2.Error)
}

// panicPoolService panics from Stats, to exercise the recoverer.
type panicPoolService struct{ name string }

func (p panicPoolService) Config() pool.Config { return pool.Config{Name: p.name, MaxCount: 10} }

func (p panicPoolService) Reserve(context.Context, pool.ReserveRequest) (*pool.Reservation, error) {
	return nil, nil //nolint:nilnil // unused by this test
}

func (p panicPoolService) Release(context.Context, store.Token, []store.Outpoint) (int, error) {
	return 0, nil
}

func (p panicPoolService) Spend(context.Context, store.Token, []store.Outpoint, chainhash.Hash) (int, error) {
	return 0, nil
}

func (p panicPoolService) Stats(context.Context) (store.Stats, error) {
	panic("boom")
}

func (p panicPoolService) BEEF(context.Context, chainhash.Hash) ([]byte, int64, store.ShardStatus, error) {
	return nil, 0, 0, nil
}

func (p panicPoolService) Ready(context.Context) bool { return true }

func TestPanicRecovery(t *testing.T) {
	h := api.NewHandler(api.Deps{Pools: []api.PoolService{panicPoolService{name: "main"}}, Store: memory.New()})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/v1/pools", nil)
	require.NoError(t, err)
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, "internal", decodeErr(t, resp).Code)
}

// TestAdminKeys checks that a separately configured AdminKeys rejects a
// plain fuel key on an admin route, accepts an admin key there, and does not
// disturb the fuel-serving routes.
func TestAdminKeys(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	pc.MinTTL = time.Second
	require.NoError(t, pc.Validate())
	ic := pool.DefaultInstanceConfig()
	ic.InstanceID = "api-test-admin"
	ic.AdoptTimeout = 50 * time.Millisecond
	p := pool.NewPool(st, pc, ic, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	h := api.NewHandler(api.Deps{
		Pools:     []api.PoolService{p},
		Store:     st,
		APIKeys:   []string{"fuel-key"},
		AdminKeys: []string{"admin-key"},
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	get := func(path, key string) int {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+path, nil)
		require.NoError(t, err)
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		resp, err := srv.Client().Do(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode
	}

	assert.Equal(t, http.StatusOK, get("/v1/pools", "fuel-key"))
	assert.Equal(t, http.StatusUnauthorized, get("/v1/pools/main/shards", "fuel-key"), "fuel key rejected on admin route")
	assert.Equal(t, http.StatusOK, get("/v1/pools/main/shards", "admin-key"))
	assert.Equal(t, http.StatusUnauthorized, get("/v1/pools/main/shards", ""))
}

// TestAdminRoutesFallBackToAPIKeysWhenNoAdminKeys checks today's behaviour is
// preserved when AdminKeys is left empty.
func TestAdminRoutesFallBackToAPIKeysWhenNoAdminKeys(t *testing.T) {
	e := newEnv(t, []string{"k1"})
	resp := e.do(t, http.MethodGet, "/v1/pools/main/shards", nil, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

// TestTimesAreUTC checks that a DTO time is always rendered with a "Z" UTC
// suffix even when the underlying clock runs in another location.
func TestTimesAreUTC(t *testing.T) {
	loc := time.FixedZone("X", 3600)
	clk := storetest.NewClock(storetest.Start.In(loc))
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("main", 50)
	pc.MinTTL = time.Second
	require.NoError(t, pc.Validate())
	ic := pool.DefaultInstanceConfig()
	ic.InstanceID = "api-test-tz"
	ic.AdoptTimeout = 50 * time.Millisecond
	p := pool.NewPool(st, pc, ic, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	mintPuzzleShard(t, st, "main", 1, store.ShardMined)

	h := api.NewHandler(api.Deps{Pools: []api.PoolService{p}, Store: st})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		srv.URL+"/v1/pools/main/reserve", strings.NewReader(`{"count":1}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(body, &raw))
	expiresAt, ok := raw["expires_at"].(string)
	require.True(t, ok)
	assert.True(t, strings.HasSuffix(expiresAt, "Z"), "expires_at should be UTC: %s", expiresAt)
}

// TestP2PKHPoolReturnsConstraints checks that a p2pkh-mode pool's reserve
// response and pool view both carry the fixed sighash Constraints.
func TestP2PKHPoolReturnsConstraints(t *testing.T) {
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))
	pc := pool.DefaultPoolConfig("p2pkh-pool", 50)
	pc.MinTTL = time.Second
	pc.LockModeName = "p2pkh"
	require.NoError(t, pc.Validate())
	ic := pool.DefaultInstanceConfig()
	ic.InstanceID = "api-test-p2pkh"
	ic.AdoptTimeout = 50 * time.Millisecond
	p := pool.NewPool(st, pc, ic, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = p.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	txid := storetest.NewTxID()
	shard := store.Shard{
		Pool: "p2pkh-pool", TxID: txid, Size: 1, Denomination: 50,
		LockMode: store.LockP2PKH, Status: store.ShardMined,
		BEEF: append([]byte("beef-"), txid[:4]...),
	}
	rows := []store.Row{{
		Outpoint: store.Outpoint{TxID: txid, Vout: 0}, Pool: "p2pkh-pool", Satoshis: 50,
		LockingScript: []byte{0x76, 0xa9}, UnlockingScript: []byte{0x30, 0x44},
	}}
	require.NoError(t, st.PutShard(context.Background(), shard, rows))

	h := api.NewHandler(api.Deps{Pools: []api.PoolService{p}, Store: st})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		srv.URL+"/v1/pools/p2pkh-pool/reserve", strings.NewReader(`{"count":1}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	var reserved wire.ReserveResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&reserved))
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotNil(t, reserved.Constraints)
	assert.Equal(t, uint32(1), reserved.Constraints.TxVersion)
	assert.Equal(t, uint32(0), reserved.Constraints.LockTime)
	assert.Equal(t, uint32(0xffffffff), reserved.Constraints.Sequence)

	req2, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/v1/pools/p2pkh-pool", nil)
	require.NoError(t, err)
	resp2, err := srv.Client().Do(req2)
	require.NoError(t, err)
	var p2 wire.Pool
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&p2))
	require.NoError(t, resp2.Body.Close())
	require.Equal(t, http.StatusOK, resp2.StatusCode)
	require.NotNil(t, p2.Constraints)
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
