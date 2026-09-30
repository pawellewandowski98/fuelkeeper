package client_test

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api"
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

	_, err = client.New("://bad")
	require.Error(t, err)

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = c.Pools(cctx)
	require.ErrorIs(t, err, context.Canceled, "caller cancellation is surfaced as the context error, not ErrUnavailable")

	down, err := client.New("http://127.0.0.1:1")
	require.NoError(t, err)
	_, err = down.Pools(ctx)
	require.ErrorIs(t, err, client.ErrUnavailable)
	assert.True(t, client.IsRetryable(err))
}
