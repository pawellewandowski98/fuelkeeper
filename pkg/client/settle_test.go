package client_test

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"sync"
	"sync/atomic"
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

// settleTestRows is how many rows startSettleAPI mints in each of the two
// pools: comfortably more than the number of subtests that reserve one.
const settleTestRows = 10

// startSettleAPI runs an in-process FuelKeeper over two pools sharing one
// store: "trust" (spend_policy: trust, a Spend is terminal immediately) and
// "verify" (spend_policy: verify, a Spend only quarantines as a hint),
// each seeded with settleTestRows mined rows. The manual clock never
// advances, so reservations never expire mid-test.
func startSettleAPI(t *testing.T) (*httptest.Server, store.Store) {
	t.Helper()
	clk := storetest.NewClock(storetest.Start)
	st := memory.New(memory.WithClock(clk.Now))

	trustCfg := pool.DefaultPoolConfig("trust", 50)
	trustCfg.SpendPolicy = pool.SpendTrust
	trustCfg.MinTTL = time.Second
	require.NoError(t, trustCfg.Validate())

	verifyCfg := pool.DefaultPoolConfig("verify", 50)
	verifyCfg.SpendPolicy = pool.SpendVerify
	verifyCfg.MinTTL = time.Second
	require.NoError(t, verifyCfg.Validate())

	ic := pool.DefaultInstanceConfig()
	ic.InstanceID = "settle-test"
	ic.AdoptTimeout = 50 * time.Millisecond

	trustPool := pool.NewPool(st, trustCfg, ic, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))
	verifyPool := pool.NewPool(st, verifyCfg, ic, pool.ClockFunc(clk.Now), slog.New(slog.DiscardHandler))

	storetest.MintShard(t, st, "trust", settleTestRows, store.ShardMined)
	storetest.MintShard(t, st, "verify", settleTestRows, store.ShardMined)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _ = trustPool.Run(ctx) }()
		go func() { defer wg.Done(); _ = verifyPool.Run(ctx) }()
		wg.Wait()
	}()
	srv := httptest.NewServer(api.NewHandler(api.Deps{Pools: []api.PoolService{trustPool, verifyPool}, Store: st}))
	t.Cleanup(func() { srv.Close(); cancel(); <-done })
	return srv, st
}

// reserveOne reserves a single output from poolName via fk, returning its
// token and outpoint.
func reserveOne(t *testing.T, fk *client.Client, poolName string) (token, outpoint string) {
	t.Helper()
	res, err := fk.Reserve(context.Background(), poolName, client.ReserveRequest{Count: 1, TTL: 30 * time.Second})
	require.NoError(t, err)
	require.Len(t, res.Outputs, 1)
	return res.Token, res.Outputs[0].Outpoint
}

// getRow reads one row back through st.
func getRow(t *testing.T, st store.Store, poolName, outpoint string) store.Row {
	t.Helper()
	op, err := store.ParseOutpoint(outpoint)
	require.NoError(t, err)
	rows, err := st.GetRows(context.Background(), poolName, []store.Outpoint{op})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	return rows[0]
}

// countingClient builds a Client against srv whose Observer increments n on
// every HTTP request the Client makes.
func countingClient(t *testing.T, srv *httptest.Server, n *atomic.Int32) *client.Client {
	t.Helper()
	fk, err := client.New(srv.URL, client.WithObserver(func(string, string, int, time.Duration, error) {
		n.Add(1)
	}))
	require.NoError(t, err)
	return fk
}

// TestSettleTrustBroadcastSpendsByTxID checks that OutcomeBroadcast on a
// trust pool spends the row terminally, by the consumer's own txid.
func TestSettleTrustBroadcastSpendsByTxID(t *testing.T) {
	srv, st := startSettleAPI(t)
	var reqs atomic.Int32
	fk := countingClient(t, srv, &reqs)
	token, outpoint := reserveOne(t, fk, "trust")
	txid := storetest.NewTxID().String()

	n, err := fk.Settle(context.Background(), "trust", token, []string{outpoint}, client.Settlement{
		Outcome: client.OutcomeBroadcast,
		TxID:    txid,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Positive(t, reqs.Load())

	row := getRow(t, st, "trust", outpoint)
	assert.Equal(t, store.StateSpent, row.State)
	require.NotNil(t, row.SpentBy)
	assert.Equal(t, txid, row.SpentBy.String())
}

// TestSettleTrustDoubleSpendSpendsByCompetitor checks that OutcomeDoubleSpend
// on a trust pool spends the row terminally by the first competing txid,
// never by the consumer's own TxID (competitor precedence).
func TestSettleTrustDoubleSpendSpendsByCompetitor(t *testing.T) {
	srv, st := startSettleAPI(t)
	var reqs atomic.Int32
	fk := countingClient(t, srv, &reqs)
	token, outpoint := reserveOne(t, fk, "trust")
	own := storetest.NewTxID().String()
	competitor := storetest.NewTxID().String()
	other := storetest.NewTxID().String()

	n, err := fk.Settle(context.Background(), "trust", token, []string{outpoint}, client.Settlement{
		Outcome:        client.OutcomeDoubleSpend,
		TxID:           own,
		CompetingTxIDs: []string{competitor, other},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	row := getRow(t, st, "trust", outpoint)
	assert.Equal(t, store.StateSpent, row.State)
	require.NotNil(t, row.SpentBy)
	assert.Equal(t, competitor, row.SpentBy.String(), "the competitor takes precedence over the consumer's own txid")
	assert.NotEqual(t, own, row.SpentBy.String())
}

// TestSettleTrustDoubleSpendFallsBackToTxID checks that OutcomeDoubleSpend
// with no known competitor still spends -- by the consumer's own TxID --
// rather than releasing (never release on a double-spend).
func TestSettleTrustDoubleSpendFallsBackToTxID(t *testing.T) {
	srv, st := startSettleAPI(t)
	var reqs atomic.Int32
	fk := countingClient(t, srv, &reqs)
	token, outpoint := reserveOne(t, fk, "trust")
	own := storetest.NewTxID().String()

	n, err := fk.Settle(context.Background(), "trust", token, []string{outpoint}, client.Settlement{
		Outcome: client.OutcomeDoubleSpend,
		TxID:    own,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	row := getRow(t, st, "trust", outpoint)
	assert.Equal(t, store.StateSpent, row.State)
	require.NotNil(t, row.SpentBy)
	assert.Equal(t, own, row.SpentBy.String())
}

// TestSettleTrustNotBroadcastReleases checks that OutcomeNotBroadcast
// releases the row back to available.
func TestSettleTrustNotBroadcastReleases(t *testing.T) {
	srv, st := startSettleAPI(t)
	var reqs atomic.Int32
	fk := countingClient(t, srv, &reqs)
	token, outpoint := reserveOne(t, fk, "trust")

	n, err := fk.Settle(context.Background(), "trust", token, []string{outpoint}, client.Settlement{
		Outcome: client.OutcomeNotBroadcast,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	row := getRow(t, st, "trust", outpoint)
	assert.Equal(t, store.StateAvailable, row.State)
}

// TestSettleUnknownMakesNoCall checks that OutcomeUnknown leaves the row
// reserved and makes no HTTP request at all (the observer's counter proves
// it, not just the returned count).
func TestSettleUnknownMakesNoCall(t *testing.T) {
	srv, st := startSettleAPI(t)
	var reqs atomic.Int32
	fk := countingClient(t, srv, &reqs)
	token, outpoint := reserveOne(t, fk, "trust")
	reqs.Store(0) // ignore the reserve call above

	n, err := fk.Settle(context.Background(), "trust", token, []string{outpoint}, client.Settlement{
		Outcome: client.OutcomeUnknown,
	})
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Zero(t, reqs.Load(), "OutcomeUnknown must not make an HTTP request")

	row := getRow(t, st, "trust", outpoint)
	assert.Equal(t, store.StateReserved, row.State)
}

// TestSettleVerifyBroadcastQuarantinesWithHint checks that OutcomeBroadcast
// on a verify pool quarantines the row (a Spend is only a hint there),
// recording the consumer's txid as SpentHint.
func TestSettleVerifyBroadcastQuarantinesWithHint(t *testing.T) {
	srv, st := startSettleAPI(t)
	var reqs atomic.Int32
	fk := countingClient(t, srv, &reqs)
	token, outpoint := reserveOne(t, fk, "verify")
	txid := storetest.NewTxID().String()

	n, err := fk.Settle(context.Background(), "verify", token, []string{outpoint}, client.Settlement{
		Outcome: client.OutcomeBroadcast,
		TxID:    txid,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	row := getRow(t, st, "verify", outpoint)
	assert.Equal(t, store.StateQuarantined, row.State)
	require.NotNil(t, row.SpentHint)
	assert.Equal(t, txid, row.SpentHint.String())
}

// TestSettleVerifyDoubleSpendQuarantinesWithCompetitorHint checks that
// OutcomeDoubleSpend on a verify pool quarantines the row with the
// competitor (not the consumer's own txid) as SpentHint.
func TestSettleVerifyDoubleSpendQuarantinesWithCompetitorHint(t *testing.T) {
	srv, st := startSettleAPI(t)
	var reqs atomic.Int32
	fk := countingClient(t, srv, &reqs)
	token, outpoint := reserveOne(t, fk, "verify")
	own := storetest.NewTxID().String()
	competitor := storetest.NewTxID().String()

	n, err := fk.Settle(context.Background(), "verify", token, []string{outpoint}, client.Settlement{
		Outcome:        client.OutcomeDoubleSpend,
		TxID:           own,
		CompetingTxIDs: []string{competitor},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	row := getRow(t, st, "verify", outpoint)
	assert.Equal(t, store.StateQuarantined, row.State)
	require.NotNil(t, row.SpentHint)
	assert.Equal(t, competitor, row.SpentHint.String())
}

// TestSettleMissingTxIDErrors checks that OutcomeBroadcast and
// OutcomeDoubleSpend (with no competitor) without a txid return
// ErrSettleTxIDRequired and never reach the network.
func TestSettleMissingTxIDErrors(t *testing.T) {
	cases := map[string]client.Settlement{
		"broadcast, no txid":                   {Outcome: client.OutcomeBroadcast},
		"double-spend, no txid, no competitor": {Outcome: client.OutcomeDoubleSpend},
		"broadcast, malformed txid":            {Outcome: client.OutcomeBroadcast, TxID: "not-hex"},
		"broadcast, short txid":                {Outcome: client.OutcomeBroadcast, TxID: "ab"},
	}
	for name, s := range cases {
		t.Run(name, func(t *testing.T) {
			srv, _ := startSettleAPI(t)
			var reqs atomic.Int32
			fk := countingClient(t, srv, &reqs)
			token, outpoint := reserveOne(t, fk, "trust")
			reqs.Store(0)

			n, err := fk.Settle(context.Background(), "trust", token, []string{outpoint}, s)
			require.ErrorIs(t, err, client.ErrSettleTxIDRequired)
			assert.Zero(t, n)
			assert.Zero(t, reqs.Load(), "a rejected-before-sending settlement must not reach the network")
		})
	}
}

// TestSettleInvalidOutcomeErrors checks that an Outcome outside the defined
// constants is rejected rather than silently treated as one of them.
func TestSettleInvalidOutcomeErrors(t *testing.T) {
	srv, _ := startSettleAPI(t)
	var reqs atomic.Int32
	fk := countingClient(t, srv, &reqs)
	token, outpoint := reserveOne(t, fk, "trust")
	reqs.Store(0)

	n, err := fk.Settle(context.Background(), "trust", token, []string{outpoint}, client.Settlement{Outcome: client.Outcome(99)})
	require.Error(t, err)
	require.NotErrorIs(t, err, client.ErrSettleTxIDRequired)
	assert.Zero(t, n)
	assert.Zero(t, reqs.Load())
}

// TestOutcomeString pins Outcome's String values.
func TestOutcomeString(t *testing.T) {
	assert.Equal(t, "broadcast", client.OutcomeBroadcast.String())
	assert.Equal(t, "not_broadcast", client.OutcomeNotBroadcast.String())
	assert.Equal(t, "double_spend", client.OutcomeDoubleSpend.String())
	assert.Equal(t, "unknown", client.OutcomeUnknown.String())
	assert.Contains(t, client.Outcome(99).String(), "99")
}
