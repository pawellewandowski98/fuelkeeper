package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// testCancelledContextRejected pins store.go's "Errors" section: every
// Store/Leaser method that takes a context must reject an already-cancelled one
// with an error satisfying errors.Is(err, context.Canceled), and must not apply
// any change while doing so (store.ErrUnavailable may wrap the cancellation, but
// the cancellation itself must always be reachable via errors.Is). Every call
// below is given otherwise-well-formed arguments, so a failure here can only be
// the ctx check, not unrelated argument validation.
func testCancelledContextRejected(t *testing.T, s store.Store, clk *Clock) {
	live := context.Background()
	ctx, cancel := context.WithCancel(live)
	cancel()

	shard, rows := MintShard(t, s, "main", 4, store.ShardMined)
	tok := store.NewToken()
	MustReserve(t, s, "main", rows[:1], tok, clk.Now().Add(time.Minute))
	_, err := s.TryAcquireOrRenew(live, "jobs/cancel-test", "h", time.Minute)
	require.NoError(t, err)

	before, err := s.Stats(live, "main")
	require.NoError(t, err)

	assertCancelled := func(err error, what string) {
		t.Helper()
		require.Error(t, err, "%s must reject an already-cancelled context", what)
		assert.ErrorIs(t, err, context.Canceled, "%s: error %v must satisfy errors.Is(err, context.Canceled)", what, err)
	}

	assertCancelled(s.Ping(ctx), "Ping")

	_, err = s.GetShard(ctx, "main", shard.TxID)
	assertCancelled(err, "GetShard")
	assertCancelled(s.UpdateShard(ctx, "main", shard.TxID, store.ShardMined, nil), "UpdateShard")
	_, err = s.RetireShard(ctx, "main", shard.TxID)
	assertCancelled(err, "RetireShard")
	_, err = s.AcquireShard(ctx, "main", "h2", clk.Now(), clk.Now().Add(time.Minute), []store.ShardStatus{store.ShardMined}, nil)
	assertCancelled(err, "AcquireShard")
	_, err = s.RenewShard(ctx, "main", shard.TxID, "h2", clk.Now().Add(time.Minute))
	assertCancelled(err, "RenewShard")
	assertCancelled(s.ReleaseShard(ctx, "main", shard.TxID, "h2"), "ReleaseShard")
	_, err = s.ListShards(ctx, "main", store.ShardFilter{})
	assertCancelled(err, "ListShards")

	freshShard, freshRows := BuildShard("main", 1, store.ShardMined)
	assertCancelled(s.PutShard(ctx, freshShard, freshRows), "PutShard")
	_, err = s.GetShard(live, "main", freshShard.TxID)
	require.ErrorIs(t, err, store.ErrNotFound, "PutShard must not have inserted anything under a cancelled context")

	_, err = s.ListAvailable(ctx, "main", shard.TxID, 0, 10)
	assertCancelled(err, "ListAvailable")
	_, err = s.GetRows(ctx, "main", Ops(rows))
	assertCancelled(err, "GetRows")

	applied, err := s.Reserve(ctx, "main", ReserveOps(rows[1:2], store.NewToken(), clk.Now().Add(time.Minute)))
	assertCancelled(err, "Reserve")
	assert.Empty(t, applied, "Reserve must apply nothing under a cancelled context")

	n, err := s.Release(ctx, "main", tok, Ops(rows[:1]))
	assertCancelled(err, "Release")
	assert.Zero(t, n)

	n, err = s.Spend(ctx, "main", tok, Ops(rows[:1]), NewTxID(), false)
	assertCancelled(err, "Spend")
	assert.Zero(t, n)

	n, err = s.Quarantine(ctx, "main", clk.Now().Add(time.Hour), 10)
	assertCancelled(err, "Quarantine")
	assert.Zero(t, n)

	_, err = s.ListQuarantined(ctx, "main", clk.Now().Add(time.Hour), nil, 10)
	assertCancelled(err, "ListQuarantined")

	n, err = s.Resolve(ctx, "main", Ops(rows[:1]), store.StateAvailable, nil)
	assertCancelled(err, "Resolve")
	assert.Zero(t, n)

	_, err = s.Stats(ctx, "main")
	assertCancelled(err, "Stats")

	_, err = s.TryAcquireOrRenew(ctx, "jobs/cancel-test", "h", time.Minute)
	assertCancelled(err, "TryAcquireOrRenew")
	assertCancelled(s.ReleaseLease(ctx, "jobs/cancel-test", "h"), "ReleaseLease")

	after, err := s.Stats(live, "main")
	require.NoError(t, err)
	assert.Equal(t, before, after, "no cancelled-context call above may have changed anything")
}
