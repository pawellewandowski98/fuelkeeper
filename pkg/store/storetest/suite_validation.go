package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// testArgumentValidation pins the uniform argument-validation rule: every method
// taking a pool rejects "" with ErrInvalidArgument, regardless of what else it does.
// The row methods (Reserve, Release, ...) are still stubs until Task 5; they
// implement the pool check now so this assertion already holds for them.
func testArgumentValidation(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	shard, rows := MintShard(t, s, "main", 1, store.ShardMined)
	txid := shard.TxID
	tok := store.NewToken()
	future := clk.Now().Add(time.Minute)

	_, err := s.GetShard(ctx, "", txid)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	require.ErrorIs(t, s.UpdateShard(ctx, "", txid, store.ShardMined, nil), store.ErrInvalidArgument)
	_, err = s.RetireShard(ctx, "", txid)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.AcquireShard(ctx, "", "h", clk.Now(), future, []store.ShardStatus{store.ShardMined}, nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.RenewShard(ctx, "", txid, "h", future)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	require.ErrorIs(t, s.ReleaseShard(ctx, "", txid, "h"), store.ErrInvalidArgument)
	_, err = s.ListShards(ctx, "", store.ShardFilter{})
	require.ErrorIs(t, err, store.ErrInvalidArgument)

	_, err = s.ListAvailable(ctx, "", txid, 0, 10)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.GetRows(ctx, "", Ops(rows))
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.Reserve(ctx, "", ReserveOps(rows, tok, future))
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.Release(ctx, "", tok, Ops(rows))
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.Spend(ctx, "", tok, Ops(rows), txid, false)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.Quarantine(ctx, "", future, 10)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.ListQuarantined(ctx, "", future, 10)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.Resolve(ctx, "", Ops(rows), store.StateAvailable, nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.Stats(ctx, "")
	require.ErrorIs(t, err, store.ErrInvalidArgument)

	_, err = s.ListShards(ctx, "main", store.ShardFilter{Limit: -1})
	require.ErrorIs(t, err, store.ErrInvalidArgument, "negative limit")
	_, err = s.ListShards(ctx, "main", store.ShardFilter{Statuses: []store.ShardStatus{store.ShardStatus(7)}})
	require.ErrorIs(t, err, store.ErrInvalidArgument, "unknown status")

	require.ErrorIs(t, s.ReleaseShard(ctx, "main", txid, ""), store.ErrInvalidArgument, "empty holder")
	require.ErrorIs(t, s.ReleaseLease(ctx, "", "h"), store.ErrInvalidArgument, "empty lease name")
}
