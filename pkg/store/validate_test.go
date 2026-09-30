package store_test

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

func hashN(n byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = n
	return h
}

func rows(txid chainhash.Hash, pool string, n int) []store.Row { //nolint:unparam // pool is a general-purpose helper param; every call site here happens to pass "main"
	out := make([]store.Row, n)
	for i := range out {
		out[i] = store.Row{
			Outpoint:        store.Outpoint{TxID: txid, Vout: uint32(i)},
			Pool:            pool,
			Satoshis:        50,
			LockingScript:   []byte{0xa8},
			UnlockingScript: []byte{0x20},
		}
	}
	return out
}

func TestValidatePutShard(t *testing.T) {
	txid := hashN(1)
	ok := store.Shard{Pool: "main", TxID: txid, Size: 3, Denomination: 50, Status: store.ShardUnmined}
	require.NoError(t, store.ValidatePutShard(ok, rows(txid, "main", 3)))

	bad := ok
	bad.Pool = ""
	require.ErrorIs(t, store.ValidatePutShard(bad, rows(txid, "main", 3)), store.ErrInvalidArgument)

	bad = ok
	bad.Size = 2
	require.ErrorIs(t, store.ValidatePutShard(bad, rows(txid, "main", 3)), store.ErrInvalidArgument)

	bad = ok
	bad.Status = store.ShardInvalid
	require.ErrorIs(t, store.ValidatePutShard(bad, rows(txid, "main", 3)), store.ErrInvalidArgument)

	bad = ok
	bad.Denomination = 0
	require.ErrorIs(t, store.ValidatePutShard(bad, rows(txid, "main", 3)), store.ErrInvalidArgument)

	r := rows(txid, "main", 3)
	r[1].TxID = hashN(2)
	require.ErrorIs(t, store.ValidatePutShard(ok, r), store.ErrInvalidArgument)

	r = rows(txid, "main", 3)
	r[2].Vout = 0
	require.ErrorIs(t, store.ValidatePutShard(ok, r), store.ErrInvalidArgument)

	r = rows(txid, "main", 3)
	r[0].Pool = "other"
	require.ErrorIs(t, store.ValidatePutShard(ok, r), store.ErrInvalidArgument)

	r = rows(txid, "main", 3)
	r[0].UnlockingScript = nil
	require.ErrorIs(t, store.ValidatePutShard(ok, r), store.ErrInvalidArgument)

	r = rows(txid, "main", 3)
	r[0].Satoshis = 0
	require.ErrorIs(t, store.ValidatePutShard(ok, r), store.ErrInvalidArgument)
}

func TestValidateReserveOps(t *testing.T) {
	tok := store.NewToken()
	exp := time.Now().Add(time.Minute)
	op := func(v uint32) store.ReserveOp {
		return store.ReserveOp{Outpoint: store.Outpoint{TxID: hashN(1), Vout: v}, Token: tok, ExpiresAt: exp}
	}
	require.NoError(t, store.ValidateReserveOps([]store.ReserveOp{op(0), op(1)}, 10))
	require.ErrorIs(t, store.ValidateReserveOps(nil, 10), store.ErrInvalidArgument)
	require.ErrorIs(t, store.ValidateReserveOps([]store.ReserveOp{op(0), op(0)}, 10), store.ErrInvalidArgument)
	require.ErrorIs(t, store.ValidateReserveOps([]store.ReserveOp{op(0), op(1), op(2)}, 2), store.ErrInvalidArgument)

	zero := op(0)
	zero.Token = store.Token{}
	require.ErrorIs(t, store.ValidateReserveOps([]store.ReserveOp{zero}, 10), store.ErrInvalidArgument)

	noExp := op(0)
	noExp.ExpiresAt = time.Time{}
	require.ErrorIs(t, store.ValidateReserveOps([]store.ReserveOp{noExp}, 10), store.ErrInvalidArgument)
}

const testMaxBatch = 1000

func TestValidateReserveOpsAtMaxBatch(t *testing.T) {
	tok := store.NewToken()
	exp := time.Now().Add(time.Minute)
	ops := make([]store.ReserveOp, testMaxBatch)
	for i := range ops {
		ops[i] = store.ReserveOp{Outpoint: store.Outpoint{TxID: hashN(1), Vout: uint32(i)}, Token: tok, ExpiresAt: exp}
	}
	require.NoError(t, store.ValidateReserveOps(ops, testMaxBatch))
}

func TestValidateOutpointsAtMaxBatch(t *testing.T) {
	ops := make([]store.Outpoint, testMaxBatch)
	for i := range ops {
		ops[i] = store.Outpoint{TxID: hashN(1), Vout: uint32(i)}
	}
	require.NoError(t, store.ValidateOutpoints(ops, testMaxBatch))
}

func TestValidateOutpointsAndMisc(t *testing.T) {
	ops := []store.Outpoint{{TxID: hashN(1), Vout: 0}, {TxID: hashN(1), Vout: 1}} //nolint:prealloc // fixed 2-element literal; the append() below builds a throwaway slice, not a grown copy of ops
	require.NoError(t, store.ValidateOutpoints(ops, 10))
	require.ErrorIs(t, store.ValidateOutpoints(nil, 10), store.ErrInvalidArgument)
	require.ErrorIs(t, store.ValidateOutpoints(append(ops, ops[0]), 10), store.ErrInvalidArgument)
	require.ErrorIs(t, store.ValidateOutpoints(ops, 1), store.ErrInvalidArgument)

	require.NoError(t, store.ValidatePool("main"))
	require.ErrorIs(t, store.ValidatePool(""), store.ErrInvalidArgument)
	require.ErrorIs(t, store.ValidatePool(string(make([]byte, 65))), store.ErrInvalidArgument)

	require.NoError(t, store.ValidateHolder("inst-1/abc"))
	require.ErrorIs(t, store.ValidateHolder(""), store.ErrInvalidArgument)

	require.NoError(t, store.ValidateLimit(1))
	require.ErrorIs(t, store.ValidateLimit(0), store.ErrInvalidArgument)

	require.NoError(t, store.ValidateLease("jobs/reaper", "inst-1", time.Second))
	require.ErrorIs(t, store.ValidateLease("", "inst-1", time.Second), store.ErrInvalidArgument)
	require.ErrorIs(t, store.ValidateLease("jobs/reaper", "", time.Second), store.ErrInvalidArgument)
	require.ErrorIs(t, store.ValidateLease("jobs/reaper", "inst-1", 0), store.ErrInvalidArgument)

	h := hashN(9)
	require.NoError(t, store.ValidateResolve(store.StateAvailable, nil))
	require.NoError(t, store.ValidateResolve(store.StateSpent, &h))
	require.ErrorIs(t, store.ValidateResolve(store.StateSpent, nil), store.ErrInvalidArgument)
	require.ErrorIs(t, store.ValidateResolve(store.StateReserved, nil), store.ErrInvalidArgument)

	require.NoError(t, store.ValidateShardStatuses([]store.ShardStatus{store.ShardMined}))
	require.ErrorIs(t, store.ValidateShardStatuses(nil), store.ErrInvalidArgument)
	require.ErrorIs(t, store.ValidateShardStatuses([]store.ShardStatus{store.ShardStatus(7)}), store.ErrInvalidArgument)

	require.NoError(t, store.ValidateUpdateStatus(store.ShardRetired))
	require.ErrorIs(t, store.ValidateUpdateStatus(store.ShardInvalid), store.ErrInvalidArgument)
}
