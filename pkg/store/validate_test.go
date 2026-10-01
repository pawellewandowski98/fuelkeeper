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
	ok := store.Shard{Pool: "main", TxID: txid, Size: 3, Denomination: 50, Status: store.ShardUnmined, BEEF: []byte("beef")}
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

	bad = ok
	bad.BEEF = nil
	require.ErrorIs(t, store.ValidatePutShard(bad, rows(txid, "main", 3)), store.ErrInvalidArgument, "BEEF must not be empty")

	bad = ok
	bad.BEEF = []byte{}
	require.ErrorIs(t, store.ValidatePutShard(bad, rows(txid, "main", 3)), store.ErrInvalidArgument, "an empty (non-nil) BEEF is also rejected")

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
	require.ErrorIs(t, store.ValidatePool("p\xff"), store.ErrInvalidArgument, "invalid UTF-8 byte")
	require.ErrorIs(t, store.ValidatePool("p\x00"), store.ErrInvalidArgument, "NUL byte")
	require.NoError(t, store.ValidatePool("café"), "valid multi-byte UTF-8 is fine")

	require.NoError(t, store.ValidateHolder("inst-1/abc"))
	require.ErrorIs(t, store.ValidateHolder(""), store.ErrInvalidArgument)
	require.ErrorIs(t, store.ValidateHolder("h\xfe"), store.ErrInvalidArgument, "invalid UTF-8 byte")
	require.ErrorIs(t, store.ValidateHolder("h\x00"), store.ErrInvalidArgument, "NUL byte")

	require.NoError(t, store.ValidateLeaseName("jobs/reaper"))
	require.ErrorIs(t, store.ValidateLeaseName("jobs/\xff"), store.ErrInvalidArgument, "invalid UTF-8 byte")
	require.ErrorIs(t, store.ValidateLeaseName("jobs/\x00reaper"), store.ErrInvalidArgument, "NUL byte")

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

func TestValidateTime(t *testing.T) {
	require.NoError(t, store.ValidateTime(time.Time{}, "x"), "the zero time is always allowed")
	require.NoError(t, store.ValidateTime(time.Unix(0, 1), "x"), "1ns after the epoch is fine")
	require.NoError(t, store.ValidateTime(time.Now(), "x"))

	require.ErrorIs(t, store.ValidateTime(time.Unix(0, 0), "x"), store.ErrInvalidArgument, "the exact epoch instant collides with the zero-time sentinel on disk")
	require.ErrorIs(t, store.ValidateTime(time.Unix(-1, 0), "x"), store.ErrInvalidArgument, "before the epoch")
	require.ErrorIs(t, store.ValidateTime(time.Unix(0, 0).Add(-time.Hour), "x"), store.ErrInvalidArgument)
}

func TestValidateAcquireUntilRejectsPreEpoch(t *testing.T) {
	now := time.Now()
	require.ErrorIs(t, store.ValidateAcquireUntil(time.Unix(0, 0), now), store.ErrInvalidArgument)
	require.ErrorIs(t, store.ValidateAcquireUntil(time.Unix(-100, 0), now), store.ErrInvalidArgument)
	require.NoError(t, store.ValidateAcquireUntil(now.Add(time.Minute), now))
}

func TestValidateRenewUntilRejectsEpochAndPreEpoch(t *testing.T) {
	require.ErrorIs(t, store.ValidateRenewUntil(time.Time{}), store.ErrInvalidArgument, "zero until is indistinguishable from unowned")
	require.ErrorIs(t, store.ValidateRenewUntil(time.Unix(0, 0)), store.ErrInvalidArgument, "the exact epoch instant collides with the zero-time sentinel on disk")
	require.ErrorIs(t, store.ValidateRenewUntil(time.Unix(-1, 0)), store.ErrInvalidArgument, "before the epoch")
	require.NoError(t, store.ValidateRenewUntil(time.Now().Add(time.Minute)))
}

func TestValidateReserveOpsRejectsPreEpochExpiry(t *testing.T) {
	tok := store.NewToken()
	preEpoch := []store.ReserveOp{{Outpoint: store.Outpoint{TxID: hashN(1), Vout: 0}, Token: tok, ExpiresAt: time.Unix(0, 0)}}
	require.ErrorIs(t, store.ValidateReserveOps(preEpoch, 10), store.ErrInvalidArgument)

	negative := []store.ReserveOp{{Outpoint: store.Outpoint{TxID: hashN(1), Vout: 0}, Token: tok, ExpiresAt: time.Unix(-1, 0)}}
	require.ErrorIs(t, store.ValidateReserveOps(negative, 10), store.ErrInvalidArgument)

	ok := []store.ReserveOp{{Outpoint: store.Outpoint{TxID: hashN(1), Vout: 0}, Token: tok, ExpiresAt: time.Now().Add(time.Minute)}}
	require.NoError(t, store.ValidateReserveOps(ok, 10))
}

func TestValidateSpendRequest(t *testing.T) {
	tok := store.NewToken()
	op := func(v uint32) store.Outpoint { return store.Outpoint{TxID: hashN(1), Vout: v} }
	ok := store.SpendRequest{Token: tok, Ops: []store.Outpoint{op(0), op(1)}, TxID: hashN(9), Terminal: true}
	require.NoError(t, store.ValidateSpendRequest(ok, 10))

	hint := ok
	hint.Terminal, hint.TxID = false, chainhash.Hash{}
	require.NoError(t, store.ValidateSpendRequest(hint, 10), "non-terminal accepts a zero txid")

	for name, bad := range map[string]store.SpendRequest{
		"zero token":              {Ops: ok.Ops, TxID: ok.TxID},
		"empty ops":               {Token: tok, TxID: ok.TxID},
		"duplicate outpoint":      {Token: tok, Ops: []store.Outpoint{op(0), op(0)}, TxID: ok.TxID},
		"too many ops":            {Token: tok, Ops: []store.Outpoint{op(0), op(1), op(2)}, TxID: ok.TxID},
		"terminal with zero txid": {Token: tok, Ops: ok.Ops, Terminal: true},
	} {
		require.ErrorIs(t, store.ValidateSpendRequest(bad, 2), store.ErrInvalidArgument, name)
	}
}

func TestValidateSpendBatch(t *testing.T) {
	tok := store.NewToken()
	req := func(vs ...uint32) store.SpendRequest {
		r := store.SpendRequest{Token: tok, TxID: hashN(9)}
		for _, v := range vs {
			r.Ops = append(r.Ops, store.Outpoint{TxID: hashN(1), Vout: v})
		}
		return r
	}
	require.NoError(t, store.ValidateSpendBatch([]store.SpendRequest{req(0, 1), req(1, 2)}, 4),
		"the same outpoint in two requests is allowed; the later one simply finds it already spent")
	require.ErrorIs(t, store.ValidateSpendBatch(nil, 4), store.ErrInvalidArgument, "empty batch")
	require.ErrorIs(t, store.ValidateSpendBatch([]store.SpendRequest{req(0, 1), req(2, 3), req(4)}, 4),
		store.ErrInvalidArgument, "total outpoints over maxBatch")
	require.ErrorIs(t, store.ValidateSpendBatch([]store.SpendRequest{req(0), {Token: tok}}, 4),
		store.ErrInvalidArgument, "one invalid request rejects the batch")
}
