package storetest

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

var shardSeq atomic.Uint64

// NewTxID returns a unique txid for tests.
func NewTxID() chainhash.Hash {
	n := shardSeq.Add(1)
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint64(buf[8:], n)
	sum := sha256.Sum256(buf[:])
	h, _ := chainhash.NewHash(sum[:])
	return *h
}

// MintShard builds a shard of n puzzle rows (denomination 50) and inserts it.
func MintShard(t *testing.T, s store.Store, pool string, n int, status store.ShardStatus) (store.Shard, []store.Row) {
	t.Helper()
	txid := NewTxID()
	shard := store.Shard{
		Pool:         pool,
		TxID:         txid,
		Size:         n,
		Denomination: 50,
		LockMode:     store.LockPuzzle,
		Status:       status,
		BEEF:         append([]byte("beef-"), txid[:4]...),
	}
	rows := make([]store.Row, n)
	for i := range rows {
		rows[i] = store.Row{
			Outpoint:        store.Outpoint{TxID: txid, Vout: uint32(i)},
			Pool:            pool,
			Satoshis:        50,
			LockingScript:   []byte{0xa8, 0x20, byte(i), 0x87},
			UnlockingScript: []byte{0x20, byte(i)},
		}
	}
	require.NoError(t, s.PutShard(context.Background(), shard, rows))
	return shard, rows
}

// Ops extracts outpoints.
func Ops(rows []store.Row) []store.Outpoint {
	out := make([]store.Outpoint, len(rows))
	for i, r := range rows {
		out[i] = r.Outpoint
	}
	return out
}

// ReserveOps builds reserve ops for rows under tok expiring at exp.
func ReserveOps(rows []store.Row, tok store.Token, exp time.Time) []store.ReserveOp {
	out := make([]store.ReserveOp, len(rows))
	for i, r := range rows {
		out[i] = store.ReserveOp{Outpoint: r.Outpoint, Token: tok, ExpiresAt: exp}
	}
	return out
}

// MustReserve reserves rows and asserts every op applied.
func MustReserve(t *testing.T, s store.Store, pool string, rows []store.Row, tok store.Token, exp time.Time) {
	t.Helper()
	applied, err := s.Reserve(context.Background(), pool, ReserveOps(rows, tok, exp))
	require.NoError(t, err)
	require.Len(t, applied, len(rows), "all ops should apply")
}

// RowsByOutpoint fetches rows and indexes them by outpoint.
func RowsByOutpoint(t *testing.T, s store.Store, pool string, ops []store.Outpoint) map[store.Outpoint]store.Row {
	t.Helper()
	rows, err := s.GetRows(context.Background(), pool, ops)
	require.NoError(t, err)
	m := make(map[store.Outpoint]store.Row, len(rows))
	for _, r := range rows {
		m[r.Outpoint] = r
	}
	return m
}
