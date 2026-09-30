package pebble

import (
	"encoding/binary"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// us converts t to a microsecond Unix timestamp, or 0 for the zero time.
func us(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixMicro()) //nolint:gosec // dates after 1970
}

// fromUS is the inverse of us.
func fromUS(v uint64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.UnixMicro(int64(v)).UTC() //nolint:gosec // bounded
}

// unixEpoch mirrors store.ValidateTime's rejection boundary (that var is
// unexported there): see matchesNothingBefore in rows.go.
var unixEpoch = time.Unix(0, 0).UTC()

// appendPool appends a length-prefixed pool name to k. store.ValidatePool caps
// pool names at 64 bytes but places no restriction on which bytes a pool name
// may contain (the design sketch's "pool + 0x00" separator would be ambiguous
// whenever one pool name is a byte-prefix of another, or a pool name itself
// contains a 0x00 byte). A one-byte length prefix removes that ambiguity: the
// exact number of pool bytes that follow is stated up front instead of being
// inferred by scanning for a delimiter, so two different pool names can never
// produce colliding key prefixes.
func appendPool(k []byte, pool string) []byte {
	k = append(k, byte(len(pool))) //nolint:gosec // store.ValidatePool caps pool at 64 bytes
	return append(k, pool...)
}

func rowKey(op store.Outpoint) []byte {
	k := make([]byte, 0, 2+32+4)
	k = append(k, 'r', '/')
	k = append(k, op.TxID[:]...)
	return binary.BigEndian.AppendUint32(k, op.Vout)
}

func rowPrefix(txid chainhash.Hash) []byte {
	k := make([]byte, 0, 2+32)
	k = append(k, 'r', '/')
	return append(k, txid[:]...)
}

func shardKey(pool string, txid chainhash.Hash) []byte {
	k := []byte("s/")
	k = appendPool(k, pool)
	return append(k, txid[:]...)
}

func shardPrefix(pool string) []byte {
	k := []byte("s/")
	return appendPool(k, pool)
}

func adoptKey(pool string, status store.ShardStatus, created time.Time, txid chainhash.Hash) []byte {
	k := []byte("is/")
	k = appendPool(k, pool)
	k = append(k, byte(status))
	k = binary.BigEndian.AppendUint64(k, us(created))
	return append(k, txid[:]...)
}

func adoptPrefix(pool string, status store.ShardStatus) []byte {
	k := []byte("is/")
	k = appendPool(k, pool)
	return append(k, byte(status))
}

func expiryKey(pool string, at time.Time, op store.Outpoint) []byte {
	k := []byte("ie/")
	k = appendPool(k, pool)
	k = binary.BigEndian.AppendUint64(k, us(at))
	k = append(k, op.TxID[:]...)
	return binary.BigEndian.AppendUint32(k, op.Vout)
}

func expiryPrefix(pool string) []byte {
	k := []byte("ie/")
	return appendPool(k, pool)
}

func quarantineKey(pool string, at time.Time, op store.Outpoint) []byte {
	k := []byte("iq/")
	k = appendPool(k, pool)
	k = binary.BigEndian.AppendUint64(k, us(at))
	k = append(k, op.TxID[:]...)
	return binary.BigEndian.AppendUint32(k, op.Vout)
}

func quarantinePrefix(pool string) []byte {
	k := []byte("iq/")
	return appendPool(k, pool)
}

// upperBoundBefore returns the exclusive iterator bound for entries with
// timestamp < before: any real key sharing prefix continues past the 8-byte
// timestamp with more bytes (txid, vout), which makes it compare greater than
// this exact boundary, so a strict "< before" filter falls out of using this
// as an IterOptions.UpperBound (which is itself exclusive) with no separate
// row-by-row comparison needed.
func upperBoundBefore(prefix []byte, before time.Time) []byte {
	return binary.BigEndian.AppendUint64(append([]byte{}, prefix...), us(before))
}

// nextKey returns the smallest key strictly greater than k, for use as an
// exclusive-lower-bound IterOptions.LowerBound (LowerBound is otherwise
// inclusive). It is safe here because every key family that uses it has a
// fixed total length for a given pool, so no real key can equal k+0x00.
func nextKey(k []byte) []byte {
	return append(append([]byte{}, k...), 0x00)
}

func rowCounterKey(pool string, st store.State) []byte {
	k := []byte("c/")
	k = appendPool(k, pool)
	k = append(k, 'r')
	return append(k, byte(st))
}

func shardCounterKey(pool string, st store.ShardStatus) []byte {
	k := []byte("c/")
	k = appendPool(k, pool)
	k = append(k, 's')
	return append(k, byte(st))
}

// txidPoolKey maps a shard's txid to its owning pool, so PutShard can reject a
// txid reused under a different pool (a shard's txid is globally unique
// across pools) without scanning every pool's shard records.
func txidPoolKey(txid chainhash.Hash) []byte {
	k := []byte("x/")
	return append(k, txid[:]...)
}

func leaseKey(name string) []byte { return append([]byte("l/"), name...) }

// metaVersionKey stores the schema version as a big-endian uint32.
var metaVersionKey = []byte("meta/version")

// prefixEnd returns the smallest key greater than every key with the prefix,
// or nil if the prefix is all 0xff bytes (meaning "no upper bound").
func prefixEnd(prefix []byte) []byte {
	end := append([]byte{}, prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
