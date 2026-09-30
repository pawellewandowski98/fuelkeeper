package mongodb

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// rowDoc is the BSON encoding of a store.Row in the fuel_rows collection.
// Field names are abbreviated to keep documents small; this is a stable
// on-disk format, not a wire format, so it is never exposed outside this
// package.
type rowDoc struct {
	ID    []byte `bson:"_id"`
	Pool  string `bson:"p"`
	Shard []byte `bson:"sh"`
	Vout  int64  `bson:"v"`
	Sats  int64  `bson:"sat"`
	Lock  []byte `bson:"ls"`
	Unl   []byte `bson:"us"`
	State int32  `bson:"st"`
	RB    []byte `bson:"rb,omitempty"`
	RA    int64  `bson:"ra,omitempty"`
	EA    int64  `bson:"ea,omitempty"`
	QA    int64  `bson:"qa,omitempty"`
	Hint  []byte `bson:"h,omitempty"`
	SB    []byte `bson:"sb,omitempty"`
	UA    int64  `bson:"ua"`
}

// shardDoc is the BSON encoding of a store.Shard in the fuel_shards collection.
type shardDoc struct {
	ID      string `bson:"_id"`
	Pool    string `bson:"p"`
	TxID    []byte `bson:"t"`
	Size    int32  `bson:"n"`
	Denom   int64  `bson:"d"`
	Mode    int32  `bson:"m"`
	Status  int32  `bson:"s"`
	Owner   string `bson:"o"`
	LU      int64  `bson:"lu,omitempty"`
	Version int64  `bson:"ver"`
	BEEF    []byte `bson:"b"`
	CA      int64  `bson:"ca"`
	UA      int64  `bson:"ua"`
}

// leaseDoc is the BSON encoding of a job lease in the fuel_leases collection.
type leaseDoc struct {
	ID      string `bson:"_id"`
	Holder  string `bson:"h"`
	Expires int64  `bson:"e"`
}

// metaDoc is the BSON encoding of the single schema-version record in the
// fuel_meta collection.
type metaDoc struct {
	ID      string `bson:"_id"`
	Version int64  `bson:"v"`
}

// us converts t to a microsecond Unix timestamp, or 0 for the zero time.
func us(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMicro()
}

// fromUS is the inverse of us.
func fromUS(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.UnixMicro(v).UTC()
}

// rowID returns the fuel_rows _id for op: its txid's 32 internal bytes
// followed by its big-endian uint32 vout. Rows of one txid therefore share a
// 32-byte prefix and sort by vout within it, which is what lets ListAvailable
// page a shard's rows by _id alone.
func rowID(op store.Outpoint) []byte {
	id := make([]byte, 0, 36)
	id = append(id, op.TxID[:]...)
	return binary.BigEndian.AppendUint32(id, op.Vout)
}

// shardID returns the fuel_shards _id for (pool, txid): shards are unique per
// pool by construction (fuel_shard_txids enforces global txid uniqueness
// across pools separately), so a simple "pool|txidhex" string is enough to
// key this collection directly by primary key.
func shardID(pool string, txid chainhash.Hash) string {
	return pool + "|" + hex.EncodeToString(txid[:])
}

// newRowDoc builds the document PutShard inserts for a fresh row (always
// state available, no provenance).
func newRowDoc(r *store.Row) rowDoc {
	d := rowDoc{
		ID:    rowID(r.Outpoint),
		Pool:  r.Pool,
		Shard: append([]byte(nil), r.TxID[:]...),
		Vout:  int64(r.Vout),
		Sats:  int64(r.Satoshis), //nolint:gosec // satoshi amounts fit in int64
		Lock:  r.LockingScript,
		Unl:   r.UnlockingScript,
		State: int32(r.State),
		RA:    us(r.ReservedAt),
		EA:    us(r.ExpiresAt),
		QA:    us(r.QuarantinedAt),
		UA:    us(r.UpdatedAt),
	}
	if !r.ReservedBy.IsZero() {
		tok := r.ReservedBy
		d.RB = tok[:]
	}
	if r.SpentHint != nil {
		d.Hint = r.SpentHint[:]
	}
	if r.SpentBy != nil {
		d.SB = r.SpentBy[:]
	}
	return d
}

// toRow decodes a fuel_rows document into a store.Row.
func toRow(d rowDoc) (store.Row, error) {
	txid, err := chainhash.NewHash(d.Shard)
	if err != nil {
		return store.Row{}, fmt.Errorf("mongodb: decode row shard txid: %w", err)
	}
	r := store.Row{
		Outpoint:        store.Outpoint{TxID: *txid, Vout: uint32(d.Vout)}, //nolint:gosec // vout is bounded by tx output count
		Pool:            d.Pool,
		Satoshis:        uint64(d.Sats), //nolint:gosec // stored from uint64
		LockingScript:   d.Lock,
		UnlockingScript: d.Unl,
		State:           store.State(d.State), //nolint:gosec // small enum
		ReservedAt:      fromUS(d.RA),
		ExpiresAt:       fromUS(d.EA),
		QuarantinedAt:   fromUS(d.QA),
		UpdatedAt:       fromUS(d.UA),
	}
	if len(d.RB) == len(store.Token{}) {
		copy(r.ReservedBy[:], d.RB)
	}
	if len(d.Hint) == chainhash.HashSize {
		h, herr := chainhash.NewHash(d.Hint)
		if herr != nil {
			return store.Row{}, fmt.Errorf("mongodb: decode row hint: %w", herr)
		}
		r.SpentHint = h
	}
	if len(d.SB) == chainhash.HashSize {
		h, herr := chainhash.NewHash(d.SB)
		if herr != nil {
			return store.Row{}, fmt.Errorf("mongodb: decode row spent-by: %w", herr)
		}
		r.SpentBy = h
	}
	return r, nil
}

// newShardDoc builds the fuel_shards document for sh.
func newShardDoc(sh *store.Shard) shardDoc {
	return shardDoc{
		ID:      shardID(sh.Pool, sh.TxID),
		Pool:    sh.Pool,
		TxID:    append([]byte(nil), sh.TxID[:]...),
		Size:    int32(sh.Size),         //nolint:gosec // ValidatePutShard: 0 < size == len(rows) <= maxBatch
		Denom:   int64(sh.Denomination), //nolint:gosec // denominations fit in int64
		Mode:    int32(sh.LockMode),     //nolint:gosec // small enum
		Status:  int32(sh.Status),       //nolint:gosec // small enum
		Owner:   sh.Owner,
		LU:      us(sh.LeaseUntil),
		Version: sh.Version,
		BEEF:    sh.BEEF,
		CA:      us(sh.CreatedAt),
		UA:      us(sh.UpdatedAt),
	}
}

// toShard decodes a fuel_shards document into a store.Shard. BEEF is only
// populated when withBEEF is true (ListShards omits it).
func toShard(d shardDoc, withBEEF bool) (store.Shard, error) {
	txid, err := chainhash.NewHash(d.TxID)
	if err != nil {
		return store.Shard{}, fmt.Errorf("mongodb: decode shard txid: %w", err)
	}
	sh := store.Shard{
		Pool:         d.Pool,
		TxID:         *txid,
		Size:         int(d.Size),
		Denomination: uint64(d.Denom),             //nolint:gosec // stored from uint64
		LockMode:     store.LockMode(d.Mode),      //nolint:gosec // small enum
		Status:       store.ShardStatus(d.Status), //nolint:gosec // small enum
		Owner:        d.Owner,
		LeaseUntil:   fromUS(d.LU),
		Version:      d.Version,
		CreatedAt:    fromUS(d.CA),
		UpdatedAt:    fromUS(d.UA),
	}
	if withBEEF {
		sh.BEEF = d.BEEF
	}
	return sh, nil
}
