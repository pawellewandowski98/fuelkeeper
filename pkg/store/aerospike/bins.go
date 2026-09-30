package aerospike

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// Bin names (rows set). Every name is <= 15 bytes, the server's bin-name
// limit. ea and qa double as both the provenance value (kept across every
// transition that is not a return to available, per store.go "Provenance")
// and the source bin for the ea/qa secondary indexes; unlike ak/pst they are
// never removed on a state transition, since removing them would destroy
// provenance data the suite pins. Every query that ranges over ea or qa also
// carries a FilterExpression re-checking state, so a value left over from an
// earlier reservation never produces a wrong result, only a larger index.
const (
	binPool  = "pool"
	binTxID  = "txid"
	binVout  = "vout"
	binSat   = "sat"
	binLock  = "ls"
	binUnl   = "us"
	binState = "st"
	binRB    = "rb"
	binRA    = "ra"
	binEA    = "ea"
	binQA    = "qa"
	binHint  = "hint"
	binSB    = "sb"
	binUA    = "ua"
	// binAK is present iff state == available: it is the sole discovery path
	// for ListAvailable, so unlike ea/qa its presence must exactly track
	// state (see rows.go).
	binAK = "ak"
	// binPST ("pool|state") is always present and always current.
	binPST = "pst"
)

// Bin names (shards set).
const (
	binSize   = "size"
	binDenom  = "denom"
	binMode   = "mode"
	binStatus = "status"
	binOwner  = "owner"
	binLU     = "lu"
	binVer    = "ver"
	binBEEF   = "beef"
	binCA     = "ca"
	// binPS ("pool|status") is always present and always current.
	binPS = "ps"
)

// Bin names (leases and meta sets).
const (
	binHolder = "holder"
	binExp    = "exp"
	binSchema = "v"
)

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

// unwrapOpResult undoes the client's OpResults wrapping: when one Operate/
// BatchWrite call names the same bin more than once (as Reserve's per-bin
// ExpWriteOp followed by a GetBinOp of the same name does, to read back the
// value it just conditionally wrote), the result for that bin comes back as
// as.OpResults, one slot per op that touched it in order, rather than a bare
// value. The read op is always the last one touching the bin in this
// package's op lists, so the last slot is the value callers want.
func unwrapOpResult(v any) any {
	if results, ok := v.(as.OpResults); ok {
		if len(results) == 0 {
			return nil
		}
		return results[len(results)-1]
	}
	return v
}

// toInt64 normalizes a decoded integer bin value: the Aerospike Go client
// decodes integer bins as Go int on 64-bit platforms and int64 otherwise.
func toInt64(v any) int64 {
	switch n := unwrapOpResult(v).(type) {
	case int64:
		return n
	case int:
		return int64(n)
	default:
		return 0
	}
}

// toBytes normalizes a decoded blob bin value.
func toBytes(v any) []byte {
	b, _ := unwrapOpResult(v).([]byte)
	return b
}

// rowKeyBytes returns the rows-set user key for op: its txid's 32 internal
// bytes followed by its big-endian uint32 vout.
func rowKeyBytes(op store.Outpoint) []byte {
	b := make([]byte, 0, 36)
	b = append(b, op.TxID[:]...)
	return binary.BigEndian.AppendUint32(b, op.Vout)
}

func (s *Store) rowKey(op store.Outpoint) (*as.Key, error) {
	k, err := as.NewKey(s.ns, s.rowsSet, rowKeyBytes(op))
	if err != nil {
		return nil, fmt.Errorf("aerospike: row key: %w", err)
	}
	return k, nil
}

// shardIDStr is the shards-set user key and the ak/ps index-bin value prefix
// for (pool, txid): "pool|txidhex".
func shardIDStr(pool string, txid chainhash.Hash) string {
	return joinPoolTxID(pool, hex.EncodeToString(txid[:]))
}

func (s *Store) shardKey(pool string, txid chainhash.Hash) (*as.Key, error) {
	k, err := as.NewKey(s.ns, s.shardsSet, shardIDStr(pool, txid))
	if err != nil {
		return nil, fmt.Errorf("aerospike: shard key: %w", err)
	}
	return k, nil
}

func (s *Store) txidKey(txid chainhash.Hash) (*as.Key, error) {
	k, err := as.NewKey(s.ns, s.txidsSet, hex.EncodeToString(txid[:]))
	if err != nil {
		return nil, fmt.Errorf("aerospike: txid key: %w", err)
	}
	return k, nil
}

func (s *Store) leaseKey(name string) (*as.Key, error) {
	k, err := as.NewKey(s.ns, s.leasesSet, name)
	if err != nil {
		return nil, fmt.Errorf("aerospike: lease key: %w", err)
	}
	return k, nil
}

func (s *Store) metaKey() (*as.Key, error) {
	k, err := as.NewKey(s.ns, s.metaSet, "schema")
	if err != nil {
		return nil, fmt.Errorf("aerospike: meta key: %w", err)
	}
	return k, nil
}

// akFor is the rows.ak index-bin value for every available row of (pool, txid).
func akFor(pool string, txid chainhash.Hash) string { return shardIDStr(pool, txid) }

// pstFor is the rows.pst index-bin value for (pool, state).
func pstFor(pool string, st store.State) string { return pool + "|" + strconv.Itoa(int(st)) }

// psFor is the shards.ps index-bin value for (pool, status).
func psFor(pool string, status store.ShardStatus) string {
	return pool + "|" + strconv.Itoa(int(status))
}

// rowBins builds the bin map PutShard inserts for a fresh, available row.
func rowBins(r store.Row) as.BinMap {
	return as.BinMap{
		binPool:  r.Pool,
		binTxID:  append([]byte(nil), r.TxID[:]...),
		binVout:  int64(r.Vout),
		binSat:   int64(r.Satoshis), //nolint:gosec // satoshi amounts fit in int64
		binLock:  r.LockingScript,
		binUnl:   r.UnlockingScript,
		binState: int64(store.StateAvailable),
		binUA:    us(r.UpdatedAt),
		binAK:    akFor(r.Pool, r.TxID),
		binPST:   pstFor(r.Pool, store.StateAvailable),
	}
}

// outpointFromBins decodes the outpoint carried by every row record's own
// txid/vout bins (independent of the record's key, so callers reading by
// query/index never need to also parse the digest-only key).
func outpointFromBins(bins as.BinMap) (store.Outpoint, error) {
	txid, err := chainhash.NewHash(toBytes(bins[binTxID]))
	if err != nil {
		return store.Outpoint{}, fmt.Errorf("aerospike: decode row txid: %w", err)
	}
	vout := toInt64(bins[binVout])
	return store.Outpoint{TxID: *txid, Vout: uint32(vout)}, nil //nolint:gosec // vout is bounded by tx output count
}

// rowFromRecord decodes a rows-set record into a store.Row.
func rowFromRecord(rec *as.Record) (store.Row, error) {
	op, err := outpointFromBins(rec.Bins)
	if err != nil {
		return store.Row{}, err
	}
	r := store.Row{
		Outpoint:        op,
		Pool:            stringBin(rec.Bins, binPool),
		Satoshis:        uint64(toInt64(rec.Bins[binSat])), //nolint:gosec // stored from uint64
		LockingScript:   toBytes(rec.Bins[binLock]),
		UnlockingScript: toBytes(rec.Bins[binUnl]),
		State:           store.State(toInt64(rec.Bins[binState])), //nolint:gosec // small enum
		UpdatedAt:       fromUS(toInt64(rec.Bins[binUA])),
	}
	if rb := toBytes(rec.Bins[binRB]); len(rb) == len(store.Token{}) {
		copy(r.ReservedBy[:], rb)
	}
	if v, ok := rec.Bins[binRA]; ok {
		r.ReservedAt = fromUS(toInt64(v))
	}
	if v, ok := rec.Bins[binEA]; ok {
		r.ExpiresAt = fromUS(toInt64(v))
	}
	if v, ok := rec.Bins[binQA]; ok {
		r.QuarantinedAt = fromUS(toInt64(v))
	}
	if hint := toBytes(rec.Bins[binHint]); len(hint) == chainhash.HashSize {
		h, herr := chainhash.NewHash(hint)
		if herr != nil {
			return store.Row{}, fmt.Errorf("aerospike: decode row hint: %w", herr)
		}
		r.SpentHint = h
	}
	if sb := toBytes(rec.Bins[binSB]); len(sb) == chainhash.HashSize {
		h, herr := chainhash.NewHash(sb)
		if herr != nil {
			return store.Row{}, fmt.Errorf("aerospike: decode row spent-by: %w", herr)
		}
		r.SpentBy = h
	}
	return r, nil
}

func stringBin(bins as.BinMap, name string) string {
	v, _ := unwrapOpResult(bins[name]).(string)
	return v
}

// shardBins builds the bin map PutShard inserts for a fresh shard record.
func shardBins(sh *store.Shard) as.BinMap {
	return as.BinMap{
		binPool:   sh.Pool,
		binTxID:   append([]byte(nil), sh.TxID[:]...),
		binSize:   int64(sh.Size),         //nolint:gosec // ValidatePutShard: 0 < size == len(rows) <= maxBatch
		binDenom:  int64(sh.Denomination), //nolint:gosec // denominations fit in int64
		binMode:   int64(sh.LockMode),     //nolint:gosec // small enum
		binStatus: int64(sh.Status),       //nolint:gosec // small enum
		binOwner:  "",
		binLU:     int64(0),
		binVer:    int64(1),
		binBEEF:   sh.BEEF,
		binCA:     us(sh.CreatedAt),
		binUA:     us(sh.UpdatedAt),
		binPS:     psFor(sh.Pool, sh.Status),
	}
}

// shardFromRecord decodes a shards-set record into a store.Shard. BEEF is
// only populated when withBEEF is true (ListShards omits it).
func shardFromRecord(rec *as.Record, withBEEF bool) (store.Shard, error) {
	txid, err := chainhash.NewHash(toBytes(rec.Bins[binTxID]))
	if err != nil {
		return store.Shard{}, fmt.Errorf("aerospike: decode shard txid: %w", err)
	}
	sh := store.Shard{
		Pool:         stringBin(rec.Bins, binPool),
		TxID:         *txid,
		Size:         int(toInt64(rec.Bins[binSize])),
		Denomination: uint64(toInt64(rec.Bins[binDenom])),             //nolint:gosec // stored from uint64
		LockMode:     store.LockMode(toInt64(rec.Bins[binMode])),      //nolint:gosec // small enum
		Status:       store.ShardStatus(toInt64(rec.Bins[binStatus])), //nolint:gosec // small enum
		Owner:        stringBin(rec.Bins, binOwner),
		LeaseUntil:   fromUS(toInt64(rec.Bins[binLU])),
		Version:      toInt64(rec.Bins[binVer]),
		CreatedAt:    fromUS(toInt64(rec.Bins[binCA])),
		UpdatedAt:    fromUS(toInt64(rec.Bins[binUA])),
	}
	if withBEEF {
		sh.BEEF = toBytes(rec.Bins[binBEEF])
	}
	return sh, nil
}
