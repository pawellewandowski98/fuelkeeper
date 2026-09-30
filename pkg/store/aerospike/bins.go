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
// limit. ea and qa are provenance only (store.go "Provenance" keeps them
// across every transition that is not a return to available) and are NOT
// indexed: a terminal row would otherwise stay in the expiry/quarantine
// index forever and every Quarantine/ListQuarantined call would have to
// examine it again. The indexed bins are index-only copies whose presence
// exactly tracks state, like ak: rx (== ea) exists only while the row is
// reserved and qx (== qa) only while it is quarantined; every operate that
// changes state writes or removes them in the same atomic operation.
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
	// for ListAvailable.
	binAK = "ak"
	// binRX is present iff state == reserved, holding ExpiresAt (NUMERIC
	// index; Quarantine's discovery path).
	binRX = "rx"
	// binQX is present iff state == quarantined, holding QuarantinedAt
	// (NUMERIC index; ListQuarantined's discovery path).
	binQX = "qx"
	// binPST ("pool|state") is always present and always current.
	binPST = "pst"
)

// binTN is the tenant bin every record carries when Config.KeyPrefix is
// non-empty (absent otherwise); see Truncate and tenantExpr.
const binTN = "tn"

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
	// binVO is the shard's vouts as packed big-endian uint32s, written once
	// by PutShard; RetireShard addresses the shard's rows by key from it.
	binVO = "vo"
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

// rowKeyBytes returns the rows-set user key for op: the store's KeyPrefix
// bytes, then its txid's 32 internal bytes, then its big-endian uint32 vout.
func (s *Store) rowKeyBytes(op store.Outpoint) []byte {
	b := make([]byte, 0, len(s.keyPrefix)+36)
	b = append(b, s.keyPrefix...)
	b = append(b, op.TxID[:]...)
	return binary.BigEndian.AppendUint32(b, op.Vout)
}

func (s *Store) rowKey(op store.Outpoint) (*as.Key, error) {
	k, err := as.NewKey(s.ns, s.rowsSet, s.rowKeyBytes(op))
	if err != nil {
		return nil, fmt.Errorf("aerospike: row key: %w", err)
	}
	return k, nil
}

// shardID is the shards-set user key and the ak index-bin value for
// (pool, txid): "[KeyPrefix|]pool|txidhex".
func (s *Store) shardID(pool string, txid chainhash.Hash) string {
	return s.tenant + joinPoolTxID(pool, hex.EncodeToString(txid[:]))
}

func (s *Store) shardKey(pool string, txid chainhash.Hash) (*as.Key, error) {
	k, err := as.NewKey(s.ns, s.shardsSet, s.shardID(pool, txid))
	if err != nil {
		return nil, fmt.Errorf("aerospike: shard key: %w", err)
	}
	return k, nil
}

func (s *Store) txidKey(txid chainhash.Hash) (*as.Key, error) {
	k, err := as.NewKey(s.ns, s.txidsSet, s.tenant+hex.EncodeToString(txid[:]))
	if err != nil {
		return nil, fmt.Errorf("aerospike: txid key: %w", err)
	}
	return k, nil
}

func (s *Store) leaseKey(name string) (*as.Key, error) {
	k, err := as.NewKey(s.ns, s.leasesSet, s.tenant+name)
	if err != nil {
		return nil, fmt.Errorf("aerospike: lease key: %w", err)
	}
	return k, nil
}

func (s *Store) metaKey() (*as.Key, error) {
	k, err := as.NewKey(s.ns, s.metaSet, s.tenant+"schema")
	if err != nil {
		return nil, fmt.Errorf("aerospike: meta key: %w", err)
	}
	return k, nil
}

// akFor is the rows.ak index-bin value for every available row of (pool, txid).
func (s *Store) akFor(pool string, txid chainhash.Hash) string { return s.shardID(pool, txid) }

// pstFor is the rows.pst index-bin value for (pool, state).
func (s *Store) pstFor(pool string, st store.State) string {
	return s.tenant + pool + "|" + strconv.Itoa(int(st))
}

// psFor is the shards.ps index-bin value for (pool, status).
func (s *Store) psFor(pool string, status store.ShardStatus) string {
	return s.tenant + pool + "|" + strconv.Itoa(int(status))
}

// withTenant adds the tn bin to bins when this store has a KeyPrefix.
func (s *Store) withTenant(bins as.BinMap) as.BinMap {
	if s.keyPrefix != "" {
		bins[binTN] = s.keyPrefix
	}
	return bins
}

// tenantExpr matches exactly this store's records: tn == KeyPrefix, or no
// tn bin at all when KeyPrefix is empty. Range queries (on rx/qx, whose
// NUMERIC values cannot carry a prefix) add it to their filter expression.
func (s *Store) tenantExpr() *as.Expression {
	if s.keyPrefix == "" {
		return as.ExpNot(as.ExpBinExists(binTN))
	}
	return as.ExpEq(as.ExpStringBin(binTN), as.ExpStringVal(s.keyPrefix))
}

// packVouts encodes vouts as packed big-endian uint32s (the shard vo bin).
func packVouts(rows []store.Row) []byte {
	b := make([]byte, 0, 4*len(rows))
	for i := range rows {
		b = binary.BigEndian.AppendUint32(b, rows[i].Vout)
	}
	return b
}

// shardOutpoints decodes a shard record's vo bin into its rows' outpoints.
// A record without vo (none exist under schema version 2) falls back to
// vouts 0..size-1, the layout every minted shard uses.
func shardOutpoints(txid chainhash.Hash, vo []byte, size int) []store.Outpoint {
	if len(vo) == 0 {
		out := make([]store.Outpoint, size)
		for i := range out {
			out[i] = store.Outpoint{TxID: txid, Vout: uint32(i)} //nolint:gosec // size <= maxBatch
		}
		return out
	}
	out := make([]store.Outpoint, 0, len(vo)/4)
	for i := 0; i+4 <= len(vo); i += 4 {
		out = append(out, store.Outpoint{TxID: txid, Vout: binary.BigEndian.Uint32(vo[i:])})
	}
	return out
}

// rowBins builds the bin map PutShard inserts for a fresh, available row.
func (s *Store) rowBins(r store.Row) as.BinMap {
	return s.withTenant(as.BinMap{
		binPool:  r.Pool,
		binTxID:  append([]byte(nil), r.TxID[:]...),
		binVout:  int64(r.Vout),
		binSat:   int64(r.Satoshis), //nolint:gosec // satoshi amounts fit in int64
		binLock:  r.LockingScript,
		binUnl:   r.UnlockingScript,
		binState: int64(store.StateAvailable),
		binUA:    us(r.UpdatedAt),
		binAK:    s.akFor(r.Pool, r.TxID),
		binPST:   s.pstFor(r.Pool, store.StateAvailable),
	})
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
func (s *Store) shardBins(sh *store.Shard, rows []store.Row) as.BinMap {
	return s.withTenant(as.BinMap{
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
		binPS:     s.psFor(sh.Pool, sh.Status),
		binVO:     packVouts(rows),
	})
}

// shardFromRecord decodes a shards-set record into a store.Shard. BEEF is
// only populated when withBEEF is true (ListShards omits it; its query does
// not even fetch the bin).
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
