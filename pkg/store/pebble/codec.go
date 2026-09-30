package pebble

import (
	"encoding/json"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// rowDoc is the on-disk JSON encoding of a store.Row. Field names are
// abbreviated to keep the value small; this is a stable on-disk format, not a
// wire format, so it is never exposed outside this package.
//
// Pool is []byte, not string: encoding/json's string encoding replaces an
// invalid UTF-8 byte with the Unicode replacement character U+FFFD instead of
// failing, which would silently corrupt a pool name on write. ValidatePool
// already rejects invalid UTF-8 before a name ever reaches this codec (see
// store.go, "Names"), so this is defence in depth, not the primary guard: a
// []byte field is base64-encoded, which round-trips any byte sequence
// exactly regardless of content.
type rowDoc struct {
	TxID  []byte `json:"t"`
	Vout  uint32 `json:"v"`
	Pool  []byte `json:"p"`
	Sats  uint64 `json:"s"`
	Lock  []byte `json:"l"`
	Unl   []byte `json:"u"`
	State uint8  `json:"st"`
	RB    []byte `json:"rb,omitempty"`
	RA    uint64 `json:"ra,omitempty"`
	EA    uint64 `json:"ea,omitempty"`
	QA    uint64 `json:"qa,omitempty"`
	Hint  []byte `json:"h,omitempty"`
	SB    []byte `json:"sb,omitempty"`
	UA    uint64 `json:"ua"`
}

// encodeRow marshals a row to its on-disk JSON form.
func encodeRow(r *store.Row) []byte {
	d := rowDoc{
		TxID:  r.TxID[:],
		Vout:  r.Vout,
		Pool:  []byte(r.Pool),
		Sats:  r.Satoshis,
		Lock:  r.LockingScript,
		Unl:   r.UnlockingScript,
		State: uint8(r.State), //nolint:gosec // small enum
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
	b, err := json.Marshal(&d)
	if err != nil {
		panic("pebble: encode row: " + err.Error()) // marshalling plain data cannot fail
	}
	return b
}

// decodeRow unmarshals a row from its on-disk JSON form. Every error path
// wraps store.ErrUnavailable: a decode failure here means the on-disk record
// is corrupt (or from an incompatible future format), which is an
// infrastructure problem the caller may retry or escalate, not a malformed
// argument it passed in.
func decodeRow(b []byte) (store.Row, error) {
	var d rowDoc
	if err := json.Unmarshal(b, &d); err != nil {
		return store.Row{}, fmt.Errorf("%w: pebble: corrupt row record: %w", store.ErrUnavailable, err)
	}
	txid, err := chainhash.NewHash(d.TxID)
	if err != nil {
		return store.Row{}, fmt.Errorf("%w: pebble: corrupt row record: bad txid: %w", store.ErrUnavailable, err)
	}
	r := store.Row{
		Outpoint:        store.Outpoint{TxID: *txid, Vout: d.Vout},
		Pool:            string(d.Pool),
		Satoshis:        d.Sats,
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
			return store.Row{}, fmt.Errorf("%w: pebble: corrupt row record: bad spend hint: %w", store.ErrUnavailable, herr)
		}
		r.SpentHint = h
	}
	if len(d.SB) == chainhash.HashSize {
		h, herr := chainhash.NewHash(d.SB)
		if herr != nil {
			return store.Row{}, fmt.Errorf("%w: pebble: corrupt row record: bad spent-by: %w", store.ErrUnavailable, herr)
		}
		r.SpentBy = h
	}
	return r, nil
}

// shardDoc is the on-disk JSON encoding of a store.Shard. Pool and Owner are
// []byte for the same reason as rowDoc.Pool (see its doc comment).
type shardDoc struct {
	Pool    []byte `json:"p"`
	TxID    []byte `json:"t"`
	Size    int    `json:"n"`
	Denom   uint64 `json:"d"`
	Mode    uint8  `json:"m"`
	Status  uint8  `json:"s"`
	Owner   []byte `json:"o,omitempty"`
	LU      uint64 `json:"lu,omitempty"`
	Version int64  `json:"v"`
	BEEF    []byte `json:"b"`
	CA      uint64 `json:"ca"`
	UA      uint64 `json:"ua"`
}

// encodeShard marshals a shard to its on-disk JSON form.
func encodeShard(sh *store.Shard) []byte {
	d := shardDoc{
		Pool:    []byte(sh.Pool),
		TxID:    sh.TxID[:],
		Size:    sh.Size,
		Denom:   sh.Denomination,
		Mode:    uint8(sh.LockMode), //nolint:gosec // small enum
		Status:  uint8(sh.Status),   //nolint:gosec // small enum
		Owner:   []byte(sh.Owner),
		LU:      us(sh.LeaseUntil),
		Version: sh.Version,
		BEEF:    sh.BEEF,
		CA:      us(sh.CreatedAt),
		UA:      us(sh.UpdatedAt),
	}
	b, err := json.Marshal(&d)
	if err != nil {
		panic("pebble: encode shard: " + err.Error()) // marshalling plain data cannot fail
	}
	return b
}

// decodeShard unmarshals a shard from its on-disk JSON form. Every error path
// wraps store.ErrUnavailable; see decodeRow's doc comment for why.
func decodeShard(b []byte) (store.Shard, error) {
	var d shardDoc
	if err := json.Unmarshal(b, &d); err != nil {
		return store.Shard{}, fmt.Errorf("%w: pebble: corrupt shard record: %w", store.ErrUnavailable, err)
	}
	txid, err := chainhash.NewHash(d.TxID)
	if err != nil {
		return store.Shard{}, fmt.Errorf("%w: pebble: corrupt shard record: bad txid: %w", store.ErrUnavailable, err)
	}
	return store.Shard{
		Pool:         string(d.Pool),
		TxID:         *txid,
		Size:         d.Size,
		Denomination: d.Denom,
		LockMode:     store.LockMode(d.Mode),      //nolint:gosec // small enum
		Status:       store.ShardStatus(d.Status), //nolint:gosec // small enum
		Owner:        string(d.Owner),
		LeaseUntil:   fromUS(d.LU),
		Version:      d.Version,
		BEEF:         d.BEEF,
		CreatedAt:    fromUS(d.CA),
		UpdatedAt:    fromUS(d.UA),
	}, nil
}

// leaseDoc is the on-disk JSON encoding of a job lease. Holder is []byte for
// the same reason as rowDoc.Pool (see its doc comment).
type leaseDoc struct {
	Holder  []byte `json:"h"`
	Expires uint64 `json:"e"`
}

// encodeLease marshals a lease to its on-disk JSON form.
func encodeLease(d *leaseDoc) []byte {
	b, err := json.Marshal(d)
	if err != nil {
		panic("pebble: encode lease: " + err.Error()) // marshalling plain data cannot fail
	}
	return b
}

// decodeLease unmarshals a lease from its on-disk JSON form. Its error path
// wraps store.ErrUnavailable; see decodeRow's doc comment for why.
func decodeLease(b []byte) (leaseDoc, error) {
	var d leaseDoc
	if err := json.Unmarshal(b, &d); err != nil {
		return leaseDoc{}, fmt.Errorf("%w: pebble: corrupt lease record: %w", store.ErrUnavailable, err)
	}
	return d, nil
}
