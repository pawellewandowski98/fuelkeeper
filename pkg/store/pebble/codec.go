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
type rowDoc struct {
	TxID  []byte `json:"t"`
	Vout  uint32 `json:"v"`
	Pool  string `json:"p"`
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
		Pool:  r.Pool,
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

// decodeRow unmarshals a row from its on-disk JSON form.
func decodeRow(b []byte) (store.Row, error) {
	var d rowDoc
	if err := json.Unmarshal(b, &d); err != nil {
		return store.Row{}, fmt.Errorf("pebble: decode row: %w", err)
	}
	txid, err := chainhash.NewHash(d.TxID)
	if err != nil {
		return store.Row{}, fmt.Errorf("pebble: decode row txid: %w", err)
	}
	r := store.Row{
		Outpoint:        store.Outpoint{TxID: *txid, Vout: d.Vout},
		Pool:            d.Pool,
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
			return store.Row{}, fmt.Errorf("pebble: decode row hint: %w", herr)
		}
		r.SpentHint = h
	}
	if len(d.SB) == chainhash.HashSize {
		h, herr := chainhash.NewHash(d.SB)
		if herr != nil {
			return store.Row{}, fmt.Errorf("pebble: decode row spent-by: %w", herr)
		}
		r.SpentBy = h
	}
	return r, nil
}

// shardDoc is the on-disk JSON encoding of a store.Shard.
type shardDoc struct {
	Pool    string `json:"p"`
	TxID    []byte `json:"t"`
	Size    int    `json:"n"`
	Denom   uint64 `json:"d"`
	Mode    uint8  `json:"m"`
	Status  uint8  `json:"s"`
	Owner   string `json:"o,omitempty"`
	LU      uint64 `json:"lu,omitempty"`
	Version int64  `json:"v"`
	BEEF    []byte `json:"b"`
	CA      uint64 `json:"ca"`
	UA      uint64 `json:"ua"`
}

// encodeShard marshals a shard to its on-disk JSON form.
func encodeShard(sh *store.Shard) []byte {
	d := shardDoc{
		Pool:    sh.Pool,
		TxID:    sh.TxID[:],
		Size:    sh.Size,
		Denom:   sh.Denomination,
		Mode:    uint8(sh.LockMode), //nolint:gosec // small enum
		Status:  uint8(sh.Status),   //nolint:gosec // small enum
		Owner:   sh.Owner,
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

// decodeShard unmarshals a shard from its on-disk JSON form.
func decodeShard(b []byte) (store.Shard, error) {
	var d shardDoc
	if err := json.Unmarshal(b, &d); err != nil {
		return store.Shard{}, fmt.Errorf("pebble: decode shard: %w", err)
	}
	txid, err := chainhash.NewHash(d.TxID)
	if err != nil {
		return store.Shard{}, fmt.Errorf("pebble: decode shard txid: %w", err)
	}
	return store.Shard{
		Pool:         d.Pool,
		TxID:         *txid,
		Size:         d.Size,
		Denomination: d.Denom,
		LockMode:     store.LockMode(d.Mode),      //nolint:gosec // small enum
		Status:       store.ShardStatus(d.Status), //nolint:gosec // small enum
		Owner:        d.Owner,
		LeaseUntil:   fromUS(d.LU),
		Version:      d.Version,
		BEEF:         d.BEEF,
		CreatedAt:    fromUS(d.CA),
		UpdatedAt:    fromUS(d.UA),
	}, nil
}

// leaseDoc is the on-disk JSON encoding of a job lease.
type leaseDoc struct {
	Holder  string `json:"h"`
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

// decodeLease unmarshals a lease from its on-disk JSON form.
func decodeLease(b []byte) (leaseDoc, error) {
	var d leaseDoc
	if err := json.Unmarshal(b, &d); err != nil {
		return leaseDoc{}, fmt.Errorf("pebble: decode lease: %w", err)
	}
	return d, nil
}
