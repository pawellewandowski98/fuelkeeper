package api

import (
	"encoding/hex"
	"fmt"
	"net/url"
	"regexp"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// beefURL is the pool-scoped BEEF URL for txid. poolName is escaped: pool
// names are only length-bounded (see store.ValidatePool), not charset
// restricted, so this must not assume it is already URL-safe.
func beefURL(poolName string, txid chainhash.Hash) string {
	return "/v1/pools/" + url.PathEscape(poolName) + "/beef/" + txid.String()
}

// toReserveResponse converts a pool.Reservation to its wire form.
func toReserveResponse(poolName string, r *pool.Reservation) wire.ReserveResponse {
	out := wire.ReserveResponse{
		Token:      r.Token.String(),
		ExpiresAt:  r.ExpiresAt.UTC(),
		TTLSeconds: int(r.TTL / time.Second),
		LockMode:   r.LockMode.String(),
		Outputs:    make([]wire.Output, 0, len(r.Outputs)),
		Shards:     make([]wire.Shard, 0, len(r.Shards)),
	}
	for _, o := range r.Outputs {
		out.Outputs = append(out.Outputs, wire.Output{
			TxID:          o.TxID.String(),
			Vout:          o.Vout,
			Outpoint:      o.String(),
			Satoshis:      o.Satoshis,
			LockingScript: hex.EncodeToString(o.LockingScript),
			Unlock: wire.Unlock{
				Mode:   r.LockMode.String(),
				Script: hex.EncodeToString(o.UnlockingScript),
				Length: len(o.UnlockingScript),
			},
		})
	}
	for _, s := range r.Shards {
		out.Shards = append(out.Shards, wire.Shard{
			TxID:    s.TxID.String(),
			Version: s.Version,
			BEEFURL: beefURL(poolName, s.TxID),
		})
	}
	if r.LockMode == store.LockP2PKH {
		out.Constraints = &wire.Constraints{TxVersion: 1, LockTime: 0, Sequence: 0xffffffff}
	}
	return out
}

// toShardCounts converts store shard counts to their wire form.
func toShardCounts(s store.Stats) wire.ShardCounts {
	return wire.ShardCounts{
		Unmined: s.ShardsUnmined,
		Mined:   s.ShardsMined,
		Retired: s.ShardsRetired,
		Invalid: s.ShardsInvalid,
	}
}

// toStats converts store.Stats to its wire form.
func toStats(s store.Stats) wire.Stats {
	return wire.Stats{
		Available:   s.Available,
		Reserved:    s.Reserved,
		Quarantined: s.Quarantined,
		Spent:       s.Spent,
		Invalid:     s.Invalid,
		Shards:      toShardCounts(s),
	}
}

// toPool converts a pool config and its store stats to the wire Pool view.
func toPool(c pool.Config, s store.Stats) wire.Pool {
	p := wire.Pool{
		Name:              c.Name,
		Denomination:      c.Denomination,
		LockMode:          c.LockMode.String(),
		SplitSize:         c.SplitSize,
		ServePolicy:       string(c.ServePolicy),
		ReleasePolicy:     string(c.ReleasePolicy),
		SpendPolicy:       string(c.SpendPolicy),
		MaxCount:          c.MaxCount,
		DefaultTTLSeconds: int(c.DefaultTTL / time.Second),
		MinTTLSeconds:     int(c.MinTTL / time.Second),
		MaxTTLSeconds:     int(c.MaxTTL / time.Second),
		Stats:             toStats(s),
	}
	if c.LockMode == store.LockP2PKH {
		p.Constraints = &wire.Constraints{TxVersion: 1, LockTime: 0, Sequence: 0xffffffff}
	}
	return p
}

// toShardInfo converts a store.Shard to its admin wire view.
func toShardInfo(sh store.Shard) wire.ShardInfo {
	info := wire.ShardInfo{
		TxID:      sh.TxID.String(),
		Size:      sh.Size,
		Status:    sh.Status.String(),
		Owner:     sh.Owner,
		Version:   sh.Version,
		CreatedAt: sh.CreatedAt.UTC(),
	}
	if !sh.LeaseUntil.IsZero() {
		lu := sh.LeaseUntil.UTC()
		info.LeaseUntil = &lu
	}
	return info
}

// hashStr renders h for the wire, or "" for a nil or zero hash.
func hashStr(h *chainhash.Hash) string {
	if h == nil || *h == (chainhash.Hash{}) {
		return ""
	}
	return h.String()
}

// timePtr returns nil for a zero time, else a pointer to t.UTC(). Calling
// UTC() on the zero Time is safe: it only changes the Location, not the
// instant, so IsZero() is unaffected.
func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

// toRow converts a store.Row to its admin wire view.
func toRow(r store.Row) wire.Row {
	row := wire.Row{
		Outpoint:      r.String(),
		State:         r.State.String(),
		ExpiresAt:     timePtr(r.ExpiresAt),
		QuarantinedAt: timePtr(r.QuarantinedAt),
		SpentHint:     hashStr(r.SpentHint),
		SpentBy:       hashStr(r.SpentBy),
	}
	if !r.ReservedBy.IsZero() {
		row.ReservedBy = r.ReservedBy.String()
	}
	return row
}

// parseOutpoints parses 1..maxCount outpoints, rejecting duplicates.
func parseOutpoints(in []string, maxCount int) ([]store.Outpoint, error) {
	if len(in) == 0 || len(in) > maxCount {
		return nil, fmt.Errorf("%w: 1..%d outpoints required", pool.ErrBadRequest, maxCount)
	}
	out := make([]store.Outpoint, len(in))
	seen := make(map[store.Outpoint]bool, len(in))
	for i, s := range in {
		op, err := store.ParseOutpoint(s)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", pool.ErrBadRequest, err)
		}
		if seen[op] {
			return nil, fmt.Errorf("%w: duplicate outpoint %s", pool.ErrBadRequest, op)
		}
		seen[op] = true
		out[i] = op
	}
	return out, nil
}

// parseToken parses a wire token, wrapping a malformed one as ErrBadRequest.
func parseToken(s string) (store.Token, error) {
	tok, err := store.ParseToken(s)
	if err != nil {
		return store.Token{}, fmt.Errorf("%w: %w", pool.ErrBadRequest, err)
	}
	return tok, nil
}

// hexTxIDPattern matches a display-form txid: exactly 64 lowercase hex
// characters. chainhash.NewHashFromHex accepts (and left-pads) shorter input,
// which would silently store a wrong hash, so callers must check this first.
var hexTxIDPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// parseTxID parses a wire txid, wrapping a malformed one as ErrBadRequest.
// The input must be exactly 64 lowercase hex characters: chainhash.NewHashFromHex
// on its own accepts and zero-pads shorter input (e.g. "abc"), which would
// silently store the wrong SpentHint/txid rather than reject the request.
func parseTxID(s string) (chainhash.Hash, error) {
	if !hexTxIDPattern.MatchString(s) {
		return chainhash.Hash{}, fmt.Errorf("%w: txid must be 64 lowercase hex characters", pool.ErrBadRequest)
	}
	h, err := chainhash.NewHashFromHex(s)
	if err != nil {
		return chainhash.Hash{}, fmt.Errorf("%w: txid: %w", pool.ErrBadRequest, err)
	}
	return *h, nil
}
