// Package wire holds the JSON request and response types of the FuelKeeper
// HTTP API. It has no dependencies beyond the standard library so clients can
// import it freely.
package wire

import "time"

// ReserveRequest asks for fuel.
type ReserveRequest struct {
	Count      int `json:"count"`
	MinCount   int `json:"min_count,omitempty"`
	TTLSeconds int `json:"ttl_seconds,omitempty"`
}

// Unlock carries the unlocking data for one output.
type Unlock struct {
	Mode   string `json:"mode"`   // "puzzle" | "p2pkh"
	Script string `json:"script"` // hex unlocking script
	Length int    `json:"length"` // bytes, for unlockingScriptLength
}

// Output is one reserved fuel output.
type Output struct {
	TxID          string `json:"txid"`
	Vout          uint32 `json:"vout"`
	Outpoint      string `json:"outpoint"`
	Satoshis      uint64 `json:"satoshis"`
	LockingScript string `json:"locking_script"`
	Unlock        Unlock `json:"unlock"`
}

// Shard points at a shard's BEEF.
type Shard struct {
	TxID    string `json:"txid"`
	Version int64  `json:"version"`
	BEEFURL string `json:"beef_url"`
}

// Constraints lists what a consuming tx must satisfy (presigned P2PKH only).
type Constraints struct {
	TxVersion uint32 `json:"tx_version"`
	LockTime  uint32 `json:"lock_time"`
	Sequence  uint32 `json:"sequence"`
}

// ReserveResponse is the reserve result.
type ReserveResponse struct {
	Token       string       `json:"token"`
	ExpiresAt   time.Time    `json:"expires_at"`
	TTLSeconds  int          `json:"ttl_seconds"`
	LockMode    string       `json:"lock_mode"`
	Outputs     []Output     `json:"outputs"`
	Shards      []Shard      `json:"shards"`
	Constraints *Constraints `json:"constraints,omitempty"`
}

// ReleaseRequest returns reserved outputs.
type ReleaseRequest struct {
	Token     string   `json:"token"`
	Outpoints []string `json:"outpoints"`
}

// SpendRequest reports outputs spent in txid.
type SpendRequest struct {
	Token     string   `json:"token"`
	Outpoints []string `json:"outpoints"`
	TxID      string   `json:"txid"`
}

// OutpointsRequest lists outpoints for an admin action.
type OutpointsRequest struct {
	Outpoints []string `json:"outpoints"`
}

// CountResponse reports how many rows changed. Only the field named by the
// endpoint that returned it is meaningful (e.g. release populates Released
// only); the other three are always present, not omitted when zero, so a
// client can rely on every field's presence rather than treating a missing
// key as "unknown".
type CountResponse struct {
	Released int `json:"released"`
	Accepted int `json:"accepted"`
	Requeued int `json:"requeued"`
	Retired  int `json:"retired"`
}

// ShardCounts are shards by status.
type ShardCounts struct {
	Unmined int64 `json:"unmined"`
	Mined   int64 `json:"mined"`
	Retired int64 `json:"retired"`
	Invalid int64 `json:"invalid"`
}

// Stats mirrors the store's pool counts.
type Stats struct {
	Available   int64       `json:"available"`
	Reserved    int64       `json:"reserved"`
	Quarantined int64       `json:"quarantined"`
	Spent       int64       `json:"spent"`
	Invalid     int64       `json:"invalid"`
	Shards      ShardCounts `json:"shards"`
}

// Pool describes a pool and its counts.
type Pool struct {
	Name              string       `json:"name"`
	Denomination      uint64       `json:"denomination"`
	LockMode          string       `json:"lock_mode"`
	SplitSize         int          `json:"split_size"`
	ServePolicy       string       `json:"serve_policy"`
	ReleasePolicy     string       `json:"release_policy"`
	SpendPolicy       string       `json:"spend_policy"`
	MaxCount          int          `json:"max_count"`
	DefaultTTLSeconds int          `json:"default_ttl_seconds"`
	MinTTLSeconds     int          `json:"min_ttl_seconds"`
	MaxTTLSeconds     int          `json:"max_ttl_seconds"`
	Constraints       *Constraints `json:"constraints,omitempty"`
	Stats             Stats        `json:"stats"`
}

// ShardInfo is an admin view of a shard.
type ShardInfo struct {
	TxID       string     `json:"txid"`
	Size       int        `json:"size"`
	Status     string     `json:"status"`
	Owner      string     `json:"owner,omitempty"`
	LeaseUntil *time.Time `json:"lease_until,omitempty"`
	Version    int64      `json:"version"`
	CreatedAt  time.Time  `json:"created_at"`
}

// Row is an admin view of a row.
type Row struct {
	Outpoint      string     `json:"outpoint"`
	State         string     `json:"state"`
	ReservedBy    string     `json:"reserved_by,omitempty"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
	QuarantinedAt *time.Time `json:"quarantined_at,omitempty"`
	SpentHint     string     `json:"spent_hint,omitempty"`
	SpentBy       string     `json:"spent_by,omitempty"`
}

// Error is the error envelope.
type Error struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	RetryAfter int    `json:"retry_after,omitempty"`
}

// Ready is the readiness body.
type Ready struct {
	Status     string `json:"status"`
	Pools      int    `json:"pools"`
	PoolsReady int    `json:"pools_ready"`
	Error      string `json:"error,omitempty"`
}
