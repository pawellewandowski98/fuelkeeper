package store

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// Outpoint identifies one transaction output.
type Outpoint struct {
	TxID chainhash.Hash
	Vout uint32
}

// String renders "<txid-hex>:<vout>" using the display (reversed) hex form.
func (o Outpoint) String() string {
	return o.TxID.String() + ":" + strconv.FormatUint(uint64(o.Vout), 10)
}

// ParseOutpoint accepts "<txid>:<vout>" and the BRC-100 form "<txid>.<vout>".
func ParseOutpoint(s string) (Outpoint, error) {
	i := strings.LastIndexAny(s, ":.")
	if i <= 0 || i == len(s)-1 {
		return Outpoint{}, fmt.Errorf("%w: outpoint %q", ErrInvalidArgument, s)
	}
	h, err := chainhash.NewHashFromHex(s[:i])
	if err != nil {
		return Outpoint{}, fmt.Errorf("%w: outpoint txid: %w", ErrInvalidArgument, err)
	}
	v, err := strconv.ParseUint(s[i+1:], 10, 32)
	if err != nil {
		return Outpoint{}, fmt.Errorf("%w: outpoint vout: %w", ErrInvalidArgument, err)
	}
	return Outpoint{TxID: *h, Vout: uint32(v)}, nil
}

// Token is a reservation token: 16 random bytes, base64url (no padding) on the wire.
type Token [16]byte

// NewToken returns a cryptographically random token.
func NewToken() Token {
	var t Token
	if _, err := rand.Read(t[:]); err != nil {
		panic("store: crypto/rand failed: " + err.Error())
	}
	return t
}

// String encodes the token as 22 base64url characters.
func (t Token) String() string { return base64.RawURLEncoding.EncodeToString(t[:]) }

// IsZero reports whether the token is the all-zero value (meaning "no reservation").
func (t Token) IsZero() bool { return t == Token{} }

// ParseToken decodes a token produced by Token.String.
func ParseToken(s string) (Token, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(b) != len(Token{}) {
		return Token{}, fmt.Errorf("%w: token %q", ErrInvalidArgument, s)
	}
	var t Token
	copy(t[:], b)
	return t, nil
}

// State is the lifecycle state of one fuel row. The numeric codes are part of the
// storage format and must not change.
type State uint8

// State values; the numeric codes are part of the storage format and must not change.
const (
	StateAvailable   State = 0
	StateReserved    State = 1
	StateQuarantined State = 2
	StateSpent       State = 3
	StateInvalid     State = 4
)

func (s State) String() string {
	switch s {
	case StateAvailable:
		return "available"
	case StateReserved:
		return "reserved"
	case StateQuarantined:
		return "quarantined"
	case StateSpent:
		return "spent"
	case StateInvalid:
		return "invalid"
	}
	return "state(" + strconv.Itoa(int(s)) + ")"
}

// Terminal reports whether no transition leaves this state.
func (s State) Terminal() bool { return s == StateSpent || s == StateInvalid }

// ShardStatus is the lifecycle status of a split transaction.
type ShardStatus uint8

// ShardStatus values.
const (
	ShardUnmined ShardStatus = 0
	ShardMined   ShardStatus = 1
	ShardRetired ShardStatus = 2 // all rows terminal; kept for history
	ShardInvalid ShardStatus = 3 // split tx rejected or double-spent; rows invalidated
)

func (s ShardStatus) String() string {
	switch s {
	case ShardUnmined:
		return "unmined"
	case ShardMined:
		return "mined"
	case ShardRetired:
		return "retired"
	case ShardInvalid:
		return "invalid"
	}
	return "status(" + strconv.Itoa(int(s)) + ")"
}

// Terminal reports whether no UpdateShard transition leaves this status: UpdateShard
// cannot change a retired or invalid shard. RetireShard may still invalidate a
// terminal (retired) shard, moving it to invalid.
func (s ShardStatus) Terminal() bool { return s == ShardRetired || s == ShardInvalid }

// LockMode says how a fuel output is unlocked.
type LockMode uint8

// LockMode values.
const (
	LockPuzzle LockMode = 0 // OP_SHA256 <32B> OP_EQUAL; unlocking script pushes the preimage
	LockP2PKH  LockMode = 1 // P2PKH presigned with NONE|ANYONECANPAY|FORKID
)

func (m LockMode) String() string {
	switch m {
	case LockPuzzle:
		return "puzzle"
	case LockP2PKH:
		return "p2pkh"
	}
	return "lockmode(" + strconv.Itoa(int(m)) + ")"
}

// Row is one fuel output.
type Row struct {
	Outpoint
	Pool            string
	Satoshis        uint64
	LockingScript   []byte
	UnlockingScript []byte
	State           State
	ReservedBy      Token           // zero unless reserved (kept for provenance after quarantine/spend)
	ReservedAt      time.Time       // zero unless reserved
	ExpiresAt       time.Time       // zero unless reserved
	QuarantinedAt   time.Time       // zero unless quarantined
	SpentHint       *chainhash.Hash // txid the client claims spent this row; nil if none
	SpentBy         *chainhash.Hash // oracle-proven spending txid; set only in StateSpent
	UpdatedAt       time.Time
}

// Shard is one split transaction and the lease that lets an instance own its rows.
type Shard struct {
	Pool         string
	TxID         chainhash.Hash
	Size         int
	Denomination uint64
	LockMode     LockMode
	Status       ShardStatus
	Owner        string    // "" when unowned; Owner == "" iff LeaseUntil is zero
	LeaseUntil   time.Time // zero when unowned; the lease is held while now < LeaseUntil
	Version      int64     // starts at 1; bumped by UpdateShard and RetireShard
	BEEF         []byte    // nil in ListShards results
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// ShardHead is what RenewShard returns: enough for an owner to notice status/BEEF changes.
type ShardHead struct {
	Status  ShardStatus
	Version int64
}

// ShardFilter narrows ListShards.
type ShardFilter struct {
	Statuses []ShardStatus // nil = all; a listed status must be a known ShardStatus
	// Owner: nil = any; pointer to "" = unowned only. Matches the stored owner string
	// regardless of lease expiry (an expired lease still has a non-empty Owner).
	Owner *string
	Limit int // 0 = no limit; negative is invalid (ErrInvalidArgument)
}

// ReserveOp asks to move one row from available to reserved under Token.
type ReserveOp struct {
	Outpoint
	Token     Token
	ExpiresAt time.Time
}

// Stats are per-pool counts.
type Stats struct {
	Available, Reserved, Quarantined, Spent, Invalid         int64
	ShardsUnmined, ShardsMined, ShardsRetired, ShardsInvalid int64
}

// Caps describes what a backend can do; the coalescer and the suite adapt to it.
type Caps struct {
	MaxConcurrentFlushes int  // how many Reserve calls may usefully run in parallel
	MaxBatch             int  // maximum ops per Reserve/Release/Spend/Resolve/GetRows call
	AtomicBatch          bool // Reserve applies all-or-nothing on backend failure
	MultiProcess         bool // several processes may share this store
}
