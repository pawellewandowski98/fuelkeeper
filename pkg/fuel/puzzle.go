// Package fuel holds the fuel-output primitives shared by the pool, treasury and
// clients: hash-puzzle scripts, preimage derivation and fee arithmetic.
package fuel

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
)

// PreimageSize is the puzzle preimage length in bytes.
const PreimageSize = 32

const (
	opSHA256 = 0xa8
	opEQUAL  = 0x87
	push32   = 0x20
)

// Seed is a per-shard secret from which every output's preimage is derived.
type Seed [32]byte

// NewSeed returns a cryptographically random seed.
func NewSeed() Seed {
	var s Seed
	if _, err := rand.Read(s[:]); err != nil {
		panic("fuel: crypto/rand failed: " + err.Error())
	}
	return s
}

// Preimage derives the preimage for output vout: SHA256(seed || big-endian vout).
func Preimage(seed Seed, vout uint32) [PreimageSize]byte {
	var buf [36]byte
	copy(buf[:32], seed[:])
	binary.BigEndian.PutUint32(buf[32:], vout)
	return sha256.Sum256(buf[:])
}

// PuzzleHash is the value committed in the locking script.
func PuzzleHash(preimage [PreimageSize]byte) [32]byte { return sha256.Sum256(preimage[:]) }

// PuzzleLock returns the locking script OP_SHA256 <hash> OP_EQUAL (35 bytes).
func PuzzleLock(hash [32]byte) []byte {
	out := make([]byte, 0, 35)
	out = append(out, opSHA256, push32)
	out = append(out, hash[:]...)
	return append(out, opEQUAL)
}

// PuzzleUnlock returns the unlocking script: a single 32-byte push (33 bytes).
func PuzzleUnlock(preimage [PreimageSize]byte) []byte {
	out := make([]byte, 0, 33)
	out = append(out, push32)
	return append(out, preimage[:]...)
}

// ParsePuzzleLock extracts the hash from a PuzzleLock script.
func ParsePuzzleLock(lock []byte) ([32]byte, bool) {
	var h [32]byte
	if len(lock) != 35 || lock[0] != opSHA256 || lock[1] != push32 || lock[34] != opEQUAL {
		return h, false
	}
	copy(h[:], lock[2:34])
	return h, true
}

// VerifyPuzzle reports whether unlock satisfies lock.
func VerifyPuzzle(lock, unlock []byte) bool {
	h, ok := ParsePuzzleLock(lock)
	if !ok || len(unlock) != 33 || unlock[0] != push32 {
		return false
	}
	var p [PreimageSize]byte
	copy(p[:], unlock[1:])
	return PuzzleHash(p) == h
}
