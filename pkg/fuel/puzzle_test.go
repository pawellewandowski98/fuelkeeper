package fuel_test

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
)

func TestPuzzleRoundTrip(t *testing.T) {
	seed := fuel.NewSeed()
	p0 := fuel.Preimage(seed, 0)
	p1 := fuel.Preimage(seed, 1)
	assert.NotEqual(t, p0, p1)
	assert.Equal(t, p0, fuel.Preimage(seed, 0), "deterministic")

	lock := fuel.PuzzleLock(fuel.PuzzleHash(p0))
	require.Len(t, lock, 35)
	assert.Equal(t, byte(0xa8), lock[0], "OP_SHA256")
	assert.Equal(t, byte(0x20), lock[1], "push 32")
	assert.Equal(t, byte(0x87), lock[34], "OP_EQUAL")

	unlock := fuel.PuzzleUnlock(p0)
	require.Len(t, unlock, 33)
	assert.Equal(t, byte(0x20), unlock[0])

	h, ok := fuel.ParsePuzzleLock(lock)
	require.True(t, ok)
	assert.Equal(t, sha256.Sum256(p0[:]), h)
	assert.True(t, fuel.VerifyPuzzle(lock, unlock))
	assert.False(t, fuel.VerifyPuzzle(lock, fuel.PuzzleUnlock(p1)))

	_, ok = fuel.ParsePuzzleLock([]byte{0x76, 0xa9})
	assert.False(t, ok)
	assert.False(t, fuel.VerifyPuzzle(lock, []byte{0x01, 0x02}))
}

func TestFeeSats(t *testing.T) {
	assert.Equal(t, uint64(1), fuel.FeeSats(1, 100))
	assert.Equal(t, uint64(10), fuel.FeeSats(100, 100))
	assert.Equal(t, uint64(11), fuel.FeeSats(101, 100))
	assert.Equal(t, uint64(1), fuel.FeeSats(300, 1))
	assert.Equal(t, uint64(0), fuel.FeeSats(0, 100))
	assert.Equal(t, 74, fuel.PuzzleInputSize)
	assert.Equal(t, 148, fuel.P2PKHInputSize)
}
