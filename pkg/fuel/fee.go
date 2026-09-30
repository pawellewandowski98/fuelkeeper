package fuel

// Input sizes in bytes for fee estimation (outpoint 36 + sequence 4 + varint 1 + script).
const (
	PuzzleInputSize = 74  // 41 + 33-byte unlocking script
	P2PKHInputSize  = 148 // 41 + 107-byte unlocking script
)

// FeeSats is ceil(sizeBytes × satPerKB / 1000), the rounding ARC, arcade, go-sdk
// and the wallet toolboxes use (the node floors, so ceil always satisfies it).
func FeeSats(sizeBytes int, satPerKB uint64) uint64 {
	if sizeBytes <= 0 {
		return 0
	}
	n := uint64(sizeBytes) * satPerKB //nolint:gosec // guarded above: sizeBytes > 0
	return (n + 999) / 1000
}
