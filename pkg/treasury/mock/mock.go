// Package mock is an in-memory Treasury that mints synthetic split transactions
// with real hash-puzzle rows. Used by tests, benchmarks and the "nullnet" mode.
package mock

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"sync"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
)

// Treasury is an in-memory treasury.Treasury that mints synthetic hash-puzzle
// splits, for tests, benchmarks and the "nullnet" mode. It is safe for
// concurrent use.
type Treasury struct {
	mu        sync.Mutex
	built     map[chainhash.Hash]*treasury.BuiltShard
	sent      map[chainhash.Hash]int // send count per txid
	failBuild error
	failSend  error
	nBuilt    int
}

var _ treasury.Treasury = (*Treasury)(nil)

// New returns an empty mock treasury.
func New() *Treasury {
	return &Treasury{built: map[chainhash.Hash]*treasury.BuiltShard{}, sent: map[chainhash.Hash]int{}}
}

// Sends returns how many times txid was sent.
func (t *Treasury) Sends(txid chainhash.Hash) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sent[txid]
}

// FailBuild makes Build return err (nil clears it).
func (t *Treasury) FailBuild(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failBuild = err
}

// FailSend makes Send return err (nil clears it).
func (t *Treasury) FailSend(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failSend = err
}

// Sent reports whether txid was sent at least once.
func (t *Treasury) Sent(txid chainhash.Hash) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sent[txid] > 0
}

// Built returns the number of successful builds.
func (t *Treasury) Built() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.nBuilt
}

// Build mints a synthetic unmined shard with real puzzle rows.
func (t *Treasury) Build(_ context.Context, req treasury.MintRequest) (*treasury.BuiltShard, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	// The zero value of store.LockMode is store.LockPuzzle, so a caller that
	// leaves LockMode unset is asking for puzzle rows, which is all this mock
	// supports.
	if req.LockMode != store.LockPuzzle {
		return nil, treasury.ErrUnsupportedLockMode
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.failBuild != nil {
		return nil, t.failBuild
	}
	var rnd [32]byte
	_, _ = rand.Read(rnd[:])
	sum := sha256.Sum256(rnd[:])
	txid, _ := chainhash.NewHash(sum[:])
	seed := fuel.NewSeed()
	rows := make([]store.Row, req.Count)
	for i := range rows {
		pre := fuel.Preimage(seed, uint32(i)) //nolint:gosec // count is small
		rows[i] = store.Row{
			Outpoint:        store.Outpoint{TxID: *txid, Vout: uint32(i)}, //nolint:gosec // count is small
			Pool:            req.Pool,
			Satoshis:        req.Denomination,
			LockingScript:   fuel.PuzzleLock(fuel.PuzzleHash(pre)),
			UnlockingScript: fuel.PuzzleUnlock(pre),
			State:           store.StateAvailable,
		}
	}
	raw := append([]byte("mock-rawtx-"), txid[:]...)
	b := &treasury.BuiltShard{
		Shard: store.Shard{
			Pool:         req.Pool,
			TxID:         *txid,
			Size:         req.Count,
			Denomination: req.Denomination,
			LockMode:     store.LockPuzzle,
			Status:       store.ShardUnmined,
			BEEF:         append([]byte("mock-beef-"), txid[:]...),
		},
		Rows:  rows,
		RawTx: raw,
	}
	t.built[*txid] = b
	t.nBuilt++
	return cloneBuilt(b), nil
}

// cloneBuilt returns a copy of b whose Rows slice is independent of b's, so a
// caller that appends to or reorders the returned Rows cannot corrupt the
// mock's internal record.
func cloneBuilt(b *treasury.BuiltShard) *treasury.BuiltShard {
	rows := make([]store.Row, len(b.Rows))
	copy(rows, b.Rows)
	clone := *b
	clone.Rows = rows
	return &clone
}

// Send records a broadcast of a previously built split.
func (t *Treasury) Send(_ context.Context, txid chainhash.Hash) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.built[txid]; !ok {
		return treasury.ErrUnknownTx
	}
	if t.failSend != nil {
		return t.failSend
	}
	t.sent[txid]++
	return nil
}
