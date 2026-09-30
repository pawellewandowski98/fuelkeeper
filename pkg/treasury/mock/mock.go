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
	mu             sync.Mutex
	built          map[chainhash.Hash]*treasury.BuiltShard
	sent           map[chainhash.Hash]int // send count per txid
	failBuild      error
	failSend       error
	failMinedBEEF  error
	nBuilt         int
	nBuildAttempts int
	onSend         func(txid chainhash.Hash)
}

var _ treasury.Treasury = (*Treasury)(nil)

// Option configures a Treasury built by New.
type Option func(*Treasury)

// WithOnSend registers a callback invoked after every successful Send, with
// the mutex released (see Send). Used by "nullnet" mode to simulate mining a
// split some time after it is sent.
func WithOnSend(fn func(txid chainhash.Hash)) Option {
	return func(t *Treasury) { t.onSend = fn }
}

// New returns an empty mock treasury.
func New(opts ...Option) *Treasury {
	t := &Treasury{built: map[chainhash.Hash]*treasury.BuiltShard{}, sent: map[chainhash.Hash]int{}}
	for _, opt := range opts {
		opt(t)
	}
	return t
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

// FailMinedBEEF makes MinedBEEF return err (nil clears it).
func (t *Treasury) FailMinedBEEF(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failMinedBEEF = err
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

// BuildAttempts returns the total number of Build calls, including ones that
// failed (an invalid request, an unsupported lock mode, or a configured
// FailBuild error). Callers use it to check that a minter or other caller
// stopped after a single attempt instead of retrying in a loop.
func (t *Treasury) BuildAttempts() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.nBuildAttempts
}

// Build mints a synthetic unmined shard with real puzzle rows.
func (t *Treasury) Build(_ context.Context, req treasury.MintRequest) (*treasury.BuiltShard, error) {
	t.mu.Lock()
	t.nBuildAttempts++
	t.mu.Unlock()
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
	if _, err := rand.Read(rnd[:]); err != nil {
		panic("mock: crypto/rand failed: " + err.Error())
	}
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

// cloneBuilt returns a deep copy of b: the Rows slice, Shard.BEEF, RawTx and
// each row's LockingScript/UnlockingScript are all freshly allocated, so the
// returned BuiltShard shares no memory with the mock's internal record. A
// caller that mutates the returned value in any way — reordering or
// appending to Rows, writing into a script or into BEEF/RawTx — cannot
// corrupt what the mock has on file for this txid.
func cloneBuilt(b *treasury.BuiltShard) *treasury.BuiltShard {
	clone := *b
	clone.Shard.BEEF = cloneBytes(b.Shard.BEEF)
	clone.RawTx = cloneBytes(b.RawTx)
	clone.Rows = make([]store.Row, len(b.Rows))
	for i, r := range b.Rows {
		r.LockingScript = cloneBytes(r.LockingScript)
		r.UnlockingScript = cloneBytes(r.UnlockingScript)
		clone.Rows[i] = r
	}
	return &clone
}

// cloneBytes returns an independent copy of b, or nil for a nil/empty b.
func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// Send records a broadcast of a previously built split. On success, the
// WithOnSend callback (if any) is invoked after the mutex is released, so it
// may safely call back into the mock (e.g. from a timer) without deadlocking.
func (t *Treasury) Send(_ context.Context, txid chainhash.Hash) error {
	t.mu.Lock()
	if _, ok := t.built[txid]; !ok {
		t.mu.Unlock()
		return treasury.ErrUnknownTx
	}
	if t.failSend != nil {
		err := t.failSend
		t.mu.Unlock()
		return err
	}
	t.sent[txid]++
	fn := t.onSend
	t.mu.Unlock()
	if fn != nil {
		fn(txid)
	}
	return nil
}

// MinedBEEF returns a synthetic mined BEEF: a marker, the txid, the stored
// unmined BEEF and the BUMP bytes. Incorporating unminedBEEF lets a caller
// prove that the shard's actual stored BEEF (from GetShard, since ListShards
// omits it) was threaded through, not e.g. a nil or truncated value.
func (t *Treasury) MinedBEEF(_ context.Context, txid chainhash.Hash, unminedBEEF, merklePath []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.failMinedBEEF != nil {
		return nil, t.failMinedBEEF
	}
	out := append([]byte("mock-mined-beef-"), txid[:]...)
	out = append(out, unminedBEEF...)
	return append(out, merklePath...), nil
}
