// Package mock is an in-memory Treasury that mints real (unsigned, unfunded)
// split transactions with real hash-puzzle rows, in parseable BEEF. Used by tests, benchmarks and the "nullnet" mode.
package mock

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"sync"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

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
	sendBEEF       map[chainhash.Hash][]byte
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
	t := &Treasury{built: map[chainhash.Hash]*treasury.BuiltShard{}, sent: map[chainhash.Hash]int{}, sendBEEF: map[chainhash.Hash][]byte{}}
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

// Build mints an unmined shard with real puzzle rows: Shard.BEEF is a BEEF v2
// holding the split transaction (one dummy input, one puzzle output per row,
// no change), so fuel.RawTxFromBEEF and BEEF merging work on it.
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
	seed := fuel.NewSeed()
	tx, pres := splitTx(seed, req)
	txid := *tx.TxID()
	rows := make([]store.Row, req.Count)
	for i := range rows {
		rows[i] = store.Row{
			Outpoint:        store.Outpoint{TxID: txid, Vout: uint32(i)}, //nolint:gosec // count is small
			Pool:            req.Pool,
			Satoshis:        req.Denomination,
			LockingScript:   []byte(*tx.Outputs[i].LockingScript),
			UnlockingScript: fuel.PuzzleUnlock(pres[i]),
			State:           store.StateAvailable,
		}
	}
	raw := tx.Bytes()
	beef := transaction.NewBeefV2()
	if _, err := beef.MergeRawTx(raw, nil); err != nil {
		return nil, fmt.Errorf("mock: merge split: %w", err)
	}
	beefBytes, err := beef.Bytes()
	if err != nil {
		return nil, fmt.Errorf("mock: serialise split beef: %w", err)
	}
	b := &treasury.BuiltShard{
		Shard: store.Shard{
			Pool:         req.Pool,
			TxID:         txid,
			Size:         req.Count,
			Denomination: req.Denomination,
			LockMode:     store.LockPuzzle,
			Status:       store.ShardUnmined,
			BEEF:         beefBytes,
		},
		Rows:  rows,
		RawTx: raw,
	}
	t.built[txid] = b
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

// Send records a broadcast of a previously built split. beef, when non-nil,
// must be a BEEF holding txid (the shard's stored BEEF); a malformed one is
// an error and is not counted. On success, the WithOnSend callback (if any)
// is invoked after the mutex is released, so it may safely call back into
// the mock (e.g. from a timer) without deadlocking.
func (t *Treasury) Send(_ context.Context, txid chainhash.Hash, beef []byte) error {
	if beef != nil {
		if _, err := fuel.RawTxFromBEEF(beef, txid); err != nil {
			return fmt.Errorf("mock: send beef: %w", err)
		}
	}
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
	t.sendBEEF[txid] = cloneBytes(beef)
	fn := t.onSend
	t.mu.Unlock()
	if fn != nil {
		fn(txid)
	}
	return nil
}

// LastSendBEEF returns the beef passed to the last successful Send of txid
// (nil if none, or if that Send passed nil).
func (t *Treasury) LastSendBEEF(txid chainhash.Hash) []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return cloneBytes(t.sendBEEF[txid])
}

// MinedBEEF converts a shard's stored unmined BEEF and the oracle's BUMP
// into the real mined BEEF v2 (the raw split tx plus the BUMP), delegating
// to fuel.MinedBEEF exactly as the toolbox treasury does (see
// pkg/treasury/toolbox/core.go). A nullnet-served shard's BEEF is therefore
// just as parseable by a real wallet as a toolbox-served one.
func (t *Treasury) MinedBEEF(_ context.Context, txid chainhash.Hash, unminedBEEF, merklePath []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.failMinedBEEF != nil {
		return nil, t.failMinedBEEF
	}
	return fuel.MinedBEEF(unminedBEEF, txid, merklePath)
}

// SingleTxMerklePath returns the serialised BRC-74 (BUMP) bytes for a
// single-transaction block at height whose root is txid itself: one leaf at
// offset 0, naming txid, with the Txid flag set. This is what a "block"
// containing only one transaction (the common case for a freshly mined
// split) looks like as a merkle path, and it is real enough that
// fuel.MinedBEEF and (*transaction.Beef).Verify both accept it -- unlike a
// fabricated placeholder such as []byte{0}.
//
// Used by cmd/fuelkeeper/serve.go's nullnet mock-treasury send hook, and by
// tests that need a real mined merkle path for Treasury.MinedBEEF, so both
// share the exact same construction.
func SingleTxMerklePath(txid chainhash.Hash, height uint32) []byte {
	isTxid := true
	mp := transaction.NewMerklePath(height, [][]*transaction.PathElement{{
		{Offset: 0, Hash: &txid, Txid: &isTxid},
	}})
	return mp.Bytes()
}

// splitTx builds a real (unsigned, unfunded) split transaction: one dummy
// input spending a random outpoint, then one puzzle output per row and no
// change. It returns the preimages so Build can derive the unlocking scripts.
func splitTx(seed fuel.Seed, req treasury.MintRequest) (*transaction.Transaction, [][fuel.PreimageSize]byte) {
	var rnd [32]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		panic("mock: crypto/rand failed: " + err.Error())
	}
	src := chainhash.Hash(sha256.Sum256(rnd[:]))
	tx := transaction.NewTransaction()
	tx.AddInput(&transaction.TransactionInput{
		SourceTXID:       &src,
		SourceTxOutIndex: 0,
		SequenceNumber:   0xffffffff,
		UnlockingScript:  script.NewFromBytes([]byte{0x51}), // OP_TRUE placeholder
	})
	pres := make([][fuel.PreimageSize]byte, req.Count)
	for i := range pres {
		pres[i] = fuel.Preimage(seed, uint32(i)) //nolint:gosec // count is small
		tx.AddOutput(&transaction.TransactionOutput{
			Satoshis:      req.Denomination,
			LockingScript: script.NewFromBytes(fuel.PuzzleLock(fuel.PuzzleHash(pres[i]))),
		})
	}
	return tx, pres
}
