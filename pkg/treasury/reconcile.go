package treasury

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// ReconcileReport is what a Reconciler did with the splits its wallet holds
// as built but never sent.
type ReconcileReport struct {
	// Resent are splits the store knows, handed to Send.
	Resent []chainhash.Hash
	// Aborted are splits no store record points at, seen in both listings;
	// they can never reach the chain.
	Aborted []chainhash.Hash
	// Deferred are unknown splits seen only in the second listing (possibly a
	// Build whose PutShard is still in flight), left alone until next time.
	Deferred []chainhash.Hash
	// Errors holds per-split failures; a split here is in no other list.
	Errors map[chainhash.Hash]error
}

// Reconciler is implemented by treasuries whose wallet can hold splits that
// were built but never recorded or sent (a crash, or a PutShard that failed
// or timed out, between Build and Send). Reconciliation is two phases, at
// least ReconcileGrace apart, so no caller ever blocks for the grace:
//
//   - ReconcileStart lists the built-but-unsent splits, sends those the store
//     records, and returns the whole listing as the snapshot;
//   - ReconcileFinish, called at least ReconcileGrace later with that
//     snapshot, lists again and settles each split afresh: recorded ones are
//     sent, unrecorded ones present in both listings are aborted, and
//     unrecorded ones only in the second listing are Deferred.
//
// known reports whether the store records a split, and returns an error only
// when it could not check; a Reconciler never aborts a split on an error.
type Reconciler interface {
	ReconcileGrace() time.Duration
	ReconcileStart(ctx context.Context, known func(ctx context.Context, txid chainhash.Hash) (bool, error)) ([]chainhash.Hash, ReconcileReport, error)
	ReconcileFinish(ctx context.Context, snapshot []chainhash.Hash, known func(ctx context.Context, txid chainhash.Hash) (bool, error)) (ReconcileReport, error)
}

// ChunkEnsurer is implemented by treasuries that keep pre-sized funding
// chunks ready. The minter calls it at the start of every round.
type ChunkEnsurer interface {
	EnsureChunks(ctx context.Context) error
}
