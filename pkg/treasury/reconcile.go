package treasury

import (
	"context"

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
// were built but never recorded or sent (a crash between Build, PutShard and
// Send). The minter runs it on the first round of each lease. known reports
// whether the store records a split, and returns an error only when it could
// not check; a Reconciler never aborts a split on an error.
type Reconciler interface {
	Reconcile(ctx context.Context, known func(ctx context.Context, txid chainhash.Hash) (bool, error)) (ReconcileReport, error)
}

// ChunkEnsurer is implemented by treasuries that keep pre-sized funding
// chunks ready. The minter calls it at the start of every round.
type ChunkEnsurer interface {
	EnsureChunks(ctx context.Context) error
}
