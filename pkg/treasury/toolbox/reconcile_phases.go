package toolbox

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// ReconcileGrace is the minimum time between ReconcileStart and
// ReconcileFinish (Config.ReconcileGrace).
func (c *Core) ReconcileGrace() time.Duration { return c.cfg.ReconcileGrace }

// ReconcileStart is the first half of a non-blocking Reconcile: it lists the
// no-send splits (the first listing), sends at once each one known reports
// recorded, and returns the whole listing as the snapshot to hand to
// ReconcileFinish no sooner than ReconcileGrace later. Nothing is aborted
// here. A failed lookup or send is recorded in the report's Errors; the
// split stays in the snapshot, so ReconcileFinish looks at it afresh. Only a
// listing failure returns an error.
func (c *Core) ReconcileStart(ctx context.Context, known func(context.Context, chainhash.Hash) (bool, error)) ([]chainhash.Hash, ReconcileReport, error) {
	rep := ReconcileReport{Errors: map[chainhash.Hash]error{}}
	first, err := c.noSendSplits(ctx)
	if err != nil {
		return nil, rep, err
	}
	for _, txid := range first {
		isKnown, err := known(ctx, txid)
		switch {
		case err != nil:
			rep.Errors[txid] = fmt.Errorf("treasury: look up split: %w", err)
		case isKnown:
			if err := c.Send(ctx, txid, nil); err != nil {
				rep.Errors[txid] = err
				continue
			}
			rep.Resent = append(rep.Resent, txid)
		}
	}
	return first, rep, nil
}

// ReconcileFinish is the second half: it lists the no-send splits again and
// settles each exactly as Reconcile does after its grace (see Reconcile):
// recorded ones are sent, unrecorded ones present in snapshot (the first
// listing) are aborted, unrecorded ones absent from it are Deferred, and a
// failed lookup never leads to an abort. The caller must call it no sooner
// than ReconcileGrace after the ReconcileStart that returned snapshot.
func (c *Core) ReconcileFinish(ctx context.Context, snapshot []chainhash.Hash, known func(context.Context, chainhash.Hash) (bool, error)) (ReconcileReport, error) {
	rep := ReconcileReport{Errors: map[chainhash.Hash]error{}}
	seen := make(map[chainhash.Hash]bool, len(snapshot))
	for _, txid := range snapshot {
		seen[txid] = true
	}
	second, err := c.noSendSplits(ctx)
	if err != nil {
		return rep, err
	}
	for _, txid := range second {
		isKnown, err := known(ctx, txid)
		switch {
		case err != nil:
			rep.Errors[txid] = fmt.Errorf("treasury: look up split: %w", err)
		case isKnown:
			if err := c.Send(ctx, txid, nil); err != nil {
				rep.Errors[txid] = err
				continue
			}
			rep.Resent = append(rep.Resent, txid)
		case !seen[txid]:
			rep.Deferred = append(rep.Deferred, txid)
		default:
			c.abortUnrecorded(ctx, txid, &rep)
		}
	}
	c.log.Info("treasury reconciled", "resent", len(rep.Resent), "aborted", len(rep.Aborted),
		"deferred", len(rep.Deferred), "errors", len(rep.Errors))
	return rep, nil
}
