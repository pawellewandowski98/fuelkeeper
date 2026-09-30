package toolbox_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// TestReconcilePhases runs TestReconcile's scenario through the two
// non-blocking phases: the same splits end in the same lists, and neither
// phase waits.
func TestReconcilePhases(t *testing.T) {
	ctx := context.Background()
	w := newFakeWallet(t)
	c := newCore(t, w, &fakeBroadcaster{})
	c.SetSleep(func(context.Context, time.Duration) error {
		t.Fatal("the phases never wait")
		return nil
	})
	assert.Equal(t, 2*time.Minute, c.ReconcileGrace())
	ids := map[string]chainhash.Hash{}
	for _, n := range []string{"knownOK", "unknownOK", "knownFail", "unknownFail", "mined", "lookupFails", "late", "gone", "recordedLate"} {
		ids[n] = storetest.NewTxID()
	}
	nosend := func(names ...string) []sdk.Action {
		out := make([]sdk.Action, 0, len(names))
		for _, n := range names {
			out = append(out, sdk.Action{Txid: ids[n], Status: sdk.ActionStatusNoSend})
		}
		return out
	}
	w.actions = append(nosend("knownOK", "unknownOK", "knownFail", "unknownFail", "lookupFails", "gone", "recordedLate"),
		sdk.Action{Txid: ids["mined"], Status: sdk.ActionStatusUnproven})
	w.pageCap = 3
	w.sendStatus[ids["knownFail"]] = sdk.ActionResultStatusFailed
	w.abortFail[ids["unknownFail"]] = errors.New("not abortable")
	known := map[chainhash.Hash]bool{ids["knownOK"]: true, ids["knownFail"]: true, ids["mined"]: true}
	lookup := func(_ context.Context, h chainhash.Hash) (bool, error) {
		if h == ids["lookupFails"] {
			return false, errors.New("store unavailable")
		}
		return known[h], nil
	}

	snap, rep, err := c.ReconcileStart(ctx, lookup)
	require.NoError(t, err)
	assert.ElementsMatch(t, []chainhash.Hash{ids["knownOK"], ids["unknownOK"], ids["knownFail"], ids["unknownFail"],
		ids["lookupFails"], ids["gone"], ids["recordedLate"]}, snap, "the snapshot is the whole first listing")
	assert.Equal(t, []chainhash.Hash{ids["knownOK"]}, rep.Resent, "recorded splits are sent at once")
	assert.Len(t, rep.Errors, 2)
	assert.Contains(t, rep.Errors, ids["knownFail"])
	assert.Contains(t, rep.Errors, ids["lookupFails"])
	assert.Empty(t, rep.Aborted, "the first phase never aborts")
	assert.Empty(t, w.abortRefs())

	// Between the phases: "gone" was sent by its owner, "late" was built by
	// another minter, "recordedLate" got its PutShard; "knownOK" was sent.
	w.actions = append(nosend("unknownOK", "knownFail", "unknownFail", "lookupFails", "late", "recordedLate"),
		sdk.Action{Txid: ids["mined"], Status: sdk.ActionStatusUnproven})
	known[ids["recordedLate"]] = true

	rep, err = c.ReconcileFinish(ctx, snap, lookup)
	require.NoError(t, err)
	assert.Equal(t, []chainhash.Hash{ids["recordedLate"]}, rep.Resent)
	assert.Equal(t, []chainhash.Hash{ids["unknownOK"]}, rep.Aborted)
	assert.Equal(t, []chainhash.Hash{ids["late"]}, rep.Deferred, "seen once: deferred, not aborted")
	require.Len(t, rep.Errors, 3)
	assert.Contains(t, rep.Errors, ids["knownFail"])
	assert.Contains(t, rep.Errors, ids["unknownFail"])
	require.ErrorContains(t, rep.Errors[ids["lookupFails"]], "store unavailable")
	assert.Equal(t, []string{ids["unknownOK"].String(), ids["unknownFail"].String()}, w.abortRefs(),
		"only unrecorded splits seen in both listings are aborted; a failed lookup never is")

	// A split unknown to an empty snapshot is only deferred.
	w.actions = nosend("late")
	rep, err = c.ReconcileFinish(ctx, nil, func(context.Context, chainhash.Hash) (bool, error) { return false, nil })
	require.NoError(t, err)
	assert.Equal(t, []chainhash.Hash{ids["late"]}, rep.Deferred)
	assert.Empty(t, rep.Aborted)

	w.listErr = errors.New("storage down")
	_, _, err = c.ReconcileStart(ctx, lookup)
	require.ErrorContains(t, err, "storage down")
	_, err = c.ReconcileFinish(ctx, snap, lookup)
	require.ErrorContains(t, err, "storage down")
}
