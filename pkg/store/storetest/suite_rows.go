package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

func testReserveBasic(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	_, rows := MintShard(t, s, "main", 4, store.ShardMined)
	tokA, tokB := store.NewToken(), store.NewToken()
	exp := clk.Now().Add(time.Minute)
	t0 := clk.Now()

	applied, err := s.Reserve(ctx, "main", ReserveOps(rows[:2], tokA, exp))
	require.NoError(t, err)
	require.Len(t, applied, 2)
	for _, op := range applied {
		assert.Equal(t, tokA, op.Token)
		assert.Equal(t, exp, op.ExpiresAt.UTC())
	}
	got := RowsByOutpoint(t, s, "main", Ops(rows[:2]))
	for _, r := range rows[:2] {
		g := got[r.Outpoint]
		assert.Equal(t, store.StateReserved, g.State)
		assert.Equal(t, tokA, g.ReservedBy)
		assert.Equal(t, t0, g.ReservedAt.UTC())
		assert.Equal(t, exp, g.ExpiresAt.UTC())
		assert.Equal(t, t0, g.UpdatedAt.UTC())
	}

	applied, err = s.Reserve(ctx, "main", ReserveOps(rows[:2], tokB, exp))
	require.NoError(t, err)
	assert.Empty(t, applied, "another token cannot take reserved rows")

	clk.Advance(5 * time.Second)
	applied, err = s.Reserve(ctx, "main", ReserveOps(rows[:2], tokA, clk.Now().Add(time.Hour)))
	require.NoError(t, err)
	require.Len(t, applied, 2, "same-token replay is applied (idempotent)")
	for _, op := range applied {
		assert.Equal(t, exp, op.ExpiresAt.UTC(), "replay reports the original expiry")
	}
	got = RowsByOutpoint(t, s, "main", Ops(rows[:2]))
	assert.Equal(t, t0, got[rows[0].Outpoint].ReservedAt.UTC(), "replay does not re-stamp reserved_at")
	assert.Equal(t, exp, got[rows[0].Outpoint].ExpiresAt.UTC(), "replay does not extend expiry")

	unknown := []store.ReserveOp{{Outpoint: store.Outpoint{TxID: NewTxID(), Vout: 0}, Token: tokA, ExpiresAt: exp}}
	applied, err = s.Reserve(ctx, "main", unknown)
	require.NoError(t, err)
	assert.Empty(t, applied, "unknown outpoints are skipped, not errors")

	applied, err = s.Reserve(ctx, "other", ReserveOps(rows[2:3], tokA, exp))
	require.NoError(t, err)
	assert.Empty(t, applied, "pool must match")

	_, err = s.Reserve(ctx, "main", nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.Reserve(ctx, "main", append(ReserveOps(rows[2:3], tokA, exp), ReserveOps(rows[2:3], tokB, exp)...))
	require.ErrorIs(t, err, store.ErrInvalidArgument, "duplicate outpoint in one batch")
	_, err = s.Reserve(ctx, "main", ReserveOps(rows[2:3], store.Token{}, exp))
	require.ErrorIs(t, err, store.ErrInvalidArgument, "zero token")
	tooMany := make([]store.ReserveOp, s.Capabilities().MaxBatch+1)
	for i := range tooMany {
		tooMany[i] = store.ReserveOp{Outpoint: store.Outpoint{TxID: NewTxID(), Vout: uint32(i)}, Token: tokA, ExpiresAt: exp}
	}
	_, err = s.Reserve(ctx, "main", tooMany)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "batch above MaxBatch")

	// One batch mixing every reservation outcome in a single call: an available
	// row, one reserved under another token, a same-token replay, an unknown
	// outpoint, an outpoint from another pool, a spent row and an invalid row.
	_, otherPoolRows := MintShard(t, s, "other", 1, store.ShardMined)
	invalidShard, invalidRows := MintShard(t, s, "main", 1, store.ShardMined)
	_, err = s.RetireShard(ctx, "main", invalidShard.TxID)
	require.NoError(t, err)

	_, mixed := MintShard(t, s, "main", 4, store.ShardMined)
	tokMixed, tokOther := store.NewToken(), store.NewToken()
	mixedExp := clk.Now().Add(time.Minute)
	MustReserve(t, s, "main", mixed[1:2], tokMixed, mixedExp) // replayed below
	clk.Advance(time.Second)                                  // so a re-stamped UpdatedAt on replay would be detectable
	MustReserve(t, s, "main", mixed[2:3], tokMixed, mixedExp) // spent below
	MustReserve(t, s, "main", mixed[3:4], tokOther, mixedExp) // reserved by another token

	spentN, err := s.Spend(ctx, "main", tokMixed, Ops(mixed[2:3]), NewTxID(), true)
	require.NoError(t, err)
	require.Equal(t, 1, spentN)

	beforeReplay := RowsByOutpoint(t, s, "main", Ops(mixed[1:2]))[mixed[1].Outpoint]
	beforeOther := RowsByOutpoint(t, s, "main", Ops(mixed[3:4]))[mixed[3].Outpoint]
	beforeSpent := RowsByOutpoint(t, s, "main", Ops(mixed[2:3]))[mixed[2].Outpoint]
	beforeInvalid := RowsByOutpoint(t, s, "main", Ops(invalidRows))[invalidRows[0].Outpoint]

	unknownOp := store.Outpoint{TxID: NewTxID(), Vout: 0}
	mixedOps := []store.ReserveOp{
		{Outpoint: mixed[0].Outpoint, Token: tokMixed, ExpiresAt: mixedExp},                 // available
		{Outpoint: mixed[1].Outpoint, Token: tokMixed, ExpiresAt: clk.Now().Add(time.Hour)}, // same-token replay
		{Outpoint: mixed[3].Outpoint, Token: tokMixed, ExpiresAt: mixedExp},                 // reserved by another token
		{Outpoint: unknownOp, Token: tokMixed, ExpiresAt: mixedExp},                         // unknown
		{Outpoint: otherPoolRows[0].Outpoint, Token: tokMixed, ExpiresAt: mixedExp},         // wrong pool
		{Outpoint: mixed[2].Outpoint, Token: tokMixed, ExpiresAt: mixedExp},                 // spent
		{Outpoint: invalidRows[0].Outpoint, Token: tokMixed, ExpiresAt: mixedExp},           // invalid
	}
	mixedApplied, err := s.Reserve(ctx, "main", mixedOps)
	require.NoError(t, err)
	require.Len(t, mixedApplied, 2, "only the available row and the same-token replay apply")
	appliedSet := make(map[store.Outpoint]store.ReserveOp, len(mixedApplied))
	for _, op := range mixedApplied {
		appliedSet[op.Outpoint] = op
	}
	_, ok := appliedSet[mixed[0].Outpoint]
	assert.True(t, ok, "the available row applies")
	replayOp, ok := appliedSet[mixed[1].Outpoint]
	assert.True(t, ok, "the same-token replay applies")
	assert.Equal(t, mixedExp, replayOp.ExpiresAt.UTC(), "replay reports the original expiry, not the new one")

	afterReplay := RowsByOutpoint(t, s, "main", Ops(mixed[1:2]))[mixed[1].Outpoint]
	assert.Equal(t, beforeReplay.UpdatedAt, afterReplay.UpdatedAt, "same-token replay does not stamp UpdatedAt")
	afterOther := RowsByOutpoint(t, s, "main", Ops(mixed[3:4]))[mixed[3].Outpoint]
	assert.Equal(t, beforeOther, afterOther, "row reserved by another token is unchanged")
	afterSpent := RowsByOutpoint(t, s, "main", Ops(mixed[2:3]))[mixed[2].Outpoint]
	assert.Equal(t, beforeSpent, afterSpent, "spent row is unchanged")
	afterInvalid := RowsByOutpoint(t, s, "main", Ops(invalidRows))[invalidRows[0].Outpoint]
	assert.Equal(t, beforeInvalid, afterInvalid, "invalid row is unchanged")
}

// testSameTokenNeverRevives pins that once a row leaves the reserved state for any
// reason other than Release, a Reserve replay under the very token that reserved
// it never revives the row: the CAS predicate (available OR reserved-by-this-
// token) already excludes quarantined, spent and invalid rows, but this pins it
// explicitly for every path that leads there. It also pins the opposite: a replay
// after expiry but before the reaper's Quarantine call IS still applied, because
// expiry is Quarantine's job, not Reserve's.
func testSameTokenNeverRevives(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	tok := store.NewToken()
	exp := clk.Now().Add(time.Minute)

	assertDead := func(row store.Row, msg string) {
		t.Helper()
		before := RowsByOutpoint(t, s, "main", []store.Outpoint{row.Outpoint})[row.Outpoint]
		applied, err := s.Reserve(ctx, "main", ReserveOps([]store.Row{row}, tok, clk.Now().Add(time.Minute)))
		require.NoError(t, err)
		assert.Empty(t, applied, msg)
		after := RowsByOutpoint(t, s, "main", []store.Outpoint{row.Outpoint})[row.Outpoint]
		assert.Equal(t, before, after, msg+": row must be left unchanged")
	}

	// Quarantine, via the reaper's expiry sweep.
	_, qRows := MintShard(t, s, "main", 1, store.ShardMined)
	shortExp := clk.Now().Add(time.Second)
	MustReserve(t, s, "main", qRows, tok, shortExp)
	clk.Advance(2 * time.Second)
	n, err := s.Quarantine(ctx, "main", clk.Now(), 10)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	assertDead(qRows[0], "a quarantined row does not revive under the original token")

	// Spend(false): reserved -> quarantined directly.
	_, sfRows := MintShard(t, s, "main", 1, store.ShardMined)
	MustReserve(t, s, "main", sfRows, tok, exp)
	_, err = s.Spend(ctx, "main", tok, Ops(sfRows), NewTxID(), false)
	require.NoError(t, err)
	assertDead(sfRows[0], "a spend(false)-quarantined row does not revive")

	// Spend(true): reserved -> spent directly.
	_, stRows := MintShard(t, s, "main", 1, store.ShardMined)
	MustReserve(t, s, "main", stRows, tok, exp)
	_, err = s.Spend(ctx, "main", tok, Ops(stRows), NewTxID(), true)
	require.NoError(t, err)
	assertDead(stRows[0], "a spend(true)-spent row does not revive")

	// Resolve to spent, via quarantine first.
	_, rsRows := MintShard(t, s, "main", 1, store.ShardMined)
	MustReserve(t, s, "main", rsRows, tok, exp)
	_, err = s.Spend(ctx, "main", tok, Ops(rsRows), NewTxID(), false)
	require.NoError(t, err)
	resolveTx := NewTxID()
	_, err = s.Resolve(ctx, "main", Ops(rsRows), store.StateSpent, &resolveTx)
	require.NoError(t, err)
	assertDead(rsRows[0], "a resolved-to-spent row does not revive")

	// RetireShard: reserved -> invalid.
	rtShard, rtRows := MintShard(t, s, "main", 1, store.ShardMined)
	MustReserve(t, s, "main", rtRows, tok, exp)
	_, err = s.RetireShard(ctx, "main", rtShard.TxID)
	require.NoError(t, err)
	assertDead(rtRows[0], "an invalidated (retired) row does not revive")

	// The counterpoint: a replay after expiry but before quarantine IS applied and
	// reports the original (past) expiry. Idempotent replay is what the coalescer
	// relies on; sweeping expired reservations is the reaper's job, not Reserve's.
	_, epRows := MintShard(t, s, "main", 1, store.ShardMined)
	epExp := clk.Now().Add(time.Second)
	MustReserve(t, s, "main", epRows, tok, epExp)
	clk.Advance(2 * time.Second)
	applied, err := s.Reserve(ctx, "main", ReserveOps(epRows, tok, clk.Now().Add(time.Minute)))
	require.NoError(t, err)
	require.Len(t, applied, 1, "same-token replay still applies after expiry, before quarantine")
	assert.Equal(t, epExp, applied[0].ExpiresAt.UTC(), "replay reports the original (past) expiry")
}

func testReserveMixedTokensMixedShards(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	_, a := MintShard(t, s, "main", 3, store.ShardMined)
	_, b := MintShard(t, s, "main", 3, store.ShardMined)
	x, y, z := store.NewToken(), store.NewToken(), store.NewToken()
	e := func(sec int) time.Time { return clk.Now().Add(time.Duration(sec) * time.Second) }
	ops := []store.ReserveOp{
		{Outpoint: a[0].Outpoint, Token: x, ExpiresAt: e(10)},
		{Outpoint: b[1].Outpoint, Token: y, ExpiresAt: e(20)},
		{Outpoint: a[2].Outpoint, Token: x, ExpiresAt: e(30)},
		{Outpoint: b[0].Outpoint, Token: z, ExpiresAt: e(40)},
	}
	applied, err := s.Reserve(ctx, "main", ops)
	require.NoError(t, err)
	require.Len(t, applied, 4)

	got := RowsByOutpoint(t, s, "main", []store.Outpoint{a[0].Outpoint, b[1].Outpoint, a[2].Outpoint, b[0].Outpoint, a[1].Outpoint, b[2].Outpoint})
	for _, op := range ops {
		g := got[op.Outpoint]
		assert.Equal(t, store.StateReserved, g.State, op.Outpoint)
		assert.Equal(t, op.Token, g.ReservedBy, "row carries exactly its own token")
		assert.Equal(t, op.ExpiresAt, g.ExpiresAt.UTC(), "row carries exactly its own expiry")
	}
	assert.Equal(t, store.StateAvailable, got[a[1].Outpoint].State)
	assert.Equal(t, store.StateAvailable, got[b[2].Outpoint].State)
}

func testReleaseGuard(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	shard, rows := MintShard(t, s, "main", 3, store.ShardMined)
	tokA, tokB := store.NewToken(), store.NewToken()
	MustReserve(t, s, "main", rows, tokA, clk.Now().Add(time.Minute))

	n, err := s.Release(ctx, "main", tokB, Ops(rows[:1]))
	require.NoError(t, err)
	assert.Equal(t, 0, n, "wrong token")

	clk.Advance(time.Second)
	tRelease := clk.Now()
	n, err = s.Release(ctx, "main", tokA, Ops(rows[:2]))
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	got := RowsByOutpoint(t, s, "main", Ops(rows))
	for _, r := range rows[:2] {
		g := got[r.Outpoint]
		assert.Equal(t, store.StateAvailable, g.State)
		assert.True(t, g.ReservedBy.IsZero())
		assert.True(t, g.ReservedAt.IsZero())
		assert.True(t, g.ExpiresAt.IsZero())
		assert.True(t, g.QuarantinedAt.IsZero(), "Release clears QuarantinedAt too")
		assert.Nil(t, g.SpentHint, "Release clears SpentHint too")
		assert.Equal(t, tRelease, g.UpdatedAt.UTC(), "UpdatedAt is stamped by Release")
	}
	assert.Equal(t, store.StateReserved, got[rows[2].Outpoint].State)

	n, err = s.Release(ctx, "main", tokA, Ops(rows[:1]))
	require.NoError(t, err)
	assert.Equal(t, 0, n, "already available")

	n, err = s.Spend(ctx, "main", tokA, Ops(rows[2:3]), NewTxID(), false)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	n, err = s.Release(ctx, "main", tokA, Ops(rows[2:3]))
	require.NoError(t, err)
	assert.Equal(t, 0, n, "quarantined rows are not releasable")

	avail, err := s.ListAvailable(ctx, "main", shard.TxID, 0, 10)
	require.NoError(t, err)
	require.Len(t, avail, 2)

	_, err = s.Release(ctx, "main", tokA, nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
}

func testSpendGuard(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	_, rows := MintShard(t, s, "main", 5, store.ShardMined)
	tokA, tokB := store.NewToken(), store.NewToken()
	tx1, tx2 := NewTxID(), NewTxID()
	reservedAt := clk.Now()
	exp := reservedAt.Add(time.Minute)
	MustReserve(t, s, "main", rows, tokA, exp)
	clk.Advance(time.Second)
	t1 := clk.Now()

	n, err := s.Spend(ctx, "main", tokB, Ops(rows[:1]), tx1, false)
	require.NoError(t, err)
	assert.Equal(t, 0, n, "wrong token")

	n, err = s.Spend(ctx, "main", tokA, Ops(rows[:1]), tx1, false)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	g := RowsByOutpoint(t, s, "main", Ops(rows[:1]))[rows[0].Outpoint]
	assert.Equal(t, store.StateQuarantined, g.State)
	require.NotNil(t, g.SpentHint)
	assert.Equal(t, tx1, *g.SpentHint)
	assert.Nil(t, g.SpentBy)
	assert.Equal(t, t1, g.QuarantinedAt.UTC())
	assert.Equal(t, tokA, g.ReservedBy, "provenance kept")
	assert.Equal(t, reservedAt, g.ReservedAt.UTC(), "ReservedAt kept for provenance")
	assert.Equal(t, exp, g.ExpiresAt.UTC(), "ExpiresAt kept for provenance")
	assert.Equal(t, t1, g.UpdatedAt.UTC(), "UpdatedAt is stamped by Spend(false)")

	n, err = s.Spend(ctx, "main", tokA, Ops(rows[:1]), tx2, false)
	require.NoError(t, err)
	assert.Equal(t, 0, n, "second spend is ignored")
	g = RowsByOutpoint(t, s, "main", Ops(rows[:1]))[rows[0].Outpoint]
	assert.Equal(t, tx1, *g.SpentHint, "hint unchanged")

	n, err = s.Spend(ctx, "main", tokA, Ops(rows[1:2]), tx1, true)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	g = RowsByOutpoint(t, s, "main", Ops(rows[1:2]))[rows[1].Outpoint]
	assert.Equal(t, store.StateSpent, g.State)
	require.NotNil(t, g.SpentBy)
	assert.Equal(t, tx1, *g.SpentBy)
	assert.Nil(t, g.SpentHint)
	assert.Equal(t, tokA, g.ReservedBy, "provenance kept (terminal spend)")
	assert.Equal(t, reservedAt, g.ReservedAt.UTC(), "ReservedAt kept (terminal spend)")
	assert.Equal(t, exp, g.ExpiresAt.UTC(), "ExpiresAt kept (terminal spend)")
	assert.Equal(t, t1, g.UpdatedAt.UTC(), "UpdatedAt is stamped by Spend(true)")

	// terminal=true requires a non-zero txid: SpentBy is meant to be an
	// oracle-proven fact, so a zero txid there is always invalid, not a hint.
	n, err = s.Spend(ctx, "main", tokA, Ops(rows[2:3]), chainhash.Hash{}, true)
	require.ErrorIs(t, err, store.ErrInvalidArgument, "terminal spend with a zero txid is invalid")
	assert.Equal(t, 0, n)
	g = RowsByOutpoint(t, s, "main", Ops(rows[2:3]))[rows[2].Outpoint]
	assert.Equal(t, store.StateReserved, g.State, "a rejected terminal spend must not change the row")

	// terminal=false accepts a zero txid: it is only a client-supplied hint, so a
	// zero txid there still quarantines the row but records no hint.
	n, err = s.Spend(ctx, "main", tokA, Ops(rows[3:4]), chainhash.Hash{}, false)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	g = RowsByOutpoint(t, s, "main", Ops(rows[3:4]))[rows[3].Outpoint]
	assert.Equal(t, store.StateQuarantined, g.State)
	assert.Nil(t, g.SpentHint, "a zero txid records no hint")
	assert.Equal(t, tokA, g.ReservedBy, "provenance kept")
	assert.Equal(t, reservedAt, g.ReservedAt.UTC(), "ReservedAt kept for provenance")
	assert.Equal(t, exp, g.ExpiresAt.UTC(), "ExpiresAt kept for provenance")

	applied, err := s.Reserve(ctx, "main", ReserveOps(rows[1:2], tokB, clk.Now().Add(time.Minute)))
	require.NoError(t, err)
	assert.Empty(t, applied, "spent is terminal")
	n, err = s.Release(ctx, "main", tokA, Ops(rows[1:2]))
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func testQuarantineByExpiry(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	_, rows := MintShard(t, s, "main", 6, store.ShardMined)
	tokA, tokB, tokC := store.NewToken(), store.NewToken(), store.NewToken()
	t0 := clk.Now()
	MustReserve(t, s, "main", rows[0:2], tokA, t0.Add(10*time.Second))
	MustReserve(t, s, "main", rows[2:4], tokB, t0.Add(20*time.Second))
	MustReserve(t, s, "main", rows[4:5], tokC, t0.Add(30*time.Second))

	n, err := s.Quarantine(ctx, "main", clk.Now(), 10)
	require.NoError(t, err)
	assert.Equal(t, 0, n, "nothing expired yet")
	_, err = s.Quarantine(ctx, "main", clk.Now(), 0)
	require.ErrorIs(t, err, store.ErrInvalidArgument)

	clk.Advance(15 * time.Second)
	n, err = s.Quarantine(ctx, "main", clk.Now(), 1)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "limit honoured")
	n, err = s.Quarantine(ctx, "main", clk.Now(), 10)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "the other tokA row")
	got := RowsByOutpoint(t, s, "main", Ops(rows))
	for _, r := range rows[0:2] {
		g := got[r.Outpoint]
		assert.Equal(t, store.StateQuarantined, g.State)
		assert.Equal(t, clk.Now(), g.QuarantinedAt.UTC())
		assert.Equal(t, clk.Now(), g.UpdatedAt.UTC(), "quarantine stamps updated_at")
		assert.Nil(t, g.SpentHint, "Quarantine (the reaper's expiry sweep, not a Spend) never sets a hint")
		assert.Equal(t, tokA, g.ReservedBy, "ReservedBy kept for provenance")
		assert.Equal(t, t0, g.ReservedAt.UTC(), "ReservedAt kept for provenance")
		assert.Equal(t, t0.Add(10*time.Second), g.ExpiresAt.UTC(), "ExpiresAt kept for provenance")
	}
	for _, r := range rows[2:5] {
		assert.Equal(t, store.StateReserved, got[r.Outpoint].State)
	}
	assert.Equal(t, store.StateAvailable, got[rows[5].Outpoint].State)

	q, err := s.ListQuarantined(ctx, "main", clk.Now().Add(time.Second), nil, 10)
	require.NoError(t, err)
	require.Len(t, q, 2)
	q, err = s.ListQuarantined(ctx, "main", clk.Now(), nil, 10)
	require.NoError(t, err)
	assert.Empty(t, q, "before is exclusive")

	n, err = s.Release(ctx, "main", tokA, Ops(rows[0:1]))
	require.NoError(t, err)
	assert.Equal(t, 0, n, "expired-and-quarantined rows cannot be released")

	clk.Advance(10 * time.Second) // t0+25s: tokB expired
	n, err = s.Release(ctx, "main", tokB, Ops(rows[2:3]))
	require.NoError(t, err)
	assert.Equal(t, 1, n, "a not-yet-quarantined expired row can still be released")
	n, err = s.Quarantine(ctx, "main", clk.Now(), 10)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "only rows[3]")

	q, err = s.ListQuarantined(ctx, "main", clk.Now().Add(time.Second), nil, 10)
	require.NoError(t, err)
	require.Len(t, q, 3)
	assert.Equal(t, rows[3].Outpoint, q[2].Outpoint, "oldest quarantined first")
	q, err = s.ListQuarantined(ctx, "main", clk.Now().Add(time.Second), nil, 2)
	require.NoError(t, err)
	require.Len(t, q, 2)

	// The rest of this test uses a fresh pool so its counts cannot be disturbed by
	// rows[4] above, which is deliberately left reserved (its tokC expiry, t0+30s,
	// is never reached in the flow above).
	//
	// Three rows with distinct, already-due expiries: Quarantine(limit=1) always
	// moves the earliest by (expires_at, txid internal bytes, vout); a row whose
	// expires_at equals `before` is NOT quarantined (before is exclusive, matching
	// ListQuarantined's own semantics).
	_, distinct := MintShard(t, s, "distinct", 3, store.ShardMined)
	tokD := store.NewToken()
	base := clk.Now()
	MustReserve(t, s, "distinct", distinct[0:1], tokD, base.Add(time.Second))
	MustReserve(t, s, "distinct", distinct[1:2], tokD, base.Add(2*time.Second))
	MustReserve(t, s, "distinct", distinct[2:3], tokD, base.Add(3*time.Second))
	clk.Advance(10 * time.Second) // all three now due

	n1, err := s.Quarantine(ctx, "distinct", clk.Now(), 1)
	require.NoError(t, err)
	assert.Equal(t, 1, n1)
	afterFirst := RowsByOutpoint(t, s, "distinct", Ops(distinct))
	assert.Equal(t, store.StateQuarantined, afterFirst[distinct[0].Outpoint].State, "the earliest expiry is quarantined first")
	assert.Equal(t, store.StateReserved, afterFirst[distinct[1].Outpoint].State)
	assert.Equal(t, store.StateReserved, afterFirst[distinct[2].Outpoint].State)

	nBoundary, err := s.Quarantine(ctx, "distinct", base.Add(3*time.Second), 10)
	require.NoError(t, err)
	assert.Equal(t, 1, nBoundary, "only distinct[1] is strictly before the boundary; distinct[0] is already quarantined")
	afterBoundary := RowsByOutpoint(t, s, "distinct", Ops(distinct))
	assert.Equal(t, store.StateQuarantined, afterBoundary[distinct[1].Outpoint].State)
	assert.Equal(t, store.StateReserved, afterBoundary[distinct[2].Outpoint].State, "expires_at == before is not quarantined (strict <)")

	nRest, err := s.Quarantine(ctx, "distinct", clk.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	assert.Equal(t, 1, nRest, "distinct[2] is the only row left")

	// With limit=2, ListQuarantined returns the two oldest by the documented tuple
	// order (quarantined_at, txid internal bytes, vout).
	qAll, err := s.ListQuarantined(ctx, "distinct", clk.Now().Add(time.Hour), nil, 10)
	require.NoError(t, err)
	require.Len(t, qAll, 3)
	qTwo, err := s.ListQuarantined(ctx, "distinct", clk.Now().Add(time.Hour), nil, 2)
	require.NoError(t, err)
	require.Len(t, qTwo, 2)
	assert.Equal(t, []store.Outpoint{qAll[0].Outpoint, qAll[1].Outpoint}, []store.Outpoint{qTwo[0].Outpoint, qTwo[1].Outpoint},
		"limit=2 returns the two oldest by (quarantined_at, txid, vout)")

	// Cursor pagination: page with limit=1 twice using the cursor and get the two
	// oldest rows in order; a cursor past the last row returns empty.
	page1, err := s.ListQuarantined(ctx, "distinct", clk.Now().Add(time.Hour), nil, 1)
	require.NoError(t, err)
	require.Len(t, page1, 1)
	assert.Equal(t, qAll[0].Outpoint, page1[0].Outpoint)
	cursor1 := page1[0].QuarantineCursor()
	page2, err := s.ListQuarantined(ctx, "distinct", clk.Now().Add(time.Hour), &cursor1, 1)
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.Equal(t, qAll[1].Outpoint, page2[0].Outpoint, "the cursor resumes strictly after the first page")
	cursorLast := qAll[2].QuarantineCursor()
	pageEnd, err := s.ListQuarantined(ctx, "distinct", clk.Now().Add(time.Hour), &cursorLast, 10)
	require.NoError(t, err)
	assert.Empty(t, pageEnd, "a cursor past the last row returns empty")

	// Deterministic tie-break: two rows from two different shards (so two
	// different txids) whose reservations expire at the same instant land in the
	// same Quarantine call and so share quarantined_at exactly; both Quarantine's
	// own candidate selection and ListQuarantined must then break the tie by the
	// txid's internal bytes, not its display (reversed) hex string -- a random
	// txid pair only catches a display-hex ordering bug part of the time.
	tbA, tbB := TxIDPairDisplayReversed()
	tbShardA, tbRowsA := MintShardWithTxID(t, s, "tiebreak", tbA, 1, store.ShardMined)
	tbShardB, tbRowsB := MintShardWithTxID(t, s, "tiebreak", tbB, 1, store.ShardMined)
	tbTok := store.NewToken()
	tbExp := clk.Now().Add(time.Second)
	MustReserve(t, s, "tiebreak", tbRowsA, tbTok, tbExp)
	MustReserve(t, s, "tiebreak", tbRowsB, tbTok, tbExp)
	clk.Advance(2 * time.Second)

	tbN1, err := s.Quarantine(ctx, "tiebreak", clk.Now(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, tbN1)
	tbAfterFirst := RowsByOutpoint(t, s, "tiebreak", append(Ops(tbRowsA), Ops(tbRowsB)...))
	assert.Equal(t, store.StateQuarantined, tbAfterFirst[tbRowsA[0].Outpoint].State,
		"Quarantine's own candidate order breaks a quarantined_at tie by txid internal bytes, not display hex")
	assert.Equal(t, store.StateReserved, tbAfterFirst[tbRowsB[0].Outpoint].State)

	tbN2, err := s.Quarantine(ctx, "tiebreak", clk.Now(), 10)
	require.NoError(t, err)
	require.Equal(t, 1, tbN2)

	tbList, err := s.ListQuarantined(ctx, "tiebreak", clk.Now().Add(time.Second), nil, 10)
	require.NoError(t, err)
	require.Len(t, tbList, 2)
	assert.Equal(t, tbShardA.TxID, tbList[0].TxID, "ListQuarantined breaks a quarantined_at tie by txid internal bytes, not display hex")
	assert.Equal(t, tbShardB.TxID, tbList[1].TxID)
}

func testResolveGuard(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	shard, rows := MintShard(t, s, "main", 3, store.ShardMined)
	tokA := store.NewToken()
	tx1 := NewTxID()
	MustReserve(t, s, "main", rows, tokA, clk.Now().Add(time.Minute))
	_, err := s.Spend(ctx, "main", tokA, Ops(rows[:2]), tx1, false)
	require.NoError(t, err)
	beforeResolve := RowsByOutpoint(t, s, "main", Ops(rows[1:2]))[rows[1].Outpoint]
	require.NotNil(t, beforeResolve.SpentHint)
	require.False(t, beforeResolve.QuarantinedAt.IsZero())

	clk.Advance(time.Second)
	tResolveAvail := clk.Now()
	n, err := s.Resolve(ctx, "main", Ops(rows[:1]), store.StateAvailable, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	g := RowsByOutpoint(t, s, "main", Ops(rows[:1]))[rows[0].Outpoint]
	assert.Equal(t, store.StateAvailable, g.State)
	assert.True(t, g.ReservedBy.IsZero())
	assert.True(t, g.ReservedAt.IsZero())
	assert.True(t, g.ExpiresAt.IsZero())
	assert.True(t, g.QuarantinedAt.IsZero())
	assert.Nil(t, g.SpentHint)
	assert.Equal(t, tResolveAvail, g.UpdatedAt.UTC(), "UpdatedAt is stamped by Resolve")
	avail, err := s.ListAvailable(ctx, "main", shard.TxID, 0, 10)
	require.NoError(t, err)
	require.Len(t, avail, 1)

	n, err = s.Resolve(ctx, "main", Ops(rows[:1]), store.StateAvailable, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, n, "only quarantined rows resolve")

	clk.Advance(time.Second)
	tResolveSpent := clk.Now()
	n, err = s.Resolve(ctx, "main", Ops(rows[1:2]), store.StateSpent, &tx1)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	g = RowsByOutpoint(t, s, "main", Ops(rows[1:2]))[rows[1].Outpoint]
	assert.Equal(t, store.StateSpent, g.State)
	require.NotNil(t, g.SpentBy)
	assert.Equal(t, tx1, *g.SpentBy)
	assert.Equal(t, beforeResolve.ReservedBy, g.ReservedBy, "ReservedBy kept for provenance after Resolve to spent")
	assert.Equal(t, beforeResolve.ReservedAt, g.ReservedAt, "ReservedAt kept for provenance after Resolve to spent")
	assert.Equal(t, beforeResolve.ExpiresAt, g.ExpiresAt, "ExpiresAt kept for provenance after Resolve to spent")
	require.NotNil(t, g.SpentHint, "SpentHint kept for provenance after Resolve to spent")
	assert.Equal(t, *beforeResolve.SpentHint, *g.SpentHint)
	assert.Equal(t, beforeResolve.QuarantinedAt, g.QuarantinedAt, "QuarantinedAt kept for provenance after Resolve to spent")
	assert.Equal(t, tResolveSpent, g.UpdatedAt.UTC(), "UpdatedAt is stamped by Resolve")

	n, err = s.Resolve(ctx, "main", Ops(rows[2:3]), store.StateSpent, &tx1)
	require.NoError(t, err)
	assert.Equal(t, 0, n, "reserved rows do not resolve")

	_, err = s.Resolve(ctx, "main", Ops(rows[2:3]), store.StateReserved, nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	_, err = s.Resolve(ctx, "main", Ops(rows[2:3]), store.StateSpent, nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument)

	// spent is terminal for every operation
	applied, err := s.Reserve(ctx, "main", ReserveOps(rows[1:2], store.NewToken(), clk.Now().Add(time.Minute)))
	require.NoError(t, err)
	assert.Empty(t, applied)
	n, err = s.Release(ctx, "main", tokA, Ops(rows[1:2]))
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	n, err = s.Spend(ctx, "main", tokA, Ops(rows[1:2]), tx1, false)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	n, err = s.Resolve(ctx, "main", Ops(rows[1:2]), store.StateAvailable, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func testListAvailablePagination(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	shard, rows := MintShard(t, s, "main", 25, store.ShardMined)
	MintShard(t, s, "main", 5, store.ShardMined) // another shard must not leak in

	const pageLimit = 10
	page := func(from uint32) []uint32 {
		got, err := s.ListAvailable(ctx, "main", shard.TxID, from, pageLimit)
		require.NoError(t, err)
		vouts := make([]uint32, len(got))
		for i, r := range got {
			require.Equal(t, shard.TxID, r.TxID)
			vouts[i] = r.Vout
		}
		return vouts
	}
	assert.Equal(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, page(0))
	assert.Equal(t, []uint32{10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, page(10))
	assert.Equal(t, []uint32{20, 21, 22, 23, 24}, page(20))
	assert.Empty(t, page(25))

	tok := store.NewToken()
	MustReserve(t, s, "main", rows[3:4], tok, clk.Now().Add(time.Minute))
	assert.Equal(t, []uint32{0, 1, 2, 4, 5, 6, 7, 8, 9, 10}, page(0), "reserved row skipped")

	n, err := s.Spend(ctx, "main", tok, Ops(rows[3:4]), NewTxID(), false)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	n, err = s.Resolve(ctx, "main", Ops(rows[3:4]), store.StateAvailable, nil)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	assert.Equal(t, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, page(0), "resolved row is back")

	_, err = s.ListAvailable(ctx, "main", shard.TxID, 0, 0)
	require.ErrorIs(t, err, store.ErrInvalidArgument)

	unknownAvail, err := s.ListAvailable(ctx, "main", NewTxID(), 0, 10)
	require.NoError(t, err)
	assert.Empty(t, unknownAvail, "an unknown txid returns an empty slice, not ErrNotFound, like GetRows")

	all := RowsByOutpoint(t, s, "main", Ops(rows))
	require.Len(t, all, 25)
	_, err = s.GetRows(ctx, "main", nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument)
	missing, err := s.GetRows(ctx, "main", []store.Outpoint{{TxID: NewTxID(), Vout: 0}})
	require.NoError(t, err)
	assert.Empty(t, missing)
}

func testStatsCounts(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	_, a := MintShard(t, s, "main", 5, store.ShardUnmined)
	b, _ := MintShard(t, s, "main", 5, store.ShardMined)
	MintShard(t, s, "other", 2, store.ShardMined)
	tok := store.NewToken()
	tx := NewTxID()
	MustReserve(t, s, "main", a[:3], tok, clk.Now().Add(time.Minute))
	_, err := s.Spend(ctx, "main", tok, Ops(a[:1]), tx, false)
	require.NoError(t, err)
	_, err = s.Spend(ctx, "main", tok, Ops(a[1:2]), tx, true)
	require.NoError(t, err)
	_, err = s.RetireShard(ctx, "main", b.TxID)
	require.NoError(t, err)
	c, _ := MintShard(t, s, "main", 2, store.ShardMined)
	require.NoError(t, s.UpdateShard(ctx, "main", c.TxID, store.ShardRetired, nil))

	st, err := s.Stats(ctx, "main")
	require.NoError(t, err)
	assert.Equal(t, store.Stats{
		Available: 4, Reserved: 1, Quarantined: 1, Spent: 1, Invalid: 5,
		ShardsUnmined: 1, ShardsMined: 0, ShardsRetired: 1, ShardsInvalid: 1,
	}, st)

	empty, err := s.Stats(ctx, "nothing")
	require.NoError(t, err)
	assert.Equal(t, store.Stats{}, empty)
}

// testTimePrecisionMicros pins the contract's "all stored times have
// microsecond precision" rule (store.go, "Clocks"): Postgres's TIMESTAMPTZ has
// no finer native resolution, so every backend must truncate a caller-supplied
// time to the microsecond on write, and every caller must compare with
// time.Time.Truncate(time.Microsecond) rather than assuming full nanosecond
// round-tripping.
func testTimePrecisionMicros(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	_, rows := MintShard(t, s, "main", 1, store.ShardMined)
	tok := store.NewToken()
	exp := clk.Now().Add(123456789 * time.Nanosecond)
	wantExp := exp.Truncate(time.Microsecond)
	require.NotEqual(t, exp, wantExp, "test setup: exp must carry a sub-microsecond remainder")

	applied, err := s.Reserve(ctx, "main", ReserveOps(rows, tok, exp))
	require.NoError(t, err)
	require.Len(t, applied, 1)
	assert.Equal(t, wantExp, applied[0].ExpiresAt.UTC(), "Reserve truncates ExpiresAt to microsecond precision")

	got := RowsByOutpoint(t, s, "main", Ops(rows))[rows[0].Outpoint]
	assert.Equal(t, wantExp, got.ExpiresAt.UTC(), "the stored ExpiresAt is truncated to microsecond precision")

	ttl := 1500*time.Microsecond + 700*time.Nanosecond
	wantLeaseExp := clk.Now().Add(ttl).Truncate(time.Microsecond)
	leaseExp, err := s.TryAcquireOrRenew(ctx, "jobs/precision", "h", ttl)
	require.NoError(t, err)
	assert.Equal(t, wantLeaseExp, leaseExp.UTC(), "TryAcquireOrRenew truncates the lease expiry to microsecond precision")
}

// testSubMicrosecondArguments pins that every backend treats a caller-supplied
// time argument at microsecond precision, not raw nanosecond precision (store.go,
// "Clocks"): a value only a few hundred nanoseconds past a boundary must compare
// as if it were exactly at that boundary. It sets the manual clock to an instant
// carrying a nanosecond remainder so "now" itself is not already microsecond-
// aligned, then checks AcquireShard's until-after-now validation and Quarantine/
// ListQuarantined's exclusive "before" comparisons all truncate before comparing.
func testSubMicrosecondArguments(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	// now's within-microsecond remainder (100ns) plus the 500ns added below stays
	// under 1us, so both truncate into the same microsecond bucket.
	now := clk.Set(Start.Add(100 * time.Nanosecond))
	mined := []store.ShardStatus{store.ShardMined}

	MintShard(t, s, "main", 1, store.ShardMined)
	_, err := s.AcquireShard(ctx, "main", "h", now, now.Add(500*time.Nanosecond), mined, nil)
	require.ErrorIs(t, err, store.ErrInvalidArgument,
		"until only 500ns after now truncates to the same microsecond as now and must be rejected")

	got, err := s.AcquireShard(ctx, "main", "h", now, now.Add(time.Microsecond+500*time.Nanosecond), mined, nil)
	require.NoError(t, err)
	require.NotNil(t, got, "until a full microsecond after now must be accepted")

	// Quarantine's `before` compared against the row's stored (already-truncated)
	// expires_at.
	_, rows := MintShard(t, s, "main", 1, store.ShardMined)
	tok := store.NewToken()
	MustReserve(t, s, "main", rows, tok, clk.Now().Add(time.Minute))
	stored := RowsByOutpoint(t, s, "main", Ops(rows))[rows[0].Outpoint]
	require.False(t, stored.ExpiresAt.IsZero())

	n, err := s.Quarantine(ctx, "main", stored.ExpiresAt.Add(500*time.Nanosecond), 10)
	require.NoError(t, err)
	assert.Equal(t, 0, n, "before 500ns past expires_at truncates to expires_at itself; strict < must not match")

	q, err := s.ListQuarantined(ctx, "main", stored.ExpiresAt.Add(500*time.Nanosecond), nil, 10)
	require.NoError(t, err)
	assert.Empty(t, q, "ListQuarantined's before must apply the same truncation")

	n, err = s.Quarantine(ctx, "main", stored.ExpiresAt.Add(time.Microsecond), 10)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "a full microsecond past expires_at must quarantine the row")

	quarantined := RowsByOutpoint(t, s, "main", Ops(rows))[rows[0].Outpoint]
	require.Equal(t, store.StateQuarantined, quarantined.State)
	require.False(t, quarantined.QuarantinedAt.IsZero())

	// ListQuarantined's `before` compared against the row's stored (already-
	// truncated) quarantined_at.
	q, err = s.ListQuarantined(ctx, "main", quarantined.QuarantinedAt.Add(500*time.Nanosecond), nil, 10)
	require.NoError(t, err)
	assert.Empty(t, q, "before 500ns past quarantined_at truncates to quarantined_at itself; strict < must not match")

	q, err = s.ListQuarantined(ctx, "main", quarantined.QuarantinedAt.Add(time.Microsecond), nil, 10)
	require.NoError(t, err)
	require.Len(t, q, 1, "a full microsecond past quarantined_at must include the row")
}
