package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// edgeTimeBounds are the "before" values store.go documents as matching
// nothing in Quarantine and ListQuarantined: the zero time, the exact Unix
// epoch instant, and a pre-epoch instant. Every stored expires_at/
// quarantined_at is strictly after the epoch (store.ValidateTime enforces
// this on write), so none of these can ever match a real row.
var edgeTimeBounds = []time.Time{
	{},
	time.Unix(0, 0),
	time.Unix(-1, 0),
}

// testEdgeTimeBoundsMatchNothing pins Settlement 1 (store.go, "Clocks";
// Quarantine, ListQuarantined): before and after are comparison bounds, not
// stored data. An edge-or-earlier before matches nothing and returns 0, nil /
// an empty page, nil -- never ErrInvalidArgument -- and an after cursor at or
// past before also matches nothing. It also checks that none of the
// rejected-as-data-but-not-as-a-bound edge calls poison the connection: a
// normal Quarantine/ListQuarantined call still works right after them.
func testEdgeTimeBoundsMatchNothing(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	const pool = "edge-time"
	_, rows := MintShard(t, s, pool, 3, store.ShardMined)
	tok := store.NewToken()
	exp := clk.Now().Add(time.Second)
	MustReserve(t, s, pool, rows, tok, exp)
	clk.Advance(time.Minute) // past exp: every row is now due for quarantine

	// Quarantine(before) for each edge bound matches nothing: no row moves, and
	// every row is still reserved, so a later iteration is not vacuous.
	for _, before := range edgeTimeBounds {
		n, err := s.Quarantine(ctx, pool, before, 100)
		require.NoError(t, err, "Quarantine(before=%v) must not error", before)
		assert.Zero(t, n, "Quarantine(before=%v) must match nothing", before)
	}
	stillReserved := RowsByOutpoint(t, s, pool, Ops(rows))
	for _, r := range rows {
		assert.Equal(t, store.StateReserved, stillReserved[r.Outpoint].State,
			"the edge-time Quarantine calls above must not have touched any row")
	}

	// Quarantine them properly, with a real before: this also proves the edge
	// calls above left no poisoned connection/transaction state behind.
	realBefore := clk.Now()
	n, err := s.Quarantine(ctx, pool, realBefore, 100)
	require.NoError(t, err)
	require.Equal(t, len(rows), n, "a normal Quarantine call must still work after the edge-time calls above")
	quarantined := RowsByOutpoint(t, s, pool, Ops(rows))
	for _, r := range rows {
		require.Equal(t, store.StateQuarantined, quarantined[r.Outpoint].State)
	}

	// ListQuarantined(before) for each edge bound matches nothing too, even
	// though real quarantined rows now exist to (not) find.
	for _, before := range edgeTimeBounds {
		q, err := s.ListQuarantined(ctx, pool, before, nil, 100)
		require.NoError(t, err, "ListQuarantined(before=%v) must not error", before)
		assert.Empty(t, q, "ListQuarantined(before=%v) must match nothing", before)
	}

	// An after cursor at or past a real before also matches nothing, whether
	// exactly at before or strictly past it.
	listBefore := clk.Now().Add(time.Hour)
	atCursor := store.RowCursor{At: listBefore}
	q, err := s.ListQuarantined(ctx, pool, listBefore, &atCursor, 100)
	require.NoError(t, err, "cursor.At == before")
	assert.Empty(t, q, "a cursor at before must match nothing")

	pastCursor := store.RowCursor{At: listBefore.Add(time.Second)}
	q, err = s.ListQuarantined(ctx, pool, listBefore, &pastCursor, 100)
	require.NoError(t, err, "cursor.At > before")
	assert.Empty(t, q, "a cursor past before must match nothing")

	// Finally, the normal call (no restrictive after) still returns them.
	q, err = s.ListQuarantined(ctx, pool, listBefore, nil, 100)
	require.NoError(t, err)
	assert.Len(t, q, len(rows), "the normal ListQuarantined call must still return the quarantined rows")
}
