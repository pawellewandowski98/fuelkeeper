package mock

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
)

// TestBuildReturnsIndependentCopy is a white-box test (package mock, not
// mock_test) checking that mutating the BuiltShard returned by Build cannot
// reach the mock's internal record: Build must hand out a deep copy.
func TestBuildReturnsIndependentCopy(t *testing.T) {
	ctx := context.Background()
	m := New()

	built, err := m.Build(ctx, treasury.MintRequest{Pool: "main", Denomination: 50, Count: 2})
	require.NoError(t, err)

	txid := built.Shard.TxID
	internal := m.built[txid]
	require.NotNil(t, internal)

	// Snapshot what the internal record looks like before we poke at the
	// value Build handed back.
	wantRows := make([]store.Row, len(internal.Rows))
	copy(wantRows, internal.Rows)
	wantBEEF := append([]byte(nil), internal.Shard.BEEF...)
	wantRawTx := append([]byte(nil), internal.RawTx...)
	wantRow0Unlock := append([]byte(nil), internal.Rows[0].UnlockingScript...)

	// Mutate the returned value every way the review asked for.
	built.Rows[0], built.Rows[1] = built.Rows[1], built.Rows[0]
	require.NotEmpty(t, built.Rows[0].UnlockingScript)
	built.Rows[0].UnlockingScript[0] ^= 0xff
	require.NotEmpty(t, built.Shard.BEEF)
	built.Shard.BEEF[0] ^= 0xff

	internal = m.built[txid]
	require.NotNil(t, internal)
	assert.Equal(t, wantRows, internal.Rows, "internal rows must be unaffected by mutating the returned copy")
	assert.Equal(t, wantBEEF, internal.Shard.BEEF, "internal BEEF must be unaffected")
	assert.Equal(t, wantRawTx, internal.RawTx, "internal RawTx must be unaffected")
	assert.Equal(t, wantRow0Unlock, internal.Rows[0].UnlockingScript, "internal row 0 unlocking script must be unaffected")
}
