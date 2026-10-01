package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// TestSpendBatchIsOneCommit pins SpendBatch's point: a multi-request batch,
// with nothing else queued on the writer, is exactly one group commit (one
// WAL fsync), not one per request.
func TestSpendBatchIsOneCommit(t *testing.T) {
	ctx := context.Background()
	clk := storetest.NewClock(storetest.Start)
	s, err := Open(ctx, Config{Path: filepath.Join(t.TempDir(), "batch.sqlite")}, WithClock(clk.Now))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	_, rows := storetest.MintShard(t, s, "main", 6, store.ShardMined)
	reqs := make([]store.SpendRequest, 3)
	for i := range reqs {
		tok := store.NewToken()
		storetest.MustReserve(t, s, "main", rows[2*i:2*i+2], tok, clk.Now().Add(time.Minute))
		reqs[i] = store.SpendRequest{Token: tok, Ops: storetest.Ops(rows[2*i : 2*i+2]), TxID: storetest.NewTxID(), Terminal: i%2 == 0}
	}

	before := s.commits.Load()
	counts, err := s.SpendBatch(ctx, "main", reqs)
	require.NoError(t, err)
	assert.Equal(t, []int{2, 2, 2}, counts)
	assert.Equal(t, before+1, s.commits.Load(), "one durable commit for the whole batch")
}
