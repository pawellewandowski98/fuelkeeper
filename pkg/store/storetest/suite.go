package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// Factory builds a fresh, empty store whose clock is now. It is called once per
// subtest; subtests run in parallel, so each store must be isolated.
type Factory func(t *testing.T, now func() time.Time) store.Store

// Start is the manual clock's starting instant for every subtest.
var Start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type subtest struct {
	name string
	fn   func(t *testing.T, s store.Store, clk *Clock)
}

func subtests() []subtest {
	return []subtest{
		{"Close", testClose},
		{"LeaserContract", testLeaserContract},
		{"PutShardAtomicAndIdempotent", testPutShardAtomicAndIdempotent},
		{"UpdateShardBumpsVersion", testUpdateShardBumpsVersion},
		{"ListShards", testListShards},
		{"ListShardsTieBreak", testListShardsTieBreak},
		{"AcquireRenewRelease", testAcquireRenewRelease},
		{"RetireShardCascade", testRetireShardCascade},
		{"ShardLeaseBoundary", testShardLeaseBoundary},
		{"ArgumentValidation", testArgumentValidation},
		{"PoolIsolation", testPoolIsolation},
		{"ReserveBasic", testReserveBasic},
		{"ReserveMixedTokensMixedShards", testReserveMixedTokensMixedShards},
		{"SameTokenNeverRevives", testSameTokenNeverRevives},
		{"ReleaseGuard", testReleaseGuard},
		{"SpendGuard", testSpendGuard},
		{"QuarantineByExpiry", testQuarantineByExpiry},
		{"ResolveGuard", testResolveGuard},
		{"ListAvailablePagination", testListAvailablePagination},
		{"StatsCounts", testStatsCounts},
		{"ReserveExclusivityConcurrent", testReserveExclusivityConcurrent},
		{"ReserveReleaseCycleConcurrent", testReserveReleaseCycleConcurrent},
		{"AcquireShardExclusiveConcurrent", testAcquireShardExclusiveConcurrent},
		{"ReserveVsQuarantineRace", testReserveVsQuarantineRace},
	}
}

// RunSuite runs every conformance subtest against stores built by newStore.
func RunSuite(t *testing.T, newStore Factory) {
	t.Helper()
	for _, tc := range subtests() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			clk := NewClock(Start)
			s := newStore(t, clk.Now)
			t.Cleanup(func() { _ = s.Close() })
			tc.fn(t, s, clk)
		})
	}
}

func testClose(t *testing.T, s store.Store, _ *Clock) {
	require.NoError(t, s.Ping(context.Background()))
	caps := s.Capabilities()
	require.Positive(t, caps.MaxBatch)
	require.Positive(t, caps.MaxConcurrentFlushes)
	require.NoError(t, s.Close())
	require.NoError(t, s.Close(), "Close must be idempotent")
}
