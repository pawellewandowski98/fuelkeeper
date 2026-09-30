package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

func testLeaserContract(t *testing.T, s store.Store, clk *Clock) {
	ctx := context.Background()
	const name = "jobs/reaper"

	_, err := s.TryAcquireOrRenew(ctx, "", "a", time.Second)
	require.ErrorIs(t, err, store.ErrInvalidArgument)

	until, err := s.TryAcquireOrRenew(ctx, name, "a", 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, clk.Now().Add(10*time.Second), until.UTC())

	lost, err := s.TryAcquireOrRenew(ctx, name, "b", 10*time.Second)
	require.NoError(t, err)
	assert.True(t, lost.IsZero(), "b must not steal an unexpired lease")

	clk.Advance(5 * time.Second)
	until, err = s.TryAcquireOrRenew(ctx, name, "a", 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, clk.Now().Add(10*time.Second), until.UTC(), "holder renews")

	// Prove the renewal persisted: at t+12s (past the original 10s lease, but before
	// the renewed one expiring at t+15s) b must still be refused.
	clk.Advance(7 * time.Second)
	lost, err = s.TryAcquireOrRenew(ctx, name, "b", 10*time.Second)
	require.NoError(t, err)
	assert.True(t, lost.IsZero(), "renewal persisted: b still refused before the new expiry")

	// Boundary rule: a lease is free when lease_until <= now, i.e. held only while
	// now < lease_until. Use an independent lease name so this doesn't disturb the
	// ownership of the main lease above.
	boundaryUntil, err := s.TryAcquireOrRenew(ctx, "jobs/boundary", "p", 3*time.Second)
	require.NoError(t, err)
	assert.Equal(t, clk.Now().Add(3*time.Second), boundaryUntil.UTC())

	clk.Advance(3 * time.Second) // exactly the boundary lease's (and the main lease's) expiry instant
	boundaryUntil2, err := s.TryAcquireOrRenew(ctx, "jobs/boundary", "q", time.Second)
	require.NoError(t, err)
	assert.False(t, boundaryUntil2.IsZero(), "a lease is free at exactly its expiry instant")

	clk.Advance(1 * time.Second)
	until, err = s.TryAcquireOrRenew(ctx, name, "b", 10*time.Second)
	require.NoError(t, err)
	assert.False(t, until.IsZero(), "expired lease is adoptable")

	lost, err = s.TryAcquireOrRenew(ctx, name, "a", 10*time.Second)
	require.NoError(t, err)
	assert.True(t, lost.IsZero(), "old holder lost it")

	require.NoError(t, s.ReleaseLease(ctx, name, "a"), "non-holder release is a no-op")
	lost, err = s.TryAcquireOrRenew(ctx, name, "a", 10*time.Second)
	require.NoError(t, err)
	assert.True(t, lost.IsZero())

	require.NoError(t, s.ReleaseLease(ctx, name, "b"))
	until, err = s.TryAcquireOrRenew(ctx, name, "a", 10*time.Second)
	require.NoError(t, err)
	assert.False(t, until.IsZero(), "released lease is free")

	other, err := s.TryAcquireOrRenew(ctx, "jobs/minter", "b", time.Second)
	require.NoError(t, err)
	assert.False(t, other.IsZero(), "leases are independent by name")
}
