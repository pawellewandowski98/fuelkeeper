package pool_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

func TestPoolConfigDefaultsAndValidate(t *testing.T) {
	c := pool.DefaultPoolConfig("main", 50)
	require.NoError(t, c.Validate())
	assert.Equal(t, 500, c.SplitSize)
	assert.Equal(t, pool.ServeMinedOnly, c.ServePolicy)
	assert.Equal(t, []store.ShardStatus{store.ShardMined}, c.ServeStatuses())
	c.ServePolicy = pool.ServeAny
	assert.ElementsMatch(t, []store.ShardStatus{store.ShardMined, store.ShardUnmined}, c.ServeStatuses())
	assert.Equal(t, 2*time.Minute, c.DefaultTTL)
	assert.Equal(t, 100, c.MaxCount)

	bad := c
	bad.Name = ""
	require.Error(t, bad.Validate())
	bad = c
	bad.Denomination = 0
	require.Error(t, bad.Validate())
	bad = c
	bad.MinTTL = c.MaxTTL + time.Second
	require.Error(t, bad.Validate())
	bad = c
	bad.DefaultTTL = c.MaxTTL + time.Second
	require.Error(t, bad.Validate())
	bad = c
	bad.LowWater = c.TargetAvailable + 1
	require.Error(t, bad.Validate())
	bad = c
	bad.ServePolicy = "sometimes"
	require.Error(t, bad.Validate())
	bad = c
	bad.SplitSize = 0
	require.Error(t, bad.Validate())
	bad = c
	bad.LockModeName = "p2pkh"
	require.NoError(t, bad.Validate())
	assert.Equal(t, store.LockP2PKH, bad.LockMode)
	bad = c
	bad.LockModeName = "weird"
	require.Error(t, bad.Validate())
	bad = c
	bad.LockModeName = ""
	bad.LockMode = store.LockP2PKH
	require.NoError(t, bad.Validate())
	assert.Equal(t, store.LockP2PKH, bad.LockMode, "an empty LockModeName must not clobber an explicitly set LockMode")
	require.NoError(t, bad.Validate())
	assert.Equal(t, store.LockP2PKH, bad.LockMode, "Validate must be idempotent when LockModeName is empty")
	bad = c
	bad.ReleasePolicy = "maybe"
	require.Error(t, bad.Validate())
	bad = c
	bad.SpendPolicy = "maybe"
	require.Error(t, bad.Validate())
	bad = c
	bad.ExpectedTPS = -1
	require.Error(t, bad.Validate())

	ic := pool.DefaultInstanceConfig()
	require.NoError(t, ic.Validate())
	assert.NotEmpty(t, ic.InstanceID)
	assert.Equal(t, 8, ic.MaxShards)
	assert.Equal(t, 30*time.Second, ic.LeaseTTL)
	bad2 := ic
	bad2.LeaseTTL = 0
	require.Error(t, bad2.Validate())
	bad2 = ic
	bad2.RescanInterval = ic.LeaseTTL
	require.Error(t, bad2.Validate(), "rescan must be shorter than the lease")
	bad2 = ic
	bad2.InstanceID = ""
	require.Error(t, bad2.Validate())
	bad2 = ic
	bad2.InstanceID = strings.Repeat("a", 120)
	require.Error(t, bad2.Validate())
	ok2 := ic
	ok2.InstanceID = strings.Repeat("a", 119)
	require.NoError(t, ok2.Validate())
	require.NoError(t, store.ValidateHolder(pool.NewHolder(ok2.InstanceID)))
	bad2 = ic
	bad2.FlushWindow = -time.Second
	require.Error(t, bad2.Validate())
	bad2 = ic
	bad2.MaxBatch = -1
	require.Error(t, bad2.Validate())

	h1, h2 := pool.NewHolder("node-a"), pool.NewHolder("node-a")
	assert.NotEqual(t, h1, h2, "boot nonce differs per call")
	assert.Contains(t, h1, "node-a/")
	require.NoError(t, store.ValidateHolder(h1))

	var fc pool.Clock = pool.ClockFunc(func() time.Time { return time.Unix(42, 0) })
	assert.Equal(t, time.Unix(42, 0), fc.Now())
	assert.WithinDuration(t, time.Now(), pool.RealClock{}.Now(), time.Second)
}
