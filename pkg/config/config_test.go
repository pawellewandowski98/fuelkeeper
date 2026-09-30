package config_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/config"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/factory"
)

func TestLoadMinimalAppliesDefaults(t *testing.T) {
	c, err := config.Load("testdata/minimal.yaml")
	require.NoError(t, err)
	assert.Equal(t, factory.BackendMemory, c.Store.Backend)
	require.Len(t, c.Pools, 1)
	p := c.Pools[0]
	assert.Equal(t, "main", p.Name)
	assert.Equal(t, uint64(50), p.Denomination)
	assert.Equal(t, 500, p.SplitSize, "pool defaults applied")
	assert.Equal(t, pool.ServeMinedOnly, p.ServePolicy)
	assert.Equal(t, store.LockPuzzle, p.LockMode)
	assert.Equal(t, pool.SpendTrust, p.SpendPolicy, "no oracle → spend policy defaults to trust")
	assert.Equal(t, pool.ReleaseTrust, p.ReleasePolicy)
	assert.Equal(t, 8, c.Instance.MaxShards)
	assert.Equal(t, ":8080", c.API.Listen)
	assert.True(t, c.Jobs.Enabled)
	assert.Equal(t, time.Second, c.Jobs.Reaper.Interval)
	assert.Equal(t, "none", c.Oracle.Kind)
	assert.Equal(t, "mock", c.Treasury.Kind)
	assert.Equal(t, "info", c.Log.Level)
}

func TestLoadFullOverridesAndExpandsEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.sqlite")
	t.Setenv("FK_TEST_SQLITE_PATH", path)
	c, err := config.Load("testdata/full.yaml")
	require.NoError(t, err)
	assert.Equal(t, "node-1", c.Instance.InstanceID)
	assert.Equal(t, 4, c.Instance.MaxShards)
	assert.Equal(t, 20*time.Second, c.Instance.LeaseTTL)
	assert.Equal(t, factory.BackendSQLite, c.Store.Backend)
	assert.Equal(t, path, c.Store.SQLite.Path, "${VAR} expanded")
	require.Len(t, c.Pools, 2)
	assert.Equal(t, 200, c.Pools[0].SplitSize)
	assert.Equal(t, pool.ServeAny, c.Pools[0].ServePolicy)
	assert.Equal(t, pool.ReleaseVerify, c.Pools[0].ReleasePolicy)
	assert.Equal(t, pool.SpendTrust, c.Pools[0].SpendPolicy)
	assert.Equal(t, 90*time.Second, c.Pools[0].DefaultTTL)
	assert.InDelta(t, 100.0, c.Pools[0].ExpectedTPS, 0.001)
	assert.Equal(t, store.LockP2PKH, c.Pools[1].LockMode)
	assert.Equal(t, 500, c.Pools[1].SplitSize, "defaults still fill unspecified fields")
	assert.Equal(t, pool.SpendVerify, c.Pools[1].SpendPolicy, "oracle configured → spend policy defaults to verify")
	assert.Equal(t, 45*time.Second, c.Jobs.LeaseTTL)
	assert.Equal(t, 2*time.Second, c.Jobs.Reaper.Interval)
	assert.Equal(t, 5*time.Second, c.Jobs.Reaper.Grace)
	assert.Equal(t, 30*time.Second, c.Jobs.Reaper.QuarantineMinAge, "unspecified reaper fields keep defaults")
	assert.Equal(t, 5, c.Jobs.Minter.MaxPerRound)
	assert.Equal(t, "mock", c.Oracle.Kind)
	assert.Equal(t, "127.0.0.1:9090", c.API.Listen)
	assert.Equal(t, []string{"k1", "k2"}, c.API.APIKeys)
	assert.Equal(t, int64(65536), c.API.MaxBodyBytes)
	assert.Equal(t, "json", c.Log.Format)
}

func TestValidateRejectsBadConfigs(t *testing.T) {
	base := func() *config.Config {
		c, err := config.Load("testdata/minimal.yaml")
		require.NoError(t, err)
		return c
	}
	c := base()
	c.Pools = nil
	require.ErrorContains(t, c.Validate(), "at least one pool")

	c = base()
	c.Pools = append(c.Pools, c.Pools[0])
	require.ErrorContains(t, c.Validate(), "duplicate pool")

	c = base()
	c.Oracle.Kind = "psychic"
	require.ErrorContains(t, c.Validate(), "oracle.kind")

	c = base()
	c.Treasury.Kind = "vault"
	require.ErrorContains(t, c.Validate(), "treasury.kind")

	c = base()
	c.Jobs.LeaseTTL = c.Jobs.Minter.Interval / 2
	require.ErrorContains(t, c.Validate(), "jobs.lease_ttl")

	c = base()
	c.Jobs.Watcher.Limit = c.Jobs.Minter.MaxPendingShards
	require.ErrorContains(t, c.Validate(), "jobs.watcher.limit")

	c = base()
	c.Jobs.Reaper.Batch = 0
	require.ErrorContains(t, c.Validate(), "jobs.reaper")

	c = base()
	c.Log.Level = "loud"
	require.ErrorContains(t, c.Validate(), "log.level")

	// Mint-capacity check: 500 rows × 20 splits / 30 s ≈ 333 rows/s; 1.2 × 1000 TPS exceeds it.
	c = base()
	c.Pools[0].ExpectedTPS = 1000
	err := c.Validate()
	require.ErrorContains(t, err, "mint capacity")
	c.Pools[0].ExpectedTPS = 200
	require.NoError(t, c.Validate())

	c = base()
	c.Pools[0].SpendPolicy = pool.SpendVerify
	c.Oracle.Kind = "none"
	require.ErrorContains(t, c.Validate(), "spend_policy=verify requires an oracle")

	c = base()
	c.Pools[0].ReleasePolicy = pool.ReleaseVerify
	c.Oracle.Kind = "none"
	require.ErrorContains(t, c.Validate(), "release_policy=verify requires an oracle")

	c = base()
	c.Instance.InstanceID = ""
	require.ErrorContains(t, c.Validate(), "instance")

	_, err = config.Load("testdata/does-not-exist.yaml")
	require.Error(t, err)

	_, err = config.Parse([]byte("pools: [\n"))
	require.ErrorContains(t, err, "parse")
}
