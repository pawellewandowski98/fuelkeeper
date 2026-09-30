package config_test

import (
	"fmt"
	"os"
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
	assert.True(t, c.Oracle.Mock.AssumeUnspent, "nullnet default: mock oracle assumes unspent")
	assert.Equal(t, 2*time.Second, c.Treasury.Mock.ConfirmAfter, "nullnet default: mock treasury confirms after 2s")
	assert.Equal(t, 10*time.Second, c.API.RequestTimeout)
	assert.Empty(t, c.API.AdminKeys, "admin_keys defaults empty (falls back to api_keys)")
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

	c = base()
	c.Store.Backend = factory.BackendPostgres
	c.Store.Postgres.DSN = ""
	require.ErrorContains(t, c.Validate(), "store.postgres.dsn")

	c = base()
	c.Store.Backend = factory.BackendSQLite
	c.Store.SQLite.Path = ""
	require.ErrorContains(t, c.Validate(), "store.sqlite.path")

	c = base()
	c.Store.Backend = "cassandra"
	require.ErrorContains(t, c.Validate(), "unknown store backend")

	c = base()
	c.Store.Backend = ""
	require.ErrorContains(t, c.Validate(), "store.backend")

	c = base()
	c.API.Listen = "not-a-valid-listen-address"
	require.ErrorContains(t, c.Validate(), "api.listen")

	c = base()
	c.API.ReadTimeout = 0
	require.ErrorContains(t, c.Validate(), "read_timeout")

	c = base()
	c.API.RequestTimeout = 0
	require.ErrorContains(t, c.Validate(), "api.request_timeout")

	c = base()
	c.API.RequestTimeout = c.Instance.ReserveTimeout
	err = c.Validate()
	require.ErrorContains(t, err, "api.request_timeout")
	require.ErrorContains(t, err, "instance.reserve_timeout")

	c = base()
	c.Treasury.Mock.ConfirmAfter = 0
	require.ErrorContains(t, c.Validate(), "treasury.mock.confirm_after")

	c = base()
	c.Treasury.Mock.ConfirmAfter = -time.Second
	require.ErrorContains(t, c.Validate(), "treasury.mock.confirm_after")

	_, err = config.Load("testdata/does-not-exist.yaml")
	require.Error(t, err)

	_, err = config.Parse([]byte("pools: [\n"))
	require.ErrorContains(t, err, "parse")
}

// TestPoolDefaultsPreserveExplicitZero guards against a fill-if-zero
// post-decode pass mistaking an operator's explicit zero for "unset": a pool
// entry is pre-filled from pool.DefaultPoolConfig before its own YAML fields
// are decoded onto it (see the unexported rawPool type), so low_water: 0
// (documented as "always refill below target") must survive, while an
// omitted field (split_size) still gets its default.
func TestPoolDefaultsPreserveExplicitZero(t *testing.T) {
	c, err := config.Parse([]byte("store:\n  backend: memory\npools:\n" +
		"  - name: main\n    denomination: 50\n    low_water: 0\n"))
	require.NoError(t, err)
	require.Len(t, c.Pools, 1)
	assert.Equal(t, int64(0), c.Pools[0].LowWater, "explicit low_water: 0 must survive")
	assert.Equal(t, 500, c.Pools[0].SplitSize, "split_size omitted keeps the pool default")

	// instance.low_water_rows: 0 already survived before this change (Instance
	// is a single struct decoded onto its already-defaulted value, not a
	// freshly zeroed slice element) -- this is a regression guard, not a fix.
	c, err = config.Parse([]byte("instance:\n  low_water_rows: 0\nstore:\n  backend: memory\npools:\n" +
		"  - name: main\n    denomination: 50\n"))
	require.NoError(t, err)
	assert.Equal(t, 0, c.Instance.LowWaterRows, "explicit instance.low_water_rows: 0 must survive")
}

// TestParseRejectsUnknownPoolField guards against typos such as spilt_size
// silently vanishing (yaml.v3 ignores keys with no matching field unless
// KnownFields is set); rawPool's UnmarshalYAML re-decodes each pool entry
// through its own strict decoder since yaml.Node.Decode never inherits an
// outer decoder's KnownFields setting.
func TestParseRejectsUnknownPoolField(t *testing.T) {
	_, err := config.Parse([]byte("store:\n  backend: memory\npools:\n" +
		"  - name: main\n    denomination: 50\n    spilt_size: 200\n"))
	require.ErrorContains(t, err, "spilt_size")
}

// TestParseRejectsUnknownTopLevelField is the same guard at the top level.
func TestParseRejectsUnknownTopLevelField(t *testing.T) {
	_, err := config.Parse([]byte("stroe:\n  backend: memory\npools:\n" +
		"  - name: main\n    denomination: 50\n"))
	require.ErrorContains(t, err, "stroe")
}

// TestLoadStrictEnvExpansion covers Load's "${VAR}" / "${VAR:-default}" /
// "$$" / bare "$word" syntax: unlike os.ExpandEnv, an unset ${VAR} is an
// error rather than silently becoming "", and a literal "$" (as in an API
// key) is never mangled unless it is written "$$".
func TestLoadStrictEnvExpansion(t *testing.T) {
	writeFile := func(t *testing.T, body string) string {
		t.Helper()
		p := filepath.Join(t.TempDir(), "c.yaml")
		require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
		return p
	}
	const tmpl = "store:\n  backend: memory\npools:\n  - name: main\n    denomination: 50\napi:\n  api_keys: [%s]\n"

	t.Run("unset variable errors naming it", func(t *testing.T) {
		p := writeFile(t, fmt.Sprintf(tmpl, `"${FK_TEST_UNSET_VAR}"`))
		_, err := config.Load(p)
		require.ErrorContains(t, err, "FK_TEST_UNSET_VAR")
	})

	t.Run("unset variable with default falls back", func(t *testing.T) {
		p := writeFile(t, fmt.Sprintf(tmpl, `"${FK_TEST_UNSET_VAR:-fallback}"`))
		c, err := config.Load(p)
		require.NoError(t, err)
		assert.Equal(t, []string{"fallback"}, c.API.APIKeys)
	})

	t.Run("dollar-dollar is a literal dollar; a bare dollar-word is untouched", func(t *testing.T) {
		p := writeFile(t, fmt.Sprintf(tmpl, `"s3cr3t$$pecial", "$bareword"`))
		c, err := config.Load(p)
		require.NoError(t, err)
		assert.Equal(t, []string{"s3cr3t$pecial", "$bareword"}, c.API.APIKeys)
	})
}

// TestApplyPoolDefaultsForProgrammaticConfig covers the exported helper Go
// callers use when composing a pool.Config without going through YAML.
// pool.DefaultPoolConfig assumes an oracle (SpendPolicy: SpendVerify);
// ApplyPoolDefaults corrects that assumption to SpendTrust once it is told
// there is no oracle.
func TestApplyPoolDefaultsForProgrammaticConfig(t *testing.T) {
	c := config.Default()
	p := pool.DefaultPoolConfig("main", 50)
	require.Equal(t, pool.SpendVerify, p.SpendPolicy, "DefaultPoolConfig assumes an oracle")
	config.ApplyPoolDefaults(&p, c.Oracle.Kind) // c.Oracle.Kind == "none"
	c.Pools = []pool.Config{p}
	require.NoError(t, c.Validate())
	assert.Equal(t, pool.SpendTrust, p.SpendPolicy)
}
