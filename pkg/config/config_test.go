package config_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/config"
	"github.com/bsv-blockchain/fuelkeeper/pkg/jobs"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/factory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/sqlite"
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

// TestLoadStoreBackendPebble loads a full store: block for the pebble
// backend through the strict (KnownFields) loader, covering every field
// factory.PebbleConfig declares.
func TestLoadStoreBackendPebble(t *testing.T) {
	c, err := config.Parse([]byte(`
store:
  backend: pebble
  pebble:
    path: /var/lib/fuelkeeper/pebble
    no_sync: false
    cache_bytes: 268435456
pools:
  - name: main
    denomination: 50
api: {insecure: true}
`))
	require.NoError(t, err)
	assert.Equal(t, factory.BackendPebble, c.Store.Backend)
	assert.Equal(t, "/var/lib/fuelkeeper/pebble", c.Store.Pebble.Path)
	assert.False(t, c.Store.Pebble.NoSync)
	assert.Equal(t, int64(268435456), c.Store.Pebble.CacheBytes)
}

// TestLoadStoreBackendMongoDB loads a full store: block for the mongodb
// backend through the strict (KnownFields) loader, covering every field
// factory.MongoDBConfig declares.
func TestLoadStoreBackendMongoDB(t *testing.T) {
	c, err := config.Parse([]byte(`
store:
  backend: mongodb
  mongodb:
    uri: mongodb://localhost:27017
    database: fuelkeeper
    max_pool_size: 64
    op_timeout: 5s
pools:
  - name: main
    denomination: 50
api: {insecure: true}
`))
	require.NoError(t, err)
	assert.Equal(t, factory.BackendMongoDB, c.Store.Backend)
	assert.Equal(t, "mongodb://localhost:27017", c.Store.MongoDB.URI)
	assert.Equal(t, "fuelkeeper", c.Store.MongoDB.Database)
	assert.Equal(t, uint64(64), c.Store.MongoDB.MaxPoolSize)
	assert.Equal(t, 5*time.Second, c.Store.MongoDB.OpTimeout)
}

// TestLoadStoreBackendAerospike loads a full store: block for the aerospike
// backend through the strict (KnownFields) loader, covering every field
// factory.AerospikeConfig declares.
func TestLoadStoreBackendAerospike(t *testing.T) {
	c, err := config.Parse([]byte(`
store:
  backend: aerospike
  aerospike:
    hosts: ["localhost:3000", "localhost:3010"]
    namespace: fuelkeeper
    set_prefix: fk_
    key_prefix: tenant1
    allow_availability_mode: true
    timeout: 3s
    pool_size: 128
pools:
  - name: main
    denomination: 50
api: {insecure: true}
`))
	require.NoError(t, err)
	assert.Equal(t, factory.BackendAerospike, c.Store.Backend)
	assert.Equal(t, []string{"localhost:3000", "localhost:3010"}, c.Store.Aerospike.Hosts)
	assert.Equal(t, "fuelkeeper", c.Store.Aerospike.Namespace)
	assert.Equal(t, "fk_", c.Store.Aerospike.SetPrefix)
	assert.Equal(t, "tenant1", c.Store.Aerospike.KeyPrefix)
	assert.True(t, c.Store.Aerospike.AllowAvailabilityMode)
	assert.Equal(t, 3*time.Second, c.Store.Aerospike.Timeout)
	assert.Equal(t, 128, c.Store.Aerospike.PoolSize)
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
	require.ErrorContains(t, c.Validate(), "store.sqlite.path is required")

	c = base()
	c.Store.Backend = factory.BackendPebble
	c.Store.Pebble.Path = ""
	require.ErrorContains(t, c.Validate(), "store.pebble.path")

	c = base()
	c.Store.Backend = factory.BackendMongoDB
	c.Store.MongoDB.URI = ""
	c.Store.MongoDB.Database = "fk"
	require.ErrorContains(t, c.Validate(), "store.mongodb.uri")

	c = base()
	c.Store.Backend = factory.BackendMongoDB
	c.Store.MongoDB.URI = "mongodb://localhost:27017"
	c.Store.MongoDB.Database = ""
	require.ErrorContains(t, c.Validate(), "store.mongodb.database")

	c = base()
	c.Store.Backend = factory.BackendAerospike
	c.Store.Aerospike.Hosts = nil
	c.Store.Aerospike.Namespace = "fk"
	require.ErrorContains(t, c.Validate(), "store.aerospike.hosts")

	c = base()
	c.Store.Backend = factory.BackendAerospike
	c.Store.Aerospike.Hosts = []string{"localhost:3000"}
	c.Store.Aerospike.Namespace = ""
	require.ErrorContains(t, c.Validate(), "store.aerospike.namespace")

	c = base()
	c.Store.Backend = "cassandra"
	err = c.Validate()
	require.ErrorContains(t, err, "unknown store backend")
	require.ErrorContains(t, err, "memory|sqlite|postgres|pebble|mongodb|aerospike", "the message lists every known backend")

	c = base()
	c.Store.Backend = ""
	err = c.Validate()
	require.ErrorContains(t, err, "store.backend")
	require.ErrorContains(t, err, "memory|sqlite|postgres|pebble|mongodb|aerospike", "the message lists every known backend")

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

	// Insecure-listener guard: no api_keys on a non-loopback listen is
	// refused unless api.insecure is set; loopback needs no such flag; a
	// non-empty api_keys list never needs it either.
	c = base()
	c.API.Insecure = false
	c.API.Listen = ":8080"
	err = c.Validate()
	require.ErrorContains(t, err, "api.listen")
	require.ErrorContains(t, err, "insecure")

	c = base()
	c.API.Insecure = false
	c.API.Listen = "0.0.0.0:8080"
	require.ErrorContains(t, c.Validate(), "insecure")

	c = base()
	c.API.Insecure = false
	c.API.Listen = "127.0.0.1:8080"
	require.NoError(t, c.Validate(), "loopback needs no insecure flag")

	c = base()
	c.API.Insecure = false
	c.API.Listen = "localhost:8080"
	require.NoError(t, c.Validate(), "localhost is loopback")

	c = base()
	c.API.Insecure = false
	c.API.Listen = ":8080"
	c.API.APIKeys = []string{"k1"}
	require.NoError(t, c.Validate(), "a configured api_keys list never needs the insecure flag")

	c = base()
	c.API.Insecure = true
	c.API.Listen = ":8080"
	require.NoError(t, c.Validate(), "insecure: true allows an open non-loopback listener")

	// Empty api_keys/admin_keys entries (e.g. an unset "${K:-}") are rejected
	// outright rather than silently admitting an empty bearer token.
	c = base()
	c.API.APIKeys = []string{"k1", ""}
	require.ErrorContains(t, c.Validate(), "api.api_keys")

	c = base()
	c.API.APIKeys = []string{"k1"}
	c.API.AdminKeys = []string{""}
	require.ErrorContains(t, c.Validate(), "api.admin_keys")

	// api.listen's port must be numeric and in range.
	c = base()
	c.API.Listen = "127.0.0.1:0"
	require.NoError(t, c.Validate(), "port 0 lets the OS pick an ephemeral port; used by tests")

	c = base()
	c.API.Listen = "127.0.0.1:99999"
	require.ErrorContains(t, c.Validate(), "port")

	c = base()
	c.API.Listen = "127.0.0.1:abc"
	require.ErrorContains(t, c.Validate(), "port")

	// store.sqlite.sync is validated case-insensitively and normalized.
	c = base()
	c.Store.Backend = "sqlite"
	c.Store.SQLite.Path = "/tmp/does-not-need-to-exist.sqlite"
	c.Store.SQLite.Sync = "full"
	require.NoError(t, c.Validate())
	assert.Equal(t, sqlite.SyncFull, c.Store.SQLite.Sync, "accepted lowercase is normalized to canonical case")

	c = base()
	c.Store.Backend = "sqlite"
	c.Store.SQLite.Path = "/tmp/does-not-need-to-exist.sqlite"
	c.Store.SQLite.Sync = "NoRmAl"
	require.NoError(t, c.Validate())
	assert.Equal(t, sqlite.SyncNormal, c.Store.SQLite.Sync)

	c = base()
	c.Store.Backend = "sqlite"
	c.Store.SQLite.Path = "/tmp/does-not-need-to-exist.sqlite"
	c.Store.SQLite.Sync = "eventually"
	require.ErrorContains(t, c.Validate(), "store.sqlite.sync")

	_, err = config.Load("testdata/does-not-exist.yaml")
	require.Error(t, err)

	_, err = config.Parse([]byte("pools: [\n"))
	require.ErrorContains(t, err, "parse")
}

// TestParseRejectsSecondYAMLDocument guards against a "---"-separated second
// document silently being ignored: only the first document in the stream is
// ever decoded, so a second one (whatever it contains) is a config error, not
// something Parse quietly drops.
func TestParseRejectsSecondYAMLDocument(t *testing.T) {
	_, err := config.Parse([]byte("store:\n  backend: memory\napi:\n  insecure: true\npools:\n" +
		"  - name: main\n    denomination: 50\n---\nstore:\n  backend: memory\n"))
	require.ErrorContains(t, err, "multiple YAML documents")
}

// TestPoolDefaultsPreserveExplicitZero guards against a fill-if-zero
// post-decode pass mistaking an operator's explicit zero for "unset": a pool
// entry is pre-filled from pool.DefaultPoolConfig before its own YAML fields
// are decoded onto it (see the unexported rawPool type), so low_water: 0
// (documented as "always refill below target") must survive, while an
// omitted field (split_size) still gets its default.
func TestPoolDefaultsPreserveExplicitZero(t *testing.T) {
	c, err := config.Parse([]byte("store:\n  backend: memory\napi:\n  insecure: true\npools:\n" +
		"  - name: main\n    denomination: 50\n    low_water: 0\n"))
	require.NoError(t, err)
	require.Len(t, c.Pools, 1)
	assert.Equal(t, int64(0), c.Pools[0].LowWater, "explicit low_water: 0 must survive")
	assert.Equal(t, 500, c.Pools[0].SplitSize, "split_size omitted keeps the pool default")

	// instance.low_water_rows: 0 already survived before this change (Instance
	// is a single struct decoded onto its already-defaulted value, not a
	// freshly zeroed slice element) -- this is a regression guard, not a fix.
	c, err = config.Parse([]byte("instance:\n  low_water_rows: 0\nstore:\n  backend: memory\napi:\n  insecure: true\npools:\n" +
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
	config.ApplyPoolDefaults(&p, c.Oracle) // c.Oracle.Kind == "" (resolves like none: mock treasury, no oracle)
	c.Pools = []pool.Config{p}
	// Default().API.Listen (":8080") is not loopback-only and carries no
	// api_keys; opt into the insecure guard here since this test is about
	// SpendPolicy resolution, not the listener.
	c.API.Insecure = true
	require.NoError(t, c.Validate())
	assert.Equal(t, pool.SpendTrust, p.SpendPolicy)
}

func TestLoadExampleNullnet(t *testing.T) {
	c, err := config.Load("../../examples/nullnet.yaml")
	require.NoError(t, err)
	assert.Equal(t, "mock", c.Oracle.Kind)
	assert.True(t, c.Oracle.Mock.AssumeUnspent)
	assert.Equal(t, "mock", c.Treasury.Kind)
	assert.Equal(t, 500*time.Millisecond, c.Treasury.Mock.ConfirmAfter)
}

// TestLoadExamplePebble covers examples/pebble.yaml, the nullnet-style,
// single-process example for the pebble backend (see docs/OPERATIONS.md's
// "Deployment Topologies").
func TestLoadExamplePebble(t *testing.T) {
	c, err := config.Load("../../examples/pebble.yaml")
	require.NoError(t, err)
	assert.Equal(t, factory.BackendPebble, c.Store.Backend)
	assert.Equal(t, "./fuelkeeper-pebble", c.Store.Pebble.Path, "${VAR:-default} falls back when FUELKEEPER_STORE_PATH is unset")
	assert.False(t, c.Store.Pebble.NoSync)
	assert.Equal(t, "mock", c.Oracle.Kind)
	assert.True(t, c.Oracle.Mock.AssumeUnspent)
	assert.Equal(t, "mock", c.Treasury.Kind)
	assert.Equal(t, 500*time.Millisecond, c.Treasury.Mock.ConfirmAfter)

	t.Setenv("FUELKEEPER_STORE_PATH", filepath.Join(t.TempDir(), "pebble"))
	c, err = config.Load("../../examples/pebble.yaml")
	require.NoError(t, err)
	assert.NotEqual(t, "./fuelkeeper-pebble", c.Store.Pebble.Path, "${VAR} overrides the default when set")
}

func setTeratestnetEnv(t *testing.T) {
	t.Helper()
	t.Setenv("FUELKEEPER_TREASURY_KEY", "0000000000000000000000000000000000000000000000000000000000000001")
	t.Setenv("FUELKEEPER_API_KEY", "api")
	t.Setenv("FUELKEEPER_ADMIN_KEY", "admin")
	t.Setenv("FUELKEEPER_STORE_PATH", filepath.Join(t.TempDir(), "fk.sqlite"))
}

func TestLoadExampleTeratestnet(t *testing.T) {
	setTeratestnetEnv(t)
	c, err := config.Load("../../examples/teratestnet.yaml")
	require.NoError(t, err)
	assert.Equal(t, "arcade", c.Oracle.Kind)
	assert.Equal(t, "https://arcade-v2-ttn-us-1.bsvblockchain.tech", c.Oracle.ArcadeURL, "toolbox ttn default")
	assert.Empty(t, c.Oracle.TeranodeURL)
	tb := c.Treasury.Toolbox
	assert.Equal(t, "toolbox", c.Treasury.Kind)
	assert.Equal(t, "ttn", tb.Network)
	assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000001", tb.RootKeyHex)
	assert.Equal(t, "https://arcade-v2-ttn-us-1.bsvblockchain.tech/chaintracks", tb.ChaintracksURL)
	assert.Equal(t, 2*time.Minute, tb.ReconcileGrace)
	assert.Equal(t, 60*time.Second, c.Jobs.Minter.BuildTimeout)
	require.Len(t, c.Pools, 1)
	assert.Equal(t, 50, c.Pools[0].SplitSize)
	assert.Equal(t, int64(500), c.Pools[0].TargetAvailable)
	assert.Equal(t, pool.SpendTrust, c.Pools[0].SpendPolicy, "arcade-only oracle: spends are trusted")
	assert.Equal(t, pool.ReleaseTrust, c.Pools[0].ReleasePolicy)
}

// TestTeratestnetTeranodeOptIn is the YAML-literal form of uncommenting the
// composite/teranode_url opt-in block in examples/teratestnet.yaml (the
// file itself no longer wires teranode_url through an environment
// variable): configuring it makes the oracle answer outpoint queries, so
// spend_policy defaults to verify instead of trust.
func TestTeratestnetTeranodeOptIn(t *testing.T) {
	c, err := config.Parse([]byte(`
store: {backend: memory}
pools:
  - {name: main, denomination: 50, split_size: 50}
oracle:
  kind: composite
  teranode_url: http://teranode:8090/api/v1
treasury:
  kind: toolbox
  toolbox:
    root_key_hex: "0000000000000000000000000000000000000000000000000000000000000001"
api: {listen: "127.0.0.1:0"}
`))
	require.NoError(t, err)
	assert.Equal(t, "composite", c.Oracle.Kind)
	assert.Equal(t, "http://teranode:8090/api/v1", c.Oracle.TeranodeURL)
	assert.NotEmpty(t, c.Oracle.ArcadeURL, "composite falls back to the toolbox treasury's arcade too")
	assert.Equal(t, pool.SpendVerify, c.Pools[0].SpendPolicy, "with teranode, spends default to verify")
}

func TestLoadExampleTeratestnetNeedsTreasuryKey(t *testing.T) {
	setTeratestnetEnv(t)
	require.NoError(t, os.Unsetenv("FUELKEEPER_TREASURY_KEY"))
	_, err := config.Load("../../examples/teratestnet.yaml")
	require.ErrorContains(t, err, "FUELKEEPER_TREASURY_KEY")
}

func toolboxConfig(t *testing.T) *config.Config {
	t.Helper()
	setTeratestnetEnv(t)
	c, err := config.Load("../../examples/teratestnet.yaml")
	require.NoError(t, err)
	return c
}

func TestValidateOracleAndTreasuryFactories(t *testing.T) {
	c := toolboxConfig(t)
	c.Oracle.ArcadeURL = ""
	c.Treasury.Toolbox.ArcadeURL = "https://arcade.example"
	require.NoError(t, c.Validate())
	assert.Equal(t, "https://arcade.example", c.Oracle.ArcadeURL, "the oracle falls back to the treasury's arcade")

	c = toolboxConfig(t)
	c.Oracle.ArcadeURL = ""
	c.Treasury.Kind = "mock"
	require.ErrorContains(t, c.Validate(), "oracle.arcade_url is required", "no toolbox treasury to fall back to")

	c = toolboxConfig(t)
	c.Oracle.Kind = "teranode"
	require.ErrorContains(t, c.Validate(), "oracle.teranode_url is required")

	c = toolboxConfig(t)
	c.Treasury.Toolbox.Network = "test"
	c.Treasury.Toolbox.ChaintracksURL = ""
	require.ErrorContains(t, c.Validate(), "treasury.toolbox.chaintracks_url", "test has no public default")

	c = toolboxConfig(t)
	big := c.Pools[0]
	big.Name = "big"
	big.Denomination = 500
	c.Pools = append(c.Pools, big)
	require.ErrorContains(t, c.Validate(), "leaf cost", "one treasury funds one leaf cost class")

	c = toolboxConfig(t)
	c.Pools[0].SpendPolicy = pool.SpendVerify
	c.Oracle.Kind = "none"
	require.ErrorContains(t, c.Validate(), "spend_policy=verify requires an oracle")
}

func TestValidateReconcileGraceExceedsBuildTimeout(t *testing.T) {
	c := toolboxConfig(t)
	c.Treasury.Toolbox.ReconcileGrace = c.Jobs.Minter.BuildTimeout + jobs.ReconcileBudget
	err := c.Validate()
	require.ErrorContains(t, err, "treasury.toolbox.reconcile_grace")
	require.ErrorContains(t, err, "jobs.minter.build_timeout")
	require.ErrorContains(t, err, "reconcile budget")

	c.Treasury.Toolbox.ReconcileGrace = c.Jobs.Minter.BuildTimeout + jobs.ReconcileBudget + time.Second
	require.NoError(t, c.Validate())

	c.Jobs.Minter.BuildTimeout = 0
	require.ErrorContains(t, c.Validate(), "build_timeout")

	c = toolboxConfig(t)
	c.Jobs.LeaseTTL = c.Jobs.Minter.BuildTimeout + 2*jobs.ReconcileBudget
	err = c.Validate()
	require.ErrorContains(t, err, "jobs.lease_ttl")
	require.ErrorContains(t, err, "reconcile budget")
	c.Jobs.LeaseTTL += time.Second
	require.NoError(t, c.Validate())
	c.Jobs.LeaseTTL = time.Second
	c.Jobs.Enabled = false
	require.NoError(t, c.Validate(), "no jobs, no minter round")

	// The mock treasury does not reconcile: no constraint.
	c = toolboxConfig(t)
	c.Treasury.Kind = "mock"
	c.Treasury.Toolbox.ReconcileGrace = time.Second
	require.NoError(t, c.Validate())
}

func TestDefaultTreasuryToolboxIsTTN(t *testing.T) {
	c := config.Default()
	assert.Equal(t, "ttn", c.Treasury.Toolbox.Network)
	assert.Equal(t, "fuelkeeper", c.Treasury.Toolbox.Originator)
	assert.Equal(t, time.Minute, c.Jobs.Minter.BuildTimeout)
}

// TestOracleKindDefaultsToArcadeWithToolboxTreasury covers
// resolveOracleDefault's toolbox branch: an omitted oracle.kind resolves to
// arcade (never teranode or composite -- a direct teranode connection is
// never assumed) when a toolbox treasury is configured, and the oracle
// falls back to that treasury's own arcade endpoint. An arcade-only oracle
// answers no outpoint queries, so spend_policy still defaults to trust.
func TestOracleKindDefaultsToArcadeWithToolboxTreasury(t *testing.T) {
	c, err := config.Parse([]byte(`
store: {backend: memory}
pools:
  - {name: main, denomination: 50}
treasury:
  kind: toolbox
  toolbox:
    root_key_hex: "0000000000000000000000000000000000000000000000000000000000000001"
api: {listen: "127.0.0.1:0"}
`))
	require.NoError(t, err)
	assert.Equal(t, "arcade", c.Oracle.Kind)
	assert.NotEmpty(t, c.Oracle.ArcadeURL, "falls back to the toolbox treasury's arcade")
	assert.Equal(t, pool.SpendTrust, c.Pools[0].SpendPolicy, "arcade-only oracle: spends default to trust")
}

// TestOracleKindDefaultsToNoneWithMockTreasury covers resolveOracleDefault's
// other branch (mock treasury, i.e. nullnet: no chain to query) via both
// entry points -- Parse, and Validate alone for a programmatic caller that
// never calls Parse (config.Default() leaves Oracle.Kind empty).
func TestOracleKindDefaultsToNoneWithMockTreasury(t *testing.T) {
	c, err := config.Parse([]byte(`
store: {backend: memory}
pools:
  - {name: main, denomination: 50}
api: {listen: "127.0.0.1:0"}
`))
	require.NoError(t, err)
	assert.Equal(t, "none", c.Oracle.Kind)

	dc := config.Default()
	assert.Empty(t, dc.Oracle.Kind, "Default leaves the oracle kind unresolved")
	p := pool.DefaultPoolConfig("main", 50)
	config.ApplyPoolDefaults(&p, dc.Oracle)
	dc.Pools = []pool.Config{p}
	dc.API.Insecure = true
	require.NoError(t, dc.Validate())
	assert.Equal(t, "none", dc.Oracle.Kind, "Validate resolves it for a programmatic caller")
}

// TestExpiryPolicyParsesAndDefaults covers pool.Config.ExpiryPolicy end to
// end through YAML: an explicit value parses, an omitted one defaults to
// requeue (via ApplyPoolDefaults/rawPool), and an invalid one is rejected
// naming the pool.
func TestExpiryPolicyParsesAndDefaults(t *testing.T) {
	c, err := config.Parse(oracleYAML("  kind: none", "    expiry_policy: hold"))
	require.NoError(t, err)
	assert.Equal(t, pool.ExpiryHold, c.Pools[0].ExpiryPolicy)

	c, err = config.Parse(oracleYAML("  kind: none", ""))
	require.NoError(t, err)
	assert.Equal(t, pool.ExpiryRequeue, c.Pools[0].ExpiryPolicy, "omitted expiry_policy defaults to requeue")

	_, err = config.Parse(oracleYAML("  kind: none", "    expiry_policy: sometimes"))
	require.ErrorContains(t, err, "pool main")
	require.ErrorContains(t, err, "expiry_policy")
}

// TestReaperConfigKeysParse covers jobs.reaper's expiry-related keys
// (unknown_requeue_age, hint_max_wait, tx_recheck_interval) through YAML,
// and that jobs.reaper.Validate's bounds on them surface as config errors.
func TestReaperConfigKeysParse(t *testing.T) {
	c, err := config.Parse([]byte(`
store: {backend: memory}
pools:
  - {name: main, denomination: 50}
jobs:
  reaper:
    unknown_requeue_age: 5m
    hint_max_wait: 2h
    tx_recheck_interval: 15s
api: {listen: "127.0.0.1:0"}
`))
	require.NoError(t, err)
	assert.Equal(t, 5*time.Minute, c.Jobs.Reaper.UnknownRequeueAge)
	assert.Equal(t, 2*time.Hour, c.Jobs.Reaper.HintMaxWait)
	assert.Equal(t, 15*time.Second, c.Jobs.Reaper.TxRecheckInterval)

	// unknown_requeue_age must be 0 or >= min_unspent_age.
	_, err = config.Parse([]byte(`
store: {backend: memory}
pools:
  - {name: main, denomination: 50}
jobs:
  reaper:
    unknown_requeue_age: 1s
    min_unspent_age: 30s
api: {listen: "127.0.0.1:0"}
`))
	require.ErrorContains(t, err, "jobs.reaper")
	require.ErrorContains(t, err, "unknown_requeue_age")

	// tx_recheck_interval must be > 0.
	_, err = config.Parse([]byte(`
store: {backend: memory}
pools:
  - {name: main, denomination: 50}
jobs:
  reaper:
    tx_recheck_interval: 0s
api: {listen: "127.0.0.1:0"}
`))
	require.ErrorContains(t, err, "jobs.reaper")
	require.ErrorContains(t, err, "tx_recheck_interval")
}

// TestOperationsDocConfigBlocksLoad parses every fenced yaml block of
// docs/OPERATIONS.md with the strict loader, so the documented templates
// cannot drift from what FuelKeeper accepts.
func TestOperationsDocConfigBlocksLoad(t *testing.T) {
	raw, err := os.ReadFile("../../docs/OPERATIONS.md")
	require.NoError(t, err)
	blocks := regexp.MustCompile("(?s)```yaml\n(.*?)```").FindAllStringSubmatch(string(raw), -1)
	require.GreaterOrEqual(t, len(blocks), 2, "the full template and the nullnet example")
	for i, b := range blocks {
		_, err := config.Parse([]byte(b[1]))
		require.NoError(t, err, "docs/OPERATIONS.md yaml block %d", i+1)
	}
}

// oracleYAML is a minimal config with the given oracle block and pool extras.
func oracleYAML(oracleBlock, poolExtra string) []byte {
	return []byte(`
store: {backend: memory}
pools:
  - name: main
    denomination: 50
` + poolExtra + `
oracle:
` + oracleBlock + `
api: {listen: "127.0.0.1:0"}
`)
}

func TestSpendPolicyDefaultFollowsOutpointOracle(t *testing.T) {
	cases := []struct {
		oracle string
		want   pool.SpendPolicy
	}{
		{"  kind: none", pool.SpendTrust},
		{"  kind: mock", pool.SpendVerify},
		{"  kind: arcade\n  arcade_url: http://arcade", pool.SpendTrust},
		{"  kind: composite\n  arcade_url: http://arcade", pool.SpendTrust},
		{"  kind: composite\n  arcade_url: http://arcade\n  teranode_url: http://teranode", pool.SpendVerify},
	}
	for _, tc := range cases {
		c, err := config.Parse(oracleYAML(tc.oracle, ""))
		require.NoError(t, err, tc.oracle)
		assert.Equal(t, tc.want, c.Pools[0].SpendPolicy, tc.oracle)
	}

	// An explicit verify with an arcade-only oracle is kept (serve warns).
	c, err := config.Parse(oracleYAML("  kind: arcade\n  arcade_url: http://arcade", "    spend_policy: verify"))
	require.NoError(t, err)
	assert.Equal(t, pool.SpendVerify, c.Pools[0].SpendPolicy)
	assert.False(t, c.Oracle.AnswersOutpoints())
}

// TestReleaseVerifyAllowsArcadeOnlyOracle covers the relaxed rule: a
// release_policy: verify pool now loads with an arcade-only oracle (arcade,
// or composite without teranode_url) -- it is a delayed release, requeued
// after jobs.reaper.unknown_requeue_age, not an error -- exactly as it does
// with an oracle that answers outpoint queries directly. Only oracle.kind:
// none (no chain source at all) is still rejected.
func TestReleaseVerifyAllowsArcadeOnlyOracle(t *testing.T) {
	for _, o := range []string{
		"  kind: arcade\n  arcade_url: http://arcade",
		"  kind: composite\n  arcade_url: http://arcade",
		"  kind: composite\n  arcade_url: http://arcade\n  teranode_url: http://teranode",
		"  kind: mock",
	} {
		_, err := config.Parse(oracleYAML(o, "    release_policy: verify"))
		require.NoError(t, err, o)
	}
	_, err := config.Parse(oracleYAML("  kind: none", "    release_policy: verify"))
	require.ErrorContains(t, err, "release_policy=verify requires an oracle")
}

func TestTeranodeOnlyOracleRejectedWhenJobsRun(t *testing.T) {
	c, err := config.Parse(oracleYAML("  kind: teranode\n  teranode_url: http://teranode", ""))
	require.ErrorContains(t, err, "oracle.kind teranode cannot report transactions as mined")
	require.ErrorContains(t, err, "use kind: composite with arcade_url and teranode_url")
	assert.Nil(t, c)

	// Without jobs nothing waits for a mined verdict: teranode may answer
	// outpoint queries on its own.
	c, err = config.Parse(append(oracleYAML("  kind: teranode\n  teranode_url: http://teranode", "    release_policy: verify"),
		[]byte("jobs: {enabled: false}\n")...))
	require.NoError(t, err)
	assert.Equal(t, pool.SpendVerify, c.Pools[0].SpendPolicy)
}
