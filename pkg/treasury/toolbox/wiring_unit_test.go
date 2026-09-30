package toolbox

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/wdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
)

const testRootKey = "0000000000000000000000000000000000000000000000000000000000000001"

func wiringConfig() Config {
	c := DefaultConfig("ttn")
	c.RootKeyHex = testRootKey
	c.ArcadeURL = "https://arcade.invalid"
	c.ChaintracksURL = "https://arcade.invalid/chaintracks"
	return c
}

func wiringPools() []pool.Config {
	return []pool.Config{pool.DefaultPoolConfig("main", 50)}
}

// noNetwork makes any fee-policy lookup fail the test.
func noNetwork(t *testing.T) {
	t.Helper()
	old := policyFee
	policyFee = func(context.Context, string) (uint64, error) {
		t.Fatal("unexpected network use")
		return 0, nil
	}
	t.Cleanup(func() { policyFee = old })
}

func TestOpenFailsFastOnBadConfig(t *testing.T) {
	noNetwork(t)
	cases := map[string]func(*Config) []pool.Config{
		"no root key": func(c *Config) []pool.Config { c.RootKeyHex = ""; return wiringPools() },
		"bad network": func(c *Config) []pool.Config { c.Network = "moon"; return wiringPools() },
		"no arcade":   func(c *Config) []pool.Config { c.ArcadeURL = ""; return wiringPools() },
		"no pools":    func(*Config) []pool.Config { return nil },
		"two classes": func(*Config) []pool.Config {
			return []pool.Config{pool.DefaultPoolConfig("a", 50), pool.DefaultPoolConfig("b", 500)}
		},
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			c := wiringConfig()
			c.Storage.SQLite = t.TempDir() + "/never.sqlite"
			pools := mut(&c)
			tr, err := Open(context.Background(), c, pools, nil)
			require.Error(t, err)
			assert.Nil(t, tr)
			assert.NotContains(t, err.Error(), testRootKey)
		})
	}
}

func TestServicesConfig(t *testing.T) {
	c := wiringConfig()
	id, err := wdk.IdentityKey(c.RootKeyHex)
	require.NoError(t, err)

	s, err := servicesConfig(c, defs.NetworkTTN, id)
	require.NoError(t, err)
	assert.True(t, s.Arcade.Enabled)
	assert.Equal(t, c.ArcadeURL, s.Arcade.URL)
	assert.Equal(t, c.ArcadeURL, s.Arcade.EventsURL, "events default to the arcade URL")
	assert.Equal(t, wdk.DeriveArcadeCallbackToken(id), s.Arcade.CallbackToken)
	assert.True(t, s.ChaintracksClient.Enabled)
	assert.Equal(t, c.ChaintracksURL, s.ChaintracksClient.RemoteURL)

	c.ArcadeEvents = "https://events.invalid"
	s, err = servicesConfig(c, defs.NetworkTestnet, id)
	require.NoError(t, err)
	assert.Equal(t, "https://events.invalid", s.Arcade.EventsURL)
	assert.True(t, s.Arcade.Enabled, "enabled even where the toolbox default is off")
}

func TestDBConfig(t *testing.T) {
	db := dbConfig(StorageConfig{Engine: "sqlite", SQLite: "/tmp/t.sqlite"})
	assert.Equal(t, defs.DBTypeSQLite, db.Engine)
	assert.Equal(t, "/tmp/t.sqlite", db.SQLite.ConnectionString)

	db = dbConfig(StorageConfig{Engine: "postgres", Postgres: PostgresConfig{Host: "h", Port: 6543, DBName: "d", User: "u", Password: "p", SSLMode: "disable"}})
	assert.Equal(t, defs.DBTypePostgres, db.Engine)
	pg := db.PostgreSQL
	assert.Equal(t, []string{"h", "6543", "d", "u", "p", "disable"}, []string{pg.Host, pg.Port, pg.DBName, pg.User, pg.Password, pg.SslMode})
	require.NoError(t, db.Validate())

	db = dbConfig(StorageConfig{Engine: "postgres", Postgres: PostgresConfig{Host: "h", DBName: "d"}})
	def := defs.DefaultDBConfig().PostgreSQL
	assert.Equal(t, def.Port, db.PostgreSQL.Port, "port 0 keeps the default")
	assert.Equal(t, def.SslMode, db.PostgreSQL.SslMode, "empty sslmode keeps the default")
}

func TestUTXOManagementAndKeeperAgree(t *testing.T) {
	c := wiringConfig()
	c.Chunking.TargetChunks = 70
	c.Chunking.LowWaterPercent = 40
	c.Chunking.FanoutOutputsPerTx = 25
	c.Chunking.Interval = 7 * time.Second
	c.Originator = "fk-test"
	p := wiringPools()[0]
	fee := defs.FeeModel{Type: defs.SatPerKB, Value: 100}
	chunk := c.ChunkValue(p, 100)

	um, err := utxoManagement(c, chunk, fee)
	require.NoError(t, err)
	th := um.Throughput
	assert.True(t, um.Enabled())
	assert.Equal(t, chunk, th.DenominationSatoshis)
	assert.Equal(t, "chunks", th.PoolBasket)
	assert.Equal(t, "reserve", th.ReserveBasket)
	assert.Equal(t, uint64(25), th.FanoutOutputsPerTx)
	assert.Equal(t, uint64(fanoutMaxTxsPerRound), th.FanoutMaxTxsPerRound)
	assert.Equal(t, defs.SpendPolicyPreferMined, th.SpendPolicy)
	assert.Equal(t, uint64(70), th.TargetPool())
	assert.Equal(t, uint64(40), th.LowWaterPercent)
	assert.Equal(t, uint64(100), th.HighWaterPercent)
	assert.False(t, th.TopUp.Enabled)

	k := keeperConfig(c, um, chunk, 100)
	assert.Equal(t, chunk, k.Denomination)
	assert.Equal(t, uint64(70), k.TargetPoolSize)
	assert.Equal(t, uint64(40), k.LowWaterPercent)
	assert.Equal(t, uint64(100), k.HighWaterPercent)
	assert.Equal(t, uint64(25), k.FanoutOutputsPerTx)
	assert.Equal(t, uint64(fanoutMaxTxsPerRound), k.FanoutMaxTxsPerRound)
	assert.Equal(t, "chunks", k.PoolBasket)
	assert.Equal(t, "reserve", k.ReserveBasket)
	assert.Equal(t, 7*time.Second, k.Interval)
	assert.Equal(t, "fk-test", k.Originator)
	assert.Equal(t, max(uint64(1000), 8*chunk), k.ChunkFeeHeadroom, "FromThroughput's headroom at a low rate")

	// At a high rate, twice the fan-out fee wins: 25 P2PKH outputs are
	// 10 + 148 + 25 × 34 + 34 = 1042 bytes.
	assert.Equal(t, 1042, fanoutSizeBytes(25))
	k = keeperConfig(c, um, chunk, 1_000_000)
	assert.Equal(t, 2*uint64(1_042_000), k.ChunkFeeHeadroom)
}

func TestUTXOManagementRejectsDenominationBelowInputFee(t *testing.T) {
	// A chunk that cannot even pay for its own input at the fee rate.
	_, err := utxoManagement(wiringConfig(), 10, defs.FeeModel{Type: defs.SatPerKB, Value: 100})
	require.Error(t, err)
}

func TestResolveFee(t *testing.T) {
	c := wiringConfig()
	c.FeeSatPerKB = 7
	noNetwork(t)
	fee, err := resolveFee(context.Background(), c)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), fee)
}

func TestResolveFeeFromArcadePolicy(t *testing.T) {
	body := `{"policy":{"miningFee":{"satoshis":1,"bytes":1000}},"timestamp":"x"}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/policy" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()
	c := wiringConfig()
	c.ArcadeURL = srv.URL

	fee, err := resolveFee(context.Background(), c)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), fee)

	body = `{"policy":{"miningFee":{"satoshis":0,"bytes":1000}}}`
	fee, err = resolveFee(context.Background(), c)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), fee, "floor 1")

	body = `{"policy":{"miningFee":{"satoshis":50,"bytes":1000}}}`
	fee, err = resolveFee(context.Background(), c)
	require.NoError(t, err)
	assert.Equal(t, uint64(50), fee)
}

func TestResolveFeePolicyError(t *testing.T) {
	old := policyFee
	defer func() { policyFee = old }()
	policyFee = func(context.Context, string) (uint64, error) { return 0, errors.New("down") }
	_, err := resolveFee(context.Background(), wiringConfig())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fee_sat_per_kb")
}

func TestTreasuryCloseIsIdempotentOnPartialOpen(t *testing.T) {
	cancelled := 0
	tr := &Treasury{cancel: func() { cancelled++ }}
	require.NoError(t, tr.Close())
	require.NoError(t, tr.Close())
	assert.Equal(t, 1, cancelled)
}

// TestOpenLocalStack opens the whole toolbox stack against a local stand-in
// for arcade and chaintracks (every endpoint 404s) and an empty sqlite
// wallet: services, migrated storage, wallet, monitor and keeper come up,
// the funding handshake round-trips, an unfunded wallet reports
// ErrInsufficientFunds, and Close tears everything down.
func TestOpenLocalStack(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	c := wiringConfig()
	c.ArcadeURL = srv.URL
	c.ChaintracksURL = srv.URL + "/chaintracks"
	c.FeeSatPerKB = 1
	c.Storage.SQLite = t.TempDir() + "/treasury.sqlite"
	noNetwork(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tr, err := Open(ctx, c, wiringPools(), nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, tr.Close()) }()
	assert.Equal(t, uint64(1), tr.FeeSatPerKB())
	assert.Equal(t, c.ChunkValue(wiringPools()[0], 1), tr.ChunkValue())

	fi, err := tr.FundingInfo(ctx)
	require.NoError(t, err)
	assert.Equal(t, "ttn", fi.Network)
	id, err := wdk.IdentityKey(testRootKey)
	require.NoError(t, err)
	assert.Equal(t, id, fi.IdentityKeyHex)
	ok, err := tr.nonces.Verify(ctx, fi.DerivationPrefixB64)
	require.NoError(t, err)
	assert.True(t, ok)

	require.NoError(t, tr.EnsureChunks(ctx), "an unfunded keeper round is not an error")
	_, err = tr.Build(ctx, treasury.MintRequest{Pool: "main", Denomination: 50, Count: 500})
	require.ErrorIs(t, err, treasury.ErrInsufficientFunds)

	rep, err := tr.Reconcile(ctx, func(context.Context, chainhash.Hash) (bool, error) {
		t.Fatal("no splits to look up")
		return false, nil
	})
	require.NoError(t, err)
	assert.Empty(t, rep.Resent)
	assert.Empty(t, rep.Aborted)
}
