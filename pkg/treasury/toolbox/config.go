// Package toolbox is the go-wallet-toolbox treasury: it receives BRC-29 payments
// and mints fuel splits with the embedded wallet.
//
// The core (Core) is everything that needs no live network: building a split
// from a createAction result (puzzle rows or presigned P2PKH rows), sending it
// separately from building it, mined-BEEF conversion, the BRC-29 funding
// handshake, and chunk maintenance delegated to the toolbox's own keeper. It
// talks to the wallet through the narrow WalletAPI interface.
//
// v1 constraint: the toolbox's throughput UTXO strategy keeps one chunk
// denomination per wallet storage, so one treasury funds only pools whose leaf
// cost class (split_size × denomination + leaf fee, see LeafCost) is identical.
// Config.Validate enforces it. Deployments with several denominations run one
// FuelKeeper per class.
package toolbox

import (
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// Config configures the toolbox treasury.
type Config struct {
	// Network is main | test | ttn | tstn (the toolbox's defs.BSVNetwork values).
	Network string `yaml:"network"`
	// RootKeyHex is the wallet root private key, 64 hex chars (e.g. ${FUELKEEPER_TREASURY_KEY}).
	RootKeyHex string `yaml:"root_key_hex"`
	// Originator is the BRC-100 originator the treasury uses; default "fuelkeeper".
	Originator string `yaml:"originator"`
	// FeeSatPerKB is the fee rate; 0 = read arcade /policy at start, floor 1.
	FeeSatPerKB uint64 `yaml:"fee_sat_per_kb"`
	// ArcadeURL is the arcade broadcast endpoint.
	ArcadeURL string `yaml:"arcade_url"`
	// ArcadeEvents is the arcade status events endpoint.
	ArcadeEvents string `yaml:"arcade_events_url"`
	// ChaintracksURL is the header service used for SPV.
	ChaintracksURL string `yaml:"chaintracks_url"`
	// Storage selects the wallet storage.
	Storage StorageConfig `yaml:"storage"`
	// Chunking tunes the toolbox's chunk keeper.
	Chunking ChunkConfig `yaml:"chunking"`
	// ReconcileGrace separates Reconcile's two listings of no-send splits; it
	// must exceed the longest Build to PutShard window. Default 2m.
	ReconcileGrace time.Duration `yaml:"reconcile_grace"`
}

// StorageConfig selects the toolbox wallet storage.
type StorageConfig struct {
	// Engine is sqlite | postgres.
	Engine string `yaml:"engine"`
	// SQLite is the sqlite database path.
	SQLite string `yaml:"sqlite_path"`
	// Postgres is the postgres connection.
	Postgres PostgresConfig `yaml:"postgres"`
}

// PostgresConfig is a postgres connection for the toolbox storage.
type PostgresConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	DBName   string `yaml:"dbname"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"sslmode"`
}

// ChunkConfig tunes how many leaf-sized chunks the toolbox keeps ready.
type ChunkConfig struct {
	// FanoutOutputsPerTx is the number of chunks per fan-out tx (M), default 100.
	FanoutOutputsPerTx int `yaml:"fanout_outputs_per_tx"`
	// TargetChunks is how many leaf-sized chunks to keep ready, default 50.
	TargetChunks int `yaml:"target_chunks"`
	// LowWaterPercent refills below this share of TargetChunks, default 60.
	LowWaterPercent int `yaml:"low_water_percent"`
	// HeadroomSats is added to each chunk so the leaf's surplus stays below dust, default 20.
	HeadroomSats uint64 `yaml:"headroom_sats"`
	// Interval is the keeper cadence, default 10s.
	Interval time.Duration `yaml:"interval"`
	// ReserveBasket holds the fan-out reserve, default "reserve".
	ReserveBasket string `yaml:"reserve_basket"`
	// ChunkBasket holds the leaf-sized chunks, default "chunks".
	ChunkBasket string `yaml:"chunk_basket"`
}

// DefaultConfig returns the defaults for network net.
func DefaultConfig(net string) Config {
	return Config{
		Network:        net,
		Originator:     "fuelkeeper",
		ReconcileGrace: 2 * time.Minute,
		Storage:        StorageConfig{Engine: "sqlite", SQLite: "treasury.sqlite"},
		Chunking: ChunkConfig{
			FanoutOutputsPerTx: 100,
			TargetChunks:       50,
			LowWaterPercent:    60,
			HeadroomSats:       20,
			Interval:           10 * time.Second,
			ReserveBasket:      "reserve",
			ChunkBasket:        "chunks",
		},
	}
}

// defaultBasket is the wallet's own change basket; the chunk baskets must not
// collide with it.
const defaultBasket = "default"

// defaultLeafFeeSatPerKB is the fee rate LeafCost assumes when given 0.
const defaultLeafFeeSatPerKB = 100

// Validate checks the config against the pools this treasury funds.
func (c *Config) Validate(pools []pool.Config) error {
	switch c.Network {
	case "main", "test", "ttn", "tstn":
	default:
		return fmt.Errorf("treasury.toolbox.network %q must be one of main|test|ttn|tstn", c.Network)
	}
	if err := validateRootKey(c.RootKeyHex); err != nil {
		return err
	}
	if c.Originator == "" {
		return errors.New("treasury.toolbox.originator is required")
	}
	if c.ArcadeURL == "" {
		return errors.New("treasury.toolbox.arcade_url is required")
	}
	if c.ChaintracksURL == "" {
		return errors.New("treasury.toolbox.chaintracks_url is required")
	}
	if err := c.Storage.validate(); err != nil {
		return err
	}
	if err := c.Chunking.validate(); err != nil {
		return err
	}
	if c.ReconcileGrace <= 0 {
		return errors.New("treasury.toolbox.reconcile_grace must be > 0")
	}
	if len(pools) == 0 {
		return errors.New("treasury.toolbox: at least one pool is required")
	}
	// The class is compared without a fee rate: two pools share a leaf chunk
	// value at every rate exactly when their outputs total and their leaf
	// size are both equal.
	first := pools[0]
	for _, p := range pools[1:] {
		if leafOutputs(p) != leafOutputs(first) || LeafSizeBytes(p) != LeafSizeBytes(first) {
			return fmt.Errorf("treasury.toolbox: pools %q (leaf outputs %d sat, leaf size %d B) and %q (leaf outputs %d sat, leaf size %d B) differ in leaf cost: one treasury funds one leaf cost class (run one FuelKeeper per class)",
				first.Name, leafOutputs(first), LeafSizeBytes(first), p.Name, leafOutputs(p), LeafSizeBytes(p))
		}
	}
	return nil
}

// validateRootKey checks that s is a 32-byte hex private key. The key itself
// never appears in an error message.
func validateRootKey(s string) error {
	if len(s) != 64 {
		return fmt.Errorf("treasury.toolbox.root_key_hex must be 64 hex chars, got %d", len(s))
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return errors.New("treasury.toolbox.root_key_hex is not valid hex")
	}
	for _, x := range b {
		if x != 0 {
			return nil
		}
	}
	return errors.New("treasury.toolbox.root_key_hex must not be zero")
}

func (s StorageConfig) validate() error {
	switch s.Engine {
	case "sqlite":
		if s.SQLite == "" {
			return errors.New("treasury.toolbox.storage.sqlite_path is required for engine sqlite")
		}
	case "postgres":
		if s.Postgres.Host == "" {
			return errors.New("treasury.toolbox.storage.postgres.host is required for engine postgres")
		}
		if s.Postgres.DBName == "" {
			return errors.New("treasury.toolbox.storage.postgres.dbname is required for engine postgres")
		}
		if s.Postgres.Port < 0 || s.Postgres.Port > 65535 {
			return fmt.Errorf("treasury.toolbox.storage.postgres.port %d must be 0..65535 (0 = default)", s.Postgres.Port)
		}
	default:
		return fmt.Errorf("treasury.toolbox.storage.engine %q must be sqlite or postgres", s.Engine)
	}
	return nil
}

func (k ChunkConfig) validate() error {
	if k.FanoutOutputsPerTx <= 0 {
		return errors.New("treasury.toolbox.chunking.fanout_outputs_per_tx must be > 0")
	}
	if k.TargetChunks <= 0 {
		return errors.New("treasury.toolbox.chunking.target_chunks must be > 0")
	}
	if k.LowWaterPercent < 1 || k.LowWaterPercent > 100 {
		return errors.New("treasury.toolbox.chunking.low_water_percent must be 1..100")
	}
	if k.Interval <= 0 {
		return errors.New("treasury.toolbox.chunking.interval must be > 0")
	}
	if k.ReserveBasket == "" {
		return errors.New("treasury.toolbox.chunking.reserve_basket is required")
	}
	if k.ChunkBasket == "" {
		return errors.New("treasury.toolbox.chunking.chunk_basket is required")
	}
	if k.ReserveBasket == k.ChunkBasket {
		return errors.New("treasury.toolbox.chunking: reserve_basket and chunk_basket must be distinct")
	}
	if k.ReserveBasket == defaultBasket || k.ChunkBasket == defaultBasket {
		return fmt.Errorf("treasury.toolbox.chunking: reserve_basket and chunk_basket must not be %q (the wallet's change basket)", defaultBasket)
	}
	return nil
}

// Size components in bytes. An output is an 8-byte value, a 1-byte script
// length and the script.
const (
	puzzleOutputSize = 9 + 35 // OP_SHA256 <32B> OP_EQUAL
	p2pkhOutputSize  = 9 + 25
	changeOutputSize = 34
	txFixedSize      = 8 // version + lockTime
	leafInputCount   = 1
)

// varIntSize is the size of Bitcoin's CompactSize encoding of n.
func varIntSize(n uint64) int {
	switch {
	case n < 0xfd:
		return 1
	case n <= 0xffff:
		return 3
	case n <= 0xffffffff:
		return 5
	default:
		return 9
	}
}

// LeafSizeBytes estimates a leaf split: one P2PKH input, SplitSize fuel
// outputs and one change output, including the CompactSize input and output
// counts (3 bytes once there are 253 or more outputs).
func LeafSizeBytes(p pool.Config) int {
	n := max(p.SplitSize, 0)
	out := puzzleOutputSize
	if p.LockMode == store.LockP2PKH {
		out = p2pkhOutputSize
	}
	return txFixedSize + varIntSize(leafInputCount) + varIntSize(uint64(n)+1) + //nolint:gosec // n >= 0
		fuel.P2PKHInputSize + n*out + changeOutputSize
}

// leafOutputs is the fuel value one leaf split creates.
func leafOutputs(p pool.Config) uint64 {
	return uint64(max(p.SplitSize, 0)) * p.Denomination //nolint:gosec // non-negative
}

// LeafCost is what one leaf split needs from its funding chunk (outputs + fee);
// satPerKB 0 means 100.
func LeafCost(p pool.Config, satPerKB uint64) uint64 {
	if satPerKB == 0 {
		satPerKB = defaultLeafFeeSatPerKB
	}
	return leafOutputs(p) + fuel.FeeSats(LeafSizeBytes(p), satPerKB)
}

// ChunkValue is the exact-denomination coin the toolbox keeps ready for leaves.
func (c *Config) ChunkValue(p pool.Config, satPerKB uint64) uint64 {
	return LeafCost(p, satPerKB) + c.Chunking.HeadroomSats
}

// redacted replaces secrets in String, GoString and LogValue output.
const redacted = "[REDACTED]"

// String renders the config with RootKeyHex and the postgres password
// redacted, so a config printed with %v or %+v never leaks them.
func (c Config) String() string {
	c.RootKeyHex = redactIfSet(c.RootKeyHex)
	c.Storage.Postgres.Password = redactIfSet(c.Storage.Postgres.Password)
	type plain Config // no methods: avoids recursing into String
	return fmt.Sprintf("%+v", plain(c))
}

// GoString is String for %#v.
func (c Config) GoString() string { return c.String() }

// LogValue redacts secrets in structured logs.
func (c Config) LogValue() slog.Value { return slog.StringValue(c.String()) }

// String renders the postgres config with the password redacted.
func (p PostgresConfig) String() string {
	p.Password = redactIfSet(p.Password)
	type plain PostgresConfig
	return fmt.Sprintf("%+v", plain(p))
}

// GoString is String for %#v.
func (p PostgresConfig) GoString() string { return p.String() }

// LogValue redacts the password in structured logs.
func (p PostgresConfig) LogValue() slog.Value { return slog.StringValue(p.String()) }

func redactIfSet(s string) string {
	if s == "" {
		return ""
	}
	return redacted
}
