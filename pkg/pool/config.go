package pool

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// poolNamePattern restricts a pool name to what can be safely routed in a URL
// path segment (e.g. GET /v1/pools/{pool}/...): it must start with a letter
// or digit and contain only letters, digits, '_', '.' and '-', up to 64
// bytes total (matching store.ValidatePool's length limit). Because the
// first character must be alphanumeric, this also rejects the reserved path
// segments "." and ".." outright, which cannot be routed.
var poolNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

// ServePolicy says which shard statuses an instance may serve from.
type ServePolicy string

// Serve policies.
const (
	// ServeMinedOnly serves only from mined shards.
	ServeMinedOnly ServePolicy = "mined_only"
	// ServeAny serves from mined or unmined shards.
	ServeAny ServePolicy = "any"
)

// ReleasePolicy says whether a client Release is trusted.
type ReleasePolicy string

// Release policies.
const (
	// ReleaseTrust moves a released row straight back to available.
	ReleaseTrust ReleasePolicy = "trust" // reserved → available immediately
	// ReleaseVerify quarantines a released row until the reaper asks the oracle.
	ReleaseVerify ReleasePolicy = "verify" // reserved → quarantined; the reaper asks the oracle
)

// SpendPolicy says whether a client Spend is a hint or a fact.
type SpendPolicy string

// Spend policies.
const (
	// SpendVerify quarantines a spent row (as a hint) until the oracle proves it spent.
	SpendVerify SpendPolicy = "verify" // reserved → quarantined(hint); oracle proves spent
	// SpendTrust moves a spent row straight to spent.
	SpendTrust SpendPolicy = "trust" // reserved → spent
)

// ExpiryPolicy says what the reaper does with a quarantined row that has no
// spend hint and no oracle verdict.
type ExpiryPolicy string

// Expiry policies.
const (
	// ExpiryRequeue returns such a row to the pool once it has been
	// quarantined for the reaper's unknown_requeue_age.
	ExpiryRequeue ExpiryPolicy = "requeue"
	// ExpiryHold keeps such a row quarantined for an operator.
	ExpiryHold ExpiryPolicy = "hold"
)

// Config describes one pool (one denomination).
type Config struct {
	// Name is the pool name (see store.ValidatePool).
	Name string `yaml:"name"`
	// Denomination is the satoshi value of every row in this pool.
	Denomination uint64 `yaml:"denomination"`
	// LockMode is resolved from LockModeName by Validate.
	LockMode store.LockMode `yaml:"-"`
	// LockModeName is "puzzle" | "p2pkh"; parsed into LockMode by Validate.
	LockModeName string `yaml:"lock_mode"`
	// SplitSize is the number of outputs created per split transaction.
	SplitSize int `yaml:"split_size"`
	// TargetAvailable is the desired count of available rows.
	TargetAvailable int64 `yaml:"target_available"`
	// LowWater triggers a refill when available rows drop below it (0 means
	// always refill below TargetAvailable). Once triggered, the minter's
	// refill is sticky, not a per-round recheck: it keeps minting -- even
	// across rounds where a cap (max_per_round, max_pending_shards) leaves
	// available still short and a later round finds available back at or
	// above LowWater -- until available actually reaches TargetAvailable,
	// only then stopping until the next dip below LowWater. That refill
	// state lives only in the minter's process memory (see jobs.Minter), so
	// it does not survive a restart.
	LowWater int64 `yaml:"low_water"`
	// ServePolicy says which shard statuses may be served from.
	ServePolicy ServePolicy `yaml:"serve_policy"`
	// DefaultTTL is the reservation TTL used when a caller does not specify one.
	DefaultTTL time.Duration `yaml:"default_ttl"`
	// MinTTL is the shortest reservation TTL a caller may request.
	MinTTL time.Duration `yaml:"min_ttl"`
	// MaxTTL is the longest reservation TTL a caller may request.
	MaxTTL time.Duration `yaml:"max_ttl"`
	// MaxCount is the largest number of outputs a single Reserve call may request.
	MaxCount int `yaml:"max_count"`
	// ReleasePolicy says whether a client Release is trusted.
	ReleasePolicy ReleasePolicy `yaml:"release_policy"`
	// SpendPolicy says whether a client Spend is a hint or a fact.
	SpendPolicy SpendPolicy `yaml:"spend_policy"`
	// ExpiryPolicy is what the reaper does with a quarantined row that has no
	// spend hint and no oracle verdict: `requeue` returns it to the pool
	// after the reaper's `unknown_requeue_age`; `hold` keeps it quarantined
	// for an operator.
	ExpiryPolicy ExpiryPolicy `yaml:"expiry_policy"`
	// ExpectedTPS is the pool's expected spend rate; optional (0 disables the
	// check) and, when set, enables the mint-capacity check in pkg/config.
	ExpectedTPS float64 `yaml:"expected_tps"`
	// SpendBatching groups concurrent spend reports (and release_policy=verify
	// releases) into one durable store write per flush; see SpendCoalescer.
	SpendBatching SpendBatchingConfig `yaml:"spend_batching"`
}

// SpendBatchingConfig tunes a pool's spend group commit. It only takes effect
// on a store that implements store.SpendBatcher (memory, sqlite, pebble,
// postgres); any other store gets one Spend call per request regardless.
type SpendBatchingConfig struct {
	// Enabled turns spend batching on (the default).
	Enabled bool `yaml:"enabled"`
	// MaxBatch caps the outpoints one flush carries, summed over its
	// requests; 0 means the store's max batch. It is raised to the pool's
	// max_count (one request always fits) and capped at the store's max batch.
	MaxBatch int `yaml:"max_batch"`
	// Window optionally holds a flush open this long, once a slot is free, to
	// gather more requests; 0 (the default) flushes at once -- requests that
	// arrive while a flush is in flight still join the next one.
	Window time.Duration `yaml:"window"`
}

// DefaultPoolConfig returns the spec defaults for a puzzle pool. Its
// SpendPolicy (SpendVerify) assumes an oracle is configured; a caller with no
// oracle should resolve it via config.ApplyPoolDefaults, which picks
// SpendTrust instead.
func DefaultPoolConfig(name string, denomination uint64) Config {
	return Config{
		Name:            name,
		Denomination:    denomination,
		LockMode:        store.LockPuzzle,
		LockModeName:    "puzzle",
		SplitSize:       500,
		TargetAvailable: 100_000,
		LowWater:        60_000,
		ServePolicy:     ServeMinedOnly,
		DefaultTTL:      2 * time.Minute,
		MinTTL:          30 * time.Second,
		MaxTTL:          10 * time.Minute,
		MaxCount:        100,
		ReleasePolicy:   ReleaseTrust,
		SpendPolicy:     SpendVerify,
		ExpiryPolicy:    ExpiryRequeue,
		SpendBatching:   SpendBatchingConfig{Enabled: true},
	}
}

// Validate checks the pool config and resolves LockModeName.
func (c *Config) Validate() error {
	if err := store.ValidatePool(c.Name); err != nil {
		return err
	}
	if !poolNamePattern.MatchString(c.Name) {
		return fmt.Errorf("pool %s: name must match %s (and not be \".\" or \"..\", which cannot be routed)", c.Name, poolNamePattern.String())
	}
	if c.Denomination == 0 {
		return fmt.Errorf("pool %s: denomination must be > 0", c.Name)
	}
	switch c.LockModeName {
	case "":
		// Leave LockMode as already set (its zero value is store.LockPuzzle).
	case "puzzle":
		c.LockMode = store.LockPuzzle
	case "p2pkh":
		c.LockMode = store.LockP2PKH
	default:
		return fmt.Errorf("pool %s: unknown lock_mode %q", c.Name, c.LockModeName)
	}
	if c.SplitSize <= 0 || c.SplitSize > 10_000 {
		return fmt.Errorf("pool %s: split_size must be 1..10000", c.Name)
	}
	if c.TargetAvailable <= 0 || c.LowWater < 0 || c.LowWater > c.TargetAvailable {
		return fmt.Errorf("pool %s: need 0 <= low_water <= target_available and target_available > 0", c.Name)
	}
	switch c.ServePolicy {
	case ServeMinedOnly, ServeAny:
	default:
		return fmt.Errorf("pool %s: serve_policy must be mined_only or any", c.Name)
	}
	if c.MinTTL <= 0 || c.MaxTTL < c.MinTTL || c.DefaultTTL < c.MinTTL || c.DefaultTTL > c.MaxTTL {
		return fmt.Errorf("pool %s: need 0 < min_ttl <= default_ttl <= max_ttl", c.Name)
	}
	if c.MaxCount <= 0 {
		return fmt.Errorf("pool %s: max_count must be > 0", c.Name)
	}
	switch c.ReleasePolicy {
	case ReleaseTrust, ReleaseVerify:
	default:
		return fmt.Errorf("pool %s: release_policy must be trust or verify", c.Name)
	}
	switch c.SpendPolicy {
	case SpendVerify, SpendTrust:
	default:
		return fmt.Errorf("pool %s: spend_policy must be verify or trust", c.Name)
	}
	switch c.ExpiryPolicy {
	case ExpiryRequeue, ExpiryHold:
	default:
		return fmt.Errorf("pool %s: expiry_policy must be requeue or hold, got %q", c.Name, c.ExpiryPolicy)
	}
	if c.ExpectedTPS < 0 {
		return fmt.Errorf("pool %s: expected_tps must be >= 0", c.Name)
	}
	if c.SpendBatching.MaxBatch < 0 || c.SpendBatching.Window < 0 {
		return fmt.Errorf("pool %s: spend_batching max_batch and window must be >= 0", c.Name)
	}
	return nil
}

// ServeStatuses maps the serve policy to shard statuses for AcquireShard.
func (c Config) ServeStatuses() []store.ShardStatus {
	if c.ServePolicy == ServeAny {
		return []store.ShardStatus{store.ShardMined, store.ShardUnmined}
	}
	return []store.ShardStatus{store.ShardMined}
}

// maxInstanceIDLen bounds InstanceConfig.InstanceID so that NewHolder's output
// (instance_id + "/" + 8 hex chars, a 9-byte suffix) always fits within
// store.ValidateHolder's 128-byte limit.
const maxInstanceIDLen = 119

// InstanceConfig tunes one FuelKeeper process.
type InstanceConfig struct {
	// InstanceID defaults to the hostname; the holder adds a boot nonce (see NewHolder).
	InstanceID string `yaml:"instance_id"`
	// MaxShards is the most shards this instance will own at once.
	MaxShards int `yaml:"max_shards"`
	// LowWaterRows adopts another shard below this many in-memory rows.
	LowWaterRows int `yaml:"low_water_rows"`
	// LeaseTTL is how long a shard lease is held before it must be renewed.
	LeaseTTL time.Duration `yaml:"lease_ttl"`
	// RescanInterval is how often the shard manager rescans for orphaned shards.
	RescanInterval time.Duration `yaml:"rescan_interval"`
	// FlushWindow batches Reserve calls; 0 flushes as soon as a slot is free.
	FlushWindow time.Duration `yaml:"flush_window"`
	// MaxBatch caps ops per flush; 0 means min(256, store MaxBatch).
	MaxBatch int `yaml:"max_batch"`
	// ReserveTimeout bounds a single Reserve call.
	ReserveTimeout time.Duration `yaml:"reserve_timeout"`
	// FlushTimeout bounds a single flush to the store.
	FlushTimeout time.Duration `yaml:"flush_timeout"`
	// AdoptTimeout bounds synchronous adoption inside Reserve.
	AdoptTimeout time.Duration `yaml:"adopt_timeout"`
}

// DefaultInstanceConfig returns the spec defaults.
func DefaultInstanceConfig() InstanceConfig {
	host, _ := os.Hostname()
	if host == "" {
		host = "fuelkeeper"
	}
	return InstanceConfig{
		InstanceID:     host,
		MaxShards:      8,
		LowWaterRows:   5000,
		LeaseTTL:       30 * time.Second,
		RescanInterval: 5 * time.Second,
		FlushWindow:    0,
		MaxBatch:       0,
		ReserveTimeout: 2 * time.Second,
		FlushTimeout:   5 * time.Second,
		AdoptTimeout:   200 * time.Millisecond,
	}
}

// Validate checks the instance config.
func (c InstanceConfig) Validate() error {
	if c.InstanceID == "" {
		return fmt.Errorf("instance_id is required")
	}
	if len(c.InstanceID) > maxInstanceIDLen {
		return fmt.Errorf("instance_id must be at most %d bytes (holder adds a 9-byte boot nonce)", maxInstanceIDLen)
	}
	if c.MaxShards <= 0 || c.LowWaterRows < 0 {
		return fmt.Errorf("max_shards must be > 0 and low_water_rows >= 0")
	}
	if c.LeaseTTL <= 0 || c.RescanInterval <= 0 || c.RescanInterval >= c.LeaseTTL {
		return fmt.Errorf("need 0 < rescan_interval < lease_ttl")
	}
	if c.FlushWindow < 0 {
		return fmt.Errorf("flush_window must be >= 0")
	}
	if c.MaxBatch < 0 {
		return fmt.Errorf("max_batch must be >= 0 (0 means the documented default)")
	}
	if c.ReserveTimeout <= 0 || c.FlushTimeout <= 0 || c.AdoptTimeout <= 0 {
		return fmt.Errorf("reserve_timeout, flush_timeout and adopt_timeout must be > 0")
	}
	return nil
}

// NewHolder returns "<instance_id>/<boot nonce>", unique per process start. The
// boot nonce adds a fixed 9-byte suffix ("/" + 8 hex chars), so callers should
// keep instanceID within maxInstanceIDLen (see InstanceConfig.Validate) for the
// result to satisfy store.ValidateHolder's 128-byte limit.
func NewHolder(instanceID string) string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("pool: crypto/rand failed: " + err.Error())
	}
	return instanceID + "/" + hex.EncodeToString(b[:])
}
