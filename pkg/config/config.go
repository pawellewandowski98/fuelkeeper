// Package config loads and validates FuelKeeper's YAML configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/bsv-blockchain/fuelkeeper/pkg/jobs"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/factory"
)

// Config is the whole file.
type Config struct {
	Instance pool.InstanceConfig `yaml:"instance"`
	Store    factory.Config      `yaml:"store"`
	Pools    []pool.Config       `yaml:"pools"`
	Jobs     JobsConfig          `yaml:"jobs"`
	Oracle   OracleConfig        `yaml:"oracle"`
	Treasury TreasuryConfig      `yaml:"treasury"`
	API      APIConfig           `yaml:"api"`
	Log      LogConfig           `yaml:"log"`
}

// JobsConfig enables and tunes the leased background jobs.
type JobsConfig struct {
	Enabled  bool               `yaml:"enabled"`
	LeaseTTL time.Duration      `yaml:"lease_ttl"`
	Reaper   jobs.ReaperConfig  `yaml:"reaper"`
	Watcher  jobs.WatcherConfig `yaml:"watcher"`
	Minter   jobs.MinterConfig  `yaml:"minter"`
}

// OracleConfig selects the chain oracle. teranode/arcade/composite are wired in Plan 5.
type OracleConfig struct {
	Kind     string `yaml:"kind"` // none | mock | teranode | arcade | composite
	Teranode struct {
		URL string `yaml:"url"`
	} `yaml:"teranode"`
	Arcade struct {
		URL string `yaml:"url"`
	} `yaml:"arcade"`
}

// TreasuryConfig selects the treasury. toolbox fields are added in Plan 5.
type TreasuryConfig struct {
	Kind string `yaml:"kind"` // mock | toolbox
}

// APIConfig configures the HTTP listener.
type APIConfig struct {
	Listen       string        `yaml:"listen"`
	APIKeys      []string      `yaml:"api_keys"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	IdleTimeout  time.Duration `yaml:"idle_timeout"`
	MaxBodyBytes int64         `yaml:"max_body_bytes"`
}

// LogConfig configures slog.
type LogConfig struct {
	Level  string `yaml:"level"`  // debug | info | warn | error
	Format string `yaml:"format"` // text | json
}

// Default returns a config with every default filled and no pools.
func Default() *Config {
	return &Config{
		Instance: pool.DefaultInstanceConfig(),
		Store:    factory.Config{Backend: factory.BackendMemory},
		Jobs: JobsConfig{
			Enabled: true,
			// LeaseTTL must exceed twice the longest default job interval
			// (minter's default Interval is 30s), so 90s keeps a safe margin.
			LeaseTTL: 90 * time.Second,
			Reaper:   jobs.DefaultReaperConfig(),
			Watcher:  jobs.DefaultWatcherConfig(),
			Minter:   jobs.DefaultMinterConfig(),
		},
		Oracle:   OracleConfig{Kind: "none"},
		Treasury: TreasuryConfig{Kind: "mock"},
		API: APIConfig{
			Listen:       ":8080",
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  120 * time.Second,
			MaxBodyBytes: 1 << 20,
		},
		Log: LogConfig{Level: "info", Format: "text"},
	}
}

// Load reads a YAML file, expands ${ENV} references, applies defaults and validates.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // operator-supplied path
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	return Parse([]byte(os.ExpandEnv(string(raw))))
}

// Parse decodes YAML bytes over the defaults, fills pool defaults and validates.
func Parse(data []byte) (*Config, error) {
	c := Default()
	if err := yaml.Unmarshal(data, c); err != nil {
		return nil, fmt.Errorf("config: parse: %w", err)
	}
	for i := range c.Pools {
		applyPoolDefaults(&c.Pools[i], c.Oracle.Kind)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// applyPoolDefaults fills zero fields of a pool from DefaultPoolConfig. The
// spend policy defaults to trust without an oracle and verify with one.
func applyPoolDefaults(p *pool.Config, oracleKind string) {
	d := pool.DefaultPoolConfig(p.Name, p.Denomination)
	if p.LockModeName == "" {
		p.LockModeName = d.LockModeName
	}
	if p.SplitSize == 0 {
		p.SplitSize = d.SplitSize
	}
	if p.TargetAvailable == 0 {
		p.TargetAvailable = d.TargetAvailable
	}
	if p.LowWater == 0 {
		p.LowWater = d.LowWater
	}
	if p.ServePolicy == "" {
		p.ServePolicy = d.ServePolicy
	}
	if p.DefaultTTL == 0 {
		p.DefaultTTL = d.DefaultTTL
	}
	if p.MinTTL == 0 {
		p.MinTTL = d.MinTTL
	}
	if p.MaxTTL == 0 {
		p.MaxTTL = d.MaxTTL
	}
	if p.MaxCount == 0 {
		p.MaxCount = d.MaxCount
	}
	if p.ReleasePolicy == "" {
		p.ReleasePolicy = d.ReleasePolicy
	}
	if p.SpendPolicy == "" {
		if oracleKind == "none" {
			p.SpendPolicy = pool.SpendTrust
		} else {
			p.SpendPolicy = pool.SpendVerify
		}
	}
}

// Validate checks cross-field rules.
func (c *Config) Validate() error {
	if err := c.Instance.Validate(); err != nil {
		return fmt.Errorf("config: instance: %w", err)
	}
	if len(c.Pools) == 0 {
		return errors.New("config: at least one pool is required")
	}
	seen := map[string]bool{}
	for i := range c.Pools {
		p := &c.Pools[i]
		if err := p.Validate(); err != nil {
			return fmt.Errorf("config: %w", err)
		}
		if seen[p.Name] {
			return fmt.Errorf("config: duplicate pool %q", p.Name)
		}
		seen[p.Name] = true
	}
	switch c.Oracle.Kind {
	case "none", "mock", "teranode", "arcade", "composite":
	default:
		return fmt.Errorf("config: oracle.kind %q is not one of none|mock|teranode|arcade|composite", c.Oracle.Kind)
	}
	switch c.Treasury.Kind {
	case "mock", "toolbox":
	default:
		return fmt.Errorf("config: treasury.kind %q is not one of mock|toolbox", c.Treasury.Kind)
	}
	if c.Jobs.Enabled {
		if err := c.Jobs.Reaper.Validate(); err != nil {
			return fmt.Errorf("config: jobs.reaper: %w", err)
		}
		if err := c.Jobs.Watcher.Validate(); err != nil {
			return fmt.Errorf("config: jobs.watcher: %w", err)
		}
		if err := c.Jobs.Minter.Validate(); err != nil {
			return fmt.Errorf("config: jobs.minter: %w", err)
		}
		longest := max(c.Jobs.Reaper.Interval, c.Jobs.Watcher.Interval, c.Jobs.Minter.Interval)
		if c.Jobs.LeaseTTL <= 2*longest {
			return fmt.Errorf("config: jobs.lease_ttl (%s) must exceed twice the longest job interval (%s)", c.Jobs.LeaseTTL, longest)
		}
		if c.Jobs.Watcher.Limit <= c.Jobs.Minter.MaxPendingShards {
			return fmt.Errorf("config: jobs.watcher.limit (%d) must exceed jobs.minter.max_pending_shards (%d)", c.Jobs.Watcher.Limit, c.Jobs.Minter.MaxPendingShards)
		}
		for _, p := range c.Pools {
			if p.ExpectedTPS <= 0 {
				continue
			}
			capacity := float64(p.SplitSize*c.Jobs.Minter.MaxPerRound) / c.Jobs.Minter.Interval.Seconds()
			required := 1.2 * p.ExpectedTPS
			if capacity < required {
				return fmt.Errorf("config: pool %s: mint capacity %.0f rows/s < 1.2 × expected_tps (%.0f); raise split_size or jobs.minter.max_per_round, or lower jobs.minter.interval", p.Name, capacity, required)
			}
		}
	}
	for _, p := range c.Pools {
		if p.SpendPolicy == pool.SpendVerify && c.Oracle.Kind == "none" {
			return fmt.Errorf("config: pool %s: spend_policy=verify requires an oracle (oracle.kind is none)", p.Name)
		}
		if p.ReleasePolicy == pool.ReleaseVerify && c.Oracle.Kind == "none" {
			return fmt.Errorf("config: pool %s: release_policy=verify requires an oracle (oracle.kind is none)", p.Name)
		}
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("config: log.level %q is not one of debug|info|warn|error", c.Log.Level)
	}
	switch c.Log.Format {
	case "text", "json":
	default:
		return fmt.Errorf("config: log.format %q is not one of text|json", c.Log.Format)
	}
	if c.API.Listen == "" || c.API.MaxBodyBytes <= 0 {
		return errors.New("config: api.listen and api.max_body_bytes are required")
	}
	return nil
}
