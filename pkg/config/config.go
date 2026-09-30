// Package config loads and validates FuelKeeper's YAML configuration.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/bsv-blockchain/fuelkeeper/pkg/jobs"
	ofactory "github.com/bsv-blockchain/fuelkeeper/pkg/oracle/factory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/factory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/sqlite"
	tfactory "github.com/bsv-blockchain/fuelkeeper/pkg/treasury/factory"
)

// storeBackendList is the human-readable enumeration of every store.Backend
// factory.Config.Validate accepts, repeated in every store.backend error
// Validate returns so an operator sees the full menu (memory, sqlite,
// postgres, pebble, mongodb, aerospike) in one place, not just the three that
// predate the pebble/mongodb/aerospike backends.
const storeBackendList = "memory|sqlite|postgres|pebble|mongodb|aerospike"

// Config is the whole file.
type Config struct {
	Instance pool.InstanceConfig `yaml:"instance"`
	Store    factory.Config      `yaml:"store"`
	Pools    []pool.Config       `yaml:"pools"`
	Jobs     JobsConfig          `yaml:"jobs"`
	Oracle   ofactory.Config     `yaml:"oracle"`
	Treasury tfactory.Config     `yaml:"treasury"`
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

// APIConfig configures the HTTP listener.
type APIConfig struct {
	Listen       string        `yaml:"listen"`
	APIKeys      []string      `yaml:"api_keys"`
	AdminKeys    []string      `yaml:"admin_keys"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	IdleTimeout  time.Duration `yaml:"idle_timeout"`
	MaxBodyBytes int64         `yaml:"max_body_bytes"`
	// RequestTimeout bounds a single HTTP request end-to-end; it must exceed
	// instance.reserve_timeout (see Validate).
	RequestTimeout time.Duration `yaml:"request_timeout"`
	// Insecure allows the server to start with no api_keys configured while
	// api.listen is not loopback-only. Without it, Validate refuses such a
	// config outright: an open /v1/ surface reachable from outside the host
	// is almost always a mistake. Running open on loopback needs no flag (the
	// caller logs a Warn instead); see Validate and runServe.
	Insecure bool `yaml:"insecure"`
}

// LogConfig configures slog.
type LogConfig struct {
	Level  string `yaml:"level"`  // debug | info | warn | error
	Format string `yaml:"format"` // text | json
}

// Default returns a config with every default filled and no pools.
func Default() *Config {
	oracleCfg := ofactory.Config{Kind: ofactory.KindNone}
	oracleCfg.Mock.AssumeUnspent = true
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
		Oracle:   oracleCfg,
		Treasury: tfactory.Default(),
		API: APIConfig{
			Listen:         ":8080",
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   30 * time.Second,
			IdleTimeout:    120 * time.Second,
			MaxBodyBytes:   1 << 20,
			RequestTimeout: 10 * time.Second,
		},
		Log: LogConfig{Level: "info", Format: "text"},
	}
}

// envRefPattern matches the two forms expandEnv understands: a literal "$$",
// or "${VAR}" / "${VAR:-default}" with VAR captured in group 1 and an
// optional default in group 2.
var envRefPattern = regexp.MustCompile(`\$\$|\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)

// expandEnv expands "${VAR}" and "${VAR:-default}" references in s.
// "${VAR}" resolves to the named environment variable, or is an error naming
// VAR if it is not set; "${VAR:-default}" falls back to default instead of
// erroring when VAR is unset. "$$" is a literal "$". A bare "$word" (no
// braces, and not part of "$$") is left untouched, so a literal "$" in a
// value (an API key, say) never needs escaping unless it is immediately
// followed by another "$" or by "{".
func expandEnv(s string) (string, error) {
	var missing []string
	out := envRefPattern.ReplaceAllStringFunc(s, func(match string) string {
		if match == "$$" {
			return "$"
		}
		sub := envRefPattern.FindStringSubmatch(match)
		name, def := sub[1], sub[2]
		if v, ok := os.LookupEnv(name); ok {
			return v
		}
		if strings.Contains(match, ":-") {
			return def
		}
		missing = append(missing, name)
		return ""
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("config: environment variable(s) not set: %s", strings.Join(missing, ", "))
	}
	return out, nil
}

// Load reads path, expands environment references, decodes, fills defaults
// and validates the result.
//
// Within the file, "${VAR}" is replaced by the value of the environment
// variable VAR (an error naming VAR if it is unset); "${VAR:-default}"
// instead falls back to default when VAR is unset; "$$" is a literal "$";
// and a bare "$word" (no braces) is left untouched. See expandEnv.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // operator-supplied path
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	expanded, err := expandEnv(string(raw))
	if err != nil {
		return nil, err
	}
	return Parse([]byte(expanded))
}

// rawPool decodes one pools[] entry. A plain struct decode gives a freshly
// appended slice element the Go zero value for every field the YAML omits,
// indistinguishable from a field explicitly set to zero. rawPool's
// UnmarshalYAML instead pre-fills the entry from pool.DefaultPoolConfig
// before the node is decoded onto it, so an omitted field keeps its default
// while an explicit zero (e.g. low_water: 0, which pool.Config documents as
// "always refill below target") survives decoding intact.
//
// SpendPolicy is the one field left unresolved ("") here: its default
// depends on the oracle kind, which is only known once the whole document
// has been read. See ApplyPoolDefaults, called from Parse.
type rawPool pool.Config

// UnmarshalYAML implements yaml.Unmarshaler.
func (p *rawPool) UnmarshalYAML(node *yaml.Node) error {
	def := pool.DefaultPoolConfig("", 0)
	def.SpendPolicy = ""
	*p = rawPool(def)

	// yaml.Node.Decode always runs through a fresh, non-strict decoder, so an
	// outer *yaml.Decoder's KnownFields(true) never reaches here on its own
	// (see Parse). Re-encode the node and decode it again through our own
	// strict decoder so a typo'd field (e.g. spilt_size) is still rejected.
	buf, err := yaml.Marshal(node)
	if err != nil {
		return fmt.Errorf("config: pool: %w", err)
	}
	type plain rawPool // avoid recursing back into UnmarshalYAML
	dec := yaml.NewDecoder(bytes.NewReader(buf))
	dec.KnownFields(true)
	if err := dec.Decode((*plain)(p)); err != nil {
		return fmt.Errorf("config: pool: %w", err)
	}
	return nil
}

// rawConfig mirrors Config field-for-field, except Pools decodes through
// rawPool instead of pool.Config directly (see rawPool's doc comment).
type rawConfig struct {
	Instance pool.InstanceConfig `yaml:"instance"`
	Store    factory.Config      `yaml:"store"`
	Pools    []rawPool           `yaml:"pools"`
	Jobs     JobsConfig          `yaml:"jobs"`
	Oracle   ofactory.Config     `yaml:"oracle"`
	Treasury tfactory.Config     `yaml:"treasury"`
	API      APIConfig           `yaml:"api"`
	Log      LogConfig           `yaml:"log"`
}

// Parse decodes YAML bytes over the defaults, rejecting unknown fields,
// resolves each pool's oracle-dependent defaults and validates.
func Parse(data []byte) (*Config, error) {
	c := Default()
	raw := rawConfig{
		Instance: c.Instance,
		Store:    c.Store,
		Jobs:     c.Jobs,
		Oracle:   c.Oracle,
		Treasury: c.Treasury,
		API:      c.API,
		Log:      c.Log,
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&raw); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("config: parse: %w", err)
	}
	// A YAML stream may contain more than one "---"-separated document; only
	// the first was decoded above. Decoding again must hit end of stream --
	// anything else (a second document decoded successfully, or a read
	// error) means the file is not the single document Load/Parse expect.
	var extra yaml.Node
	switch err := dec.Decode(&extra); {
	case errors.Is(err, io.EOF):
	case err == nil:
		return nil, errors.New("config: parse: multiple YAML documents are not allowed")
	default:
		return nil, fmt.Errorf("config: parse: %w", err)
	}
	c.Instance, c.Store, c.Jobs = raw.Instance, raw.Store, raw.Jobs
	c.Oracle, c.Treasury, c.API, c.Log = raw.Oracle, raw.Treasury, raw.API, raw.Log
	c.Pools = make([]pool.Config, len(raw.Pools))
	for i, p := range raw.Pools {
		c.Pools[i] = pool.Config(p)
	}
	for i := range c.Pools {
		// Only resolve via oracle context when the operator left spend_policy
		// unset; an explicit choice (verify or trust) is never overridden.
		if c.Pools[i].SpendPolicy == "" {
			ApplyPoolDefaults(&c.Pools[i], c.Oracle)
		}
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// ApplyPoolDefaults resolves the two pool fields whose default cannot be a
// pure, context-free constant the way the rest of pool.DefaultPoolConfig's
// fields are:
//
//   - LockModeName is left alone once set; if still empty (typically a Go
//     caller building a pool.Config{} from scratch rather than starting from
//     pool.DefaultPoolConfig) it is filled with the default lock mode name
//     ("puzzle"). A caller who set LockMode directly without LockModeName
//     keeps that choice, exactly as (*pool.Config).Validate documents for an
//     empty LockModeName.
//   - SpendPolicy is always resolved from the oracle: SpendVerify when it
//     answers outpoint queries (ofactory.Config.AnswersOutpoints: mock,
//     teranode, composite with teranode_url), SpendTrust otherwise (none,
//     arcade, composite without teranode), since an unhinted spend could
//     never be verified there. This overrides pool.DefaultPoolConfig's own
//     SpendPolicy, which assumes such an oracle (see its doc comment),
//     whenever that assumption does not hold.
//
// Go callers composing a pool.Config programmatically should call this after
// pool.DefaultPoolConfig so they get the same resolution the YAML loader
// applies. Parse itself only calls this for a pool whose YAML left
// spend_policy unset, so an operator's explicit choice is never overridden;
// a direct caller has no such guard and should only call it when it wants
// SpendPolicy recomputed from the oracle unconditionally.
func ApplyPoolDefaults(p *pool.Config, orc ofactory.Config) {
	if p.LockModeName == "" {
		p.LockModeName = pool.DefaultPoolConfig("", 0).LockModeName
	}
	if orc.AnswersOutpoints() {
		p.SpendPolicy = pool.SpendVerify
	} else {
		p.SpendPolicy = pool.SpendTrust
	}
}

// Validate checks cross-field rules.
func (c *Config) Validate() error {
	if err := c.Instance.Validate(); err != nil {
		return fmt.Errorf("config: instance: %w", err)
	}
	switch c.Store.Backend {
	case "":
		return fmt.Errorf("config: store.backend is required (%s)", storeBackendList)
	case factory.BackendMemory, factory.BackendSQLite, factory.BackendPostgres,
		factory.BackendPebble, factory.BackendMongoDB, factory.BackendAerospike:
		// Delegate presence-of-required-fields checking to the factory (the
		// same check New runs before opening the backend), so a typo'd or
		// missing store.<backend>.<field> fails at config load instead of at
		// store-open time, and pebble/mongodb/aerospike need no separate
		// copy of this logic here. factory.Config.Validate's own message
		// already names the field as "<backend>.<field>" (e.g.
		// "postgres.dsn is required"); strip its sentinel prefix ("store:
		// invalid argument: ") and reattach "store." so the field reads as
		// the full YAML path an operator can search for.
		if err := c.Store.Validate(); err != nil {
			msg := strings.TrimPrefix(err.Error(), store.ErrInvalidArgument.Error()+": ")
			return fmt.Errorf("config: store.%s", msg)
		}
		// sqlite.Sync's enum check is deeper backend validation that
		// factory.Config.Validate deliberately leaves to the backend (see its
		// doc comment); config.Load is the only strict-loader path, so
		// normalize and validate it here, before an operator-friendly
		// lowercase "full"/"normal" would otherwise fail at store-open time.
		if c.Store.Backend == factory.BackendSQLite {
			switch strings.ToUpper(string(c.Store.SQLite.Sync)) {
			case "", "FULL", "NORMAL":
				c.Store.SQLite.Sync = sqlite.Sync(strings.ToUpper(string(c.Store.SQLite.Sync)))
			default:
				return fmt.Errorf("config: store.sqlite.sync %q must be \"\", \"full\" or \"normal\" (case-insensitive)", c.Store.SQLite.Sync)
			}
		}
	default:
		return fmt.Errorf("config: unknown store backend %q (%s)", c.Store.Backend, storeBackendList)
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
	if err := c.Treasury.Validate(c.Pools); err != nil { // fills the toolbox's network default URLs
		return fmt.Errorf("config: %w", err)
	}
	// An arcade-backed oracle with no URL of its own uses the toolbox
	// treasury's arcade (after its network default).
	if (c.Oracle.Kind == ofactory.KindArcade || c.Oracle.Kind == ofactory.KindComposite) &&
		c.Oracle.ArcadeURL == "" && c.Treasury.Kind == tfactory.KindToolbox {
		c.Oracle.ArcadeURL = c.Treasury.Toolbox.ArcadeURL
	}
	if err := c.Oracle.Validate(); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if c.Oracle.Kind == ofactory.KindTeranode && c.Jobs.Enabled {
		// The watcher and reaper need a mined verdict, which teranode's
		// GET /tx never gives.
		return errors.New("config: oracle.kind teranode cannot report transactions as mined (shards would stay unmined and mined spends would never resolve); use kind: composite with arcade_url and teranode_url")
	}
	if c.Treasury.Kind == tfactory.KindToolbox && c.Treasury.Toolbox.ReconcileGrace <= c.Jobs.Minter.BuildTimeout+jobs.ReconcileBudget {
		// Reconcile's grace only protects a split whose Build and PutShard
		// finish within it; the minter bounds that window by build_timeout,
		// and a first listing (bounded by the reconcile budget) may run while
		// such a Build is in flight.
		return fmt.Errorf("config: treasury.toolbox.reconcile_grace (%s) must exceed jobs.minter.build_timeout (%s) + %s reconcile budget, "+
			"or a slow Build+PutShard could be aborted by Reconcile as unrecorded",
			c.Treasury.Toolbox.ReconcileGrace, c.Jobs.Minter.BuildTimeout, jobs.ReconcileBudget)
	}
	if c.Treasury.Kind == tfactory.KindToolbox && c.Jobs.Enabled {
		// A minter round runs one Build+PutShard per split and at most one
		// finish and one start of a reconcile; the lease is renewed only
		// between rounds, so it must outlast that.
		if round := c.Jobs.Minter.BuildTimeout + 2*jobs.ReconcileBudget; c.Jobs.LeaseTTL <= round {
			return fmt.Errorf("config: jobs.lease_ttl (%s) must exceed jobs.minter.build_timeout + 2 × %s reconcile budget (%s) with the toolbox treasury",
				c.Jobs.LeaseTTL, jobs.ReconcileBudget, round)
		}
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
		// This capacity figure is a nominal best case: it assumes every pool
		// gets its full max_per_round splits every single tick with no
		// failures or contention, so real sustained throughput is lower.
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
		if p.ReleasePolicy == pool.ReleaseVerify && !c.Oracle.AnswersOutpoints() {
			// A verified release is requeued only on an Unspent outpoint
			// verdict, which this oracle never gives: every released row
			// would stay quarantined.
			return fmt.Errorf("config: pool %s: release_policy=verify needs an oracle that answers outpoint queries, "+
				"and oracle.kind %s does not; use kind: composite with oracle.teranode_url, or release_policy: trust",
				p.Name, c.Oracle.Kind)
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
	host, portStr, err := net.SplitHostPort(c.API.Listen)
	if err != nil {
		return fmt.Errorf("config: api.listen %q is invalid: %w", c.API.Listen, err)
	}
	// 0 is accepted alongside 1..65535: it asks the OS for an ephemeral port
	// (net.Listen's usual meaning), which tests (and "just give me a free
	// port" deployments) rely on.
	if port, perr := strconv.Atoi(portStr); perr != nil || port < 0 || port > 65535 {
		return fmt.Errorf("config: api.listen %q has an invalid port (must be numeric 0..65535)", c.API.Listen)
	}
	for _, k := range c.API.APIKeys {
		if k == "" {
			return errors.New("config: api.api_keys must not contain an empty string (check for an unset ${VAR:-} default)")
		}
	}
	for _, k := range c.API.AdminKeys {
		if k == "" {
			return errors.New("config: api.admin_keys must not contain an empty string (check for an unset ${VAR:-} default)")
		}
	}
	if len(c.API.APIKeys) == 0 && !IsLoopbackHost(host) && !c.API.Insecure {
		return fmt.Errorf("config: api.listen %q has no api_keys and is not loopback-only; "+
			"set api.insecure: true to allow this, or configure api.api_keys", c.API.Listen)
	}
	if c.API.MaxBodyBytes <= 0 {
		return errors.New("config: api.max_body_bytes must be > 0")
	}
	if c.API.ReadTimeout <= 0 || c.API.WriteTimeout <= 0 || c.API.IdleTimeout <= 0 {
		return errors.New("config: api.read_timeout, api.write_timeout and api.idle_timeout must be > 0")
	}
	if c.API.RequestTimeout <= 0 {
		return errors.New("config: api.request_timeout must be > 0")
	}
	if c.API.RequestTimeout <= c.Instance.ReserveTimeout {
		return fmt.Errorf("config: api.request_timeout (%s) must exceed instance.reserve_timeout (%s)",
			c.API.RequestTimeout, c.Instance.ReserveTimeout)
	}
	return nil
}

// IsLoopbackHost reports whether host (the host part of an api.listen
// "host:port") only accepts local connections: "127.0.0.1", "::1",
// "localhost", or their bracketed/other textual forms recognized by
// net.ParseIP. An empty host (":8080", meaning "every interface") is not
// loopback: it is reachable from outside the machine. Exported so callers
// (e.g. cmd/fuelkeeper) can log a runtime warning consistent with what
// Validate enforces, instead of duplicating the loopback check.
func IsLoopbackHost(host string) bool {
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}
