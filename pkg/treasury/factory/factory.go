// Package factory builds a treasury.Treasury from configuration.
package factory

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-wallet-toolbox/pkg/defs"

	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury/mock"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury/toolbox"
)

// Treasury kinds.
const (
	KindMock    = "mock"
	KindToolbox = "toolbox"
)

// Config selects and configures the treasury.
type Config struct {
	// Kind is mock | toolbox.
	Kind string `yaml:"kind"`
	// Mock configures kind mock (nullnet development).
	Mock MockConfig `yaml:"mock"`
	// Toolbox configures kind toolbox (the go-wallet-toolbox wallet).
	Toolbox toolbox.Config `yaml:"toolbox"`
}

// MockConfig configures the mock treasury.
type MockConfig struct {
	// ConfirmAfter is how long after a successful Send the nullnet wiring
	// marks the split mined; must be > 0 when Kind is mock.
	ConfirmAfter time.Duration `yaml:"confirm_after"`
}

// Default returns the defaults: kind mock, and the toolbox defaults for ttn
// so a config that switches to kind toolbox only needs its secrets and URLs.
func Default() Config {
	return Config{
		Kind:    KindMock,
		Mock:    MockConfig{ConfirmAfter: 2 * time.Second},
		Toolbox: toolbox.DefaultConfig("ttn"),
	}
}

// ApplyDefaults fills an empty treasury.toolbox.arcade_url and
// chaintracks_url from the toolbox's network defaults
// (defs.DefaultServicesConfig, defs/services.go:126), for the networks that
// have public endpoints: main uses defs.ArcadeURL (defs/arcade.go:12), ttn
// defs.ArcadeTTNURL (defs/arcade.go:16), and chaintracks is the arcade host's
// /chaintracks (defs/network_endpoints.go:48-69, defs/networkconfig.go:65).
// test and tstn have no such default and still need both URLs. Validate
// calls it.
func (c *Config) ApplyDefaults() {
	if c.Kind != KindToolbox {
		return
	}
	tb := &c.Toolbox
	switch tb.Network {
	case string(defs.NetworkMainnet), string(defs.NetworkTTN):
	default:
		return
	}
	d := defs.DefaultServicesConfig(defs.BSVNetwork(tb.Network))
	if tb.ArcadeURL == "" {
		tb.ArcadeURL = d.Arcade.URL
	}
	if tb.ChaintracksURL == "" {
		tb.ChaintracksURL = d.ChaintracksClient.RemoteURL
	}
}

// Validate applies the network defaults and checks the config against the
// pools the treasury funds.
func (c *Config) Validate(pools []pool.Config) error {
	c.ApplyDefaults()
	switch c.Kind {
	case KindMock:
		if c.Mock.ConfirmAfter <= 0 {
			return errors.New("treasury.mock.confirm_after must be > 0")
		}
	case KindToolbox:
		return c.Toolbox.Validate(pools)
	default:
		return fmt.Errorf("treasury.kind %q is not one of mock|toolbox", c.Kind)
	}
	return nil
}

// Opened is an open treasury.
type Opened struct {
	// Treasury builds and sends splits.
	Treasury treasury.Treasury
	// Funder accepts BRC-29 payments; nil when the treasury has none (mock).
	Funder treasury.Funder
	// Closer releases the treasury's resources; call it once the jobs that
	// use Treasury have stopped and before the store closes.
	Closer io.Closer
}

// Option tunes Open.
type Option func(*options)

type options struct {
	mockOnSend func(chainhash.Hash)
}

// WithMockOnSend sets the mock treasury's send hook (the nullnet wiring
// uses it to confirm splits on a mock oracle). Other kinds ignore it.
func WithMockOnSend(fn func(txid chainhash.Hash)) Option {
	return func(o *options) { o.mockOnSend = fn }
}

// Open validates cfg and opens the configured treasury. Kind toolbox opens
// its wallet storage and starts its services, so a config error returns
// before any of that.
func Open(ctx context.Context, cfg Config, pools []pool.Config, log *slog.Logger, opts ...Option) (*Opened, error) {
	if err := cfg.Validate(pools); err != nil {
		return nil, err
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	switch cfg.Kind {
	case KindMock:
		var mopts []mock.Option
		if o.mockOnSend != nil {
			mopts = append(mopts, mock.WithOnSend(o.mockOnSend))
		}
		return &Opened{Treasury: mock.New(mopts...), Closer: nopCloser{}}, nil
	default: // KindToolbox
		t, err := openToolbox(ctx, cfg.Toolbox, pools, log)
		if err != nil {
			return nil, err
		}
		return &Opened{Treasury: t, Funder: t, Closer: t}, nil
	}
}

// openToolbox is toolbox.Open; replaceable by tests.
var openToolbox = func(ctx context.Context, cfg toolbox.Config, pools []pool.Config, log *slog.Logger) (toolboxTreasury, error) {
	return toolbox.Open(ctx, cfg, pools, log)
}

// toolboxTreasury is what Open needs from *toolbox.Treasury.
type toolboxTreasury interface {
	treasury.Treasury
	treasury.Funder
	io.Closer
}

var _ toolboxTreasury = (*toolbox.Treasury)(nil)

type nopCloser struct{}

func (nopCloser) Close() error { return nil }
