// Package factory builds an oracle.Oracle from configuration.
package factory

import (
	"errors"
	"fmt"

	"github.com/bsv-blockchain/fuelkeeper/pkg/arcadeclient"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle/arcade"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle/composite"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle/teranode"
)

// Oracle kinds.
const (
	KindNone      = "none"
	KindMock      = "mock"
	KindArcade    = "arcade"
	KindTeranode  = "teranode"
	KindComposite = "composite"
)

// Config selects and configures the chain oracle.
type Config struct {
	// Kind is none | mock | arcade | teranode | composite.
	Kind string `yaml:"kind"`
	// ArcadeURL is the arcade base URL (arcade, composite).
	ArcadeURL string `yaml:"arcade_url"`
	// TeranodeURL is the teranode asset API base URL (teranode; optional
	// for composite, where it answers Outpoints and fills Tx inputs).
	TeranodeURL string `yaml:"teranode_url"`
	// TeranodeBatchThreshold is the outpoint count above which teranode
	// batches lookups; 0 = default (8), negative disables batching.
	TeranodeBatchThreshold int `yaml:"teranode_batch_threshold"`
	// Mock configures kind mock.
	Mock MockConfig `yaml:"mock"`
}

// MockConfig configures the mock oracle.
type MockConfig struct {
	// AssumeUnspent makes the mock oracle answer Unspent instead of Unknown
	// for outpoints it has no verdict for; "nullnet" development mode only.
	AssumeUnspent bool `yaml:"assume_unspent"`
}

// Validate checks that the kind is known and has the URLs it needs.
func (c Config) Validate() error {
	switch c.Kind {
	case KindNone, KindMock:
	case KindArcade:
		if c.ArcadeURL == "" {
			return errors.New("oracle.arcade_url is required for kind arcade")
		}
	case KindTeranode:
		if c.TeranodeURL == "" {
			return errors.New("oracle.teranode_url is required for kind teranode")
		}
	case KindComposite:
		if c.ArcadeURL == "" {
			return errors.New("oracle.arcade_url is required for kind composite")
		}
	default:
		return fmt.Errorf("oracle.kind %q is not one of none|mock|arcade|teranode|composite", c.Kind)
	}
	return nil
}

// New builds the configured oracle. Kind mock returns an *oracle.Mock, so a
// caller can drive it further.
func New(c Config) (oracle.Oracle, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	switch c.Kind {
	case KindNone:
		return oracle.None{}, nil
	case KindMock:
		m := oracle.NewMock()
		m.SetAssumeUnspent(c.Mock.AssumeUnspent)
		return m, nil
	case KindArcade:
		return newArcade(c)
	case KindTeranode:
		return newTeranode(c)
	default: // KindComposite
		tx, err := newArcade(c)
		if err != nil {
			return nil, err
		}
		if c.TeranodeURL == "" {
			return composite.New(tx, nil), nil
		}
		tn, err := newTeranode(c)
		if err != nil {
			return nil, err
		}
		return composite.New(tx, tn), nil
	}
}

func newArcade(c Config) (*arcade.Oracle, error) {
	ac, err := arcadeclient.New(c.ArcadeURL)
	if err != nil {
		return nil, fmt.Errorf("oracle: %w", err)
	}
	return arcade.New(ac), nil
}

func newTeranode(c Config) (*teranode.Oracle, error) {
	tn, err := teranode.New(teranode.Config{BaseURL: c.TeranodeURL, BatchThreshold: c.TeranodeBatchThreshold})
	if err != nil {
		return nil, fmt.Errorf("oracle: %w", err)
	}
	return tn, nil
}
