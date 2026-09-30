package toolbox

import (
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
)

// BuiltLen reports how many built splits the core currently remembers.
func (c *Core) BuiltLen() int { return c.builtLen() }

// SetClock replaces the core's clock.
func (c *Core) SetClock(now func() time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

// MaxBuilt and BuiltTTL expose the bounded-memory policy to tests.
const (
	MaxBuilt = maxBuilt
	BuiltTTL = builtTTL
)

// PoolKey exposes poolKey to tests.
func PoolKey(kd *sdk.KeyDeriver, poolName string) (*ec.PrivateKey, error) {
	return poolKey(kd, poolName)
}

// SetMaxBuilt overrides the built-map cap (tests use a small one).
func (c *Core) SetMaxBuilt(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxBuilt = n
}
