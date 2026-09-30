package toolbox

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// PoolKey exposes poolKey to tests.
func PoolKey(kd *sdk.KeyDeriver, poolName string) (*ec.PrivateKey, error) {
	return poolKey(kd, poolName)
}

// VerifySplit exposes verifySplit to tests.
func VerifySplit(raw []byte, txid chainhash.Hash, want []sdk.CreateActionOutput) error {
	return verifySplit(raw, txid, want)
}

// SetValidatePutShard replaces the shard validation Build runs and returns a
// restore func.
func SetValidatePutShard(fn func(store.Shard, []store.Row) error) (restore func()) {
	old := validatePutShard
	validatePutShard = fn
	return func() { validatePutShard = old }
}

// SetSleep replaces the wait Reconcile uses for its grace period.
func (c *Core) SetSleep(fn func(ctx context.Context, d time.Duration) error) { c.sleep = fn }
