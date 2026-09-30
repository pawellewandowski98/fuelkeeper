//go:build ttn

// The teratestnet smoke: go test -tags ttn -run TestTeratestnetSmoke -v ./pkg/treasury/toolbox/
// See docs/TREASURY.md "Teratestnet smoke" for the environment and funding.

package toolbox_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/arcadeclient"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle/arcade"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle/composite"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle/teranode"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury/toolbox"
)

func ttnEnv(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		t.Skipf("%s is not set; see docs/TREASURY.md \"Teratestnet smoke\"", name)
	}
	return v
}

func TestTeratestnetSmoke(t *testing.T) {
	cfg := toolbox.DefaultConfig("ttn")
	cfg.RootKeyHex = ttnEnv(t, "FK_TTN_ROOT_KEY")
	cfg.ArcadeURL = ttnEnv(t, "FK_TTN_ARCADE_URL")
	cfg.ChaintracksURL = ttnEnv(t, "FK_TTN_CHAINTRACKS_URL")
	// FK_TTN_TERANODE_URL is optional: a direct teranode connection is never
	// assumed, only an opt-in accelerator (see ofactory.Config.AnswersOutpoints).
	// Without it the oracle below is arcade-only, the expected production
	// shape, and this test does not assert on Inputs (arcade alone answers
	// no outpoint queries, so composite.New's outpoints backend is nil).
	teranodeURL := os.Getenv("FK_TTN_TERANODE_URL")
	// The wallet storage must persist across runs: it is where funding lands.
	cfg.Storage.SQLite = os.Getenv("FK_TTN_STORAGE")
	if cfg.Storage.SQLite == "" {
		cfg.Storage.SQLite = filepath.Join(t.TempDir(), "treasury.sqlite")
	}
	// Small chunks and few of them keep the smoke cheap.
	cfg.Chunking.TargetChunks = 5
	cfg.Chunking.FanoutOutputsPerTx = 5
	p := pool.DefaultPoolConfig("smoke", 50)
	p.SplitSize = 5

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	tr, err := toolbox.Open(ctx, cfg, []pool.Config{p}, log)
	require.NoError(t, err)
	defer func() { require.NoError(t, tr.Close()) }()
	t.Logf("fee %d sat/kB, chunk value %d sat", tr.FeeSatPerKB(), tr.ChunkValue())

	info, err := tr.FundingInfo(ctx)
	require.NoError(t, err)
	t.Logf("treasury identity key %s (network %s)", info.IdentityKeyHex, info.Network)

	rep, err := tr.Reconcile(ctx, func(context.Context, chainhash.Hash) (bool, error) { return false, nil })
	require.NoError(t, err)
	t.Logf("reconcile: resent %d aborted %d deferred %d errors %d", len(rep.Resent), len(rep.Aborted), len(rep.Deferred), len(rep.Errors))

	if err := tr.EnsureChunks(ctx); err != nil {
		t.Logf("ensure chunks: %v", err)
	}

	b, err := tr.Build(ctx, treasury.MintRequest{Pool: p.Name, Denomination: p.Denomination, Count: p.SplitSize, LockMode: store.LockPuzzle})
	if errors.Is(err, treasury.ErrInsufficientFunds) {
		t.Skipf("treasury %s has no funds: fund it with `fuelkeeper fund` (docs/TREASURY.md) and set FK_TTN_STORAGE to a persistent path: %v", info.IdentityKeyHex, err)
	}
	require.NoError(t, err)
	txid := b.Shard.TxID
	t.Logf("built split %s (%d rows)", txid, len(b.Rows))
	require.NoError(t, tr.Send(ctx, txid, nil))
	t.Logf("sent split %s", txid)

	ac, err := arcadeclient.New(cfg.ArcadeURL)
	require.NoError(t, err)
	// tn stays nil (arcade-only) when FK_TTN_TERANODE_URL is unset;
	// composite.New accepts a nil outpoints backend, same as
	// ofactory.New does for oracle.kind: composite with no teranode_url.
	var tn oracle.Oracle
	if teranodeURL == "" {
		t.Log("FK_TTN_TERANODE_URL not set: arcade-only oracle")
	} else {
		tn, err = teranode.NewURL(teranodeURL)
		require.NoError(t, err)
	}
	orc := composite.New(arcade.New(ac), tn)
	var st oracle.TxStatus
	require.Eventually(t, func() bool {
		st, err = orc.Tx(ctx, txid)
		return err == nil && st.Known
	}, 2*time.Minute, 2*time.Second, "arcade never reported the split")
	t.Logf("split %s: status %s mined %v", txid, st.Status, st.Mined)
	if teranodeURL != "" {
		t.Logf("inputs %v", st.Inputs)
	}
	require.False(t, st.Rejected, "split rejected: %s", st.Status)
}
