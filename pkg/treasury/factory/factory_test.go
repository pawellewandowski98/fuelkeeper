package factory

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury/mock"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury/toolbox"
)

func pools() []pool.Config { return []pool.Config{pool.DefaultPoolConfig("main", 50)} }

func toolboxConfig() Config {
	c := Default()
	c.Kind = KindToolbox
	c.Toolbox.RootKeyHex = "0000000000000000000000000000000000000000000000000000000000000001"
	c.Toolbox.ArcadeURL = "https://arcade.invalid"
	c.Toolbox.ChaintracksURL = "https://arcade.invalid/chaintracks"
	return c
}

func TestDefault(t *testing.T) {
	c := Default()
	assert.Equal(t, KindMock, c.Kind)
	assert.Equal(t, 2*time.Second, c.Mock.ConfirmAfter)
	assert.Equal(t, toolbox.DefaultConfig("ttn"), c.Toolbox)
	require.NoError(t, c.Validate(pools()))
}

func TestValidate(t *testing.T) {
	c := Default()
	c.Mock.ConfirmAfter = 0
	require.ErrorContains(t, c.Validate(pools()), "confirm_after")

	c = Default()
	c.Kind = "vault"
	require.ErrorContains(t, c.Validate(pools()), "not one of mock|toolbox")

	c = toolboxConfig()
	require.NoError(t, c.Validate(pools()))
	c.Toolbox.RootKeyHex = ""
	require.ErrorContains(t, c.Validate(pools()), "root_key_hex", "kind toolbox delegates to toolbox.Config.Validate")

	c = toolboxConfig()
	require.Error(t, c.Validate(nil), "toolbox needs pools")

	c = Default()
	c.Toolbox.RootKeyHex = "" // ignored for kind mock
	require.NoError(t, c.Validate(pools()))
}

func TestOpenMock(t *testing.T) {
	var sent []chainhash.Hash
	o, err := Open(context.Background(), Default(), pools(), slog.Default(), WithMockOnSend(func(txid chainhash.Hash) { sent = append(sent, txid) }))
	require.NoError(t, err)
	assert.IsType(t, &mock.Treasury{}, o.Treasury)
	assert.Nil(t, o.Funder, "the mock treasury has no funder")
	require.NoError(t, o.Closer.Close())

	b, err := o.Treasury.Build(context.Background(), treasury.MintRequest{Pool: "main", Denomination: 50, Count: 2})
	require.NoError(t, err)
	require.NoError(t, o.Treasury.Send(context.Background(), b.Shard.TxID, nil))
	assert.Equal(t, []chainhash.Hash{b.Shard.TxID}, sent, "the send hook is wired")
}

func TestOpenRejectsBadConfig(t *testing.T) {
	old := openToolbox
	defer func() { openToolbox = old }()
	openToolbox = func(context.Context, toolbox.Config, []pool.Config, *slog.Logger) (toolboxTreasury, error) {
		t.Fatal("toolbox opened despite a bad config")
		return nil, nil
	}
	c := toolboxConfig()
	c.Toolbox.ArcadeURL = ""
	_, err := Open(context.Background(), c, pools(), nil)
	require.ErrorContains(t, err, "arcade_url")
}

type fakeToolbox struct {
	*mock.Treasury
	closed int
}

func (f *fakeToolbox) FundingInfo(context.Context) (treasury.FundingInfo, error) {
	return treasury.FundingInfo{Network: "ttn"}, nil
}

func (f *fakeToolbox) Internalize(context.Context, treasury.FundRequest) (treasury.FundResult, error) {
	return treasury.FundResult{}, nil
}

func (f *fakeToolbox) Close() error { f.closed++; return nil }

func TestOpenToolbox(t *testing.T) {
	old := openToolbox
	defer func() { openToolbox = old }()
	fake := &fakeToolbox{Treasury: mock.New()}
	var gotCfg toolbox.Config
	openToolbox = func(_ context.Context, cfg toolbox.Config, _ []pool.Config, _ *slog.Logger) (toolboxTreasury, error) {
		gotCfg = cfg
		return fake, nil
	}
	c := toolboxConfig()
	o, err := Open(context.Background(), c, pools(), nil, WithMockOnSend(func(chainhash.Hash) {}))
	require.NoError(t, err)
	assert.Equal(t, c.Toolbox, gotCfg)
	assert.Same(t, fake, o.Treasury)
	assert.Same(t, fake, o.Funder)
	require.NoError(t, o.Closer.Close())
	assert.Equal(t, 1, fake.closed)

	boom := errors.New("boom")
	openToolbox = func(context.Context, toolbox.Config, []pool.Config, *slog.Logger) (toolboxTreasury, error) {
		return nil, boom
	}
	_, err = Open(context.Background(), c, pools(), nil)
	require.ErrorIs(t, err, boom)
}
