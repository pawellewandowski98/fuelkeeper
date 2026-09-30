package factory_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle/arcade"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle/composite"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle/factory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle/teranode"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

func TestValidate(t *testing.T) {
	ok := []factory.Config{
		{Kind: "none"},
		{Kind: "mock"},
		{Kind: "arcade", ArcadeURL: "http://a"},
		{Kind: "teranode", TeranodeURL: "http://t"},
		{Kind: "composite", ArcadeURL: "http://a"},
		{Kind: "composite", ArcadeURL: "http://a", TeranodeURL: "http://t"},
	}
	for _, c := range ok {
		require.NoError(t, c.Validate(), c.Kind)
	}
	bad := map[string]factory.Config{
		"arcade_url":   {Kind: "arcade"},
		"teranode_url": {Kind: "teranode"},
		"composite":    {Kind: "composite", TeranodeURL: "http://t"},
		"not one of":   {Kind: "psychic"},
		"":             {},
	}
	for want, c := range bad {
		err := c.Validate()
		require.Error(t, err, c)
		assert.Contains(t, err.Error(), want)
	}
}

func TestNewNoneAndMock(t *testing.T) {
	o, err := factory.New(factory.Config{Kind: "none"})
	require.NoError(t, err)
	assert.Equal(t, oracle.None{}, o)

	o, err = factory.New(factory.Config{Kind: "mock", Mock: factory.MockConfig{AssumeUnspent: true}})
	require.NoError(t, err)
	m, ok := o.(*oracle.Mock)
	require.True(t, ok)
	res, err := m.Outpoints(context.Background(), []store.Outpoint{{Vout: 1}})
	require.NoError(t, err)
	assert.Equal(t, oracle.StatusUnspent, res[0].Status, "assume_unspent is applied")

	o, err = factory.New(factory.Config{Kind: "mock"})
	require.NoError(t, err)
	res, err = o.Outpoints(context.Background(), []store.Outpoint{{Vout: 1}})
	require.NoError(t, err)
	assert.Equal(t, oracle.StatusUnknown, res[0].Status)
}

func TestNewArcadeAndTeranode(t *testing.T) {
	o, err := factory.New(factory.Config{Kind: "arcade", ArcadeURL: "http://arcade.invalid"})
	require.NoError(t, err)
	assert.IsType(t, &arcade.Oracle{}, o)

	o, err = factory.New(factory.Config{Kind: "teranode", TeranodeURL: "http://teranode.invalid", TeranodeBatchThreshold: -1})
	require.NoError(t, err)
	assert.IsType(t, &teranode.Oracle{}, o)

	_, err = factory.New(factory.Config{Kind: "arcade", ArcadeURL: "ftp://x"})
	require.Error(t, err)
	_, err = factory.New(factory.Config{Kind: "teranode", TeranodeURL: "not-absolute"})
	require.Error(t, err)
	_, err = factory.New(factory.Config{Kind: "arcade"})
	require.Error(t, err)
}

// teranodeStub counts requests and answers every outpoint as unspent.
func teranodeStub(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":0}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestNewComposite(t *testing.T) {
	ctx := context.Background()
	ops := []store.Outpoint{{Vout: 0}}

	o, err := factory.New(factory.Config{Kind: "composite", ArcadeURL: "http://arcade.invalid"})
	require.NoError(t, err)
	assert.IsType(t, &composite.Oracle{}, o)
	res, err := o.Outpoints(ctx, ops)
	require.NoError(t, err, "without teranode, Outpoints falls back to arcade (Unknown, no network)")
	assert.Equal(t, oracle.StatusUnknown, res[0].Status)

	srv, hits := teranodeStub(t)
	o, err = factory.New(factory.Config{Kind: "composite", ArcadeURL: "http://arcade.invalid", TeranodeURL: srv.URL})
	require.NoError(t, err)
	_, _ = o.Outpoints(ctx, ops)
	assert.Positive(t, hits.Load(), "with teranode, Outpoints goes to teranode")

	_, err = factory.New(factory.Config{Kind: "composite", ArcadeURL: "http://a", TeranodeURL: "relative"})
	require.Error(t, err)
}
