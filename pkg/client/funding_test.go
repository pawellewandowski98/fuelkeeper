package client_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api"
	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/client"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/memory"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
)

// fakeFunder is a treasury.Funder test double that records the FundRequest it
// received and returns configurable results/errors.
type fakeFunder struct {
	info    treasury.FundingInfo
	infoErr error

	result    treasury.FundResult
	resultErr error

	gotReq treasury.FundRequest
}

func (f *fakeFunder) FundingInfo(context.Context) (treasury.FundingInfo, error) {
	return f.info, f.infoErr
}

func (f *fakeFunder) Internalize(_ context.Context, req treasury.FundRequest) (treasury.FundResult, error) {
	f.gotReq = req
	return f.result, f.resultErr
}

var _ treasury.Funder = (*fakeFunder)(nil)

// startFundingAPI starts a real pkg/api handler (api.NewHandler) with funder
// wired in, so the client is tested against its real HTTP encoding rather
// than a hand-rolled stub.
func startFundingAPI(t *testing.T, funder treasury.Funder, apiKeys, adminKeys []string) *httptest.Server {
	t.Helper()
	h := api.NewHandler(api.Deps{
		Store:     memory.New(),
		Funder:    funder,
		APIKeys:   apiKeys,
		AdminKeys: adminKeys,
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func TestClientFundingInfo(t *testing.T) {
	ctx := context.Background()
	funder := &fakeFunder{info: treasury.FundingInfo{
		IdentityKeyHex:      "02" + fmt.Sprintf("%064x", 0),
		DerivationPrefixB64: base64.StdEncoding.EncodeToString([]byte("nonce")),
		Network:             "main",
	}}
	srv := startFundingAPI(t, funder, []string{"admin-key"}, []string{"admin-key"})

	c, err := client.New(srv.URL, client.WithAPIKey("admin-key"))
	require.NoError(t, err)

	info, err := c.FundingInfo(ctx)
	require.NoError(t, err)
	assert.Equal(t, funder.info.IdentityKeyHex, info.IdentityKey)
	assert.Equal(t, funder.info.DerivationPrefixB64, info.DerivationPrefix)
	assert.Equal(t, "main", info.Network)
	assert.Equal(t, "3241645161d8", info.Protocol)
	assert.NotEmpty(t, info.Instructions)
}

func TestClientFund(t *testing.T) {
	ctx := context.Background()
	funder := &fakeFunder{result: treasury.FundResult{TxID: "abc123", Satoshis: 5000}}
	srv := startFundingAPI(t, funder, nil, nil)

	c, err := client.New(srv.URL)
	require.NoError(t, err)

	idx := 0
	req := wire.FundRequest{
		AtomicBEEF:        base64.StdEncoding.EncodeToString([]byte("beef-bytes")),
		OutputIndex:       &idx,
		DerivationPrefix:  base64.StdEncoding.EncodeToString([]byte("prefix")),
		DerivationSuffix:  base64.StdEncoding.EncodeToString([]byte("suffix")),
		SenderIdentityKey: "02" + fmt.Sprintf("%064x", 1),
	}
	res, err := c.Fund(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "abc123", res.TxID)
	assert.Equal(t, uint64(5000), res.Satoshis)

	assert.Equal(t, 0, funder.gotReq.OutputIndex)
	assert.Equal(t, req.DerivationPrefix, funder.gotReq.DerivationPrefixB64)
	assert.Equal(t, req.DerivationSuffix, funder.gotReq.DerivationSuffixB64)
	assert.Equal(t, req.SenderIdentityKey, funder.gotReq.SenderIdentityKeyHex)
}

// TestClientFundBadFunding checks that a treasury.ErrBadFunding-flavored
// rejection surfaces as an *APIError with Code "bad_funding" and is not
// retryable (it is a 400).
func TestClientFundBadFunding(t *testing.T) {
	ctx := context.Background()
	funder := &fakeFunder{resultErr: fmt.Errorf("%w: derivation mismatch", treasury.ErrBadFunding)}
	srv := startFundingAPI(t, funder, nil, nil)

	c, err := client.New(srv.URL)
	require.NoError(t, err)

	_, err = c.Fund(ctx, wire.FundRequest{
		AtomicBEEF:        base64.StdEncoding.EncodeToString([]byte("beef")),
		DerivationPrefix:  base64.StdEncoding.EncodeToString([]byte("prefix")),
		DerivationSuffix:  base64.StdEncoding.EncodeToString([]byte("suffix")),
		SenderIdentityKey: "02" + fmt.Sprintf("%064x", 2),
	})
	require.Error(t, err)
	var apiErr *client.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "bad_funding", apiErr.Code)
	assert.Equal(t, 400, apiErr.Status)
	assert.Contains(t, apiErr.Message, "derivation mismatch")
	assert.False(t, client.IsRetryable(err), "a 400 bad_funding must not be retryable")
}

// TestClientFundingDisabled checks that GET/POST /v1/funding without a
// configured treasury surfaces as an *APIError with Code "funding_disabled"
// and is not retryable (it is a 404).
func TestClientFundingDisabled(t *testing.T) {
	ctx := context.Background()
	srv := startFundingAPI(t, nil, nil, nil)

	c, err := client.New(srv.URL)
	require.NoError(t, err)

	_, err = c.FundingInfo(ctx)
	require.Error(t, err)
	var apiErr *client.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "funding_disabled", apiErr.Code)
	assert.Equal(t, 404, apiErr.Status)
	assert.False(t, client.IsRetryable(err), "a 404 funding_disabled must not be retryable")

	_, err = c.Fund(ctx, wire.FundRequest{
		AtomicBEEF:        base64.StdEncoding.EncodeToString([]byte("beef")),
		DerivationPrefix:  base64.StdEncoding.EncodeToString([]byte("prefix")),
		DerivationSuffix:  base64.StdEncoding.EncodeToString([]byte("suffix")),
		SenderIdentityKey: "02" + fmt.Sprintf("%064x", 3),
	})
	require.Error(t, err)
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "funding_disabled", apiErr.Code)
}

// TestClientFundUnavailable checks that an unrecognized Funder error surfaces
// as ErrUnavailable and is retryable (it is a 503).
func TestClientFundUnavailable(t *testing.T) {
	ctx := context.Background()
	funder := &fakeFunder{resultErr: errors.New("toolbox: mid-broadcast")}
	srv := startFundingAPI(t, funder, nil, nil)

	c, err := client.New(srv.URL)
	require.NoError(t, err)

	_, err = c.Fund(ctx, wire.FundRequest{
		AtomicBEEF:        base64.StdEncoding.EncodeToString([]byte("beef")),
		DerivationPrefix:  base64.StdEncoding.EncodeToString([]byte("prefix")),
		DerivationSuffix:  base64.StdEncoding.EncodeToString([]byte("suffix")),
		SenderIdentityKey: "02" + fmt.Sprintf("%064x", 4),
	})
	require.Error(t, err)
	require.ErrorIs(t, err, client.ErrUnavailable)
	assert.True(t, client.IsRetryable(err))
}
