package client

import (
	"context"
	"net/http"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
)

// FundingInfo fetches the treasury's BRC-29 payment instructions (admin key
// required when configured; see pkg/api's GET /v1/funding). A treasury.
// ErrBadFunding-flavored or "no treasury configured" response surfaces as an
// *APIError with Code "bad_funding" or "funding_disabled" respectively.
func (c *Client) FundingInfo(ctx context.Context) (*wire.FundingInfo, error) {
	var out wire.FundingInfo
	if err := c.doJSON(ctx, http.MethodGet, "/v1/funding", "", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Fund posts a completed BRC-29 payment for the treasury to internalize (see
// pkg/api's POST /v1/funding). A rejected payment surfaces as an *APIError
// with Code "bad_funding" (not retryable).
func (c *Client) Fund(ctx context.Context, req wire.FundRequest) (*wire.FundResult, error) {
	var out wire.FundResult
	if err := c.doJSON(ctx, http.MethodPost, "/v1/funding", "", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
