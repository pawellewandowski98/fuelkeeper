package api_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api"
	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
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
	calls  int
}

func (f *fakeFunder) FundingInfo(context.Context) (treasury.FundingInfo, error) {
	return f.info, f.infoErr
}

func (f *fakeFunder) Internalize(_ context.Context, req treasury.FundRequest) (treasury.FundResult, error) {
	f.calls++
	f.gotReq = req
	return f.result, f.resultErr
}

var _ treasury.Funder = (*fakeFunder)(nil)

// validSenderKey is a syntactically valid (66 hex char) compressed identity
// key for tests that don't care about its actual value.
var validSenderKey = "02" + strings.Repeat("ab", 32)

// newFundingServer starts a FuelKeeper API server with funder (nil to
// exercise the "no treasury configured" 404 path) and no pools/store activity
// beyond what NewHandler itself requires.
func newFundingServer(t *testing.T, funder treasury.Funder, apiKeys, adminKeys []string) *httptest.Server {
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

// doFundingReq issues an HTTP request against srv's "/v1/funding" (the only
// path this file exercises), JSON-encoding body when non-nil, with an
// optional bearer key.
func doFundingReq(t *testing.T, srv *httptest.Server, method, key string, body any) *http.Response {
	t.Helper()
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, srv.URL+"/v1/funding", reader)
	require.NoError(t, err)
	if reader != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	return resp
}

// fundingErr decodes and closes resp.Body as a wire.Error.
func fundingErr(t *testing.T, resp *http.Response) wire.Error {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var e wire.Error
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&e))
	return e
}

// validFundRequest is a syntactically valid wire.FundRequest: every base64
// field decodes to something non-empty and the sender key is 66 hex chars.
func validFundRequest() wire.FundRequest {
	return wire.FundRequest{
		AtomicBEEF:        base64.StdEncoding.EncodeToString([]byte("atomic-beef-bytes")),
		DerivationPrefix:  base64.StdEncoding.EncodeToString([]byte("prefix-nonce")),
		DerivationSuffix:  base64.StdEncoding.EncodeToString([]byte("suffix-chosen-by-payer")),
		SenderIdentityKey: validSenderKey,
	}
}

func TestFundingInfoHandler(t *testing.T) {
	funder := &fakeFunder{info: treasury.FundingInfo{
		IdentityKeyHex:      validSenderKey,
		DerivationPrefixB64: base64.StdEncoding.EncodeToString([]byte("nonce")),
		Network:             "main",
	}}
	srv := newFundingServer(t, funder, []string{"k1"}, nil)

	resp := doFundingReq(t, srv, http.MethodGet, "k1", nil)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out wire.FundingInfo
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, funder.info.IdentityKeyHex, out.IdentityKey)
	assert.Equal(t, funder.info.DerivationPrefixB64, out.DerivationPrefix)
	assert.Equal(t, "main", out.Network)
	assert.Equal(t, "3241645161d8", out.Protocol)
	assert.NotEmpty(t, out.Instructions)
}

func TestFundingInfoFunderError(t *testing.T) {
	funder := &fakeFunder{infoErr: errors.New("toolbox: wallet locked")}
	srv := newFundingServer(t, funder, nil, nil)

	resp := doFundingReq(t, srv, http.MethodGet, "", nil)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	e := fundingErr(t, resp)
	assert.Equal(t, "unavailable", e.Code)
	assert.NotContains(t, e.Message, "toolbox", "an unrecognized funder error must not leak its detail")
	assert.Equal(t, "1", resp.Header.Get("Retry-After"))
}

func TestFundHandlerSuccess(t *testing.T) {
	funder := &fakeFunder{result: treasury.FundResult{TxID: "abc123", Satoshis: 5000}}
	srv := newFundingServer(t, funder, []string{"k1"}, nil)

	req := validFundRequest()
	resp := doFundingReq(t, srv, http.MethodPost, "k1", req)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out wire.FundResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Equal(t, "abc123", out.TxID)
	assert.Equal(t, uint64(5000), out.Satoshis)

	require.Equal(t, 1, funder.calls)
	assert.Equal(t, -1, funder.gotReq.OutputIndex, "omitted output_index must default to auto-detect (-1)")
	assert.Equal(t, req.DerivationPrefix, funder.gotReq.DerivationPrefixB64, "prefix is passed through as base64, not decoded")
	assert.Equal(t, req.DerivationSuffix, funder.gotReq.DerivationSuffixB64, "suffix is passed through as base64, not decoded")
	assert.Equal(t, req.SenderIdentityKey, funder.gotReq.SenderIdentityKeyHex)
	wantBEEF, err := base64.StdEncoding.DecodeString(req.AtomicBEEF)
	require.NoError(t, err)
	assert.Equal(t, wantBEEF, funder.gotReq.AtomicBEEF)
}

func TestFundHandlerOutputIndexPassedThrough(t *testing.T) {
	funder := &fakeFunder{result: treasury.FundResult{TxID: "t", Satoshis: 1}}
	srv := newFundingServer(t, funder, nil, nil)

	req := validFundRequest()
	idx := 2
	req.OutputIndex = &idx
	resp := doFundingReq(t, srv, http.MethodPost, "", req)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 2, funder.gotReq.OutputIndex)
}

func TestFundHandlerBadBase64(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*wire.FundRequest)
	}{
		{"malformed atomic_beef", func(r *wire.FundRequest) { r.AtomicBEEF = "not valid base64!!" }},
		{"empty atomic_beef", func(r *wire.FundRequest) { r.AtomicBEEF = "" }},
		{"malformed derivation_prefix", func(r *wire.FundRequest) { r.DerivationPrefix = "not valid base64!!" }},
		{"empty derivation_prefix", func(r *wire.FundRequest) { r.DerivationPrefix = "" }},
		{"malformed derivation_suffix", func(r *wire.FundRequest) { r.DerivationSuffix = "not valid base64!!" }},
		{"empty derivation_suffix", func(r *wire.FundRequest) { r.DerivationSuffix = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			funder := &fakeFunder{result: treasury.FundResult{TxID: "t", Satoshis: 1}}
			srv := newFundingServer(t, funder, nil, nil)
			req := validFundRequest()
			tc.mut(&req)
			resp := doFundingReq(t, srv, http.MethodPost, "", req)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Equal(t, "bad_request", fundingErr(t, resp).Code)
			assert.Equal(t, 0, funder.calls, "a malformed request must never reach the funder")
		})
	}
}

func TestFundHandlerBadSenderKey(t *testing.T) {
	cases := []string{"", "not-hex", "02ab", strings.Repeat("a", 67), strings.ToUpper(validSenderKey) + "0"}
	for _, key := range cases {
		t.Run(key, func(t *testing.T) {
			funder := &fakeFunder{result: treasury.FundResult{TxID: "t", Satoshis: 1}}
			srv := newFundingServer(t, funder, nil, nil)
			req := validFundRequest()
			req.SenderIdentityKey = key
			resp := doFundingReq(t, srv, http.MethodPost, "", req)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Equal(t, "bad_request", fundingErr(t, resp).Code)
			assert.Equal(t, 0, funder.calls)
		})
	}
}

func TestFundHandlerBadFunding(t *testing.T) {
	funder := &fakeFunder{resultErr: fmt.Errorf("%w: derivation mismatch", treasury.ErrBadFunding)}
	srv := newFundingServer(t, funder, nil, nil)

	resp := doFundingReq(t, srv, http.MethodPost, "", validFundRequest())
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	e := fundingErr(t, resp)
	assert.Equal(t, "bad_funding", e.Code)
	assert.Contains(t, e.Message, "derivation mismatch", "ErrBadFunding's detail is safe to expose")
}

func TestFundHandlerFunderUnavailable(t *testing.T) {
	funder := &fakeFunder{resultErr: errors.New("toolbox: mid-broadcast")}
	srv := newFundingServer(t, funder, nil, nil)

	resp := doFundingReq(t, srv, http.MethodPost, "", validFundRequest())
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	e := fundingErr(t, resp)
	assert.Equal(t, "unavailable", e.Code)
	assert.NotContains(t, e.Message, "toolbox", "an unrecognized funder error must not leak its detail")
	assert.Equal(t, "1", resp.Header.Get("Retry-After"))
}

func TestFundingDisabledWhenNoFunder(t *testing.T) {
	srv := newFundingServer(t, nil, nil, nil)

	resp := doFundingReq(t, srv, http.MethodGet, "", nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "funding_disabled", fundingErr(t, resp).Code)

	resp2 := doFundingReq(t, srv, http.MethodPost, "", validFundRequest())
	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
	assert.Equal(t, "funding_disabled", fundingErr(t, resp2).Code)
}

// TestFundingRequiresAdminKeyWhenConfigured checks that both funding routes
// are gated by AdminKeys (not merely APIKeys) once AdminKeys is configured,
// mirroring TestAdminKeys for the four existing admin routes.
func TestFundingRequiresAdminKeyWhenConfigured(t *testing.T) {
	funder := &fakeFunder{
		info:   treasury.FundingInfo{IdentityKeyHex: validSenderKey, Network: "main"},
		result: treasury.FundResult{TxID: "t", Satoshis: 1},
	}
	srv := newFundingServer(t, funder, []string{"fuel-key"}, []string{"admin-key"})

	resp := doFundingReq(t, srv, http.MethodGet, "fuel-key", nil)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, "a fuel-only key must not authorise funding")
	_ = resp.Body.Close()

	resp2 := doFundingReq(t, srv, http.MethodGet, "admin-key", nil)
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	_ = resp2.Body.Close()

	resp3 := doFundingReq(t, srv, http.MethodPost, "fuel-key", validFundRequest())
	assert.Equal(t, http.StatusUnauthorized, resp3.StatusCode)
	_ = resp3.Body.Close()

	resp4 := doFundingReq(t, srv, http.MethodPost, "admin-key", validFundRequest())
	assert.Equal(t, http.StatusOK, resp4.StatusCode)
	_ = resp4.Body.Close()

	resp5 := doFundingReq(t, srv, http.MethodGet, "", nil)
	assert.Equal(t, http.StatusUnauthorized, resp5.StatusCode, "no key at all must also be rejected")
	_ = resp5.Body.Close()
}
