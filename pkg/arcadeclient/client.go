// Package arcadeclient is a minimal HTTP client for the arcade transaction
// gateway: submitting transactions (single and batch), polling their
// lifecycle verdict, and discovering the current fee/size policy.
//
// The base URL passed to New must already include any version prefix the
// caller wants (for example "https://arcade.example.com/v1"); the client
// appends the unqualified paths "/tx", "/txs", "/tx/{txid}" and "/policy" to
// it verbatim, so the caller decides whether it talks to arcade's
// unversioned or ARC-compatible "/v1" surface.
package arcadeclient

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	// maxSuccessBody bounds how much of a 200/202 response body is read.
	maxSuccessBody = 1 << 20 // 1 MiB
	// maxErrorBody bounds how much of an error response body is read.
	maxErrorBody = 64 << 10 // 64 KiB
	// defaultTimeout is the HTTP client timeout used when the caller does
	// not supply their own client via WithHTTPClient.
	defaultTimeout = 15 * time.Second
	// defaultRetryAfter is used when a backpressure response omits
	// Retry-After, or sends a value that doesn't parse as a positive
	// integer.
	defaultRetryAfter = 1
	// txidLen is the length, in hex characters, of a well-formed txid.
	txidLen = 64
)

// ErrNotFound reports that arcade has no verdict for a txid: HTTP 404 from
// GET /tx/{txid}. Per the arcade contract this means either the transaction
// was never submitted to that instance, or its only submissions were
// answered with a non-persisting validation failure.
var ErrNotFound = errors.New("arcadeclient: transaction not found")

// errEmptyBatch is returned by PostTxs when given no transactions.
var errEmptyBatch = errors.New("arcadeclient: no transactions to submit")

// RejectError is arcade's 4xx validation response for POST /tx or POST /txs:
// the request was well-formed HTTP but the transaction (or one transaction
// in the batch) failed validation. Status carries the numeric ARC error code
// (the 460-476 taxonomy) when arcade could classify the failure.
type RejectError struct {
	// Status is the numeric ARC error code, or the plain HTTP status code
	// when arcade did not classify the failure.
	Status int `json:"status"`
	// Title is the ARC error title for Status.
	Title string `json:"title"`
	// Detail is the ARC error detail for Status.
	Detail string `json:"detail"`
	// TxID is the offending transaction id, when arcade identified one.
	TxID string `json:"txid"`
}

// Error implements the error interface.
func (e *RejectError) Error() string {
	return fmt.Sprintf("arcadeclient: arcade rejected tx %s: %d %s: %s", e.TxID, e.Status, e.Title, e.Detail)
}

// BackpressureError reports that arcade's broker is under backpressure
// (HTTP 503, or 429 from an intermediary): the transaction was not queued,
// and a retry after RetryAfter seconds is safe.
type BackpressureError struct {
	// RetryAfter is the number of seconds to wait before retrying, taken
	// from the response's Retry-After header (defaulting to 1 when absent
	// or not a positive integer).
	RetryAfter int
}

// Error implements the error interface.
func (e *BackpressureError) Error() string {
	return fmt.Sprintf("arcadeclient: arcade busy; retry after %ds", e.RetryAfter)
}

// TxStatus is the body of GET /tx/{txid}: arcade's TransactionStatus schema,
// restricted to the fields this client cares about.
type TxStatus struct {
	// TxID is the transaction id (hex).
	TxID string `json:"txid"`
	// TxStatus is the lifecycle state (arcade's Status enum, e.g. MINED,
	// REJECTED, DOUBLE_SPEND_ATTEMPTED).
	TxStatus string `json:"txStatus"`
	// Status is the numeric ARC error code recorded with a REJECTED
	// verdict, when classifiable. Zero when absent.
	Status int `json:"status"`
	// BlockHash is the hash of the block the transaction was mined in.
	BlockHash string `json:"blockHash"`
	// BlockHeight is the height of that block.
	BlockHeight uint32 `json:"blockHeight"`
	// MerklePath is the hex-encoded BUMP merkle path, present once mined.
	MerklePath string `json:"merklePath"`
	// ExtraInfo is free-form detail, e.g. a rejection reason.
	ExtraInfo string `json:"extraInfo"`
	// CompetingTxs lists txids double-spending this one.
	CompetingTxs []string `json:"competingTxs"`
}

// Mined reports whether the transaction has a terminal mined verdict
// (MINED or IMMUTABLE).
func (s *TxStatus) Mined() bool { return s.TxStatus == "MINED" || s.TxStatus == "IMMUTABLE" }

// Rejected reports a terminal REJECTED verdict.
func (s *TxStatus) Rejected() bool { return s.TxStatus == "REJECTED" }

// Conflicting reports a DOUBLE_SPEND_ATTEMPTED verdict.
func (s *TxStatus) Conflicting() bool { return s.TxStatus == "DOUBLE_SPEND_ATTEMPTED" }

// MerklePathBytes decodes the hex-encoded BUMP merkle path. It returns nil
// when the path is absent or malformed (odd length or non-hex characters).
func (s *TxStatus) MerklePathBytes() []byte {
	if s.MerklePath == "" {
		return nil
	}
	b, err := hex.DecodeString(s.MerklePath)
	if err != nil {
		return nil
	}
	return b
}

// SubmitResponse is the 202 body of POST /tx: arcade's SubmitResponse
// schema.
type SubmitResponse struct {
	// TxID is the submitted transaction's id (hex).
	TxID string `json:"txid"`
	// Status is the integer HTTP status code (202).
	Status int `json:"status"`
	// TxStatus is the transaction's lifecycle state at submission time.
	TxStatus string `json:"txStatus"`
}

// BatchSubmitResponse is the 202 body of POST /txs: arcade's
// BatchSubmitResponse schema.
type BatchSubmitResponse struct {
	// Submitted is the number of transactions newly published.
	Submitted int `json:"submitted"`
	// Duplicates is the number of transactions already seen and skipped.
	Duplicates int `json:"duplicates"`
	// Total is the number of transactions parsed from the request body.
	Total int `json:"total"`
}

// Policy is arcade's ARC-compatible transaction policy, the "policy" object
// nested inside GET /policy's response.
type Policy struct {
	// MiningFee is the fee rate, expressed as satoshis per a number of
	// bytes.
	MiningFee struct {
		// Satoshis is the fee amount.
		Satoshis uint64 `json:"satoshis"`
		// Bytes is the transaction size the fee amount is quoted over.
		Bytes uint64 `json:"bytes"`
	} `json:"miningFee"`
	// MaxTxSizePolicy is the maximum accepted transaction size, in bytes.
	MaxTxSizePolicy uint64 `json:"maxtxsizepolicy"`
	// MaxScriptSizePolicy is the maximum accepted script size, in bytes.
	MaxScriptSizePolicy uint64 `json:"maxscriptsizepolicy"`
	// MaxTxSigOpsCountsPolicy is the maximum sigops per transaction;
	// 4294967295 means unlimited (the BSV post-Genesis default).
	MaxTxSigOpsCountsPolicy uint64 `json:"maxtxsigopscountspolicy"`
	// StandardFormatSupported reports whether standard (non-extended
	// format) transactions are accepted at intake.
	StandardFormatSupported bool `json:"standardFormatSupported"`
}

// SatPerKB converts the quoted mining fee to satoshis per kilobyte, rounded
// up. It returns 0 when the policy's Bytes is 0.
func (p *Policy) SatPerKB() uint64 {
	if p.MiningFee.Bytes == 0 {
		return 0
	}
	return (p.MiningFee.Satoshis*1000 + p.MiningFee.Bytes - 1) / p.MiningFee.Bytes
}

// policyResponse mirrors arcade's PolicyResponse schema: GET /policy wraps
// the Policy object alongside a timestamp, it is not returned flat.
type policyResponse struct {
	Policy    Policy `json:"policy"`
	Timestamp string `json:"timestamp"`
}

// Option configures a Client constructed by New.
type Option func(*Client)

// WithCallbackToken sets the X-CallbackToken header on every request this
// client makes to arcade (submissions and status lookups).
func WithCallbackToken(tok string) Option {
	return func(c *Client) { c.callbackToken = tok }
}

// WithHTTPClient replaces the HTTP client used for requests. A nil client is
// a no-op: the default client is kept.
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) {
		if h != nil {
			c.http = h
		}
	}
}

// Client is a minimal client for one arcade base URL (for example
// "https://arcade.example.com/v1"). It is safe for concurrent use.
type Client struct {
	base          string
	http          *http.Client
	callbackToken string
}

// New constructs a Client for the given base URL. The base URL must be
// absolute (scheme and host required); a trailing slash is stripped.
func New(base string, opts ...Option) (*Client, error) {
	if base == "" {
		return nil, errors.New("arcadeclient: base url must not be empty")
	}
	u, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("arcadeclient: invalid base url %q: %w", base, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("arcadeclient: base url %q must be absolute (scheme and host required)", base)
	}
	c := &Client{
		base: strings.TrimRight(base, "/"),
		http: &http.Client{Timeout: defaultTimeout},
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// do issues one request against the base URL, wrapping both request
// construction and transport errors so errors.Is/As (e.g. against
// context.Canceled) sees through to the underlying cause.
func (c *Client) do(ctx context.Context, method, path, contentType string, body []byte) (*http.Response, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return nil, fmt.Errorf("arcadeclient: %s %s: %w", method, path, err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.callbackToken != "" {
		req.Header.Set("X-CallbackToken", c.callbackToken)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("arcadeclient: %s %s: %w", method, path, err)
	}
	return resp, nil
}

// isValidTxID reports whether txid is exactly 64 lowercase hex characters.
func isValidTxID(txid string) bool {
	if len(txid) != txidLen {
		return false
	}
	for i := 0; i < len(txid); i++ {
		c := txid[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// decodeJSON decodes one JSON value from body into v, refusing to read past
// limit+1 bytes. It returns an error both when the body cannot be decoded
// and when the body reaches the limit (a truncated or oversized response).
func decodeJSON(body io.Reader, limit int64, v any) error {
	lr := &io.LimitedReader{R: body, N: limit + 1}
	if err := json.NewDecoder(lr).Decode(v); err != nil {
		return fmt.Errorf("arcadeclient: decode response: %w", err)
	}
	if lr.N <= 0 {
		return fmt.Errorf("arcadeclient: response body exceeds %d byte limit", limit)
	}
	return nil
}

// parseRetryAfter reads the Retry-After header as a positive integer number
// of seconds, defaulting to defaultRetryAfter when absent or invalid.
func parseRetryAfter(h http.Header) int {
	ra, err := strconv.Atoi(h.Get("Retry-After"))
	if err != nil || ra <= 0 {
		return defaultRetryAfter
	}
	return ra
}

// parseRejectError best-effort decodes a 4xx body as a RejectError, falling
// back to the HTTP status code when the body is missing, malformed, or
// larger than the error-body limit.
func parseRejectError(resp *http.Response) *RejectError {
	var re RejectError
	_ = decodeJSON(resp.Body, maxErrorBody, &re)
	if re.Status == 0 {
		re.Status = resp.StatusCode
	}
	return &re
}

// GetTx returns arcade's current verdict for txid, or ErrNotFound when
// arcade has none. txid must be 64 lowercase hex characters; malformed
// txids are rejected before any request is made.
func (c *Client) GetTx(ctx context.Context, txid string) (*TxStatus, error) {
	if !isValidTxID(txid) {
		return nil, fmt.Errorf("arcadeclient: invalid txid %q: must be 64 lowercase hex characters", txid)
	}

	resp, err := c.do(ctx, http.MethodGet, "/tx/"+txid, "", nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("arcadeclient: GET /tx/%s: unexpected status %s", txid, resp.Status)
	}

	var st TxStatus
	if err := decodeJSON(resp.Body, maxSuccessBody, &st); err != nil {
		return nil, fmt.Errorf("arcadeclient: decode tx status: %w", err)
	}
	return &st, nil
}

// PostTx submits one transaction (raw or extended format bytes) for
// validation, propagation, and lifecycle tracking.
func (c *Client) PostTx(ctx context.Context, tx []byte) (*SubmitResponse, error) {
	resp, err := c.do(ctx, http.MethodPost, "/tx", "application/octet-stream", tx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted:
		var sr SubmitResponse
		if err := decodeJSON(resp.Body, maxSuccessBody, &sr); err != nil {
			return nil, fmt.Errorf("arcadeclient: decode submit response: %w", err)
		}
		return &sr, nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable:
		return nil, &BackpressureError{RetryAfter: parseRetryAfter(resp.Header)}
	case resp.StatusCode >= http.StatusBadRequest && resp.StatusCode < http.StatusInternalServerError:
		return nil, parseRejectError(resp)
	default:
		return nil, fmt.Errorf("arcadeclient: POST /tx: unexpected status %s", resp.Status)
	}
}

// PostTxs submits a batch of concatenated raw transactions in one request.
// It returns an error without contacting arcade when rawTxs is empty.
func (c *Client) PostTxs(ctx context.Context, rawTxs [][]byte) (*BatchSubmitResponse, error) {
	if len(rawTxs) == 0 {
		return nil, errEmptyBatch
	}

	resp, err := c.do(ctx, http.MethodPost, "/txs", "application/octet-stream", bytes.Join(rawTxs, nil))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted:
		var br BatchSubmitResponse
		if err := decodeJSON(resp.Body, maxSuccessBody, &br); err != nil {
			return nil, fmt.Errorf("arcadeclient: decode batch submit response: %w", err)
		}
		return &br, nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable:
		return nil, &BackpressureError{RetryAfter: parseRetryAfter(resp.Header)}
	case resp.StatusCode >= http.StatusBadRequest && resp.StatusCode < http.StatusInternalServerError:
		return nil, parseRejectError(resp)
	default:
		return nil, fmt.Errorf("arcadeclient: POST /txs: unexpected status %s", resp.Status)
	}
}

// Policy fetches arcade's current fee and size policy.
func (c *Client) Policy(ctx context.Context) (*Policy, error) {
	resp, err := c.do(ctx, http.MethodGet, "/policy", "", nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("arcadeclient: GET /policy: unexpected status %s", resp.Status)
	}

	var pr policyResponse
	if err := decodeJSON(resp.Body, maxSuccessBody, &pr); err != nil {
		return nil, fmt.Errorf("arcadeclient: decode policy: %w", err)
	}
	return &pr.Policy, nil
}
