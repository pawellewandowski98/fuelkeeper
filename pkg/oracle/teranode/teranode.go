// Package teranode adapts a teranode node's asset HTTP API
// (/git/teranode/services/asset/httpimpl) to the oracle.Oracle interface.
//
// Outpoints uses GET /utxo/{txid}/json for small batches and POST
// /utxos/json (one binary-encoded request, one JSON response) once a batch
// grows past Config.BatchThreshold. Tx fetches the raw transaction from GET
// /tx/{txid} and parses it to report the outpoints it spends; it never
// reports Mined, since that endpoint does not say whether the transaction
// made it into a block.
package teranode

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"golang.org/x/sync/errgroup"

	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

const (
	// defaultTimeout is used for the HTTP client when Config.HTTPClient is nil.
	defaultTimeout = 10 * time.Second
	// defaultConcurrency bounds parallel single-outpoint GETs when Config.Concurrency is 0.
	defaultConcurrency = 16
	// defaultBatchThreshold is used when Config.BatchThreshold is 0.
	defaultBatchThreshold = 8
	// maxResponseBody bounds how much of a JSON or raw-tx response body is read.
	maxResponseBody = 16 << 20 // 16 MiB
	// utxoRecordSize is the on-wire size of one (txid, vout) record in the
	// POST /utxos/json request body: 32 bytes txid (internal order) + 4
	// bytes vout (little-endian).
	utxoRecordSize = 36
)

// Teranode UTXO status codes (stores/utxo/status.pb.go).
const (
	statusOK            = 0
	statusSpent         = 1
	statusImmature      = 2
	statusNotFound      = 3
	statusAlreadyExists = 4
	statusFrozen        = 5
	statusConflicting   = 6
	statusLocked        = 7
)

// Config configures a teranode Oracle.
type Config struct {
	// BaseURL is the asset API base, e.g. "http://teranode:8090/api/v1". Required.
	BaseURL string
	// HTTPClient is the HTTP client to use; a client with a 10s timeout is
	// used when nil.
	HTTPClient *http.Client
	// Concurrency bounds parallel single-outpoint GETs for batches at or
	// below BatchThreshold. 0 means the default (16).
	Concurrency int
	// BatchThreshold is the outpoint count above which Outpoints uses one
	// POST /utxos/json instead of individual GETs. 0 means the default (8);
	// a negative value disables batching entirely.
	BatchThreshold int
}

// Oracle adapts a teranode asset API to oracle.Oracle.
type Oracle struct {
	base           string
	http           *http.Client
	concurrency    int
	batchThreshold int
}

var _ oracle.Oracle = (*Oracle)(nil)

// New validates cfg and returns an Oracle for it. BaseURL must be an
// absolute URL (scheme and host required); a trailing slash is stripped.
func New(cfg Config) (*Oracle, error) {
	if cfg.BaseURL == "" {
		return nil, errors.New("teranode: base url must not be empty")
	}
	u, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("teranode: invalid base url %q: %w", cfg.BaseURL, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("teranode: base url %q must be absolute (scheme and host required)", cfg.BaseURL)
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}
	concurrency := cfg.Concurrency
	if concurrency <= 0 {
		concurrency = defaultConcurrency
	}
	threshold := cfg.BatchThreshold
	if threshold == 0 {
		threshold = defaultBatchThreshold
	}

	return &Oracle{
		base:           strings.TrimRight(cfg.BaseURL, "/"),
		http:           httpClient,
		concurrency:    concurrency,
		batchThreshold: threshold,
	}, nil
}

// NewURL is New(Config{BaseURL: base}).
func NewURL(base string) (*Oracle, error) {
	return New(Config{BaseURL: base})
}

// wireSpendingData mirrors teranode's spend.SpendingData JSON shape.
type wireSpendingData struct {
	TxID string `json:"txId"`
	Vin  int    `json:"vin"`
}

// wireSpendResponse mirrors teranode's utxo.SpendResponse JSON shape.
type wireSpendResponse struct {
	Status       int               `json:"status"`
	SpendingData *wireSpendingData `json:"spendingData,omitempty"`
	LockTime     uint32            `json:"lockTime,omitempty"`
}

// Outpoints returns one Result per op, in input order. Batches larger than
// batchThreshold are served by a single POST /utxos/json; smaller batches
// (and a negative batchThreshold, which disables batching) fan out bounded
// parallel GETs.
func (o *Oracle) Outpoints(ctx context.Context, ops []store.Outpoint) ([]oracle.Result, error) {
	if len(ops) == 0 {
		return []oracle.Result{}, nil
	}
	if o.batchThreshold >= 0 && len(ops) > o.batchThreshold {
		return o.outpointsBatch(ctx, ops)
	}
	return o.outpointsSingle(ctx, ops)
}

// outpointsSingle fetches each outpoint with its own GET, bounded by
// o.concurrency, writing each result to its own output slot so no mutex is
// needed.
func (o *Oracle) outpointsSingle(ctx context.Context, ops []store.Outpoint) ([]oracle.Result, error) {
	out := make([]oracle.Result, len(ops))
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(o.concurrency)
	for i, op := range ops {
		g.Go(func() error {
			res, err := o.getOutpoint(gCtx, op)
			if err != nil {
				return err
			}
			out[i] = res
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

// getOutpoint performs one GET /utxo/{txid}/json?vout=N lookup.
func (o *Oracle) getOutpoint(ctx context.Context, op store.Outpoint) (oracle.Result, error) {
	u := o.base + "/utxo/" + op.TxID.String() + "/json?vout=" + strconv.FormatUint(uint64(op.Vout), 10)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return oracle.Result{}, fmt.Errorf("%w: build request: %w", oracle.ErrUnavailable, err)
	}
	resp, err := o.http.Do(req)
	if err != nil {
		return oracle.Result{}, fmt.Errorf("%w: GET %s: %w", oracle.ErrUnavailable, u, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return oracle.Result{Outpoint: op, Status: oracle.StatusUnknown}, nil
	}
	if resp.StatusCode != http.StatusOK {
		// 400 is included here deliberately: it is teranode reporting a
		// malformed request built by this client (or a param it rejects for
		// reasons this client cannot introspect), which this tick cannot
		// fix by retrying differently; ErrUnavailable lets the caller retry
		// the tick later rather than treating it as a definitive verdict.
		return oracle.Result{}, fmt.Errorf("%w: GET %s: unexpected status %s", oracle.ErrUnavailable, u, resp.Status)
	}

	var sr wireSpendResponse
	if err := decodeJSONLimited(resp.Body, &sr); err != nil {
		return oracle.Result{}, fmt.Errorf("%w: decode utxo response: %w", oracle.ErrUnavailable, err)
	}
	return mapSpendResponse(op, sr), nil
}

// outpointsBatch performs one POST /utxos/json for the whole batch.
func (o *Oracle) outpointsBatch(ctx context.Context, ops []store.Outpoint) ([]oracle.Result, error) {
	body := make([]byte, 0, len(ops)*utxoRecordSize)
	for _, op := range ops {
		var rec [utxoRecordSize]byte
		copy(rec[:32], op.TxID[:])
		binary.LittleEndian.PutUint32(rec[32:36], op.Vout)
		body = append(body, rec[:]...)
	}

	u := o.base + "/utxos/json"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%w: build request: %w", oracle.ErrUnavailable, err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := o.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: POST %s: %w", oracle.ErrUnavailable, u, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		// Covers 400 (bad length), 413 (too large), 429 (rate-limited) and
		// 5xx alike: none of these are a verdict this tick can act on.
		return nil, fmt.Errorf("%w: POST %s: unexpected status %s", oracle.ErrUnavailable, u, resp.Status)
	}

	var results []wireSpendResponse
	if err := decodeJSONLimited(resp.Body, &results); err != nil {
		return nil, fmt.Errorf("%w: decode utxos batch response: %w", oracle.ErrUnavailable, err)
	}
	if len(results) != len(ops) {
		return nil, fmt.Errorf("%w: utxos batch response has %d records for %d requested", oracle.ErrUnavailable, len(results), len(ops))
	}

	out := make([]oracle.Result, len(ops))
	for i, op := range ops {
		out[i] = mapSpendResponse(op, results[i])
	}
	return out, nil
}

// mapSpendResponse converts one teranode SpendResponse into an oracle.Result
// for op, per the Status enum in stores/utxo/status.pb.go.
func mapSpendResponse(op store.Outpoint, sr wireSpendResponse) oracle.Result {
	switch sr.Status {
	case statusOK, statusImmature:
		return oracle.Result{Outpoint: op, Status: oracle.StatusUnspent}
	case statusSpent:
		res := oracle.Result{Outpoint: op, Status: oracle.StatusSpent}
		if sr.SpendingData != nil && sr.SpendingData.TxID != "" {
			if h, err := chainhash.NewHashFromHex(sr.SpendingData.TxID); err == nil {
				res.SpendingTxID = h
			}
		}
		return res
	case statusFrozen, statusConflicting:
		return oracle.Result{Outpoint: op, Status: oracle.StatusConflicting}
	case statusNotFound, statusAlreadyExists, statusLocked:
		return oracle.Result{Outpoint: op, Status: oracle.StatusUnknown}
	default:
		return oracle.Result{Outpoint: op, Status: oracle.StatusUnknown}
	}
}

// Tx fetches the raw transaction from GET /tx/{txid} and reports the
// outpoints it spends. It never sets Mined or Rejected: this endpoint only
// says whether the transaction is known, not what became of it.
func (o *Oracle) Tx(ctx context.Context, txid chainhash.Hash) (oracle.TxStatus, error) {
	u := o.base + "/tx/" + txid.String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return oracle.TxStatus{}, fmt.Errorf("%w: build request: %w", oracle.ErrUnavailable, err)
	}
	resp, err := o.http.Do(req)
	if err != nil {
		return oracle.TxStatus{}, fmt.Errorf("%w: GET %s: %w", oracle.ErrUnavailable, u, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return oracle.TxStatus{}, nil
	}
	if resp.StatusCode != http.StatusOK {
		return oracle.TxStatus{}, fmt.Errorf("%w: GET %s: unexpected status %s", oracle.ErrUnavailable, u, resp.Status)
	}

	raw, err := readLimited(resp.Body)
	if err != nil {
		return oracle.TxStatus{}, fmt.Errorf("%w: read tx body: %w", oracle.ErrUnavailable, err)
	}

	tx, err := transaction.NewTransactionFromBytes(raw)
	if err != nil {
		return oracle.TxStatus{}, fmt.Errorf("%w: parse tx %s: %w", oracle.ErrUnavailable, txid.String(), err)
	}
	got := tx.TxID()
	if got == nil || *got != txid {
		return oracle.TxStatus{}, fmt.Errorf("%w: parsed tx id does not match requested %s", oracle.ErrUnavailable, txid.String())
	}

	inputs := make([]store.Outpoint, len(tx.Inputs))
	for i, in := range tx.Inputs {
		var srcID chainhash.Hash
		if in.SourceTXID != nil {
			srcID = *in.SourceTXID
		}
		inputs[i] = store.Outpoint{TxID: srcID, Vout: in.SourceTxOutIndex}
	}

	return oracle.TxStatus{Known: true, Status: "known", Inputs: inputs}, nil
}

// decodeJSONLimited decodes one JSON value from body into v, refusing to
// read past maxResponseBody+1 bytes.
func decodeJSONLimited(body io.Reader, v any) error {
	lr := &io.LimitedReader{R: body, N: maxResponseBody + 1}
	if err := json.NewDecoder(lr).Decode(v); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if lr.N <= 0 {
		return fmt.Errorf("response body exceeds %d byte limit", maxResponseBody)
	}
	return nil
}

// readLimited reads body, refusing to read past maxResponseBody+1 bytes.
func readLimited(body io.Reader) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(body, maxResponseBody+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxResponseBody {
		return nil, fmt.Errorf("body exceeds %d byte limit", maxResponseBody)
	}
	return b, nil
}
