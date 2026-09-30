package arcadeclient_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/arcadeclient"
)

// 64-lowercase-hex txids used across the fake server and the tests below.
// GetTx validates this shape before making a request, so the shorthand
// "aa"/"bb" ids from early drafts of this test would be rejected client-side
// before ever reaching the fake server.
var (
	txidMined     = strings.Repeat("a", 64)
	txidRejected  = strings.Repeat("b", 64)
	txidCompeting = strings.Repeat("c", 64)
	txidRejectSub = strings.Repeat("d", 64)
	txidSubmitted = strings.Repeat("e", 64)
	txidUnknown   = strings.Repeat("f", 64)
)

// fakeArcade serves the subset of the arcade contract this client speaks,
// mounted under /v1 the way a real deployment's ARC-compatible alias is.
func fakeArcade(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/tx/{txid}", func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("X-CallbackToken"), "GetTx must not send X-CallbackToken; arcade scopes it to submissions")
		switch r.PathValue("txid") {
		case txidMined:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"txid": txidMined, "txStatus": "MINED", "blockHeight": 12,
				"merklePath": "fe0c", "status": 200,
			})
		case txidRejected:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"txid": txidRejected, "txStatus": "REJECTED", "status": 461,
				"extraInfo": "double spend", "competingTxs": []string{txidCompeting},
			})
		default:
			http.Error(w, `{"error":"transaction not found"}`, http.StatusNotFound)
		}
	})
	mux.HandleFunc("POST /v1/tx", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/octet-stream", r.Header.Get("Content-Type"))
		assert.Equal(t, "tok", r.Header.Get("X-CallbackToken"))
		body, _ := io.ReadAll(r.Body)
		switch string(body) {
		case "reject":
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": 463, "title": "Malformed transaction", "detail": "bad", "txid": txidRejectSub,
			})
		case "busy":
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"txid": txidSubmitted, "status": 202, "txStatus": "RECEIVED",
			})
		}
	})
	mux.HandleFunc("POST /v1/txs", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"submitted": 2, "duplicates": 0, "total": 2})
	})
	mux.HandleFunc("GET /v1/policy", func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("X-CallbackToken"), "Policy must not send X-CallbackToken; the endpoint has no callback concept")
		// The real contract's PolicyResponse wraps the policy under a
		// "policy" key alongside a "timestamp" (see arcade.openapi.yaml,
		// schemas PolicyResponse/Policy) -- it is not a flat object.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"policy": map[string]any{
				"miningFee":       map[string]int{"satoshis": 100, "bytes": 1000},
				"maxtxsizepolicy": 100000000,
			},
			"timestamp": "2026-08-07T12:00:00Z",
		})
	})
	return httptest.NewServer(mux)
}

func TestClient(t *testing.T) {
	ctx := context.Background()
	srv := fakeArcade(t)
	defer srv.Close()
	c, err := arcadeclient.New(srv.URL+"/v1", arcadeclient.WithCallbackToken("tok"))
	require.NoError(t, err)

	st, err := c.GetTx(ctx, txidMined)
	require.NoError(t, err)
	assert.Equal(t, "MINED", st.TxStatus)
	assert.Equal(t, uint32(12), st.BlockHeight)
	assert.Equal(t, []byte{0xfe, 0x0c}, st.MerklePathBytes())
	assert.True(t, st.Mined())
	assert.False(t, st.Rejected())

	st, err = c.GetTx(ctx, txidRejected)
	require.NoError(t, err)
	assert.True(t, st.Rejected())
	assert.Equal(t, []string{txidCompeting}, st.CompetingTxs)

	_, err = c.GetTx(ctx, txidUnknown)
	require.ErrorIs(t, err, arcadeclient.ErrNotFound)

	sub, err := c.PostTx(ctx, []byte("rawtx"))
	require.NoError(t, err)
	assert.Equal(t, txidSubmitted, sub.TxID)

	_, err = c.PostTx(ctx, []byte("reject"))
	var rej *arcadeclient.RejectError
	require.ErrorAs(t, err, &rej)
	assert.Equal(t, 463, rej.Status)
	assert.Equal(t, txidRejectSub, rej.TxID)

	_, err = c.PostTx(ctx, []byte("busy"))
	var bp *arcadeclient.BackpressureError
	require.ErrorAs(t, err, &bp)
	assert.Equal(t, 7, bp.RetryAfter)

	batch, err := c.PostTxs(ctx, [][]byte{[]byte("a"), []byte("b")})
	require.NoError(t, err)
	assert.Equal(t, 2, batch.Submitted)

	pol, err := c.Policy(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), pol.SatPerKB())
}

// TestPostTxDoubleSpendAttemptedBody covers a 202 SubmitResponse that
// already knows of a competitor at submission time: txStatus
// DOUBLE_SPEND_ATTEMPTED alongside competingTxs, per the arcade contract
// (arcade.openapi.yaml SubmitResponse/TransactionStatus schemas).
func TestPostTxDoubleSpendAttemptedBody(t *testing.T) {
	competitor := strings.Repeat("c", 64)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/tx", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"txid": txidSubmitted, "status": 202, "txStatus": "DOUBLE_SPEND_ATTEMPTED",
			"competingTxs": []string{competitor}, "extraInfo": "double spend",
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)

	sub, err := c.PostTx(context.Background(), []byte("tx"))
	require.NoError(t, err)
	assert.Equal(t, []string{competitor}, sub.CompetingTxs)
	assert.Equal(t, "double spend", sub.ExtraInfo)
	assert.True(t, sub.DoubleSpend())
}

// TestRejectErrorCarriesCompetingTxs covers a 4xx RejectError body that
// names competing transactions (e.g. ARC code 466, conflicting tx found).
func TestRejectErrorCarriesCompetingTxs(t *testing.T) {
	competitor := strings.Repeat("c", 64)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/tx", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": 466, "title": "Conflicting tx found", "detail": "conflict",
			"txid": txidRejectSub, "competingTxs": []string{competitor}, "extraInfo": "already spent",
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)

	_, err = c.PostTx(context.Background(), []byte("tx"))
	var rej *arcadeclient.RejectError
	require.ErrorAs(t, err, &rej)
	assert.Equal(t, 466, rej.Status)
	assert.Equal(t, []string{competitor}, rej.CompetingTxs)
	assert.Equal(t, "already spent", rej.ExtraInfo)
	assert.True(t, rej.DoubleSpend())
}

// TestCompetingTxsAbsentLeavesNil pins that SubmitResponse.CompetingTxs and
// RejectError.CompetingTxs stay nil (not an empty non-nil slice) when the
// response body carries no competingTxs field, so DoubleSpend() and a
// simple len()/nil check agree.
func TestCompetingTxsAbsentLeavesNil(t *testing.T) {
	ctx := context.Background()
	srv := fakeArcade(t)
	defer srv.Close()
	c, err := arcadeclient.New(srv.URL+"/v1", arcadeclient.WithCallbackToken("tok"))
	require.NoError(t, err)

	sub, err := c.PostTx(ctx, []byte("rawtx")) // fakeArcade's default RECEIVED response
	require.NoError(t, err)
	assert.Nil(t, sub.CompetingTxs)
	assert.False(t, sub.DoubleSpend())

	_, err = c.PostTx(ctx, []byte("reject")) // fakeArcade's reject response, no competingTxs
	var rej *arcadeclient.RejectError
	require.ErrorAs(t, err, &rej)
	assert.Nil(t, rej.CompetingTxs)
	assert.False(t, rej.DoubleSpend())
}

func TestNew(t *testing.T) {
	tests := []struct {
		name string
		base string
	}{
		{"empty", ""},
		{"no scheme", "arcade.example.com/v1"},
		{"no host", "https:///v1"},
		{"bad scheme", "ftp://arcade.example.com/v1"},
		{"query string", "https://arcade.example.com/v1?token=abc"},
		{"fragment", "https://arcade.example.com/v1#frag"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := arcadeclient.New(tt.base)
			require.Error(t, err)
		})
	}
}

func TestNew_TrailingSlashStripped(t *testing.T) {
	var gotPath string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/policy", func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewEncoder(w).Encode(map[string]any{
			"policy": map[string]any{
				"miningFee":       map[string]int{"satoshis": 1, "bytes": 1},
				"maxtxsizepolicy": 1,
			},
			"timestamp": "2026-08-07T12:00:00Z",
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c, err := arcadeclient.New(srv.URL + "/v1/")
	require.NoError(t, err)

	_, err = c.Policy(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "/v1/policy", gotPath)
	assert.NotContains(t, gotPath, "//")
}

func TestGetTx_ServerError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/tx/{txid}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)

	_, err = c.GetTx(context.Background(), txidMined)
	require.Error(t, err)
	require.NotErrorIs(t, err, arcadeclient.ErrNotFound)
	var rej *arcadeclient.RejectError
	require.NotErrorAs(t, err, &rej)
}

func TestGetTx_MalformedJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/tx/{txid}", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{not json"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)

	_, err = c.GetTx(context.Background(), txidMined)
	require.Error(t, err)
}

func TestGetTx_InvalidTxID(t *testing.T) {
	var called atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(http.ResponseWriter, *http.Request) { called.Store(true) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)

	invalid := []string{
		"",
		"zz",
		strings.Repeat("a", 63),
		strings.Repeat("a", 65),
		strings.Repeat("A", 64),
		strings.Repeat("g", 64),
	}
	for _, txid := range invalid {
		_, err := c.GetTx(context.Background(), txid)
		require.Errorf(t, err, "txid=%q", txid)
	}
	assert.False(t, called.Load(), "arcade should not have been called for an invalid txid")
}

func TestPostTx_Backpressure(t *testing.T) {
	tests := []struct {
		name           string
		status         int
		retryAfter     string
		wantRetryAfter int
	}{
		{"429 with retry-after", http.StatusTooManyRequests, "3", 3},
		{"503 with retry-after", http.StatusServiceUnavailable, "7", 7},
		{"503 missing retry-after", http.StatusServiceUnavailable, "", 1},
		{"503 non-numeric retry-after", http.StatusServiceUnavailable, "soon", 1},
		{"503 negative retry-after", http.StatusServiceUnavailable, "-4", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("POST /v1/tx", func(w http.ResponseWriter, _ *http.Request) {
				if tt.retryAfter != "" {
					w.Header().Set("Retry-After", tt.retryAfter)
				}
				w.WriteHeader(tt.status)
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()
			c, err := arcadeclient.New(srv.URL + "/v1")
			require.NoError(t, err)

			_, err = c.PostTx(context.Background(), []byte("tx"))
			var bp *arcadeclient.BackpressureError
			require.ErrorAs(t, err, &bp)
			assert.Equal(t, tt.wantRetryAfter, bp.RetryAfter)
		})
	}
}

func TestPostTxs_EmptyBatch(t *testing.T) {
	var called atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(http.ResponseWriter, *http.Request) { called.Store(true) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)

	_, err = c.PostTxs(context.Background(), nil)
	require.Error(t, err)
	assert.False(t, called.Load(), "arcade should not have been called for an empty batch")
}

func TestPostTxs_Reject4xx(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/txs", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": 422, "title": "bad batch", "txid": txidRejectSub})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)

	_, err = c.PostTxs(context.Background(), [][]byte{[]byte("a")})
	var rej *arcadeclient.RejectError
	require.ErrorAs(t, err, &rej)
	assert.Equal(t, 422, rej.Status)
}

func TestPostTxs_Backpressure503(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/txs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "4")
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)

	_, err = c.PostTxs(context.Background(), [][]byte{[]byte("a")})
	var bp *arcadeclient.BackpressureError
	require.ErrorAs(t, err, &bp)
	assert.Equal(t, 4, bp.RetryAfter)
}

func TestContextCancelled(t *testing.T) {
	srv := fakeArcade(t)
	defer srv.Close()
	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("GetTx", func(t *testing.T) {
		_, err := c.GetTx(ctx, txidMined)
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("PostTx", func(t *testing.T) {
		_, err := c.PostTx(ctx, []byte("tx"))
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("PostTxs", func(t *testing.T) {
		_, err := c.PostTxs(ctx, [][]byte{[]byte("tx")})
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("Policy", func(t *testing.T) {
		_, err := c.Policy(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestTxStatusPredicates(t *testing.T) {
	tests := []struct {
		status       string
		wantMined    bool
		wantRejected bool
		wantConflict bool
	}{
		{"MINED", true, false, false},
		{"IMMUTABLE", true, false, false},
		{"REJECTED", false, true, false},
		{"DOUBLE_SPEND_ATTEMPTED", false, false, true},
		{"RECEIVED", false, false, false},
		{"SEEN_ON_NETWORK", false, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			st := &arcadeclient.TxStatus{TxStatus: tt.status}
			assert.Equal(t, tt.wantMined, st.Mined())
			assert.Equal(t, tt.wantRejected, st.Rejected())
			assert.Equal(t, tt.wantConflict, st.Conflicting())
		})
	}
}

func TestMerklePathBytes(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []byte
	}{
		{"empty", "", nil},
		{"valid", "fe0c", []byte{0xfe, 0x0c}},
		{"odd length", "abc", nil},
		{"non-hex", "zzzz", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &arcadeclient.TxStatus{MerklePath: tt.in}
			assert.Equal(t, tt.want, st.MerklePathBytes())
		})
	}
}

func TestPolicySatPerKB(t *testing.T) {
	tests := []struct {
		name     string
		satoshis uint64
		bytes    uint64
		want     uint64
	}{
		{"rounds up", 1, 3, 334},
		{"exact", 100, 1000, 100},
		{"zero bytes", 5, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &arcadeclient.Policy{}
			p.MiningFee.Satoshis = tt.satoshis
			p.MiningFee.Bytes = tt.bytes
			assert.Equal(t, tt.want, p.SatPerKB())
		})
	}
}

func TestLargeSuccessBodyTruncated(t *testing.T) {
	junk := bytes.Repeat([]byte("x"), 2<<20) // 2 MiB of non-JSON junk.

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/policy", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(junk)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)

	_, err = c.Policy(context.Background())
	require.Error(t, err)
}

func TestLargeErrorBodyTruncated(t *testing.T) {
	junk := bytes.Repeat([]byte("x"), 2<<20) // 2 MiB of non-JSON junk.

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/tx", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(junk)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)

	_, err = c.PostTx(context.Background(), []byte("tx"))
	var rej *arcadeclient.RejectError
	require.ErrorAs(t, err, &rej)
	// The body could not be parsed as a RejectError within the bound, so the
	// HTTP status code is the fallback.
	assert.Equal(t, http.StatusBadRequest, rej.Status)
}

// recordingTransport wraps a RoundTripper and records whether it was used,
// so WithHTTPClient's effect on the client is directly observable.
type recordingTransport struct {
	called bool
	next   http.RoundTripper
}

func (rt *recordingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	rt.called = true
	resp, err := rt.next.RoundTrip(r)
	if err != nil {
		return nil, fmt.Errorf("recording transport: %w", err)
	}
	return resp, nil
}

func TestWithHTTPClient(t *testing.T) {
	srv := fakeArcade(t)
	defer srv.Close()

	rt := &recordingTransport{next: http.DefaultTransport}
	hc := &http.Client{Transport: rt}

	c, err := arcadeclient.New(srv.URL+"/v1", arcadeclient.WithHTTPClient(hc))
	require.NoError(t, err)

	_, err = c.Policy(context.Background())
	require.NoError(t, err)
	assert.True(t, rt.called)
}

func TestWithHTTPClientNilIsNoop(t *testing.T) {
	srv := fakeArcade(t)
	defer srv.Close()

	c, err := arcadeclient.New(srv.URL+"/v1", arcadeclient.WithHTTPClient(nil))
	require.NoError(t, err)

	_, err = c.Policy(context.Background())
	require.NoError(t, err)
}

// TestUnexpectedStatusError checks that all four methods surface a status
// code outside the set they understand (e.g. a 502 from a misbehaving
// intermediary) as a typed *UnexpectedStatusError carrying the method, path
// and code, rather than an opaque string.
func TestUnexpectedStatusError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)

	t.Run("GetTx", func(t *testing.T) {
		_, err := c.GetTx(context.Background(), txidMined)
		var use *arcadeclient.UnexpectedStatusError
		require.ErrorAs(t, err, &use)
		assert.Equal(t, http.MethodGet, use.Method)
		assert.Equal(t, "/tx/"+txidMined, use.Path)
		assert.Equal(t, http.StatusBadGateway, use.Code)
	})
	t.Run("PostTx", func(t *testing.T) {
		_, err := c.PostTx(context.Background(), []byte("tx"))
		var use *arcadeclient.UnexpectedStatusError
		require.ErrorAs(t, err, &use)
		assert.Equal(t, http.MethodPost, use.Method)
		assert.Equal(t, "/tx", use.Path)
		assert.Equal(t, http.StatusBadGateway, use.Code)
	})
	t.Run("PostTxs", func(t *testing.T) {
		_, err := c.PostTxs(context.Background(), [][]byte{[]byte("tx")})
		var use *arcadeclient.UnexpectedStatusError
		require.ErrorAs(t, err, &use)
		assert.Equal(t, http.MethodPost, use.Method)
		assert.Equal(t, "/txs", use.Path)
		assert.Equal(t, http.StatusBadGateway, use.Code)
	})
	t.Run("Policy", func(t *testing.T) {
		_, err := c.Policy(context.Background())
		var use *arcadeclient.UnexpectedStatusError
		require.ErrorAs(t, err, &use)
		assert.Equal(t, http.MethodGet, use.Method)
		assert.Equal(t, "/policy", use.Path)
		assert.Equal(t, http.StatusBadGateway, use.Code)
	})
}

// TestNewRedactsUserinfo checks that a credential in the base URL never
// reaches an error message.
func TestNewRedactsUserinfo(t *testing.T) {
	for _, base := range []string{
		"ftp://user:s3cr3t@arcade.example",
		"http://user:s3cr3t@arcade.example/v1?x=1",
		"http://user:s3cr3t@[::1",
		"//user:s3cr3t@arcade.example",
	} {
		_, err := arcadeclient.New(base)
		require.Error(t, err, base)
		assert.NotContains(t, err.Error(), "s3cr3t", base)
		assert.NotContains(t, err.Error(), "user:", base)
	}
}
