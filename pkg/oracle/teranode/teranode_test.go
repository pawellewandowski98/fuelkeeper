package teranode_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle/teranode"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// wireSpendingData and wireSpendResponse mirror teranode's
// utxo.SpendResponse / spend.SpendingData JSON wire shapes (see
// /git/teranode/stores/utxo/spendresponse.go and .../spend/spending_data.go):
// {"status":<int>,"spendingData":{"txId":"<display hex>","vin":<int>},"lockTime":<uint32>}.
type wireSpendingData struct {
	TxID string `json:"txId"`
	Vin  int    `json:"vin"`
}

type wireSpendResponse struct {
	Status       *int              `json:"status"`
	SpendingData *wireSpendingData `json:"spendingData,omitempty"`
	LockTime     uint32            `json:"lockTime,omitempty"`
}

// intPtr builds the *int wireSpendResponse.Status expects.
func intPtr(i int) *int { return &i }

func TestOutpointsSingleGET(t *testing.T) {
	spentOp := store.Outpoint{TxID: storetest.NewTxID(), Vout: 0}
	unspentOp := store.Outpoint{TxID: storetest.NewTxID(), Vout: 1}
	missingOp := store.Outpoint{TxID: storetest.NewTxID(), Vout: 2}
	frozenOp := store.Outpoint{TxID: storetest.NewTxID(), Vout: 3}
	spenderTxID := storetest.NewTxID()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/utxo/{txid}/json", func(w http.ResponseWriter, r *http.Request) {
		txid := r.PathValue("txid")
		vout := r.URL.Query().Get("vout")
		switch {
		case txid == spentOp.TxID.String() && vout == "0":
			_ = json.NewEncoder(w).Encode(wireSpendResponse{
				Status:       intPtr(1),
				SpendingData: &wireSpendingData{TxID: spenderTxID.String(), Vin: 0},
			})
		case txid == unspentOp.TxID.String() && vout == "1":
			_ = json.NewEncoder(w).Encode(wireSpendResponse{Status: intPtr(0)})
		case txid == frozenOp.TxID.String() && vout == "3":
			_ = json.NewEncoder(w).Encode(wireSpendResponse{Status: intPtr(5)})
		default:
			http.Error(w, `{"message":"not found"}`, http.StatusNotFound)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	o, err := teranode.NewURL(srv.URL + "/api/v1")
	require.NoError(t, err)

	ops := []store.Outpoint{spentOp, unspentOp, missingOp, frozenOp}
	res, err := o.Outpoints(context.Background(), ops)
	require.NoError(t, err)
	require.Len(t, res, 4)

	assert.Equal(t, spentOp, res[0].Outpoint)
	assert.Equal(t, oracle.StatusSpent, res[0].Status)
	require.NotNil(t, res[0].SpendingTxID)
	assert.Equal(t, spenderTxID, *res[0].SpendingTxID)

	assert.Equal(t, unspentOp, res[1].Outpoint)
	assert.Equal(t, oracle.StatusUnspent, res[1].Status)

	assert.Equal(t, missingOp, res[2].Outpoint)
	assert.Equal(t, oracle.StatusUnknown, res[2].Status)

	assert.Equal(t, frozenOp, res[3].Outpoint)
	assert.Equal(t, oracle.StatusConflicting, res[3].Status)
}

// TestOutpointsImmatureIsUnknown: IMMATURE (status 2) means "unspent
// coinbase not yet past maturity". Fuel outputs are never coinbase, so this
// status on a fuel outpoint means something is wrong with our assumptions,
// not that it is safe to spend -- the oracle must never guess, so it reports
// Unknown rather than Unspent.
func TestOutpointsImmatureIsUnknown(t *testing.T) {
	op := store.Outpoint{TxID: storetest.NewTxID(), Vout: 0}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/utxo/{txid}/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(wireSpendResponse{Status: intPtr(2)})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	o, err := teranode.NewURL(srv.URL + "/api/v1")
	require.NoError(t, err)

	res, err := o.Outpoints(context.Background(), []store.Outpoint{op})
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, oracle.StatusUnknown, res[0].Status)
}

func TestOutpointsEmpty(t *testing.T) {
	o, err := teranode.NewURL("http://127.0.0.1:0/api/v1")
	require.NoError(t, err)
	res, err := o.Outpoints(context.Background(), nil)
	require.NoError(t, err)
	assert.NotNil(t, res)
	assert.Empty(t, res)
}

func TestOutpointsServerDownIsUnavailable(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/utxo/{txid}/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(wireSpendResponse{Status: intPtr(0)})
	})
	srv := httptest.NewServer(mux)
	srv.Close() // closed before use: every request is a transport failure.

	o, err := teranode.NewURL(srv.URL + "/api/v1")
	require.NoError(t, err)

	_, err = o.Outpoints(context.Background(), []store.Outpoint{{TxID: storetest.NewTxID()}})
	require.ErrorIs(t, err, oracle.ErrUnavailable)
}

// batchServer builds a fake POST /api/v1/utxos/json handler that decodes the
// 36-byte request records, asserts their byte order matches ops exactly (raw
// internal-order txid bytes, little-endian vout), and returns statuses in
// the given order. It counts GETs and POSTs separately so tests can assert
// which transport was used.
func batchServer(t *testing.T, ops []store.Outpoint, statuses []int) (*httptest.Server, *int32, *int32) {
	t.Helper()
	var getCount, postCount int32
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/utxo/{txid}/json", func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&getCount, 1)
		_ = json.NewEncoder(w).Encode(wireSpendResponse{Status: intPtr(0)})
	})
	mux.HandleFunc("POST /api/v1/utxos/json", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&postCount, 1)
		assert.Equal(t, "application/octet-stream", r.Header.Get("Content-Type"))
		body, err := io.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Zero(t, len(body)%36) {
			return
		}
		n := len(body) / 36
		if !assert.Equal(t, len(ops), n) {
			return
		}

		results := make([]wireSpendResponse, n)
		for i := 0; i < n; i++ {
			rec := body[i*36 : i*36+36]
			var txid chainhash.Hash
			copy(txid[:], rec[:32])
			vout := binary.LittleEndian.Uint32(rec[32:36])
			assert.Equal(t, ops[i].TxID, txid, "record %d txid mismatch", i)
			assert.Equal(t, ops[i].Vout, vout, "record %d vout mismatch", i)
			results[i] = wireSpendResponse{Status: &statuses[i]}
		}
		_ = json.NewEncoder(w).Encode(results)
	})
	return httptest.NewServer(mux), &getCount, &postCount
}

func TestOutpointsBatchAboveThreshold(t *testing.T) {
	ops := []store.Outpoint{
		{TxID: storetest.NewTxID(), Vout: 0},
		{TxID: storetest.NewTxID(), Vout: 1},
		{TxID: storetest.NewTxID(), Vout: 2},
	}
	statuses := []int{0, 1, 5} // unspent, spent, frozen(conflicting)
	srv, getCount, postCount := batchServer(t, ops, statuses)
	defer srv.Close()

	o, err := teranode.New(teranode.Config{BaseURL: srv.URL + "/api/v1", BatchThreshold: 2})
	require.NoError(t, err)

	res, err := o.Outpoints(context.Background(), ops)
	require.NoError(t, err)
	require.Len(t, res, 3)
	assert.Equal(t, oracle.StatusUnspent, res[0].Status)
	assert.Equal(t, oracle.StatusSpent, res[1].Status)
	assert.Equal(t, oracle.StatusConflicting, res[2].Status)

	assert.EqualValues(t, 1, atomic.LoadInt32(postCount))
	assert.EqualValues(t, 0, atomic.LoadInt32(getCount))
}

func TestOutpointsAtThresholdUsesGETs(t *testing.T) {
	ops := []store.Outpoint{
		{TxID: storetest.NewTxID(), Vout: 0},
		{TxID: storetest.NewTxID(), Vout: 1},
	}
	statuses := []int{0, 0}
	srv, getCount, postCount := batchServer(t, ops, statuses)
	defer srv.Close()

	o, err := teranode.New(teranode.Config{BaseURL: srv.URL + "/api/v1", BatchThreshold: 2})
	require.NoError(t, err)

	res, err := o.Outpoints(context.Background(), ops)
	require.NoError(t, err)
	require.Len(t, res, 2)

	assert.EqualValues(t, 0, atomic.LoadInt32(postCount))
	assert.Positive(t, atomic.LoadInt32(getCount))
}

func TestOutpointsBatchDisabledNegativeThreshold(t *testing.T) {
	ops := make([]store.Outpoint, 20)
	statuses := make([]int, 20)
	for i := range ops {
		ops[i] = store.Outpoint{TxID: storetest.NewTxID(), Vout: uint32(i)}
	}
	srv, getCount, postCount := batchServer(t, ops, statuses)
	defer srv.Close()

	o, err := teranode.New(teranode.Config{BaseURL: srv.URL + "/api/v1", BatchThreshold: -1})
	require.NoError(t, err)

	_, err = o.Outpoints(context.Background(), ops)
	require.NoError(t, err)

	assert.EqualValues(t, 0, atomic.LoadInt32(postCount))
	assert.EqualValues(t, len(ops), atomic.LoadInt32(getCount))
}

func TestOutpointsBatchLengthMismatchIsUnavailable(t *testing.T) {
	ops := []store.Outpoint{
		{TxID: storetest.NewTxID(), Vout: 0},
		{TxID: storetest.NewTxID(), Vout: 1},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/utxos/json", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		// Only one record for a two-op request: length mismatch.
		_ = json.NewEncoder(w).Encode([]wireSpendResponse{{Status: intPtr(0)}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	o, err := teranode.New(teranode.Config{BaseURL: srv.URL + "/api/v1", BatchThreshold: 1})
	require.NoError(t, err)

	_, err = o.Outpoints(context.Background(), ops)
	require.ErrorIs(t, err, oracle.ErrUnavailable)
}

func TestOutpointsBatchRateLimitedIsUnavailable(t *testing.T) {
	ops := []store.Outpoint{
		{TxID: storetest.NewTxID(), Vout: 0},
		{TxID: storetest.NewTxID(), Vout: 1},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/utxos/json", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusTooManyRequests)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	o, err := teranode.New(teranode.Config{BaseURL: srv.URL + "/api/v1", BatchThreshold: 1})
	require.NoError(t, err)

	_, err = o.Outpoints(context.Background(), ops)
	require.ErrorIs(t, err, oracle.ErrUnavailable)
}

// buildRawTx constructs a minimal, well-formed transaction with two inputs
// spending arbitrary previous outpoints, and returns its serialized bytes
// alongside its txid and the outpoints its inputs spend (in order).
func buildRawTx(t *testing.T) (raw []byte, txid chainhash.Hash, inputs []store.Outpoint) {
	t.Helper()
	prev1 := storetest.NewTxID()
	prev2 := storetest.NewTxID()

	tx := &transaction.Transaction{
		Version: 1,
		Inputs: []*transaction.TransactionInput{
			{
				SourceTXID:       &prev1,
				SourceTxOutIndex: 0,
				UnlockingScript:  script.NewFromBytes(nil),
				SequenceNumber:   0xffffffff,
			},
			{
				SourceTXID:       &prev2,
				SourceTxOutIndex: 3,
				UnlockingScript:  script.NewFromBytes(nil),
				SequenceNumber:   0xffffffff,
			},
		},
		Outputs: []*transaction.TransactionOutput{
			{Satoshis: 1000, LockingScript: script.NewFromBytes([]byte{0x51})},
		},
		LockTime: 0,
	}

	got := tx.TxID()
	require.NotNil(t, got)
	return tx.Bytes(), *got, []store.Outpoint{
		{TxID: prev1, Vout: 0},
		{TxID: prev2, Vout: 3},
	}
}

func TestTxWithInputs(t *testing.T) {
	raw, txid, inputs := buildRawTx(t)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/tx/{txid}", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, txid.String(), r.PathValue("txid"))
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(raw)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	o, err := teranode.NewURL(srv.URL + "/api/v1")
	require.NoError(t, err)

	st, err := o.Tx(context.Background(), txid)
	require.NoError(t, err)
	assert.True(t, st.Known)
	assert.False(t, st.Mined)
	assert.False(t, st.Rejected)
	require.Len(t, st.Inputs, 2)
	assert.Equal(t, inputs, st.Inputs)
}

func TestTxNotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/tx/{txid}", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"message":"not found"}`, http.StatusNotFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	o, err := teranode.NewURL(srv.URL + "/api/v1")
	require.NoError(t, err)

	st, err := o.Tx(context.Background(), storetest.NewTxID())
	require.NoError(t, err)
	assert.Equal(t, oracle.TxStatus{}, st)
}

func TestTxServerErrorIsUnavailable(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/tx/{txid}", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	o, err := teranode.NewURL(srv.URL + "/api/v1")
	require.NoError(t, err)

	_, err = o.Tx(context.Background(), storetest.NewTxID())
	require.ErrorIs(t, err, oracle.ErrUnavailable)
}

func TestTxMismatchedTxIDIsUnavailable(t *testing.T) {
	raw, _, _ := buildRawTx(t)
	requested := storetest.NewTxID() // deliberately not the tx's real id

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/tx/{txid}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(raw)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	o, err := teranode.NewURL(srv.URL + "/api/v1")
	require.NoError(t, err)

	_, err = o.Tx(context.Background(), requested)
	require.ErrorIs(t, err, oracle.ErrUnavailable)
}

func TestTxCancelledContext(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/tx/{txid}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	o, err := teranode.NewURL(srv.URL + "/api/v1")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = o.Tx(ctx, storetest.NewTxID())
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestOutpointsCancelledContext(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/utxo/{txid}/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(wireSpendResponse{Status: intPtr(0)})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	o, err := teranode.NewURL(srv.URL + "/api/v1")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = o.Outpoints(ctx, []store.Outpoint{{TxID: storetest.NewTxID()}})
	require.Error(t, err)
}

func TestNewRejectsBadURLs(t *testing.T) {
	cases := []string{"", "not-a-url", "/no/scheme", "http://", "://bad"}
	for _, c := range cases {
		_, err := teranode.New(teranode.Config{BaseURL: c})
		assert.Errorf(t, err, "expected error for base url %q", c)
	}
}

func TestNewTrimsTrailingSlash(t *testing.T) {
	op := store.Outpoint{TxID: storetest.NewTxID(), Vout: 0}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/utxo/{txid}/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(wireSpendResponse{Status: intPtr(0)})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	o, err := teranode.NewURL(srv.URL + "/api/v1/")
	require.NoError(t, err)

	res, err := o.Outpoints(context.Background(), []store.Outpoint{op})
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, oracle.StatusUnspent, res[0].Status)
}

// missingStatusBodies are 200 response bodies that a well-behaved teranode
// never sends (SpendResponse.Status has no `omitempty`, and GetUTXOs.go's
// per-record fan-out always yields a SpendResponse with a set Status) but
// that would silently decode to StatusUnspent if Status were a plain int
// instead of *int. Each must be rejected as ErrUnavailable rather than
// guessed as Unspent, since guessing here could make the reaper requeue a
// row that is still spent.
var missingStatusBodies = map[string]string{
	"empty object":          `{}`,
	"top-level null":        `null`,
	"unrelated fields only": `{"message":"ok"}`,
	"explicit null status":  `{"status":null}`,
}

func TestOutpointsSingleGETMissingStatusIsUnavailable(t *testing.T) {
	for name, body := range missingStatusBodies {
		t.Run(name, func(t *testing.T) {
			op := store.Outpoint{TxID: storetest.NewTxID(), Vout: 0}
			mux := http.NewServeMux()
			mux.HandleFunc("GET /api/v1/utxo/{txid}/json", func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(body))
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()

			o, err := teranode.NewURL(srv.URL + "/api/v1")
			require.NoError(t, err)

			_, err = o.Outpoints(context.Background(), []store.Outpoint{op})
			require.ErrorIs(t, err, oracle.ErrUnavailable)
		})
	}
}

// missingStatusBatchBodies are POST /utxos/json response bodies with a
// record that has no status, in the two shapes teranode never sends: a
// JSON null in place of the record, and an object missing the field.
var missingStatusBatchBodies = map[string]string{
	"nil element":       `[{"status":0},null]`,
	"empty object elem": `[{"status":0},{}]`,
}

func TestOutpointsBatchMissingStatusIsUnavailable(t *testing.T) {
	ops := []store.Outpoint{
		{TxID: storetest.NewTxID(), Vout: 0},
		{TxID: storetest.NewTxID(), Vout: 1},
	}
	for name, body := range missingStatusBatchBodies {
		t.Run(name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("POST /api/v1/utxos/json", func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.ReadAll(r.Body)
				_, _ = w.Write([]byte(body))
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()

			o, err := teranode.New(teranode.Config{BaseURL: srv.URL + "/api/v1", BatchThreshold: 1})
			require.NoError(t, err)

			res, err := o.Outpoints(context.Background(), ops)
			require.ErrorIs(t, err, oracle.ErrUnavailable)
			assert.Nil(t, res)
		})
	}
}

// TestOutpointsStatusOKWithSpendingDataIsUnknown covers N4: status OK (0)
// together with a non-nil spendingData is a contradiction teranode should
// never send. The oracle must not guess which side is right, so it reports
// Unknown rather than Unspent.
func TestOutpointsStatusOKWithSpendingDataIsUnknown(t *testing.T) {
	op := store.Outpoint{TxID: storetest.NewTxID(), Vout: 0}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/utxo/{txid}/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(wireSpendResponse{
			Status:       intPtr(0),
			SpendingData: &wireSpendingData{TxID: storetest.NewTxID().String(), Vin: 0},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	o, err := teranode.NewURL(srv.URL + "/api/v1")
	require.NoError(t, err)

	res, err := o.Outpoints(context.Background(), []store.Outpoint{op})
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, oracle.StatusUnknown, res[0].Status)
}

// statusTable pins the exact verdict for every teranode UTXO status code
// (stores/utxo/status.pb.go) plus an unrecognized one, so a future edit that
// silently changes a mapping is caught here rather than by chance in some
// other test.
var statusTable = []struct {
	status   int
	expected oracle.Status
}{
	{0, oracle.StatusUnspent},     // OK
	{1, oracle.StatusSpent},       // SPENT
	{2, oracle.StatusUnknown},     // IMMATURE: never guess (see TestOutpointsImmatureIsUnknown)
	{3, oracle.StatusUnknown},     // NOT_FOUND
	{4, oracle.StatusUnknown},     // ALREADY_EXISTS
	{5, oracle.StatusConflicting}, // FROZEN
	{6, oracle.StatusConflicting}, // CONFLICTING
	{7, oracle.StatusUnknown},     // LOCKED
	{99, oracle.StatusUnknown},    // unrecognized
}

func TestOutpointsAllStatusesSingleGET(t *testing.T) {
	ops := make([]store.Outpoint, len(statusTable))
	txid := storetest.NewTxID()
	for i := range ops {
		ops[i] = store.Outpoint{TxID: txid, Vout: uint32(i)}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/utxo/{txid}/json", func(w http.ResponseWriter, r *http.Request) {
		vout, err := strconv.Atoi(r.URL.Query().Get("vout"))
		if !assert.NoError(t, err) {
			return
		}
		_ = json.NewEncoder(w).Encode(wireSpendResponse{Status: intPtr(statusTable[vout].status)})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Never batch: this table exercises the single-GET mapping specifically.
	o, err := teranode.New(teranode.Config{BaseURL: srv.URL + "/api/v1", BatchThreshold: -1})
	require.NoError(t, err)

	res, err := o.Outpoints(context.Background(), ops)
	require.NoError(t, err)
	require.Len(t, res, len(statusTable))
	for i, tc := range statusTable {
		assert.Equalf(t, tc.expected, res[i].Status, "status %d", tc.status)
	}
}

func TestOutpointsAllStatusesBatch(t *testing.T) {
	ops := make([]store.Outpoint, len(statusTable))
	for i := range ops {
		ops[i] = store.Outpoint{TxID: storetest.NewTxID(), Vout: uint32(i)}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/utxos/json", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		results := make([]wireSpendResponse, len(statusTable))
		for i, tc := range statusTable {
			results[i] = wireSpendResponse{Status: intPtr(tc.status)}
		}
		_ = json.NewEncoder(w).Encode(results)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// A low threshold forces the batch path regardless of table size.
	o, err := teranode.New(teranode.Config{BaseURL: srv.URL + "/api/v1", BatchThreshold: 1})
	require.NoError(t, err)

	res, err := o.Outpoints(context.Background(), ops)
	require.NoError(t, err)
	require.Len(t, res, len(statusTable))
	for i, tc := range statusTable {
		assert.Equalf(t, tc.expected, res[i].Status, "status %d", tc.status)
	}
}

// TestOutpointsSingleGETOneFailureIsAllOrNothing covers I3: of 8 GETs, one
// fails (500, and separately 429); the whole call must return nil results
// and ErrUnavailable rather than 7 good verdicts and one gap.
func TestOutpointsSingleGETOneFailureIsAllOrNothing(t *testing.T) {
	for _, code := range []int{http.StatusInternalServerError, http.StatusTooManyRequests} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			const failVout = "3"
			ops := make([]store.Outpoint, 8)
			for i := range ops {
				ops[i] = store.Outpoint{TxID: storetest.NewTxID(), Vout: uint32(i)}
			}

			mux := http.NewServeMux()
			mux.HandleFunc("GET /api/v1/utxo/{txid}/json", func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("vout") == failVout {
					w.WriteHeader(code)
					return
				}
				_ = json.NewEncoder(w).Encode(wireSpendResponse{Status: intPtr(0)})
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()

			// len(ops) == default BatchThreshold (8): stays on the single-GET path.
			o, err := teranode.NewURL(srv.URL + "/api/v1")
			require.NoError(t, err)

			res, err := o.Outpoints(context.Background(), ops)
			require.ErrorIs(t, err, oracle.ErrUnavailable)
			assert.Nil(t, res)
		})
	}
}

// TestDecodeJSONLimitedRejectsTrailingData covers N3: a 200 body with valid
// JSON followed by trailing garbage must not be accepted -- a truncated or
// concatenated response could otherwise decode "successfully" from just its
// leading bytes.
func TestDecodeJSONLimitedRejectsTrailingData(t *testing.T) {
	op := store.Outpoint{TxID: storetest.NewTxID(), Vout: 0}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/utxo/{txid}/json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":0}garbage`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	o, err := teranode.NewURL(srv.URL + "/api/v1")
	require.NoError(t, err)

	_, err = o.Outpoints(context.Background(), []store.Outpoint{op})
	require.ErrorIs(t, err, oracle.ErrUnavailable)
}

func TestDecodeJSONLimitedAllowsTrailingWhitespace(t *testing.T) {
	// json.NewEncoder appends a trailing newline; that must still decode.
	op := store.Outpoint{TxID: storetest.NewTxID(), Vout: 0}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/utxo/{txid}/json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{\"status\":0}\n\n  "))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	o, err := teranode.NewURL(srv.URL + "/api/v1")
	require.NoError(t, err)

	res, err := o.Outpoints(context.Background(), []store.Outpoint{op})
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, oracle.StatusUnspent, res[0].Status)
}
