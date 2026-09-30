package arcade_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/arcadeclient"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle"
	"github.com/bsv-blockchain/fuelkeeper/pkg/oracle/arcade"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// fakeArcade serves GET /v1/tx/{txid}: a body from responses when present, a
// 500 for serverErrTxid, and a 404 for anything else.
func fakeArcade(t *testing.T, responses map[string]any, serverErrTxid string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/tx/{txid}", func(w http.ResponseWriter, r *http.Request) {
		txid := r.PathValue("txid")
		if txid == serverErrTxid {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		body, ok := responses[txid]
		if !ok {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(body)
	})
	return httptest.NewServer(mux)
}

func TestTxMined(t *testing.T) {
	hash := storetest.NewTxID()
	txid := hash.String()
	srv := fakeArcade(t, map[string]any{
		txid: map[string]any{
			"txid": txid, "txStatus": "MINED", "blockHeight": 100, "merklePath": "fe0c",
		},
	}, "")
	defer srv.Close()

	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)
	o := arcade.New(c)

	st, err := o.Tx(context.Background(), hash)
	require.NoError(t, err)
	assert.True(t, st.Known)
	assert.True(t, st.Mined)
	assert.False(t, st.Rejected)
	assert.Equal(t, "MINED", st.Status)
	assert.Equal(t, uint32(100), st.BlockHeight)
	assert.Equal(t, []byte{0xfe, 0x0c}, st.MerklePath)
	assert.Nil(t, st.Inputs)
}

func TestTxRejectedWithCompetingTxs(t *testing.T) {
	hash := storetest.NewTxID()
	txid := hash.String()
	competitor := storetest.NewTxID().String()
	srv := fakeArcade(t, map[string]any{
		txid: map[string]any{
			"txid": txid, "txStatus": "REJECTED", "status": 466, "competingTxs": []string{competitor},
		},
	}, "")
	defer srv.Close()

	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)
	o := arcade.New(c)

	st, err := o.Tx(context.Background(), hash)
	require.NoError(t, err)
	assert.True(t, st.Known)
	assert.True(t, st.Rejected)
	assert.False(t, st.Mined)
	assert.Equal(t, []string{competitor}, st.CompetingTxs)
	assert.Equal(t, oracle.RejectReasonInputsSpent, st.RejectReason, "466 conflicting tx: inputs already spent")
}

// TestTxRejectReasonByCode is a table test over every ARC numeric code the
// arcade oracle classifies, plus an unmapped code, pinning the mapping
// documented on rejectReasonForCode in pkg/oracle/arcade/arcade.go.
func TestTxRejectReasonByCode(t *testing.T) {
	tests := []struct {
		code int
		want oracle.RejectReason
	}{
		{466, oracle.RejectReasonInputsSpent}, // conflicting tx found
		{460, oracle.RejectReasonTxInvalid},   // not extended format
		{461, oracle.RejectReasonTxInvalid},   // malformed unlocking scripts
		{462, oracle.RejectReasonTxInvalid},   // invalid inputs: coinbase-shaped or input-script-size policy, not spent-ness
		{463, oracle.RejectReasonTxInvalid},   // malformed transaction
		{464, oracle.RejectReasonTxInvalid},   // invalid outputs
		{465, oracle.RejectReasonTxInvalid},   // fee too low
		{467, oracle.RejectReasonTxInvalid},   // generic (wraps fee/script/policy)
		{468, oracle.RejectReasonTxInvalid},   // invalid BEEF
		{469, oracle.RejectReasonTxInvalid},   // merkle roots validation failed
		{471, oracle.RejectReasonTxInvalid},   // input frozen (policy)
		{472, oracle.RejectReasonTxInvalid},   // input frozen (consensus)
		{473, oracle.RejectReasonTxInvalid},   // cumulative fee validation failed
		{474, oracle.RejectReasonTxInvalid},   // transaction size validation failed
		{475, oracle.RejectReasonTxInvalid},   // mined ancestors not found in BUMPs
		{476, oracle.RejectReasonUnknown},     // not final: requeueing would race a later rebroadcast
		{0, oracle.RejectReasonUnknown},       // zero/unclassified
		{470, oracle.RejectReasonUnknown},     // unassigned in arcade's taxonomy
		{999, oracle.RejectReasonUnknown},     // unrecognized code
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("code_%d", tt.code), func(t *testing.T) {
			hash := storetest.NewTxID()
			txid := hash.String()
			srv := fakeArcade(t, map[string]any{
				txid: map[string]any{"txid": txid, "txStatus": "REJECTED", "status": tt.code},
			}, "")
			defer srv.Close()

			c, err := arcadeclient.New(srv.URL + "/v1")
			require.NoError(t, err)
			o := arcade.New(c)

			st, err := o.Tx(context.Background(), hash)
			require.NoError(t, err)
			require.True(t, st.Rejected)
			assert.Equal(t, tt.want, st.RejectReason)
		})
	}
}

// TestTxDoubleSpendAttempted covers DOUBLE_SPEND_ATTEMPTED: a distinct,
// non-Rejected txStatus. CompetingTxs still comes through (the reaper is
// expected to check it first for this status), but RejectReason stays
// Unknown: it is gated on Rejected and this verdict is never Rejected.
func TestTxDoubleSpendAttempted(t *testing.T) {
	hash := storetest.NewTxID()
	txid := hash.String()
	competitor := storetest.NewTxID().String()
	srv := fakeArcade(t, map[string]any{
		txid: map[string]any{
			"txid": txid, "txStatus": "DOUBLE_SPEND_ATTEMPTED", "competingTxs": []string{competitor},
		},
	}, "")
	defer srv.Close()

	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)
	o := arcade.New(c)

	st, err := o.Tx(context.Background(), hash)
	require.NoError(t, err)
	assert.True(t, st.Known)
	assert.False(t, st.Mined)
	assert.False(t, st.Rejected)
	assert.Equal(t, "DOUBLE_SPEND_ATTEMPTED", st.Status)
	assert.Equal(t, []string{competitor}, st.CompetingTxs)
	assert.Equal(t, oracle.RejectReasonUnknown, st.RejectReason)
}

// TestTxRejectReasonGatedOnRejected pins that RejectReason is computed only
// when arcade's verdict is Rejected. A txStatus that carries a stray
// nonzero numeric "status" without being REJECTED (nothing in the arcade
// contract forbids this; arcade.openapi.yaml only documents "status" as
// populated alongside REJECTED, it doesn't require every other txStatus to
// omit it) must not have that code run through rejectReasonForCode — 476
// StatusNotFinal, in particular, is also meaningful on a live, still-
// pending transaction, so leaking it through here would misreport pending
// work as reaper-actionable.
func TestTxRejectReasonGatedOnRejected(t *testing.T) {
	hash := storetest.NewTxID()
	txid := hash.String()
	srv := fakeArcade(t, map[string]any{
		txid: map[string]any{"txid": txid, "txStatus": "SEEN_ON_NETWORK", "status": 466},
	}, "")
	defer srv.Close()

	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)
	o := arcade.New(c)

	st, err := o.Tx(context.Background(), hash)
	require.NoError(t, err)
	assert.False(t, st.Rejected)
	assert.Equal(t, oracle.RejectReasonUnknown, st.RejectReason, "a stray status code on a non-Rejected verdict must not be classified")
}

func TestTxNotFound(t *testing.T) {
	hash := storetest.NewTxID()
	srv := fakeArcade(t, map[string]any{}, "")
	defer srv.Close()

	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)
	o := arcade.New(c)

	st, err := o.Tx(context.Background(), hash)
	require.NoError(t, err)
	assert.Equal(t, oracle.TxStatus{}, st)
}

func TestTxServerError(t *testing.T) {
	hash := storetest.NewTxID()
	srv := fakeArcade(t, map[string]any{}, hash.String())
	defer srv.Close()

	c, err := arcadeclient.New(srv.URL + "/v1")
	require.NoError(t, err)
	o := arcade.New(c)

	_, err = o.Tx(context.Background(), hash)
	require.ErrorIs(t, err, oracle.ErrUnavailable)
}

func TestOutpointsAlwaysUnknown(t *testing.T) {
	c, err := arcadeclient.New("http://127.0.0.1:0/v1")
	require.NoError(t, err)
	o := arcade.New(c)

	ops := []store.Outpoint{
		{TxID: storetest.NewTxID(), Vout: 0},
		{TxID: storetest.NewTxID(), Vout: 1},
	}
	res, err := o.Outpoints(context.Background(), ops)
	require.NoError(t, err)
	require.Len(t, res, len(ops))
	for i, op := range ops {
		assert.Equal(t, op, res[i].Outpoint)
		assert.Equal(t, oracle.StatusUnknown, res[i].Status)
	}
}

func TestOutpointsEmpty(t *testing.T) {
	c, err := arcadeclient.New("http://127.0.0.1:0/v1")
	require.NoError(t, err)
	o := arcade.New(c)

	res, err := o.Outpoints(context.Background(), nil)
	require.NoError(t, err)
	assert.NotNil(t, res)
	assert.Empty(t, res)
}
