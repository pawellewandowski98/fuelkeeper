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

	"github.com/bsv-blockchain/go-sdk/chainhash"

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

// TestTxRejectedWithCompetingTxs covers a REJECTED/466 verdict whose body
// carries both a top-level competingTxs entry (arcade's schema field,
// decoded when present though no current arcade code path writes it — see
// arcadeclient.TxStatus.CompetingTxs's doc) and a TX_CONFLICTING extraInfo
// line, which is what a real 466 verdict actually carries the
// classification in.
func TestTxRejectedWithCompetingTxs(t *testing.T) {
	hash := storetest.NewTxID()
	txid := hash.String()
	competitor := storetest.NewTxID().String()
	srv := fakeArcade(t, map[string]any{
		txid: map[string]any{
			"txid": txid, "txStatus": "REJECTED", "status": 466,
			"extraInfo":    "TX_CONFLICTING (36): [Validate][" + txid + "] tx is conflicting",
			"competingTxs": []string{competitor},
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
	assert.Equal(t, oracle.RejectReasonInputsSpent, st.RejectReason, "466/TX_CONFLICTING: an input is already spent or conflicting")
	assert.Nil(t, st.SpentInputs, "TX_CONFLICTING lines are not parsed for an outpoint")
}

// TestTxRejectReasonByCode is a table test over every ARC numeric code the
// arcade oracle classifies, plus an unmapped code, pinning the mapping
// documented on rejectReasonForCode in pkg/oracle/arcade/arcade.go. 466 is
// excluded: it needs an extraInfo line to classify (rejectReasonFor466) —
// see TestTxRejectReason466ByExtraInfoName.
func TestTxRejectReasonByCode(t *testing.T) {
	tests := []struct {
		code int
		want oracle.RejectReason
	}{
		{460, oracle.RejectReasonTxInvalid}, // not extended format
		{461, oracle.RejectReasonTxInvalid}, // malformed unlocking scripts
		{462, oracle.RejectReasonTxInvalid}, // invalid inputs: coinbase-shaped or input-script-size policy, not spent-ness
		{463, oracle.RejectReasonTxInvalid}, // malformed transaction
		{464, oracle.RejectReasonTxInvalid}, // invalid outputs
		{465, oracle.RejectReasonTxInvalid}, // fee too low
		{467, oracle.RejectReasonUnknown},   // generic: wraps TX_INVALID, or arcade's own internal ERR_PROCESSING
		{468, oracle.RejectReasonTxInvalid}, // invalid BEEF
		{469, oracle.RejectReasonTxInvalid}, // merkle roots validation failed
		{471, oracle.RejectReasonTxInvalid}, // input frozen (policy)
		{472, oracle.RejectReasonTxInvalid}, // input frozen (consensus)
		{473, oracle.RejectReasonTxInvalid}, // cumulative fee validation failed
		{474, oracle.RejectReasonTxInvalid}, // transaction size validation failed
		{475, oracle.RejectReasonTxInvalid}, // mined ancestors not found in BUMPs
		{476, oracle.RejectReasonUnknown},   // not final: requeueing would race a later rebroadcast
		{0, oracle.RejectReasonUnknown},     // zero/unclassified
		{470, oracle.RejectReasonUnknown},   // unassigned in arcade's taxonomy
		{999, oracle.RejectReasonUnknown},   // unrecognized code
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

// TestTxRejectReason466ByExtraInfoName is a table test over the leading
// "<NAME> (<code>):" of a 466 verdict's extraInfo, pinning
// rejectReasonFor466's mapping: UTXO_SPENT, TX_CONFLICTING and
// TX_INVALID_DOUBLE_SPEND are InputsSpent (an already-decided ownership of
// the input); TX_LOCKED is Unknown (transient: the parent tx's own UTXO
// record is still finishing its two-phase commit); and both an empty and an
// unparsable extraInfo are Unknown.
func TestTxRejectReason466ByExtraInfoName(t *testing.T) {
	spentTxID := storetest.NewTxID().String()
	spenderTxID := storetest.NewTxID().String()
	tests := []struct {
		name      string
		extraInfo string
		want      oracle.RejectReason
	}{
		{
			name:      "UTXO_SPENT",
			extraInfo: fmt.Sprintf("UTXO_SPENT (70): %s:0 utxo already spent by tx %s[0]", spentTxID, spenderTxID),
			want:      oracle.RejectReasonInputsSpent,
		},
		{
			name:      "TX_CONFLICTING",
			extraInfo: "TX_CONFLICTING (36): [Validate][deadbeef] tx is conflicting",
			want:      oracle.RejectReasonInputsSpent,
		},
		{
			name:      "TX_INVALID_DOUBLE_SPEND",
			extraInfo: "TX_INVALID_DOUBLE_SPEND (32): [ProcessTransaction][ab12] tx invalid double spend",
			want:      oracle.RejectReasonInputsSpent,
		},
		{
			name:      "TX_LOCKED",
			extraInfo: "TX_LOCKED (37): [Spend] utxo is not spendable for " + spentTxID + ":0",
			want:      oracle.RejectReasonUnknown,
		},
		{
			name:      "empty extraInfo",
			extraInfo: "",
			want:      oracle.RejectReasonUnknown,
		},
		{
			name:      "unparsable extraInfo",
			extraInfo: "malformed line with no teranode code",
			want:      oracle.RejectReasonUnknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash := storetest.NewTxID()
			txid := hash.String()
			srv := fakeArcade(t, map[string]any{
				txid: map[string]any{"txid": txid, "txStatus": "REJECTED", "status": 466, "extraInfo": tt.extraInfo},
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

// TestTxRejectReason466SpentInputsFromExtraInfo covers spentInputsFromExtraInfo:
// a well-formed UTXO_SPENT line yields SpentInputs and appends the named
// spender to CompetingTxs; teranode's documented doubled-prefix shape
// parses the same way; a malformed line, and a TX_CONFLICTING line (which
// classifies InputsSpent but is never parsed for detail), leave SpentInputs
// nil and CompetingTxs exactly as the body supplied it.
func TestTxRejectReason466SpentInputsFromExtraInfo(t *testing.T) {
	spentTxIDHash := storetest.NewTxID()
	spentTxID := spentTxIDHash.String()
	spenderTxID := storetest.NewTxID().String()
	wantOutpoint := store.Outpoint{TxID: spentTxIDHash, Vout: 3}

	newServer := func(t *testing.T, body map[string]any) (*arcadeclient.Client, chainhash.Hash) {
		t.Helper()
		hash := storetest.NewTxID()
		txid := hash.String()
		body["txid"] = txid
		body["txStatus"] = "REJECTED"
		body["status"] = 466
		srv := fakeArcade(t, map[string]any{txid: body}, "")
		t.Cleanup(srv.Close)
		c, err := arcadeclient.New(srv.URL + "/v1")
		require.NoError(t, err)
		return c, hash
	}

	t.Run("well-formed UTXO_SPENT line", func(t *testing.T) {
		extraInfo := fmt.Sprintf("UTXO_SPENT (70): %s:3 utxo already spent by tx %s[1]", spentTxID, spenderTxID)
		c, hash := newServer(t, map[string]any{"extraInfo": extraInfo})
		st, err := arcade.New(c).Tx(context.Background(), hash)
		require.NoError(t, err)
		require.Equal(t, oracle.RejectReasonInputsSpent, st.RejectReason)
		require.Len(t, st.SpentInputs, 1)
		assert.Equal(t, wantOutpoint, st.SpentInputs[0])
		assert.Equal(t, []string{spenderTxID}, st.CompetingTxs)
	})

	t.Run("doubled UTXO_SPENT prefix (mainnet shape)", func(t *testing.T) {
		// Teranode sometimes wraps the inner error's UserMessage, doubling
		// the code name (arcade's services/propagation/classify_test.go
		// documents this as observed on mainnet 409 responses).
		extraInfo := fmt.Sprintf("UTXO_SPENT (70): UTXO_SPENT (70): %s:3 utxo already spent by tx %s[1]", spentTxID, spenderTxID)
		c, hash := newServer(t, map[string]any{"extraInfo": extraInfo})
		st, err := arcade.New(c).Tx(context.Background(), hash)
		require.NoError(t, err)
		require.Len(t, st.SpentInputs, 1)
		assert.Equal(t, wantOutpoint, st.SpentInputs[0])
		assert.Equal(t, []string{spenderTxID}, st.CompetingTxs)
	})

	t.Run("pre-existing competingTxs is kept and the spender appended", func(t *testing.T) {
		existing := storetest.NewTxID().String()
		extraInfo := fmt.Sprintf("UTXO_SPENT (70): %s:3 utxo already spent by tx %s[1]", spentTxID, spenderTxID)
		c, hash := newServer(t, map[string]any{"extraInfo": extraInfo, "competingTxs": []string{existing}})
		st, err := arcade.New(c).Tx(context.Background(), hash)
		require.NoError(t, err)
		assert.Equal(t, []string{existing, spenderTxID}, st.CompetingTxs)
	})

	// The parse is all-or-nothing: a line that names a well-formed outpoint
	// but fails to name a well-formed spender (or vice versa) must not
	// yield a partial SpentInputs — pinning that a future rewrite (e.g.
	// splitting the outpoint and spender into two independently-matched
	// regexes) can't silently start returning the outpoint alone.
	malformedLines := []struct {
		name      string
		extraInfo string
	}{
		{
			name:      "both outpoint and spender malformed",
			extraInfo: "UTXO_SPENT (70): not-a-valid-outpoint utxo already spent by tx also-not-valid",
		},
		{
			// Teranode really emits this: SpendingData.String() returns
			// "<nil>" when the pointer is nil
			// (/git/teranode/stores/utxo/spend/spending_data.go:76-79), so
			// a well-formed, 64-hex outpoint can still arrive alongside an
			// unparseable spender.
			name:      "valid outpoint, spender printed as <nil>",
			extraInfo: fmt.Sprintf("UTXO_SPENT (70): %s:3 utxo already spent by tx <nil>", spentTxID),
		},
	}
	for _, tt := range malformedLines {
		t.Run(tt.name, func(t *testing.T) {
			existing := storetest.NewTxID().String()
			c, hash := newServer(t, map[string]any{
				"extraInfo":    tt.extraInfo,
				"competingTxs": []string{existing},
			})
			st, err := arcade.New(c).Tx(context.Background(), hash)
			require.NoError(t, err)
			assert.Nil(t, st.SpentInputs, "an all-or-nothing parse must not return the outpoint alone")
			assert.Equal(t, []string{existing}, st.CompetingTxs, "a malformed line must not touch the existing competingTxs")
		})
	}

	t.Run("TX_CONFLICTING classifies InputsSpent but is not parsed for an outpoint", func(t *testing.T) {
		extraInfo := fmt.Sprintf("TX_CONFLICTING (36): [Spend] tx is conflicting for %s:2", spentTxID)
		c, hash := newServer(t, map[string]any{"extraInfo": extraInfo})
		st, err := arcade.New(c).Tx(context.Background(), hash)
		require.NoError(t, err)
		require.Equal(t, oracle.RejectReasonInputsSpent, st.RejectReason)
		assert.Nil(t, st.SpentInputs, "TX_CONFLICTING is not parsed even when its message names an outpoint")
	})
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
