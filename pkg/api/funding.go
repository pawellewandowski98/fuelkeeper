package api

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/pool"
	"github.com/bsv-blockchain/fuelkeeper/pkg/treasury"
)

// brc29Protocol is the fixed BRC-29 protocol ID FuelKeeper's treasury derives
// funding payment keys under (security level 2, "every app and
// counterparty"); see wire.FundingInfo.Protocol.
const brc29Protocol = "3241645161d8"

// fundingInstructions is served verbatim as wire.FundingInfo.Instructions so a
// payer's tooling can render the BRC-29 recipe without hardcoding it.
const fundingInstructions = `Derive a BRC-29 key with GetPublicKey{protocolID:[2,"3241645161d8"], keyID:"<derivation_prefix> <your suffix>", counterparty: identity_key}, pay a P2PKH output to it, then POST the AtomicBEEF, prefix, suffix and your identity key here.`

// fundingDisabledMessage is returned by both funding endpoints when
// Deps.Funder is nil (the mock/nullnet treasury).
const fundingDisabledMessage = "this instance runs without a funding treasury"

// senderIdentityKeyPattern matches a compressed public key rendered as DER
// hex: 66 hex characters (33 bytes).
var senderIdentityKeyPattern = regexp.MustCompile(`^[0-9a-fA-F]{66}$`)

// fundingInfo tells a payer how to fund the treasury via a BRC-29 wallet
// payment: GET /v1/funding.
func (s *server) fundingInfo(w http.ResponseWriter, r *http.Request) {
	if s.funder == nil {
		writeJSON(w, http.StatusNotFound, wire.Error{Code: "funding_disabled", Message: fundingDisabledMessage})
		return
	}
	info, err := s.funder.FundingInfo(r.Context())
	if err != nil {
		s.writeFunderError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, wire.FundingInfo{
		IdentityKey:      info.IdentityKeyHex,
		DerivationPrefix: info.DerivationPrefixB64,
		Network:          info.Network,
		Protocol:         brc29Protocol,
		Instructions:     fundingInstructions,
	})
}

// fund accepts a completed BRC-29 payment and internalizes it into the
// treasury wallet: POST /v1/funding.
func (s *server) fund(w http.ResponseWriter, r *http.Request) {
	if s.funder == nil {
		writeJSON(w, http.StatusNotFound, wire.Error{Code: "funding_disabled", Message: fundingDisabledMessage})
		return
	}
	var req wire.FundRequest
	if err := decode(r, &req); err != nil {
		s.writeError(w, err)
		return
	}
	atomicBEEF, err := decodeNonEmptyBase64(req.AtomicBEEF)
	if err != nil {
		s.writeError(w, fmt.Errorf("%w: atomic_beef: %w", pool.ErrBadRequest, err))
		return
	}
	if _, err := decodeNonEmptyBase64(req.DerivationPrefix); err != nil {
		s.writeError(w, fmt.Errorf("%w: derivation_prefix: %w", pool.ErrBadRequest, err))
		return
	}
	if _, err := decodeNonEmptyBase64(req.DerivationSuffix); err != nil {
		s.writeError(w, fmt.Errorf("%w: derivation_suffix: %w", pool.ErrBadRequest, err))
		return
	}
	if !senderIdentityKeyPattern.MatchString(req.SenderIdentityKey) {
		s.writeError(w, fmt.Errorf("%w: sender_identity_key must be 66 hex characters", pool.ErrBadRequest))
		return
	}
	outputIndex := -1
	if req.OutputIndex != nil {
		outputIndex = *req.OutputIndex
	}
	res, err := s.funder.Internalize(r.Context(), treasury.FundRequest{
		AtomicBEEF:           atomicBEEF,
		OutputIndex:          outputIndex,
		DerivationPrefixB64:  req.DerivationPrefix,
		DerivationSuffixB64:  req.DerivationSuffix,
		SenderIdentityKeyHex: req.SenderIdentityKey,
	})
	if err != nil {
		s.writeFunderError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, wire.FundResult{TxID: res.TxID, Satoshis: res.Satoshis})
}

// decodeNonEmptyBase64 std-decodes s, rejecting a malformed or empty result.
func decodeNonEmptyBase64(s string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, errors.New("must not be empty")
	}
	return b, nil
}

// writeFunderError maps a treasury.Funder error to the funding endpoints'
// responses: context cancellation/deadline go through the shared mapErr (499
// client_closed_request / 503 unavailable, same as every other handler), a
// bad funding request is 400 bad_funding carrying the funder's own message
// (safe to expose; see treasury.ErrBadFunding), and anything else is 503
// unavailable -- deliberately not mapErr's "internal" default, since an
// unrecognized Funder failure (the toolbox may be mid-broadcast) is expected
// to be transient.
func (s *server) writeFunderError(w http.ResponseWriter, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, treasury.ErrBadFunding) {
		s.writeError(w, err)
		return
	}
	s.log.Warn("funding request failed", "err", err)
	w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
	writeJSON(w, http.StatusServiceUnavailable, wire.Error{
		Code:       "unavailable",
		Message:    "funding temporarily unavailable",
		RetryAfter: retryAfterSeconds,
	})
}
