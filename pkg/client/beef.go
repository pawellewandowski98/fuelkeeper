package client

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/bsv-blockchain/fuelkeeper/pkg/api/wire"
	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
)

// PuzzleInputSize and P2PKHInputSize are pkg/fuel's per-input size estimates
// (bytes), re-exported so a wallet can size a transaction without importing
// pkg/fuel directly.
const (
	PuzzleInputSize = fuel.PuzzleInputSize
	P2PKHInputSize  = fuel.P2PKHInputSize
)

// InputBEEF fetches (via BEEF, so already-cached shards cost nothing) and
// merges the BEEF of every shard res.Shards names into one BEEF v2 blob
// suitable as the inputBEEF of a go-sdk createAction call that spends
// res.Outputs.
func (c *Client) InputBEEF(ctx context.Context, pool string, res *wire.ReserveResponse) ([]byte, error) {
	merged := transaction.NewBeefV2()
	for _, sh := range res.Shards {
		b, err := c.BEEF(ctx, pool, sh)
		if err != nil {
			return nil, fmt.Errorf("client: fetch shard %s beef: %w", sh.TxID, err)
		}
		if err := merged.MergeBeefBytes(b); err != nil {
			return nil, fmt.Errorf("client: merge shard %s beef: %w", sh.TxID, err)
		}
	}
	out, err := merged.Bytes()
	if err != nil {
		return nil, fmt.Errorf("client: serialise merged beef: %w", err)
	}
	return out, nil
}

// ParseOutpoint parses "<txid>:<vout>" (and the BRC-100 form "<txid>.<vout>")
// into a go-sdk transaction.Outpoint, mirroring store.ParseOutpoint.
func ParseOutpoint(s string) (transaction.Outpoint, error) {
	i := strings.LastIndexAny(s, ":.")
	if i <= 0 || i == len(s)-1 {
		return transaction.Outpoint{}, fmt.Errorf("client: invalid outpoint %q", s)
	}
	h, err := chainhash.NewHashFromHex(s[:i])
	if err != nil {
		return transaction.Outpoint{}, fmt.Errorf("client: invalid outpoint %q: txid: %w", s, err)
	}
	v, err := strconv.ParseUint(s[i+1:], 10, 32)
	if err != nil {
		return transaction.Outpoint{}, fmt.Errorf("client: invalid outpoint %q: vout: %w", s, err)
	}
	return transaction.Outpoint{Txid: *h, Index: uint32(v)}, nil
}

// CreateActionInputs converts a reservation into go-sdk wallet.CreateActionInput
// values, each carrying the full unlocking script (no signing: FuelKeeper
// hands out fully-unlockable outputs) and description.
//
// When res.Constraints is set (presigned P2PKH mode: tx version 1, lockTime
// 0, sequence 0xffffffff — the values the presigned SIGHASH_NONE|ANYONECANPAY
// signature commits to), every input's SequenceNumber is pinned to
// res.Constraints.Sequence, since a P2PKH signature's ANYONECANPAY input
// still fixes its own sequence number. Puzzle mode carries no such
// constraint, so SequenceNumber is left nil and the wallet's own default
// applies.
func CreateActionInputs(res *wire.ReserveResponse, description string) ([]wallet.CreateActionInput, error) {
	inputs := make([]wallet.CreateActionInput, 0, len(res.Outputs))
	for _, o := range res.Outputs {
		op, err := ParseOutpoint(o.Outpoint)
		if err != nil {
			return nil, fmt.Errorf("client: output %s: %w", o.Outpoint, err)
		}
		script, err := hex.DecodeString(o.Unlock.Script)
		if err != nil {
			return nil, fmt.Errorf("client: output %s: decode unlocking script: %w", o.Outpoint, err)
		}
		var seq *uint32
		if res.Constraints != nil {
			s := res.Constraints.Sequence
			seq = &s
		}
		inputs = append(inputs, wallet.CreateActionInput{
			Outpoint:              op,
			InputDescription:      description,
			UnlockingScript:       script,
			UnlockingScriptLength: uint32(len(script)), //nolint:gosec // hex-decoded unlocking script, always small
			SequenceNumber:        seq,
		})
	}
	return inputs, nil
}

// Surplus returns denomination minus the fee for a transaction of
// estimatedTxBytes at satPerKB, floored at 0. It is meant for a wallet that
// must not create a change output: spend fuel outputs worth denomination
// each and let Surplus become the miner fee (or dust the wallet discards),
// rather than sizing a change output for it.
func Surplus(denomination uint64, estimatedTxBytes int, satPerKB uint64) uint64 {
	fee := fuel.FeeSats(estimatedTxBytes, satPerKB)
	if fee >= denomination {
		return 0
	}
	return denomination - fee
}
