package fuel

import (
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// ErrTxNotInBEEF is returned when the BEEF does not contain the requested tx.
var ErrTxNotInBEEF = errors.New("fuel: transaction not in BEEF")

// ErrTxNotInMerklePath is returned when a BUMP's leaf level does not name the
// requested txid.
var ErrTxNotInMerklePath = errors.New("fuel: transaction not in merkle path")

// RawTxFromBEEF extracts the raw bytes of txid from a BEEF blob.
func RawTxFromBEEF(beef []byte, txid chainhash.Hash) ([]byte, error) {
	b, err := transaction.NewBeefFromBytes(beef)
	if err != nil {
		return nil, fmt.Errorf("fuel: parse beef: %w", err)
	}
	tx := b.FindTransaction(txid.String())
	if tx == nil {
		return nil, ErrTxNotInBEEF
	}
	return tx.Bytes(), nil
}

// MinedBEEF builds the compact BEEF v2 for a mined split: the raw split tx plus
// its BUMP. unminedBEEF is the shard's stored BEEF (split tx plus ancestry);
// merklePath is the BUMP bytes reported by the oracle.
func MinedBEEF(unminedBEEF []byte, txid chainhash.Hash, merklePath []byte) ([]byte, error) {
	raw, err := RawTxFromBEEF(unminedBEEF, txid)
	if err != nil {
		return nil, err
	}
	mp, err := transaction.NewMerklePathFromBinary(merklePath)
	if err != nil {
		return nil, fmt.Errorf("fuel: parse merkle path: %w", err)
	}
	if !merklePathContains(mp, txid) {
		return nil, fmt.Errorf("%w: %s", ErrTxNotInMerklePath, txid)
	}
	out := transaction.NewBeefV2()
	idx := out.MergeBump(mp)
	if _, err := out.MergeRawTx(raw, &idx); err != nil {
		return nil, fmt.Errorf("fuel: merge raw tx: %w", err)
	}
	beefBytes, err := out.Bytes()
	if err != nil {
		return nil, fmt.Errorf("fuel: serialise beef: %w", err)
	}
	return beefBytes, nil
}

// merklePathContains reports whether mp's leaf level names txid.
//
// (*transaction.MerklePath).ComputeRoot cannot be used for this check: for a
// single-leaf path (block height and level of the tree containing only one
// tx, the common case for a just-mined split) it takes a shortcut and
// returns whatever txid it was given as the root, without ever comparing it
// to the leaf's hash. So a BUMP for a different transaction entirely would
// still "compute" a root and produce no error. Only an explicit scan of the
// leaf level catches that.
func merklePathContains(mp *transaction.MerklePath, txid chainhash.Hash) bool {
	if len(mp.Path) == 0 {
		return false
	}
	for _, leaf := range mp.Path[0] {
		if leaf.Hash != nil && leaf.Hash.Equal(txid) {
			return true
		}
	}
	return false
}
