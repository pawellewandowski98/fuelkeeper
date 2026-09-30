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
	if !forceTxidLeaf(mp, txid) {
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

// forceTxidLeaf finds the leaf-level entry in mp naming txid and forces its
// Txid flag on, reporting whether such a leaf was found. Duplicate leaves
// (which carry no Hash) and nil-Hash entries are skipped rather than matched.
//
// Two independent behaviors of go-sdk's transaction package (v1.4.1,
// transaction/beef.go and transaction/merklepath.go) make both the scan and
// the mutation necessary:
//
//  1. (*MerklePath).ComputeRoot cannot be used to check membership: for a
//     single-leaf path (a just-mined block containing only this one tx, the
//     common case for a split) it takes a shortcut and returns whatever txid
//     it is given as the root, without ever comparing it to the leaf's hash.
//     A BUMP naming a different transaction entirely would still "compute" a
//     root and produce no error. Only an explicit scan of the leaf level
//     catches that, which is what the loop below does.
//  2. (*Beef).verifyValid - which (*Beef).Verify relies on to decide which
//     roots to check against a chain tracker - only folds a BUMP's root into
//     that set for level-0 leaves whose Txid flag is set. A leaf that names
//     the right hash but happens to arrive with the flag unset is silently
//     skipped: Verify then has no root to check for that BUMP's height and
//     reports success without ever consulting the tracker. Forcing the flag
//     once we've independently confirmed by hash that this is the right leaf
//     guarantees Verify always performs the SPV check.
func forceTxidLeaf(mp *transaction.MerklePath, txid chainhash.Hash) bool {
	if len(mp.Path) == 0 {
		return false
	}
	for _, leaf := range mp.Path[0] {
		if leaf.Hash == nil || (leaf.Duplicate != nil && *leaf.Duplicate) {
			continue
		}
		if leaf.Hash.Equal(txid) {
			isTxid := true
			leaf.Txid = &isTxid
			return true
		}
	}
	return false
}
