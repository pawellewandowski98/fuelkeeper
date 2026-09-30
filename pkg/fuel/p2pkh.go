package fuel

import (
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sighash "github.com/bsv-blockchain/go-sdk/transaction/sighash"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
)

// ErrNilPrivateKey is returned when P2PKHPresign is called with a nil key.
var ErrNilPrivateKey = errors.New("fuel: nil private key")

// ErrNilPublicKey is returned when P2PKHLock is called with a nil key.
var ErrNilPublicKey = errors.New("fuel: nil public key")

// ErrEmptyLockingScript is returned when P2PKHPresign is given no locking
// script for the fuel output being spent.
var ErrEmptyLockingScript = errors.New("fuel: empty locking script")

// Constraints a consuming transaction must satisfy for a presigned P2PKH fuel
// input to remain valid: the FORKID sighash preimage always commits to
// nVersion, this input's nSequence and nLockTime, even under
// SIGHASH_NONE|ANYONECANPAY, so a presignature is only ever safe to attach to
// a transaction built with exactly these values. Consumers (wallet
// createAction callers spending a presigned fuel output) must build their
// transaction with version 1, lockTime 0, and this input's sequence number
// 0xffffffff - which are the toolbox defaults - or the presignature will not
// verify.
const (
	// P2PKHTxVersion is the only nVersion a consuming transaction may use.
	P2PKHTxVersion uint32 = 1
	// P2PKHLockTime is the only nLockTime a consuming transaction may use.
	P2PKHLockTime uint32 = 0
	// P2PKHSequence is the only nSequence the fuel input itself may use.
	P2PKHSequence uint32 = 0xffffffff
)

// P2PKHSigHash is SIGHASH_NONE|ANYONECANPAY|FORKID (0xC2): it signs none of
// the outputs and only this one input, so any transaction respecting
// P2PKHTxVersion/P2PKHLockTime/P2PKHSequence can attach the fuel input
// wherever it likes among its other inputs and outputs.
const P2PKHSigHash sighash.Flag = sighash.NoneForkID | sighash.AnyOneCanPay

// P2PKHLock returns the standard 25-byte P2PKH locking script for pub
// (OP_DUP OP_HASH160 <20-byte hash160> OP_EQUALVERIFY OP_CHECKSIG). The
// returned bytes are network-independent: P2PKH scripts do not encode a
// network, only the address encoding used elsewhere does.
func P2PKHLock(pub *ec.PublicKey) ([]byte, error) {
	if pub == nil {
		return nil, ErrNilPublicKey
	}
	addr, err := script.NewAddressFromPublicKey(pub, true)
	if err != nil {
		return nil, fmt.Errorf("fuel: address: %w", err)
	}
	s, err := p2pkh.Lock(addr)
	if err != nil {
		return nil, fmt.Errorf("fuel: p2pkh lock: %w", err)
	}
	return *s, nil
}

// P2PKHPresign produces the unlocking script for a fuel output at txid:vout
// (locked by lockingScript, worth satoshis) so that any consuming transaction
// honouring P2PKHTxVersion/P2PKHLockTime/P2PKHSequence can spend it, no matter
// what other inputs or outputs that transaction has.
func P2PKHPresign(priv *ec.PrivateKey, txid chainhash.Hash, vout uint32, lockingScript []byte, satoshis uint64) ([]byte, error) {
	if priv == nil {
		return nil, ErrNilPrivateKey
	}
	if len(lockingScript) == 0 {
		return nil, ErrEmptyLockingScript
	}
	tx := transaction.NewTransaction()
	tx.Version = P2PKHTxVersion
	tx.LockTime = P2PKHLockTime
	src := txid
	in := &transaction.TransactionInput{SourceTXID: &src, SourceTxOutIndex: vout, SequenceNumber: P2PKHSequence}
	in.SetSourceTxOutput(&transaction.TransactionOutput{Satoshis: satoshis, LockingScript: script.NewFromBytes(lockingScript)})
	tx.AddInput(in)
	flag := P2PKHSigHash
	tmpl, err := p2pkh.Unlock(priv, &flag)
	if err != nil {
		return nil, fmt.Errorf("fuel: unlock template: %w", err)
	}
	unlock, err := tmpl.Sign(tx, 0)
	if err != nil {
		return nil, fmt.Errorf("fuel: presign: %w", err)
	}
	return *unlock, nil
}
