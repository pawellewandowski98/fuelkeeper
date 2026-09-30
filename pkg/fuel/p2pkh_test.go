package fuel_test

import (
	"math/big"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/script/interpreter"
	"github.com/bsv-blockchain/go-sdk/script/interpreter/errs"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

// consumer builds a transaction spending the fuel output at fuelTxID:fuelVout
// with unlock, at input index 1 (an unrelated first input, with its own
// sequence number otherSeq, keeps the fuel input from landing at index 0).
func consumer(t *testing.T, version, lockTime, seq uint32, fuelTxID chainhash.Hash, fuelVout uint32, unlock []byte, extraOutputs int, otherSeq uint32) (*transaction.Transaction, int) {
	t.Helper()
	tx := transaction.NewTransaction()
	tx.Version = version
	tx.LockTime = lockTime
	// an unrelated first input so the fuel input is not at index 0
	other := storetest.NewTxID()
	tx.AddInput(&transaction.TransactionInput{SourceTXID: &other, SourceTxOutIndex: 3, SequenceNumber: otherSeq, UnlockingScript: script.NewFromBytes([]byte{0x51})})
	h := fuelTxID
	fin := &transaction.TransactionInput{SourceTXID: &h, SourceTxOutIndex: fuelVout, SequenceNumber: seq, UnlockingScript: script.NewFromBytes(unlock)}
	tx.AddInput(fin)
	for i := 0; i < extraOutputs; i++ {
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: uint64(1000 + i), LockingScript: script.NewFromBytes([]byte{0x6a, 0x01, byte(i)})})
	}
	return tx, 1
}

// assertWellFormedP2PKHUnlock checks the unlocking script structurally rather
// than by exact byte length. go-sdk always produces a low-S DER signature but
// not a fixed-width one (R and S can each independently be 31, 32 or 33
// bytes), so the overall script length varies by a few bytes run to run: a
// narrow length window like [106,107,108] is flaky (measured empirically at
// roughly 1 failure in 285 runs, and 108 never occurs at all under low-S).
func assertWellFormedP2PKHUnlock(t *testing.T, unlock []byte, pub *ec.PublicKey) {
	t.Helper()
	assert.GreaterOrEqual(t, len(unlock), 104, "2 push opcodes + sig(min) + hashtype + pubkey(33)")
	assert.LessOrEqual(t, len(unlock), 107, "2 push opcodes + sig(max under low-S) + hashtype + pubkey(33)")

	chunks, err := script.NewFromBytes(unlock).Chunks()
	require.NoError(t, err)
	require.Len(t, chunks, 2, "unlocking script must be exactly <sig+hashtype> <pubkey>")

	sigWithType := chunks[0].Data
	require.NotEmpty(t, sigWithType)
	hashType := sigWithType[len(sigWithType)-1]
	assert.Equal(t, byte(0xc2), hashType, "sighash type byte NONE|ANYONECANPAY|FORKID")

	sig, err := ec.ParseDERSignature(sigWithType[:len(sigWithType)-1])
	require.NoError(t, err, "the signature part must be valid DER")
	halfOrder := new(big.Int).Rsh(ec.S256().N, 1)
	assert.LessOrEqual(t, sig.S.Cmp(halfOrder), 0, "go-sdk always produces a low-S signature")

	assert.Equal(t, pub.Compressed(), chunks[1].Data, "second push is the signer's compressed public key")
}

// requireSignatureFailure asserts err is specifically the interpreter's "false
// left on the stack" failure - the OP_CHECKSIG rejection a bad sighash
// commitment produces - rather than some unrelated, potentially misleading
// error such as a malformed-script parse failure.
func requireSignatureFailure(t *testing.T, err error, msgAndArgs ...interface{}) {
	t.Helper()
	require.Error(t, err, msgAndArgs...)
	assert.True(t, errs.IsErrorCode(err, errs.ErrEvalFalse), "want ErrEvalFalse (signature check failed), got: %v", err)
}

func TestP2PKHPresignIsValidInAnyV1Tx(t *testing.T) {
	priv, err := ec.NewPrivateKey()
	require.NoError(t, err)
	lock, err := fuel.P2PKHLock(priv.PubKey())
	require.NoError(t, err)
	txid := storetest.NewTxID()
	const vout, sats = uint32(7), uint64(50)

	unlock, err := fuel.P2PKHPresign(priv, txid, vout, lock, sats)
	require.NoError(t, err)
	assertWellFormedP2PKHUnlock(t, unlock, priv.PubKey())

	prev := &transaction.TransactionOutput{Satoshis: sats, LockingScript: script.NewFromBytes(lock)}
	verify := func(tx *transaction.Transaction, idx int) error {
		return interpreter.NewEngine().Execute(interpreter.WithTx(tx, idx, prev), interpreter.WithForkID(), interpreter.WithAfterGenesis())
	}

	tx, idx := consumer(t, fuel.P2PKHTxVersion, fuel.P2PKHLockTime, fuel.P2PKHSequence, txid, vout, unlock, 2, 0xffffffff)
	require.NoError(t, verify(tx, idx), "different inputs and outputs than the presign tx still verify")
	tx, idx = consumer(t, fuel.P2PKHTxVersion, fuel.P2PKHLockTime, fuel.P2PKHSequence, txid, vout, unlock, 5, 0xffffffff)
	require.NoError(t, verify(tx, idx))
	tx, idx = consumer(t, fuel.P2PKHTxVersion, fuel.P2PKHLockTime, fuel.P2PKHSequence, txid, vout, unlock, 2, 5)
	require.NoError(t, verify(tx, idx), "another input's nSequence is not committed to under NONE|ANYONECANPAY")

	tx, idx = consumer(t, 2, fuel.P2PKHLockTime, fuel.P2PKHSequence, txid, vout, unlock, 2, 0xffffffff)
	requireSignatureFailure(t, verify(tx, idx), "version is committed")
	tx, idx = consumer(t, fuel.P2PKHTxVersion, 1, fuel.P2PKHSequence, txid, vout, unlock, 2, 0xffffffff)
	requireSignatureFailure(t, verify(tx, idx), "lockTime is committed")
	tx, idx = consumer(t, fuel.P2PKHTxVersion, fuel.P2PKHLockTime, 0xfffffffe, txid, vout, unlock, 2, 0xffffffff)
	requireSignatureFailure(t, verify(tx, idx), "this input's sequence is committed")

	wrongPrev := &transaction.TransactionOutput{Satoshis: sats + 1, LockingScript: script.NewFromBytes(lock)}
	tx, idx = consumer(t, fuel.P2PKHTxVersion, fuel.P2PKHLockTime, fuel.P2PKHSequence, txid, vout, unlock, 2, 0xffffffff)
	requireSignatureFailure(t, interpreter.NewEngine().Execute(interpreter.WithTx(tx, idx, wrongPrev), interpreter.WithForkID(), interpreter.WithAfterGenesis()), "value is committed")
}

// TestP2PKHLockIsStandardAndPresignCommitsToOutpoint independently proves two
// properties not exercised by TestP2PKHPresignIsValidInAnyV1Tx: the locking
// script this package builds is a standard 25-byte P2PKH script whose hash160
// matches the compressed public key, and a presignature commits to its own
// outpoint (txid:vout) so it cannot be replayed against a different one.
func TestP2PKHLockIsStandardAndPresignCommitsToOutpoint(t *testing.T) {
	priv, err := ec.NewPrivateKey()
	require.NoError(t, err)
	pub := priv.PubKey()
	lock, err := fuel.P2PKHLock(pub)
	require.NoError(t, err)

	require.Len(t, lock, 25)
	assert.Equal(t, byte(0x76), lock[0], "OP_DUP")
	assert.Equal(t, byte(0xa9), lock[1], "OP_HASH160")
	assert.Equal(t, byte(0x14), lock[2], "push 20 bytes")
	assert.Equal(t, byte(0x88), lock[23], "OP_EQUALVERIFY")
	assert.Equal(t, byte(0xac), lock[24], "OP_CHECKSIG")
	assert.Equal(t, pub.Hash(), lock[3:23], "hash160 in the script matches the compressed pubkey's hash160")

	const sats = uint64(50)
	txidA, txidB := storetest.NewTxID(), storetest.NewTxID()
	const voutA, voutB = uint32(1), uint32(2)

	unlockA, err := fuel.P2PKHPresign(priv, txidA, voutA, lock, sats)
	require.NoError(t, err)
	assertWellFormedP2PKHUnlock(t, unlockA, pub)
	unlockB, err := fuel.P2PKHPresign(priv, txidB, voutB, lock, sats)
	require.NoError(t, err)
	assertWellFormedP2PKHUnlock(t, unlockB, pub)

	prev := &transaction.TransactionOutput{Satoshis: sats, LockingScript: script.NewFromBytes(lock)}
	verify := func(txid chainhash.Hash, vout uint32, unlock []byte) error {
		tx, idx := consumer(t, fuel.P2PKHTxVersion, fuel.P2PKHLockTime, fuel.P2PKHSequence, txid, vout, unlock, 1, 0xffffffff)
		return interpreter.NewEngine().Execute(interpreter.WithTx(tx, idx, prev), interpreter.WithForkID(), interpreter.WithAfterGenesis())
	}

	require.NoError(t, verify(txidA, voutA, unlockA), "unlockA verifies against its own outpoint")
	require.NoError(t, verify(txidB, voutB, unlockB), "unlockB verifies against its own outpoint")
	requireSignatureFailure(t, verify(txidB, voutB, unlockA), "unlockA must not verify against txidB's outpoint")
	requireSignatureFailure(t, verify(txidA, voutA, unlockB), "unlockB must not verify against txidA's outpoint")
}

// TestP2PKHPresign_RejectsMismatchedLock proves a wrong lock/key pairing is
// caught at split/presign time rather than surfacing later as an opaque
// OP_EQUALVERIFY failure at spend time.
func TestP2PKHPresign_RejectsMismatchedLock(t *testing.T) {
	priv, err := ec.NewPrivateKey()
	require.NoError(t, err)
	other, err := ec.NewPrivateKey()
	require.NoError(t, err)
	wrongLock, err := fuel.P2PKHLock(other.PubKey())
	require.NoError(t, err)

	_, err = fuel.P2PKHPresign(priv, storetest.NewTxID(), 0, wrongLock, 50)
	require.ErrorIs(t, err, fuel.ErrLockMismatch)
}
