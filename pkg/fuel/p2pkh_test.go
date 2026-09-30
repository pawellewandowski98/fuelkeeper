package fuel_test

import (
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/script/interpreter"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func consumer(t *testing.T, version, lockTime, seq uint32, fuelTxID chainhash.Hash, fuelVout uint32, unlock []byte, extraOutputs int) (*transaction.Transaction, int) {
	t.Helper()
	tx := transaction.NewTransaction()
	tx.Version = version
	tx.LockTime = lockTime
	// an unrelated first input so the fuel input is not at index 0
	other := storetest.NewTxID()
	tx.AddInput(&transaction.TransactionInput{SourceTXID: &other, SourceTxOutIndex: 3, SequenceNumber: 0xffffffff, UnlockingScript: script.NewFromBytes([]byte{0x51})})
	h := fuelTxID
	fin := &transaction.TransactionInput{SourceTXID: &h, SourceTxOutIndex: fuelVout, SequenceNumber: seq, UnlockingScript: script.NewFromBytes(unlock)}
	tx.AddInput(fin)
	for i := 0; i < extraOutputs; i++ {
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: uint64(1000 + i), LockingScript: script.NewFromBytes([]byte{0x6a, 0x01, byte(i)})})
	}
	return tx, 1
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
	assert.Contains(t, []int{106, 107, 108}, len(unlock), "sig(70-72)+hashtype+pubkey(33)+2 pushes")
	assert.Equal(t, byte(0xc2), unlock[int(unlock[0])], "sighash type byte NONE|ANYONECANPAY|FORKID")

	prev := &transaction.TransactionOutput{Satoshis: sats, LockingScript: script.NewFromBytes(lock)}
	verify := func(tx *transaction.Transaction, idx int) error {
		return interpreter.NewEngine().Execute(interpreter.WithTx(tx, idx, prev), interpreter.WithForkID(), interpreter.WithAfterGenesis())
	}

	tx, idx := consumer(t, fuel.P2PKHTxVersion, fuel.P2PKHLockTime, fuel.P2PKHSequence, txid, vout, unlock, 2)
	require.NoError(t, verify(tx, idx), "different inputs and outputs than the presign tx still verify")
	tx, idx = consumer(t, fuel.P2PKHTxVersion, fuel.P2PKHLockTime, fuel.P2PKHSequence, txid, vout, unlock, 5)
	require.NoError(t, verify(tx, idx))

	tx, idx = consumer(t, 2, fuel.P2PKHLockTime, fuel.P2PKHSequence, txid, vout, unlock, 2)
	require.Error(t, verify(tx, idx), "version is committed")
	tx, idx = consumer(t, fuel.P2PKHTxVersion, 1, fuel.P2PKHSequence, txid, vout, unlock, 2)
	require.Error(t, verify(tx, idx), "lockTime is committed")
	tx, idx = consumer(t, fuel.P2PKHTxVersion, fuel.P2PKHLockTime, 0xfffffffe, txid, vout, unlock, 2)
	require.Error(t, verify(tx, idx), "this input's sequence is committed")

	wrongPrev := &transaction.TransactionOutput{Satoshis: sats + 1, LockingScript: script.NewFromBytes(lock)}
	tx, idx = consumer(t, fuel.P2PKHTxVersion, fuel.P2PKHLockTime, fuel.P2PKHSequence, txid, vout, unlock, 2)
	require.Error(t, interpreter.NewEngine().Execute(interpreter.WithTx(tx, idx, wrongPrev), interpreter.WithForkID(), interpreter.WithAfterGenesis()), "value is committed")
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
	unlockB, err := fuel.P2PKHPresign(priv, txidB, voutB, lock, sats)
	require.NoError(t, err)

	prev := &transaction.TransactionOutput{Satoshis: sats, LockingScript: script.NewFromBytes(lock)}
	verify := func(txid chainhash.Hash, vout uint32, unlock []byte) error {
		tx, idx := consumer(t, fuel.P2PKHTxVersion, fuel.P2PKHLockTime, fuel.P2PKHSequence, txid, vout, unlock, 1)
		return interpreter.NewEngine().Execute(interpreter.WithTx(tx, idx, prev), interpreter.WithForkID(), interpreter.WithAfterGenesis())
	}

	require.NoError(t, verify(txidA, voutA, unlockA), "unlockA verifies against its own outpoint")
	require.NoError(t, verify(txidB, voutB, unlockB), "unlockB verifies against its own outpoint")
	require.Error(t, verify(txidB, voutB, unlockA), "unlockA must not verify against txidB's outpoint")
	require.Error(t, verify(txidA, voutA, unlockB), "unlockB must not verify against txidA's outpoint")
}
