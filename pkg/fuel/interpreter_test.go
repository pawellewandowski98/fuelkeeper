package fuel_test

import (
	"testing"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/script/interpreter"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/fuel"
)

// TestPuzzleScripts_SatisfyGoSDKInterpreter is a permanent proof that the
// hash-puzzle locking and unlocking scripts this package builds are actually
// accepted (and rejected) by go-sdk's script interpreter, not just by our own
// ParsePuzzleLock/VerifyPuzzle logic.
func TestPuzzleScripts_SatisfyGoSDKInterpreter(t *testing.T) {
	seed := fuel.NewSeed()
	p := fuel.Preimage(seed, 0)
	lock := script.NewFromBytes(fuel.PuzzleLock(fuel.PuzzleHash(p)))

	sourceTx := transaction.NewTransaction()
	sourceTx.AddOutput(&transaction.TransactionOutput{
		Satoshis:      50,
		LockingScript: lock,
	})

	newSpendTx := func(unlock *script.Script) (*transaction.Transaction, *transaction.TransactionOutput) {
		tx := transaction.NewTransaction()
		tx.AddInput(&transaction.TransactionInput{
			SourceTXID:        sourceTx.TxID(),
			SourceTxOutIndex:  0,
			SourceTransaction: sourceTx,
			SequenceNumber:    transaction.DefaultSequenceNumber,
			UnlockingScript:   unlock,
		})
		tx.AddOutput(&transaction.TransactionOutput{
			Satoshis:      40,
			LockingScript: lock,
		})
		return tx, sourceTx.Outputs[0]
	}

	t.Run("correct preimage satisfies the puzzle", func(t *testing.T) {
		tx, prevOutput := newSpendTx(script.NewFromBytes(fuel.PuzzleUnlock(p)))

		err := interpreter.NewEngine().Execute(
			interpreter.WithTx(tx, 0, prevOutput),
			interpreter.WithForkID(),
			interpreter.WithAfterGenesis(),
		)
		require.NoError(t, err, "correct preimage satisfies the puzzle in go-sdk's interpreter")
	})

	t.Run("wrong preimage is rejected", func(t *testing.T) {
		wrong := fuel.Preimage(seed, 1)
		tx, prevOutput := newSpendTx(script.NewFromBytes(fuel.PuzzleUnlock(wrong)))

		err := interpreter.NewEngine().Execute(
			interpreter.WithTx(tx, 0, prevOutput),
			interpreter.WithForkID(),
			interpreter.WithAfterGenesis(),
		)
		require.Error(t, err, "wrong preimage must not satisfy the puzzle in go-sdk's interpreter")
	})
}
