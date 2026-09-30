package mongodb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

func TestWrap_Nil(t *testing.T) {
	assert.NoError(t, wrap(nil))
}

func TestWrap_ContextDeadlineExceeded(t *testing.T) {
	err := wrap(context.DeadlineExceeded)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrUnavailable)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestWrap_CommandErrorNetworkLabel(t *testing.T) {
	err := wrap(mongo.CommandError{Code: 1, Message: "boom", Labels: []string{"NetworkError"}})
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrUnavailable)
}

func TestWrap_CommandErrorTimeoutLabel(t *testing.T) {
	err := wrap(mongo.CommandError{Code: 2, Message: "boom", Labels: []string{"NetworkTimeoutError"}})
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrUnavailable)
}

func TestWrap_IOEOF(t *testing.T) {
	require.ErrorIs(t, wrap(io.EOF), store.ErrUnavailable)
	require.ErrorIs(t, wrap(fmt.Errorf("read: %w", io.EOF)), store.ErrUnavailable)
}

func TestWrap_ServerSelectionTimeout(t *testing.T) {
	sse := topology.ServerSelectionError{Wrapped: context.DeadlineExceeded}
	err := wrap(sse)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrUnavailable)
}

func TestWrap_ClientDisconnected(t *testing.T) {
	err := wrap(mongo.ErrClientDisconnected)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrUnavailable)
}

func TestWrap_DuplicateKeyPassesThrough(t *testing.T) {
	dupErr := mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: 11000, Message: "E11000 duplicate key"}}}
	require.True(t, mongo.IsDuplicateKeyError(dupErr))
	err := wrap(dupErr)
	require.Error(t, err)
	require.NotErrorIs(t, err, store.ErrUnavailable, "a duplicate-key error must pass through unwrapped for callers to classify")
	assert.Equal(t, dupErr, err)
}

func TestWrap_UnrelatedErrorPassesThrough(t *testing.T) {
	plain := errors.New("boom")
	assert.Same(t, plain, wrap(plain))
}

// --- MEDIUM finding 3: in-doubt write-concern/not-primary errors ----------

func TestWrap_BulkWriteExceptionMaxTimeMSExpired(t *testing.T) {
	err := wrap(mongo.BulkWriteException{WriteConcernError: &mongo.WriteConcernError{Code: 50, Message: "operation exceeded time limit"}})
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrUnavailable, "BulkWriteException was never covered by mongo.IsTimeout")
}

func TestWrap_BulkWriteExceptionWTimeout(t *testing.T) {
	err := wrap(mongo.BulkWriteException{WriteConcernError: &mongo.WriteConcernError{Code: 64, Message: "waiting for replication timed out"}})
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrUnavailable)
}

func TestWrap_WriteExceptionWTimeout(t *testing.T) {
	// mongo.IsTimeout only recognizes WriteException's WriteConcernError for
	// code 50 (MaxTimeMSExpired); code 64 (wtimeout) is not a timeout by that
	// check, so wrap must catch it via the write-concern-error path instead.
	err := wrap(mongo.WriteException{WriteConcernError: &mongo.WriteConcernError{Code: 64, Message: "wtimeout"}})
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrUnavailable)
	assert.False(t, mongo.IsTimeout(mongo.WriteException{WriteConcernError: &mongo.WriteConcernError{Code: 64}}),
		"sanity: confirms this case is not already covered by mongo.IsTimeout")
}

func TestWrap_BulkWriteDuplicateKeyStillPassesThrough(t *testing.T) {
	// A BulkWriteException with only a duplicate-key WriteError (no
	// WriteConcernError) must be unaffected by the write-concern-error check
	// added for MEDIUM finding 3.
	dupErr := mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 11000}}}}
	err := wrap(dupErr)
	require.NotErrorIs(t, err, store.ErrUnavailable)
}

func TestWrap_RetryableWriteErrorLabel(t *testing.T) {
	err := wrap(mongo.WriteException{Labels: []string{"RetryableWriteError"}})
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrUnavailable)
}

func TestWrap_TransientTransactionErrorLabel(t *testing.T) {
	err := wrap(mongo.CommandError{Code: 1, Message: "boom", Labels: []string{"TransientTransactionError"}})
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrUnavailable)
}

func TestWrap_NotPrimaryOrShutdownCodes(t *testing.T) {
	// 10107 NotWritablePrimary, 13435 NotPrimaryNoSecondaryOk,
	// 13436 NotPrimaryOrSecondary, 189 PrimarySteppedDown,
	// 91 ShutdownInProgress, 11600 InterruptedAtShutdown,
	// 11602 InterruptedDueToReplStateChange.
	for _, code := range []int32{10107, 13435, 13436, 189, 91, 11600, 11602} {
		t.Run(fmt.Sprintf("code_%d", code), func(t *testing.T) {
			err := wrap(mongo.CommandError{Code: code, Message: "not primary"})
			require.Error(t, err)
			require.ErrorIs(t, err, store.ErrUnavailable)
		})
	}
}

func TestWrap_NotPrimaryCodeNestedInWriteException(t *testing.T) {
	// The same codes must also be recognized when they appear as one of a
	// WriteException/BulkWriteException's own WriteErrors, not just as a bare
	// CommandError.
	err := wrap(mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: 10107, Message: "not primary"}}})
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrUnavailable)

	err = wrap(mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 189, Message: "stepped down"}}}})
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrUnavailable)
}

func TestWrap_UnrelatedCommandErrorCodePassesThrough(t *testing.T) {
	// Sanity check: an ordinary CommandError whose code is none of the
	// in-doubt codes above must still pass through unwrapped.
	err := wrap(mongo.CommandError{Code: 2, Message: "BadValue"})
	require.Error(t, err)
	require.NotErrorIs(t, err, store.ErrUnavailable)
}

func TestAllDuplicateKey(t *testing.T) {
	dupOnly := mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
		{WriteError: mongo.WriteError{Code: 11000}},
		{WriteError: mongo.WriteError{Code: 11000}},
	}}
	assert.True(t, allDuplicateKey(dupOnly))

	mixed := mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
		{WriteError: mongo.WriteError{Code: 11000}},
		{WriteError: mongo.WriteError{Code: 17280}},
	}}
	assert.False(t, allDuplicateKey(mixed))

	assert.False(t, allDuplicateKey(errors.New("boom")))
	assert.False(t, allDuplicateKey(nil))
}
