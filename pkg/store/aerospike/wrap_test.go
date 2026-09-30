package aerospike

import (
	"reflect"
	"testing"
	"unsafe"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

func aeErr(code types.ResultCode) *as.AerospikeError {
	return &as.AerospikeError{ResultCode: code}
}

// chained returns outer with inner attached underneath it, the way the
// client chains an earlier (e.g. pooled-connection retry) error below the
// one that decided the call. The client offers no exported constructor.
func chained(outer *as.AerospikeError, inner error) *as.AerospikeError {
	f := reflect.ValueOf(outer).Elem().FieldByName("wrapped")
	reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Set(reflect.ValueOf(&inner).Elem()) //nolint:gosec // test-only access to the client's unexported chain field
	return outer
}

func TestWrap_Nil(t *testing.T) {
	assert.NoError(t, wrap(nil))
}

func TestWrap_TransientCodes(t *testing.T) {
	for _, code := range []types.ResultCode{
		types.TIMEOUT,
		types.NETWORK_ERROR,
		types.NO_AVAILABLE_CONNECTIONS_TO_NODE,
		types.SERVER_NOT_AVAILABLE,
		types.INVALID_NODE_ERROR,
		types.KEY_BUSY,
		types.XDR_KEY_BUSY,
		types.DEVICE_OVERLOAD,
		types.PARTITION_UNAVAILABLE,
		types.MAX_RETRIES_EXCEEDED,
		types.SERVER_MEM_ERROR,
		types.BATCH_QUEUES_FULL,
		types.BATCH_MAX_REQUESTS_EXCEEDED,
		types.NO_RESPONSE,
		types.QUERY_TIMEOUT,
		types.MAX_ERROR_RATE,
		types.CLUSTER_KEY_MISMATCH,
		types.INDEX_NOTREADABLE,
	} {
		t.Run(code.String(), func(t *testing.T) {
			require.ErrorIs(t, wrap(aeErr(code)), store.ErrUnavailable)
		})
	}
}

func TestWrap_ClientConstantErrors(t *testing.T) {
	// The client returns some errors as its unexported constant type.
	require.ErrorIs(t, wrap(as.ErrTimeout), store.ErrUnavailable)
	require.ErrorIs(t, wrap(as.ErrMaxRetriesExceeded), store.ErrUnavailable)
	require.NotErrorIs(t, wrap(as.ErrInvalidParam), store.ErrUnavailable)
	assert.True(t, isFiltered(as.ErrFilteredOut))
	assert.True(t, isKeyNotFound(as.ErrKeyNotFound))
}

func TestWrap_InDoubtIsTransient(t *testing.T) {
	e := aeErr(types.PARAMETER_ERROR)
	e.InDoubt = true
	require.ErrorIs(t, wrap(e), store.ErrUnavailable, "an in-doubt write may have applied: the caller must replay it")
}

func TestWrap_PermanentCodes(t *testing.T) {
	for _, code := range []types.ResultCode{
		types.PARAMETER_ERROR, types.FILTERED_OUT, types.KEY_EXISTS_ERROR, types.KEY_NOT_FOUND_ERROR,
		types.BIN_NAME_TOO_LONG, types.ENTERPRISE_ONLY, types.FAIL_FORBIDDEN, types.INDEX_FOUND,
	} {
		t.Run(code.String(), func(t *testing.T) {
			err := wrap(aeErr(code))
			require.Error(t, err)
			require.NotErrorIs(t, err, store.ErrUnavailable)
		})
	}
}

// A permanent outer error must not become ErrUnavailable because a
// transient error from an earlier attempt is chained underneath it (the
// client's Matches walks the whole chain; classification must not).
func TestWrap_ClassifiesByOutermostCode(t *testing.T) {
	permOverTransient := chained(aeErr(types.PARAMETER_ERROR), aeErr(types.TIMEOUT))
	require.True(t, permOverTransient.Matches(types.TIMEOUT), "precondition: the chain contains TIMEOUT")
	require.NotErrorIs(t, wrap(permOverTransient), store.ErrUnavailable)

	transientOverPerm := chained(aeErr(types.TIMEOUT), aeErr(types.PARAMETER_ERROR))
	require.ErrorIs(t, wrap(transientOverPerm), store.ErrUnavailable)

	// CAS classification is outermost-only too: a FILTERED_OUT buried under
	// a timeout is not a CAS loss.
	assert.False(t, isFiltered(chained(aeErr(types.TIMEOUT), aeErr(types.FILTERED_OUT))))
	assert.False(t, isKeyNotFound(chained(aeErr(types.TIMEOUT), aeErr(types.KEY_NOT_FOUND_ERROR))))
}

func TestIsFiltered(t *testing.T) {
	assert.True(t, isFiltered(aeErr(types.FILTERED_OUT)))
	assert.False(t, isFiltered(aeErr(types.KEY_NOT_FOUND_ERROR)))
	assert.False(t, isFiltered(nil))
}

func TestIsKeyNotFound(t *testing.T) {
	assert.True(t, isKeyNotFound(aeErr(types.KEY_NOT_FOUND_ERROR)))
	assert.False(t, isKeyNotFound(aeErr(types.FILTERED_OUT)))
	assert.False(t, isKeyNotFound(nil))
}

func batchRec(code types.ResultCode, err as.Error, inDoubt bool) as.BatchRecordIfc {
	br := as.NewBatchRead(nil, &as.Key{}, nil)
	br.ResultCode = code
	br.Err = err
	br.InDoubt = inDoubt
	return br
}

// N1: only ResultCode == OK is applied; a non-OK code with a nil Err must
// not be counted as applied.
func TestClassifyBatch_OnlyOKIsApplied(t *testing.T) {
	recs := []as.BatchRecordIfc{
		batchRec(types.OK, nil, false),
		batchRec(types.FILTERED_OUT, nil, false),
		batchRec(types.KEY_NOT_FOUND_ERROR, aeErr(types.KEY_NOT_FOUND_ERROR), false),
	}
	applied, err := classifyBatch(recs, types.FILTERED_OUT, types.KEY_NOT_FOUND_ERROR)
	require.NoError(t, err)
	assert.Equal(t, []int{0}, applied)

	_, err = classifyBatch([]as.BatchRecordIfc{batchRec(types.PARAMETER_ERROR, nil, false)}, types.FILTERED_OUT)
	require.Error(t, err, "a non-OK, non-tolerated code with a nil Err is an error, not an apply")
	require.NotErrorIs(t, err, store.ErrUnavailable)
}

func TestClassifyBatch_TransientAndInDoubtAbort(t *testing.T) {
	_, err := classifyBatch([]as.BatchRecordIfc{batchRec(types.KEY_BUSY, nil, false)}, types.FILTERED_OUT)
	require.ErrorIs(t, err, store.ErrUnavailable)
	_, err = classifyBatch([]as.BatchRecordIfc{batchRec(types.FILTERED_OUT, aeErr(types.FILTERED_OUT), true)}, types.FILTERED_OUT)
	require.ErrorIs(t, err, store.ErrUnavailable, "in-doubt beats tolerate")
}
