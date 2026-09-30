package aerospike

import (
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

func aeErr(code types.ResultCode) as.Error {
	return &as.AerospikeError{ResultCode: code}
}

func TestWrap_Nil(t *testing.T) {
	assert.NoError(t, wrap(nil))
}

func TestWrap_Timeout(t *testing.T) {
	err := wrap(aeErr(types.TIMEOUT))
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrUnavailable)
}

func TestWrap_NetworkError(t *testing.T) {
	err := wrap(aeErr(types.NETWORK_ERROR))
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrUnavailable)
}

func TestWrap_NoAvailableConnections(t *testing.T) {
	err := wrap(aeErr(types.NO_AVAILABLE_CONNECTIONS_TO_NODE))
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrUnavailable)
}

func TestWrap_ServerNotAvailable(t *testing.T) {
	err := wrap(aeErr(types.SERVER_NOT_AVAILABLE))
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrUnavailable)
}

func TestWrap_InvalidNode(t *testing.T) {
	err := wrap(aeErr(types.INVALID_NODE_ERROR))
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrUnavailable)
}

func TestWrap_FilteredOutPassesThrough(t *testing.T) {
	err := wrap(aeErr(types.FILTERED_OUT))
	require.Error(t, err)
	require.NotErrorIs(t, err, store.ErrUnavailable, "FILTERED_OUT is a classified CAS-loss result, not an infrastructure failure")
}

func TestWrap_KeyExistsPassesThrough(t *testing.T) {
	err := wrap(aeErr(types.KEY_EXISTS_ERROR))
	require.Error(t, err)
	require.NotErrorIs(t, err, store.ErrUnavailable, "KEY_EXISTS_ERROR is a classified result callers switch on, not an infrastructure failure")
}

func TestWrap_KeyNotFoundPassesThrough(t *testing.T) {
	err := wrap(aeErr(types.KEY_NOT_FOUND_ERROR))
	require.Error(t, err)
	require.NotErrorIs(t, err, store.ErrUnavailable)
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
