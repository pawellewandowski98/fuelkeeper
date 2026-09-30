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
