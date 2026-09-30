package aerospike

import (
	"context"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/assert"
)

func TestWritePoliciesNeverRetry(t *testing.T) {
	s := &Store{}
	wp := s.writePolicy(context.Background())
	assert.Equal(t, 0, wp.MaxRetries)
	assert.Equal(t, as.COMMIT_ALL, wp.CommitLevel)
	assert.Equal(t, as.UPDATE_ONLY, wp.RecordExistsAction)
	bwp := s.batchWritePolicy(context.Background())
	assert.Equal(t, as.COMMIT_ALL, bwp.CommitLevel)
	assert.Equal(t, as.UPDATE_ONLY, bwp.RecordExistsAction)
	bp := s.batchPolicy(context.Background())
	assert.Equal(t, 0, bp.MaxRetries)
}

func TestCreatePoliciesInsertOnly(t *testing.T) {
	s := &Store{}
	cp := s.createPolicy(context.Background())
	assert.Equal(t, as.CREATE_ONLY, cp.RecordExistsAction)
	assert.Equal(t, 0, cp.MaxRetries)
	bcp := s.batchCreatePolicy(context.Background())
	assert.Equal(t, as.CREATE_ONLY, bcp.RecordExistsAction)
}

func TestReadPoliciesMayRetryOnce(t *testing.T) {
	s := &Store{}
	assert.Equal(t, 1, s.readPolicy(context.Background()).MaxRetries)
	assert.Equal(t, 1, s.queryPolicy(context.Background()).MaxRetries)
	assert.Equal(t, 1, s.readBatchPolicy(context.Background()).MaxRetries)
}
