package aerospike

import (
	"bytes"
	"log/slog"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeInfoNode is a canned infoNode for exercising checkNodesSC without a
// live (let alone multi-node) Aerospike cluster.
type fakeInfoNode struct {
	name    string
	sc      bool // strong-consistency=true/false in the canned namespace response
	failure bool // when true, RequestInfo fails instead of returning a response
}

func (f fakeInfoNode) RequestInfo(_ *as.InfoPolicy, names ...string) (map[string]string, as.Error) {
	if f.failure {
		return nil, &as.AerospikeError{ResultCode: types.TIMEOUT}
	}
	resp := map[string]string{}
	for _, n := range names {
		resp[n] = "strong-consistency=" + boolStr(f.sc)
	}
	return resp, nil
}

func (f fakeInfoNode) GetName() string { return f.name }

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// TestCheckNodesSC_AllStrongConsistency covers the all-SC cluster, in either
// node order, converging to true with no warning logged.
func TestCheckNodesSC_AllStrongConsistency(t *testing.T) {
	for _, order := range [][]infoNode{
		{fakeInfoNode{name: "n1", sc: true}, fakeInfoNode{name: "n2", sc: true}},
		{fakeInfoNode{name: "n2", sc: true}, fakeInfoNode{name: "n1", sc: true}},
	} {
		var buf bytes.Buffer
		s := &Store{ns: "test", log: slog.New(slog.NewTextHandler(&buf, nil))}
		sc, err := s.checkNodesSC(order, false)
		require.NoError(t, err)
		assert.True(t, sc)
		assert.Empty(t, buf.String(), "no warning when every node is SC")
	}
}

// TestCheckNodesSC_MixedClusterOrderIndependent reproduces the review
// finding: checkConsistency used to return as soon as it found the FIRST
// availability-mode node, so which node(s) it reported -- and whether it
// even reached every node in the cluster -- depended on node order. A mixed
// cluster (one AP node, one SC node) must resolve to strongConsistency=false
// regardless of which order the nodes are visited in, and the AP node must
// be named in the warning either way.
func TestCheckNodesSC_MixedClusterOrderIndependent(t *testing.T) {
	ap := fakeInfoNode{name: "ap-node", sc: false}
	scNode := fakeInfoNode{name: "sc-node", sc: true}

	for _, tc := range []struct {
		name  string
		order []infoNode
	}{
		{"AP first", []infoNode{ap, scNode}},
		{"SC first", []infoNode{scNode, ap}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			s := &Store{ns: "test", log: slog.New(slog.NewTextHandler(&buf, nil))}
			sc, err := s.checkNodesSC(tc.order, true)
			require.NoError(t, err)
			assert.False(t, sc, "a single AP node makes the whole cluster not-fully-SC")
			assert.Contains(t, buf.String(), "ap-node", "the AP node must be named in the warning regardless of order")
		})
	}
}

// TestCheckNodesSC_LogsEveryAPNodeNotJustFirst reproduces the other half of
// the same finding: with multiple AP nodes, the old code silently stopped at
// the first one and never even queried the rest. Every AP node must be
// checked and named.
func TestCheckNodesSC_LogsEveryAPNodeNotJustFirst(t *testing.T) {
	nodes := []infoNode{
		fakeInfoNode{name: "ap-1", sc: false},
		fakeInfoNode{name: "sc-1", sc: true},
		fakeInfoNode{name: "ap-2", sc: false},
	}
	var buf bytes.Buffer
	s := &Store{ns: "test", log: slog.New(slog.NewTextHandler(&buf, nil))}
	sc, err := s.checkNodesSC(nodes, true)
	require.NoError(t, err)
	assert.False(t, sc)
	assert.Contains(t, buf.String(), "ap-1")
	assert.Contains(t, buf.String(), "ap-2")
	assert.NotContains(t, buf.String(), "sc-1", "an SC node must never be listed as an AP node")
}

// TestCheckNodesSC_StrictModeRejectsAnyAPNode covers allowAP=false: any AP
// node anywhere in the cluster is a hard error naming that node, regardless
// of position.
func TestCheckNodesSC_StrictModeRejectsAnyAPNode(t *testing.T) {
	nodes := []infoNode{
		fakeInfoNode{name: "sc-1", sc: true},
		fakeInfoNode{name: "ap-1", sc: false},
		fakeInfoNode{name: "sc-2", sc: true},
	}
	s := &Store{ns: "test", log: slog.Default()}
	sc, err := s.checkNodesSC(nodes, false)
	require.Error(t, err)
	assert.False(t, sc)
	require.ErrorIs(t, err, ErrNotStrongConsistency)
	assert.Contains(t, err.Error(), "ap-1")
}

// TestCheckNodesSC_HardFailureAlwaysErrors confirms a RequestInfo failure
// (an infrastructure problem, not an availability-mode opt-out) is always a
// hard error, even when allowAP is set, and even when it is not the first
// node checked.
func TestCheckNodesSC_HardFailureAlwaysErrors(t *testing.T) {
	nodes := []infoNode{
		fakeInfoNode{name: "sc-1", sc: true},
		fakeInfoNode{name: "unreachable", failure: true},
	}
	s := &Store{ns: "test", log: slog.Default()}
	_, err := s.checkNodesSC(nodes, true)
	require.Error(t, err)
}
